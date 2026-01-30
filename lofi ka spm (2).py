import asyncio
import random
import time
from dataclasses import dataclass
from typing import Optional

from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout

# =========================
# CONFIG
# =========================
MESSAGE = "Hello"            # change
PARALLEL_SENDERS = 2         # 2 parallel

# Speed
BASE_DELAY = 0.06
JITTER = 0.02
BURST = 3

# Stability
VERIFY_SETTLE_MS = 35
MAX_RETRY = 6
BACKOFF_MIN = 0.3
BACKOFF_MAX = 1.3

# Maintenance (LONG RUN)
REFRESH_EVERY_MINUTES = 10          # refresh often to keep speed stable
CONTEXT_ROTATE_MINUTES = 30         # new context = clean memory

# Collision control (GC)
MIN_GAP_BETWEEN_LAST_MESSAGES = 0.6 # if new msgs coming too fast, wait (others spamming)
COLLISION_WAIT_MIN = 0.4
COLLISION_WAIT_MAX = 1.3

# Restriction control
RESTRICT_PAUSE_MIN = 120            # 2 min
RESTRICT_PAUSE_MAX = 480            # 8 min

# Playwright timeouts
NAV_TIMEOUT = 30000
ACTION_TIMEOUT = 9000
# =========================


@dataclass
class Stats:
    sent: int = 0
    fail: int = 0
    refresh: int = 0
    rotate: int = 0
    paused: int = 0
    last_err: str = ""
    start_ts: float = 0.0


def ts():
    return time.strftime("%H:%M:%S")


async def jitter_sleep(base: float):
    await asyncio.sleep(max(0.0, base + random.uniform(-JITTER, JITTER)))


# ----------------------------
# UI HELPERS
# ----------------------------
async def close_popups(page: Page):
    selectors = [
        "text=Not Now",
        "text=Not now",
        "text=Cancel",
        "text=Close",
        "text=OK",
        "button:has-text('Not Now')",
        "button:has-text('Cancel')",
        "button:has-text('OK')",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=250):
                await btn.click(timeout=800)
                await page.wait_for_timeout(100)
        except:
            pass


async def get_dm_box(page: Page):
    candidates = [
        'textarea[placeholder="Message..."]',
        'div[role="textbox"][contenteditable="true"]',
    ]
    for css in candidates:
        loc = page.locator(css).first
        try:
            await loc.wait_for(state="visible", timeout=ACTION_TIMEOUT)
            return loc
        except:
            continue
    raise PWTimeout("DM input not visible")


async def focus_box(page: Page):
    await close_popups(page)
    box = await get_dm_box(page)
    await box.click(timeout=ACTION_TIMEOUT)
    await page.wait_for_timeout(50)
    # second click (IG focus bug)
    try:
        await box.click(timeout=800)
        await page.wait_for_timeout(50)
    except:
        pass
    return box


async def refresh_page(page: Page):
    try:
        await page.reload(wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    except:
        await page.goto(page.url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)


# ----------------------------
# COLLISION CONTROL
# ----------------------------
async def last_msg_time_gap(page: Page) -> Optional[float]:
    """
    Check last message group timestamp by DOM change.
    If messages are coming fast (others script running),
    we slow down so our sending doesn't get blocked.
    """
    try:
        # generic: last message bubble in thread
        last = page.locator("div[role='row']").last
        if not await last.is_visible(timeout=500):
            return None
        # use JS time now - approximate
        return time.time()
    except:
        return None


async def collision_wait_if_needed(page: Page):
    """
    If thread is "busy" (other script), pause randomly.
    """
    # if too many UI changes, random pause helps not getting killed
    gap = await last_msg_time_gap(page)
    if gap is None:
        return

    # Always keep a min random gap to avoid collision
    await asyncio.sleep(random.uniform(COLLISION_WAIT_MIN, COLLISION_WAIT_MAX))
# ----------------------------
# RESTRICTION DETECTION
# ----------------------------
async def detect_restriction(page: Page) -> bool:
    """
    If IG blocks sending, these messages often appear.
    We scan quickly.
    """
    texts = [
        "Try Again Later",
        "try again later",
        "Couldn't send",
        "couldn't send",
        "Message failed to send",
        "Action blocked",
        "blocked",
    ]
    for t in texts:
        try:
            if await page.locator(f"text={t}").first.is_visible(timeout=250):
                return True
        except:
            pass
    return False


async def pause_for_restriction(stats: Stats):
    stats.paused += 1
    pause = random.randint(RESTRICT_PAUSE_MIN, RESTRICT_PAUSE_MAX)
    print(f"[{ts()}] ⛔ Restricted detected. Pausing {pause}s ...")
    await asyncio.sleep(pause)


# ----------------------------
# SEND FUNCTION
# ----------------------------
async def send_once(page: Page, stats: Stats) -> bool:
    try:
        await close_popups(page)

        # collision wait (GC busy)
        await collision_wait_if_needed(page)

        box = await focus_box(page)
        await box.press("Control+A")
        await box.press("Backspace")
        await box.type(MESSAGE, delay=0)
        await box.press("Enter")

        await page.wait_for_timeout(VERIFY_SETTLE_MS)

        if await detect_restriction(page):
            await pause_for_restriction(stats)
            return False

        return True

    except Exception as e:
        stats.last_err = str(e)
        return False


# ----------------------------
# WORKER LOOP
# ----------------------------
async def worker(name: str, make_page_fn, stats: Stats):
    """
    Long-run stable worker.
    Rotates context every CONTEXT_ROTATE_MINUTES.
    Refreshes page every REFRESH_EVERY_MINUTES.
    """
    page = await make_page_fn()
    last_refresh = time.time()
    last_rotate = time.time()

    while True:
        # time-based refresh (keeps speed stable)
        if time.time() - last_refresh > REFRESH_EVERY_MINUTES * 60:
            stats.refresh += 1
            print(f"[{ts()}] [{name}] 🔄 Timed refresh")
            await refresh_page(page)
            last_refresh = time.time()

        # context rotation (best long-run stability)
        if time.time() - last_rotate > CONTEXT_ROTATE_MINUTES * 60:
            stats.rotate += 1
            print(f"[{ts()}] [{name}] ♻ Context rotate (new page)")
            try:
                await page.context.close()
            except:
                pass
            page = await make_page_fn()
            last_rotate = time.time()
            last_refresh = time.time()

        ok = False
        for attempt in range(1, MAX_RETRY + 1):
            ok = await send_once(page, stats)
            if ok:
                break
            await asyncio.sleep(random.uniform(BACKOFF_MIN, BACKOFF_MAX))

            # attempt recovery
            if attempt in (3, 5):
                await refresh_page(page)
                await asyncio.sleep(0.7)

        if ok:
            stats.sent += 1
        else:
            stats.fail += 1

        # burst
        for _ in range(BURST - 1):
            await jitter_sleep(0.02)
            ok2 = await send_once(page, stats)
            if ok2:
                stats.sent += 1
            else:
                stats.fail += 1
                break

        await jitter_sleep(BASE_DELAY)


async def dashboard(stats: Stats):
    while True:
        await asyncio.sleep(2)
        elapsed = max(1.0, time.time() - stats.start_ts)
        speed = stats.sent / elapsed
        print(
            f"[{ts()}] ✅ Sent={stats.sent} | ❌ Fail={stats.fail} | 🔄 Refresh={stats.refresh} "
            f"| ♻ Rotate={stats.rotate} | ⛔Paused={stats.paused} | Speed≈{speed:.2f}/sec "
            f"| LastErr={stats.last_err[:80]}"
        )


async def main():
    print("=== PRO LONG-RUN PARALLEL GC SAFE SENDER ===")
    sessionid = input("Enter IG sessionid: ").strip()
    dm_link = input("Enter DM thread link: ").strip()
if not sessionid or not dm_link:
        print("❌ sessionid + DM link required")
        return

    stats = Stats(start_ts=time.time())

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-renderer-backgrounding",
                "--disable-background-timer-throttling",
                "--disable-features=TranslateUI",
                "--disable-notifications",
            ],
        )

        async def make_page():
            context = await browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
                ),
            )
            await context.add_cookies([{
                "name": "sessionid",
                "value": sessionid,
                "domain": ".instagram.com",
                "path": "/",
                "httpOnly": True,
                "secure": True,
            }])
            page = await context.new_page()
            page.set_default_timeout(ACTION_TIMEOUT)
            await page.goto(dm_link, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
            await focus_box(page)
            return page

        tasks = []
        for i in range(PARALLEL_SENDERS):
            name = f"SENDER-{i+1}"
            tasks.append(asyncio.create_task(worker(name, make_page, stats)))

        tasks.append(asyncio.create_task(dashboard(stats)))
        await asyncio.gather(*tasks)


if name == "main":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user")