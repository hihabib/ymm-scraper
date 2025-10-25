#!/usr/bin/env python3
"""
Simple Playwright script to visit Tire Rack's accessibility page.

Usage:
- Ensure dependencies are installed: `pip install -r requirements.txt`
- Install Playwright browsers: `python -m playwright install`
- Run: `python src/providers/tire_rack/tire_size.py`
"""

import asyncio
import threading
from pathlib import Path
from typing import Optional, Tuple
import time

from playwright.async_api import async_playwright, BrowserContext, Browser, Page

URL = "https://www.tirerack.com/customer-support/accessibility"

# Realistic headers to reduce bot detection
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.tirerack.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

# JavaScript to inject into the page after full load
JS_SCRIPT = r"""
(function(){
  function wait(ms) {
    return new Promise(resolve => setTimeout(() => resolve(), ms));
  }

  function waitForPopupToBeOpen(){
    return new Promise(resolve => {
      let id;
      id = setInterval(() => {
        if(document.querySelector('#saveTireSize')){
          clearInterval(id);
          resolve();
        }
      }, 300);
    });
  }

  function openPopup({make, year, model, clarifair}){
    const url = `https://www.tirerack.com/register/modalbox_save_tiresize.jsp?autoMake=${encodeURIComponent(make)}&autoYear=${encodeURIComponent(year)}&autoModel=${encodeURIComponent(model)}&autoModClar=${encodeURIComponent(clarifair)}`;
    if (typeof responsiveInfoBox === 'function') {
      responsiveInfoBox(url, 'Add Tire Size');
    } else {
      console.warn('responsiveInfoBox is not available on this page.');
    }
    return waitForPopupToBeOpen();
  }

  // get sizes
  function parseSizes() {
    const container = document.querySelector('#saveTireSize');
    if (!container) return { originalSizes: [], optionalSizes: [] };
    const html = container.innerHTML;
    const parser = new DOMParser();
    const doc = parser.parseFromString(html, 'text/html');

    function getSizes(isOriginal = true) {
        const textToSearch = isOriginal ? 'Original Equipment Tire Size' : 'Optional Size';
        const strongs = Array.from(doc.querySelectorAll('fieldset > legend > strong'));
        const found = strongs.find(el => ((el.textContent || '')).includes(textToSearch));
        if (!found) return [];
        const fieldset = found.closest('fieldset');
        if (!fieldset) return [];
        return Array.from(fieldset.querySelectorAll('.inputContainer label'))
            .map(label => ((label.textContent || '')).trim())
            .map(text => parseTireSize(text));
    }

    function parseTireSize(text) {
        // Check if text contains "Front:" and "Rear:"
        if (text.includes('Front:') && text.includes('Rear:')) {
            // Extract front and rear sizes
            const frontMatch = text.match(/Front:\s*([^\|]+)/);
            const rearMatch = text.match(/Rear:\s*([^\|]*?)(?:\||\s*$)/);
            
            const front = frontMatch ? frontMatch[1].trim() : '';
            const rear = rearMatch ? rearMatch[1].trim() : '';
            
            return { front, rear };
        } else {
            // No Front:/Rear: labels, treat entire text as front
            return { front: text, rear: '' };
        }
    }

    return { 
        originalSizes: getSizes(true), 
        optionalSizes: getSizes(false) 
    };
}

  // close popup
  window.closePopup = () => {
    const btn = document.querySelector('.closeButton');
    if (btn) btn.click();
  }

  window.runScript = async function(datum){
    await openPopup(datum);
    const tireSizes = parseSizes();
    closePopup();
    await wait(1000);
    console.log(tireSizes)
    return tireSizes;
  };

  console.log('Script Loaded');
})();
"""

# Global state for reusable browser/page when imported
_STATE = {
    "playwright": None,
    "browser": None,
    "context": None,
    "page": None,
    "injected": False,
    "loop": None,           # Asyncio loop hosting Playwright
    "loop_thread": None,    # Thread running the loop
}

# Optional override for per-worker persistent profile directory
_PROFILE_DIR_OVERRIDE: Optional[Path] = None

def set_profile_dir_override(path: str | Path) -> None:
    global _PROFILE_DIR_OVERRIDE
    _PROFILE_DIR_OVERRIDE = Path(path)


async def ensure_page() -> Page:
    """Ensure a persistent page with JS injected, keeping the browser open.

    Returns the Playwright Page instance.
    """
    if _STATE["page"] is not None:
        # Always verify script presence; re-inject if missing
        page = _STATE["page"]
        try:
            exists = await page.evaluate("() => typeof window.runScript === 'function'")
        except Exception:
            exists = False
        if not exists:
            print("[ensure_page] runScript missing on existing page; attempting re-injection...")
            for state in ("load", "networkidle"):
                try:
                    await page.wait_for_load_state(state, timeout=20000)
                except Exception:
                    pass
            try:
                await page.add_script_tag(content=JS_SCRIPT)
            except Exception:
                await page.evaluate(JS_SCRIPT)
            exists = await page.evaluate("() => typeof window.runScript === 'function'")
            print(f"[ensure_page] Re-injected script. runScript available: {exists}")
            _STATE["injected"] = bool(exists)
            # As a last resort, try a soft reload and reinject once
            if not exists:
                try:
                    await page.reload(wait_until="domcontentloaded")
                except Exception:
                    pass
                try:
                    await page.add_script_tag(content=JS_SCRIPT)
                except Exception:
                    await page.evaluate(JS_SCRIPT)
                exists = await page.evaluate("() => typeof window.runScript === 'function'")
                print(f"[ensure_page] After reload, runScript available: {exists}")
                _STATE["injected"] = bool(exists)
        return page

    # Start Playwright (without context manager to keep it alive when imported)
    _STATE["playwright"] = await async_playwright().start()
    context, browser = await create_context(_STATE["playwright"])
    page = await context.new_page()
    page.set_default_timeout(60000)

    # Establish session and navigate directly to the accessibility page
    try:
        await page.goto(URL, wait_until="domcontentloaded")
    except Exception:
        pass
    for state in ("load", "networkidle"):
        try:
            await page.wait_for_load_state(state, timeout=20000)
        except Exception:
            pass

    # Console logging
    def _on_console(message):
        try:
            typ = message.type()
        except Exception:
            typ = "log"
        try:
            txt = message.text()
        except Exception:
            txt = ""
        print(f"[console] {typ} {txt}")
    page.on("console", _on_console)

    # Inject JS once
    try:
        await page.add_script_tag(content=JS_SCRIPT)
    except Exception:
        await page.evaluate(JS_SCRIPT)
    exists = await page.evaluate("() => typeof window.runScript === 'function'")
    print(f"Injected script. runScript available: {exists}")

    _STATE.update({
        "browser": browser,
        "context": context,
        "page": page,
        "injected": True,
    })
    # Record the loop hosting Playwright
    try:
        _STATE["loop"] = asyncio.get_running_loop()
    except RuntimeError:
        pass
    return page


async def call_run_script(datum: dict) -> dict:
    """Call the injected window.runScript(datum), await the Promise, print and return result.

    Example datum:
    {"make": "Honda", "year": "2020", "model": "CRV", "clarifair": "CRV"}
    """
    page = await ensure_page()
    # Validate basic keys
    required = {"make", "year", "model", "clarifair"}
    missing = [k for k in required if k not in datum]
    if missing:
        raise ValueError(f"Missing required keys in datum: {missing}")

    # Ensure runScript exists; if not, re-inject and re-check
    try:
        exists = await page.evaluate("() => typeof window.runScript === 'function'")
    except Exception:
        exists = False
    if not exists:
        print("[call_run_script] runScript missing; invoking ensure_page for re-injection...")
        await ensure_page()
        try:
            exists = await page.evaluate("() => typeof window.runScript === 'function'")
        except Exception:
            exists = False
        if not exists:
            # Attempt direct injection once more here
            print("[call_run_script] re-injection fallback via add_script_tag/evaluate...")
            try:
                await page.add_script_tag(content=JS_SCRIPT)
            except Exception:
                await page.evaluate(JS_SCRIPT)
            exists = await page.evaluate("() => typeof window.runScript === 'function'")
        if not exists:
            raise RuntimeError("runScript is not available on the page after injection.")

    # Prepare empty result and run with timeout
    empty_result = {
        "originalSizes": [{"front": "", "rear": ""}],
        "optionalSizes": [{"front": "", "rear": ""}],
    }
    try:
        result = await asyncio.wait_for(
            page.evaluate("datum => window.runScript(datum)", datum),
            timeout=20,
        )
    except asyncio.TimeoutError:
        # On timeout, close popup and return empty result
        try:
            await page.evaluate("() => closePopup()")
        except Exception:
            pass
        return empty_result
    except Exception as e:
        print(f"Error calling runScript: {e}")
        raise

    return result

def _start_background_loop_if_needed() -> None:
    """Ensure there's a dedicated background event loop thread for Playwright.

    Initializes the browser/page within that loop and keeps it alive.
    """
    if _STATE.get("loop_thread") and _STATE["loop_thread"].is_alive():
        return

    ready_evt = threading.Event()

    def _run_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _STATE["loop"] = loop

        async def _init():
            try:
                await ensure_page()
            except Exception as e:
                print(f"[playwright loop init error] {type(e).__name__}: {e}")
            finally:
                # Signal that init finished (success or failure)
                ready_evt.set()

        loop.create_task(_init())
        print("[playwright] background loop started")
        try:
            loop.run_forever()
        except Exception as e:
            print(f"[playwright loop error] {type(e).__name__}: {e}")

    t = threading.Thread(target=_run_loop, name="tire-size-playwright-loop", daemon=True)
    _STATE["loop_thread"] = t
    t.start()
    # Wait until ensure_page has run at least once
    ready_evt.wait(timeout=60)
    if not _STATE.get("page"):
        print("[playwright] warning: page not ready after background init wait")


async def create_context(p) -> Tuple[BrowserContext, Browser]:
    """Create a Playwright browser context, preferring persistent non-headless Chromium.
    If a profile override is provided, use that directory and create it if needed.
    Otherwise, use the default profile if present; fallback to ephemeral Chromium.
    """
    if _PROFILE_DIR_OVERRIDE is not None:
        user_data_dir = _PROFILE_DIR_OVERRIDE
        user_data_dir.mkdir(parents=True, exist_ok=True)
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-extensions",
            ],
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        await context.set_extra_http_headers({
            k: v for k, v in HEADERS.items() if k not in ("User-Agent",)
        })
        return context, context.browser

    default_dir = Path(__file__).resolve().parents[2] / "data" / "chromium_profile"
    if default_dir.exists():
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(default_dir),
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-extensions",
            ],
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        await context.set_extra_http_headers({
            k: v for k, v in HEADERS.items() if k not in ("User-Agent",)
        })
        return context, context.browser

    # Fallback to ephemeral Chromium
    browser = await p.chromium.launch(
        headless=False,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-extensions",
        ],
    )
    context = await browser.new_context(
        user_agent=HEADERS["User-Agent"],
        locale="en-US",
        ignore_https_errors=True,
        extra_http_headers={
            k: v for k, v in HEADERS.items() if k not in ("User-Agent",)
        },
    )
    await context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
    )
    return context, browser


async def main() -> None:
    # Initialize and keep the browser/page alive
    page = await ensure_page()
    print("Keeping browser open indefinitely...")
     
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


def call_run_script_sync(datum: dict) -> dict:
    """Synchronous wrapper around call_run_script for easy looping.

    Ensures the page is initialized once, then calls runScript and returns the result.
    """
    # Ensure background loop is running and page initialized
    _start_background_loop_if_needed()
    loop = _STATE.get("loop")
    if loop is None:
        raise RuntimeError("Playwright loop not initialized")
    print(f"[playwright] dispatching runScript on background loop with datum: {datum}")
    fut = asyncio.run_coroutine_threadsafe(call_run_script(datum), loop)
    try:
        res = fut.result(timeout=60)
    except Exception as e:
        print(f"[playwright] runScript error: {type(e).__name__}: {e}")
        raise
    print(f"[playwright] runScript completed with result: {res}")
    return res


async def keep_browser_open() -> None:
    """Async helper to keep the browser open indefinitely."""
    await ensure_page()
    print("Keeping browser open for reuse. Press Ctrl+C to exit.")
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


def keep_browser_open_sync() -> None:
    """Synchronous wrapper to keep the browser open indefinitely."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    # Ensure page initialized, then keep open
    if not _STATE["page"]:
        loop.run_until_complete(ensure_page())
    try:
        loop.run_until_complete(keep_browser_open())
    except KeyboardInterrupt:
        pass


async def wait_for_script_loaded(timeout_ms: Optional[int] = None, poll_interval_ms: int = 300) -> bool:
    """Wait until injected script exposes window.runScript, then return True.

    - If timeout_ms is None, waits indefinitely.
    - Otherwise waits up to timeout_ms and returns True on success; raises on failure.
    """
    page = await ensure_page()
    # Quick check
    try:
        exists = await page.evaluate("() => typeof window.runScript === 'function'")
    except Exception:
        exists = False
    if exists:
        _STATE["injected"] = True
        return True

    # Attempt injection
    try:
        await page.add_script_tag(content=JS_SCRIPT)
    except Exception:
        await page.evaluate(JS_SCRIPT)

    # Poll for availability
    start = time.monotonic()
    while True:
        try:
            exists = await page.evaluate("() => typeof window.runScript === 'function'")
        except Exception:
            exists = False
        if exists:
            _STATE["injected"] = True
            return True
        if timeout_ms is not None and (time.monotonic() - start) * 1000 >= timeout_ms:
            raise TimeoutError("Timed out waiting for runScript to be available.")
        await asyncio.sleep(poll_interval_ms / 1000.0)


def wait_for_script_loaded_sync(timeout_ms: Optional[int] = None, poll_interval_ms: int = 300) -> bool:
    """Synchronous wrapper around wait_for_script_loaded."""
    _start_background_loop_if_needed()
    loop = _STATE.get("loop")
    if loop is None:
        raise RuntimeError("Playwright loop not initialized for wait_for_script_loaded")
    print("[playwright] waiting for script to load on background loop...")
    fut = asyncio.run_coroutine_threadsafe(wait_for_script_loaded(timeout_ms=timeout_ms, poll_interval_ms=poll_interval_ms), loop)
    try:
        ok = fut.result(timeout=60)
    except Exception as e:
        print(f"[playwright] wait_for_script_loaded error: {type(e).__name__}: {e}")
        raise
    print("[playwright] script loaded confirmed (background loop)")
    return ok


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Tire Rack runScript caller")
    parser.add_argument(
        "--datum-json",
        type=str,
        help="JSON object with keys: make, year, model, clarifair",
    )
    parser.add_argument(
        "--keep-open",
        action="store_true",
        help="Keep browser open indefinitely after actions",
    )
    args = parser.parse_args()

    async def run_cli():
        page = await ensure_page()
        if args.datum_json:
            try:
                datum = json.loads(args.datum_json)
            except Exception as e:
                print(f"Invalid --datum-json: {e}")
                datum = None
            if datum:
                result = await call_run_script(datum)
                print(result)
        if args.keep_open or not args.datum_json:
            print("Keeping browser open indefinitely...")
            try:
                while True:
                    await asyncio.sleep(3600)
            except asyncio.CancelledError:
                pass

    asyncio.run(run_cli())
    data = call_run_script_sync({"year": "2026", "make": "Acura", "model": "MDX SH-AWD", "clarifair": "Technology Package"})
    print(data)