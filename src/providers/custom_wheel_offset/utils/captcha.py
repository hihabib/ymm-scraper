from playwright.sync_api import sync_playwright
import os
import json
import time

def launch_browser_with_extension(attempt: int = 1):
    extension_path = "e:\\scraper\\src\\providers\\custom_wheel_offset\\extension\\ifibfemgeogfhoebkmokieepdoobkbpo\\3.7.2_0"
    """Launches a Playwright Chromium browser with a specified extension loaded."""
    user_data_dir = os.path.join(os.getcwd(), "playwright_user_data")
    cookies_file_path = os.path.join("e:\\scraper\\src\\providers\\custom_wheel_offset", "cookies.json")

    start = time.monotonic()
    deadline = start + 240  # 4 minutes in seconds

    success = False
    error: Exception | None = None

    with sync_playwright() as p:
        browser = None
        try:
            browser = p.chromium.launch_persistent_context(
                user_data_dir,
                headless=False,
                args=[
                    f"--disable-extensions-except={extension_path}",
                    f"--load-extension={extension_path}",
                ]
            )
            page = browser.new_page()

            # Check if cookies.json exists and load them
            if os.path.exists(cookies_file_path):
                print(f"Loading cookies from {cookies_file_path}...")
                with open(cookies_file_path, "r") as f:
                    loaded_cookies = json.load(f)
                browser.add_cookies(loaded_cookies)  # Add cookies to the browser context
                print("Cookies loaded successfully.")
            else:
                print("No existing cookies.json found. Starting with a fresh session.")

            page.goto("https://www.customwheeloffset.com/")
            print(f"Browser launched with extension: {extension_path}")
            print(f"Attempt {attempt}: Waiting for captcha to be resolved...")
            print(f"Current page title: {page.title()}")

            # Compute remaining time budget and wait accordingly
            remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
            if remaining_ms <= 0:
                raise TimeoutError("Operation exceeded 4-minute limit before waiting for CAPTCHA resolution.")

            # Wait until the page title is no longer "Human Verification" within remaining budget
            page.wait_for_function("document.title !== 'Human Verification'", timeout=min(remaining_ms, 210000))

            # Ensure we are still within the overall 4-minute limit
            if time.monotonic() > deadline:
                raise TimeoutError("Operation exceeded 4-minute limit after CAPTCHA resolution.")

            print("Captcha resolved! Page title is no longer 'Human Verification'.")

            # Save cookies to a JSON file
            cookies = browser.cookies()
            filtered_cookies = [cookie for cookie in cookies if "customwheeloffset" in cookie["domain"]]
            with open(cookies_file_path, "w") as f:
                json.dump(filtered_cookies, f, indent=4)
            print(f"Cookies saved to {cookies_file_path}")

            success = True
        except Exception as e:
            error = e
        finally:
            # Ensure browser is closed before Playwright stops, to avoid 'Event loop is closed'
            try:
                if browser:
                    browser.close()
            except Exception:
                pass

    if success:
        print("Browser session complete. Closing browser.")
        return True
    else:
        print(f"Attempt {attempt} failed: {error}. Restarting...")
        # Recursively retry until it completes within 4 minutes
        return launch_browser_with_extension(attempt=attempt + 1)
