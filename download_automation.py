import os
import time
import shutil
import logging
import winsound
from datetime import datetime, date, timedelta
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

load_dotenv()

# ── Logging ──
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f'download_{date.today()}.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── Config ──
DOWNLOADS_TEMP = Path('temp_downloads')
DOWNLOADS_TEMP.mkdir(exist_ok=True)
TODAY = date.today().strftime('%Y-%m-%d')

DAY_KEYS = {
    0: 'schedule_monday',
    1: 'schedule_tuesday',
    2: 'schedule_wednesday',
    3: 'schedule_thursday',
    4: 'schedule_friday',
    5: 'schedule_saturday',
    6: 'schedule_sunday'
}

# ── Schedule check ──
def get_run_time():
    """Read today's scheduled run time from Supabase."""
    try:
        sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
        day_key = DAY_KEYS[datetime.now().weekday()]
        result = sb.table('rumee_settings').select('value').eq('key', day_key).execute()
        if result.data:
            return result.data[0]['value']  # e.g. '09:00' or 'skip'
    except Exception as e:
        log.error(f"Could not read schedule from Supabase: {e}")
    return '09:00'  # fallback default


def wait_until_run_time(run_time_str):
    """
    Sleep until the scheduled run time.
    Task Scheduler starts this script at 6am.
    Script sleeps until the user-configured time then wakes up.
    """
    now = datetime.now()
    hour, minute = map(int, run_time_str.split(':'))
    run_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if run_time <= now:
        # Already past run time today — exit, will run tomorrow
        log.info(f"Run time {run_time_str} already passed for today. Exiting.")
        return False

    wait_seconds = (run_time - now).total_seconds()
    log.info(f"Waiting {int(wait_seconds/60)} minutes until {run_time_str}...")
    time.sleep(wait_seconds)
    return True


# ── Download helpers ──
def move_to_drive(temp_path: Path, drive_folder: str):
    """Move downloaded file from temp to Google Drive folder."""
    dest_dir = Path(drive_folder)
    dest_dir.mkdir(parents=True, exist_ok=True)
    suffix = temp_path.suffix
    dest_path = dest_dir / f"{TODAY}{suffix}"
    shutil.move(str(temp_path), str(dest_path))
    log.info(f"Saved → {dest_path}")
    return dest_path


def wait_for_download(page, trigger_fn, timeout=60000):
    """Trigger a download and wait for it to complete."""
    with page.expect_download(timeout=timeout) as dl:
        trigger_fn()
    download = dl.value
    temp_path = DOWNLOADS_TEMP / download.suggested_filename
    download.save_as(str(temp_path))
    log.info(f"Downloaded: {download.suggested_filename}")
    return temp_path


def beep_and_wait_for_otp(platform_name):
    """Alert user that OTP is needed and wait for login."""
    # Beep 3 times to alert user
    for _ in range(3):
        winsound.Beep(1000, 400)
        time.sleep(0.2)
    print(f"\n{'='*50}")
    print(f"{platform_name.upper()}: OTP required — check browser window")
    print(f"Waiting up to 3 minutes for login...")
    print(f"{'='*50}\n")
    log.info(f"{platform_name}: Waiting for OTP login")


# ── Meesho downloads ──
def download_meesho(page):
    log.info("=== Meesho downloads starting ===")
    page.goto('https://supplier.meesho.com', wait_until='networkidle')

    if 'login' in page.url.lower() or page.is_visible('text=Login with OTP', timeout=3000):
        beep_and_wait_for_otp('Meesho')
        page.wait_for_url('**/supplier.meesho.com/**', timeout=180000)
        page.wait_for_load_state('networkidle')

    # Orders
    try:
        page.goto('https://supplier.meesho.com/orders', wait_until='networkidle')
        page.wait_for_timeout(2000)
        btn = page.locator('button:has-text("Download"), a:has-text("Download")').first
        temp = wait_for_download(page, lambda: btn.click())
        move_to_drive(temp, os.getenv('GDRIVE_ME_ORDERS'))
        log.info("Meesho orders ✓")
    except Exception as e:
        log.error(f"Meesho orders failed: {e}")

    # Returns
    try:
        page.goto('https://supplier.meesho.com/returns', wait_until='networkidle')
        page.wait_for_timeout(2000)
        btn = page.locator('button:has-text("Download"), a:has-text("Download")').first
        temp = wait_for_download(page, lambda: btn.click())
        move_to_drive(temp, os.getenv('GDRIVE_ME_RETURNS'))
        log.info("Meesho returns ✓")
    except Exception as e:
        log.error(f"Meesho returns failed: {e}")

    # Catalog
    try:
        page.goto('https://supplier.meesho.com/products', wait_until='networkidle')
        page.wait_for_timeout(2000)
        btn = page.locator('button:has-text("Export"), button:has-text("Download Catalog")').first
        temp = wait_for_download(page, lambda: btn.click())
        move_to_drive(temp, os.getenv('GDRIVE_ME_CATALOG'))
        log.info("Meesho catalog ✓")
    except Exception as e:
        log.error(f"Meesho catalog failed: {e}")

    # Payments
    try:
        page.goto('https://supplier.meesho.com/payments', wait_until='networkidle')
        page.wait_for_timeout(2000)
        btn = page.locator('button:has-text("Download"), a:has-text("Download")').first
        temp = wait_for_download(page, lambda: btn.click())
        move_to_drive(temp, os.getenv('GDRIVE_ME_PAYMENTS'))
        log.info("Meesho payments ✓")
    except Exception as e:
        log.error(f"Meesho payments failed: {e}")

    log.info("=== Meesho complete ===")


# ── Flipkart downloads ──
def download_flipkart(page):
    log.info("=== Flipkart downloads starting ===")
    page.goto('https://seller.flipkart.com', wait_until='networkidle')

    if 'login' in page.url.lower() or page.is_visible('text=Login', timeout=3000):
        beep_and_wait_for_otp('Flipkart')
        page.wait_for_url('**/seller.flipkart.com/**', timeout=180000)
        page.wait_for_load_state('networkidle')

    downloads = [
        ('https://seller.flipkart.com/orders/manage',               os.getenv('GDRIVE_FK_ORDERS'),   'Flipkart orders'),
        ('https://seller.flipkart.com/insights/listing-performance', os.getenv('GDRIVE_FK_VIEWS'),    'Flipkart views'),
        ('https://seller.flipkart.com/insights/keyword-performance', os.getenv('GDRIVE_FK_KEYWORDS'), 'Flipkart keywords'),
        ('https://seller.flipkart.com/advertising/reports',          os.getenv('GDRIVE_FK_ADS'),      'Flipkart ads'),
        ('https://seller.flipkart.com/listing/manage-listings',      os.getenv('GDRIVE_FK_LISTINGS'), 'Flipkart listings'),
        ('https://seller.flipkart.com/payments/statement',           os.getenv('GDRIVE_FK_PAYMENTS'), 'Flipkart payments'),
    ]

    for url, folder, label in downloads:
        try:
            page.goto(url, wait_until='networkidle')
            page.wait_for_timeout(3000)
            btn = page.locator('button:has-text("Download"), a:has-text("Download"), a:has-text("Export")').first
            temp = wait_for_download(page, lambda: btn.click())
            move_to_drive(temp, folder)
            log.info(f"{label} ✓")
        except Exception as e:
            log.error(f"{label} failed: {e}")

    log.info("=== Flipkart complete ===")


# ── Main ──
def main():
    log.info(f"Download automation woke up — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # Read today's schedule
    run_time = get_run_time()

    if run_time == 'skip':
        log.info(f"Today ({datetime.now().strftime('%A')}) is set to skip. Exiting.")
        return

    # Wait until scheduled time
    should_run = wait_until_run_time(run_time)
    if not should_run:
        return

    log.info(f"Run time reached. Starting downloads...")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(Path.home() / 'AppData/Local/Google/Chrome/RumeeAutomation'),
            channel='chrome',
            headless=False,
            args=['--start-maximized'],
            accept_downloads=True,
        )
        page = context.pages[0] if context.pages else context.new_page()

        try:
            download_meesho(page)
            download_flipkart(page)
            log.info("All downloads completed successfully ✓")
            print("\n✓ Done — files saved to Google Drive")
            print("GitHub Actions will process within 6 hours")
            winsound.Beep(800, 1000)  # single long beep = done
        except Exception as e:
            log.error(f"Fatal error: {e}")
        finally:
            context.close()
            shutil.rmtree(DOWNLOADS_TEMP, ignore_errors=True)


if __name__ == '__main__':
    main()
