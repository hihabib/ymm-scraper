"""Custom Wheel Offset Scraper V2 - Main scraper class.
Object-oriented scraper with modular components and persistent browser profiles.
"""

import asyncio
import logging
import os
import json
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from sqlalchemy.orm import sessionmaker
from sqlalchemy import desc, Integer

import sys
import os

# Add the current directory to path for local imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Add src directory to path for project imports
src_dir = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.abspath(src_dir))

# Import local config from the same directory
import importlib.util
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, 'config.py')
spec = importlib.util.spec_from_file_location("local_config", config_path)
local_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local_config)

BASE_URL = local_config.BASE_URL
NAVIGATION_TIMEOUT = local_config.NAVIGATION_TIMEOUT
HEADLESS = local_config.HEADLESS
BROWSER_ARGS = local_config.BROWSER_ARGS
from profile_manager import ProfileManager
from browser_setup import BrowserSetup
from human_behavior import HumanBehavior
from db.db import engine
from core.models import CustomWheelOffsetYMM, CustomWheelOffsetData

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CustomWheelOffsetScraperV2:
    """
    Enhanced scraper for Custom Wheel Offset with captcha handling and JavaScript injection workflow.
    """
    
    def __init__(self, profile_name: str = "default", worker_id: int = None, start_year: str = None, end_year: str = None):
        """
        Initialize the scraper with a specific profile name and worker configuration.
        
        Args:
            profile_name: Unique name for the browser profile (used for persistence)
            worker_id: Unique identifier for this worker instance
            start_year: Starting year for this worker's data range
            end_year: Ending year for this worker's data range
        """
        self.profile_name = profile_name
        self.worker_id = worker_id
        self.start_year = start_year
        self.end_year = end_year
        self.logger = self._setup_logger()
        self.playwright: Optional[Playwright] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.is_existing_profile = False
        self.is_initial_startup = True  # Flag to track if this is the initial startup
        
        # Configuration using imported constants
        self.base_url = BASE_URL
        self.navigation_timeout = NAVIGATION_TIMEOUT
        
        # Initialize modular components
        self.profile_manager = ProfileManager(self.profile_name)
        self.browser_setup = BrowserSetup(self.profile_manager)
        self.human_behavior = None  # Will be initialized after page is created
        
        # Database session
        Session = sessionmaker(bind=engine)
        self.db_session = Session()
        
        # JavaScript file paths
        self.js_dir = Path(__file__).parent / "js"
        self.navigate_js_path = self.js_dir / "navigateToNext.js"
        self.extract_js_path = self.js_dir / "extract_fitment.js"
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger with worker ID prefix."""
        logger = logging.getLogger(f"CustomWheelOffsetV2_worker_{self.worker_id}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f'%(asctime)s - worker_{self.worker_id} - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    async def setup_browser(self, force_fresh_profile: bool = False) -> None:
        """Setup browser with profile and extensions."""
        
        # Start playwright
        self.playwright = await async_playwright().start()
        
        # Get profile path and status
        profile_path, self.is_existing_profile = self.profile_manager.get_profile_path(force_fresh_profile)
        
        # Extension path for captcha solving
        extension_path = os.path.join(os.path.dirname(__file__), "extension", "ifibfemgeogfhoebkmokieepdoobkbpo", "3.7.2_0")
        
        # Updated browser args to include extension
        browser_args = BROWSER_ARGS.copy()
        browser_args.extend([
            f"--load-extension={extension_path}",
            "--disable-extensions-except=" + extension_path
        ])
        
        try:
            # Launch persistent context with profile and extension
            self.context = await self.playwright.chromium.launch_persistent_context(
                user_data_dir=profile_path,
                headless=HEADLESS,
                args=browser_args,
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                ignore_default_args=["--enable-automation"],
                slow_mo=100,  # Add slight delay between actions
                timeout=60000  # Increase timeout for browser launch
            )
            
            # Get the first page or create new one
            if self.context.pages:
                self.page = self.context.pages[0]
            else:
                self.page = await self.context.new_page()
            
            # Check if extension loaded successfully and log browser serial
            try:
                # Wait a moment for extension to load
                await self.page.wait_for_timeout(2000)
                
                # Check if extension is loaded by looking for extension pages
                extension_pages = [page for page in self.context.pages if 'chrome-extension://' in page.url]
                
                if extension_pages:
                    # Get browser serial/version info
                    browser_info = await self.page.evaluate("""
                        () => {
                            return {
                                userAgent: navigator.userAgent,
                                platform: navigator.platform,
                                cookieEnabled: navigator.cookieEnabled,
                                onLine: navigator.onLine,
                                language: navigator.language
                            };
                        }
                    """)
                    
                    # Log extension success with browser serial info
                    self.logger.info(f"🔌 Extension loaded successfully!")
                    self.logger.info(f"📱 Browser Serial Info:")
                    self.logger.info(f"   - User Agent: {browser_info['userAgent']}")
                    self.logger.info(f"   - Platform: {browser_info['platform']}")
                    self.logger.info(f"   - Language: {browser_info['language']}")
                    self.logger.info(f"   - Cookie Enabled: {browser_info['cookieEnabled']}")
                    self.logger.info(f"   - Online Status: {browser_info['onLine']}")
                    self.logger.info(f"   - Extension Pages: {len(extension_pages)} found")
                    
                    # Also log to browser console
                    try:
                        await self.page.evaluate("""
                            console.log('🔌 Extension loaded successfully!');
                            console.log('Worker ID: """ + str(self.worker_id) + """');
                            console.log('Profile: """ + str(self.profile_name) + """');
                        """)
                        
                        # Log browser info separately to avoid JSON issues
                        await self.page.evaluate(f"""
                            console.log('📱 Browser Serial Info:');
                            console.log('  User Agent: {browser_info.get("user_agent", "N/A")}');
                            console.log('  Platform: {browser_info.get("platform", "N/A")}');
                            console.log('  Language: {browser_info.get("language", "N/A")}');
                            console.log('  Cookies Enabled: {browser_info.get("cookie_enabled", "N/A")}');
                            console.log('  Online Status: {browser_info.get("online_status", "N/A")}');
                        """)
                    except Exception as console_error:
                        self.logger.warning(f"Could not log to browser console: {console_error}")
                else:
                    self.logger.warning("⚠️ Extension may not have loaded properly - no extension pages found")
                    
            except Exception as ext_check_error:
                self.logger.warning(f"Could not verify extension loading: {ext_check_error}")
            
            # Initialize human behavior with the page
            self.human_behavior = HumanBehavior(self.page)
            
            self.logger.info(f"Browser setup completed ({'existing' if self.is_existing_profile else 'new'} profile)")
            
        except Exception as e:
            self.logger.error(f"Browser setup failed: {e}")
            # Clean up on failure
            await self.cleanup()
            raise
    
    async def navigate_to_target(self) -> None:
        """Navigate to the target URL."""
        await self.page.goto(self.base_url, timeout=self.navigation_timeout)
    
    async def perform_human_activities(self) -> None:
        """Perform human-like activities if using a new profile.
        Only runs on initial startup, not during restarts.
        """
        if not self.is_initial_startup:
            self.logger.info("Skipping human activities - this is a restart, not initial startup")
            return
            
        if self.is_existing_profile:
            self.logger.info("Skipping human activities - using existing profile")
            return
            
        self.logger.info("Starting human activities simulation...")
        try:
            # Use enhanced human simulation for initial startup
            await self.human_behavior.perform_enhanced_human_simulation()
            self.logger.info("Human activities simulation completed successfully")
        except Exception as e:
            self.logger.error(f"Error during human activities simulation: {e}")
            # Continue with the scraping process even if human simulation fails
    
    async def wait_for_captcha_resolution(self) -> None:
        """Wait for captcha to be resolved automatically by checking page title."""
        
        max_cycles = 10  # Maximum number of captcha cycles to handle
        cycle_count = 0
        
        while cycle_count < max_cycles:
            cycle_count += 1
            
            # Check current page title
            try:
                page_title = await self.page.title()
                
                if page_title != "Human Verification":
                    break
                    
                self.logger.info("Captcha detected - waiting for resolution...")
                
                # Wait for captcha resolution with 3-minute timeout
                max_wait_time = 180  # 3 minutes
                check_interval = 5   # Check every 5 seconds
                elapsed_time = 0
                
                captcha_resolved = False
                
                while elapsed_time < max_wait_time:
                    try:
                        current_title = await self.page.title()
                        
                        if current_title != "Human Verification":
                            self.logger.info(f"Captcha resolved")
                            captcha_resolved = True
                            break
                        
                        await asyncio.sleep(check_interval)
                        elapsed_time += check_interval
                            
                    except Exception as e:
                        self.logger.warning(f"Error checking page title: {e}")
                        await asyncio.sleep(check_interval)
                        elapsed_time += check_interval
                
                if not captcha_resolved:
                    self.logger.warning("Captcha timeout - refreshing page")
                    await self.page.reload(wait_until='networkidle')
                    await asyncio.sleep(3)  # Wait a bit after refresh
                else:
                    # Captcha was resolved, but check if another one appears
                    await asyncio.sleep(2)  # Brief wait to see if another captcha appears
                    continue
                    
            except Exception as e:
                self.logger.error(f"Error during captcha detection: {e}")
                break
        
        if cycle_count >= max_cycles:
            self.logger.warning(f"Maximum captcha cycles reached - proceeding anyway")
    
    async def wait_for_page_load(self) -> None:
        """Wait for the page to fully load after captcha resolution."""
        
        try:
            # Wait for network to be idle
            await self.page.wait_for_load_state('networkidle', timeout=30000)
            
            # Additional wait for any dynamic content
            await asyncio.sleep(3)
            
            # Check if page is accessible (not showing error or captcha)
            page_title = await self.page.title()
            self.logger.info(f"Page loaded - Title: {page_title}")
            
        except Exception as e:
            self.logger.warning(f"Error waiting for page load: {e}")
            # Continue anyway after a short wait
            await asyncio.sleep(5)
    
    async def final_wait_and_close(self) -> None:
        """Wait for 20 seconds before closing as requested."""
        self.logger.info("Waiting 20 seconds before closing...")
        await asyncio.sleep(20)
        self.logger.info("20-second wait completed - ready to close")
    
    def get_last_ymm_record(self, start_year: str = None, end_year: str = None) -> dict:
        """Fetch the last inserted record from custom_wheel_offset_ymm table within specified year range."""
        try:
            query = self.db_session.query(CustomWheelOffsetYMM)
            
            # Filter by year range if provided
            if start_year and end_year:
                # Convert years to integers for comparison
                start_year_int = int(start_year)
                end_year_int = int(end_year)
                
                # Filter records where year is between start_year and end_year (inclusive)
                query = query.filter(
                    CustomWheelOffsetYMM.year.cast(Integer) >= start_year_int,
                    CustomWheelOffsetYMM.year.cast(Integer) <= end_year_int
                )
            
            last_record = query.order_by(desc(CustomWheelOffsetYMM.id)).first()
            
            if last_record:
                resume_data = {
                    "year": last_record.year,
                    "make": last_record.make,
                    "model": last_record.model,
                    "trim": last_record.trim,
                    "drive": last_record.drive
                }
                
                # Include suspension, modification, and rubbing if they are not null or empty
                if last_record.suspension and last_record.suspension.strip():
                    resume_data["suspension"] = last_record.suspension
                
                if last_record.modification and last_record.modification.strip():
                    resume_data["modification"] = last_record.modification
                
                if last_record.rubbing and last_record.rubbing.strip():
                    resume_data["rubbing"] = last_record.rubbing
                
                return resume_data
            else:
                return {}
                
        except Exception as e:
            self.logger.error(f"Error fetching last YMM record: {e}")
            return {}
    
    def save_ymm_record(self, vehicle_data: dict) -> int:
        """Save a new YMM record and return its ID."""
        try:
            new_record = CustomWheelOffsetYMM(
                year=vehicle_data.get("year", ""),
                make=vehicle_data.get("make", ""),
                model=vehicle_data.get("model", ""),
                trim=vehicle_data.get("trim", ""),
                drive=vehicle_data.get("drive", ""),
                dr_chassis_id=vehicle_data.get("DRChassisID", ""),
                vehicle_type=vehicle_data.get("vehicle_type", ""),
                suspension=vehicle_data.get("suspension", ""),
                modification=vehicle_data.get("modification", ""),
                rubbing=vehicle_data.get("rubbing", ""),
                processed=0
            )
            
            self.db_session.add(new_record)
            self.db_session.commit()
            
            return new_record.id
            
        except Exception as e:
            self.logger.error(f"Error saving YMM record: {e}")
            self.db_session.rollback()
            return None

    def validate_vehicle_data(self, vehicle_data: dict) -> None:
        """
        Validate that all required vehicle data fields are present and valid.
        Raises ValueError if any field is missing or invalid.
        
        NOTE: Data validation is currently omitted - method returns without validation.
        Database schema has been updated to accept null values for all fields.
        """
        # Data validation omitted - return without performing any checks
        return

    def save_complete_record_with_transaction(self, vehicle_data: dict, fitment_data: dict, bolt_pattern: str = None) -> bool:
        """
        Save YMM record and fitment data in a single transaction with rollback capability.
        Returns True if both saves succeed, False otherwise.
        """
        ymm_id = None
        try:
            # Add bolt_pattern to vehicle_data for validation if provided
            validation_data = vehicle_data.copy()
            if bolt_pattern:
                validation_data["bolt_pattern"] = bolt_pattern
            else:
                validation_data["bolt_pattern"] = ""
            
            # Validate vehicle data before saving - this will raise ValueError if invalid
            self.validate_vehicle_data(validation_data)
            
            # Start transaction by saving YMM record first
            new_record = CustomWheelOffsetYMM(
                year=vehicle_data.get("year", ""),
                make=vehicle_data.get("make", ""),
                model=vehicle_data.get("model", ""),
                trim=vehicle_data.get("trim", ""),
                drive=vehicle_data.get("drive", ""),
                dr_chassis_id=vehicle_data.get("DRChassisID", ""),
                vehicle_type=vehicle_data.get("vehicle_type", ""),
                suspension=vehicle_data.get("suspension", ""),
                modification=vehicle_data.get("modification", ""),
                rubbing=vehicle_data.get("rubbing", ""),
                processed=0
            )
            
            self.db_session.add(new_record)
            self.db_session.flush()  # Get the ID without committing
            ymm_id = new_record.id
            
            self.logger.info(f"Created YMM record with ID: {ymm_id} (not yet committed)")
            
            # Save fitment data for both positions
            fitment_records_added = 0
            for position in ["front", "rear"]:
                if position in fitment_data:
                    data = fitment_data[position]
                    
                    fitment_record = CustomWheelOffsetData(
                        ymm_id=ymm_id,
                        position=position,
                        diameter_min=data.get("diameter", {}).get("min", ""),
                        diameter_max=data.get("diameter", {}).get("max", ""),
                        width_min=data.get("width", {}).get("min", ""),
                        width_max=data.get("width", {}).get("max", ""),
                        offset_min=data.get("offset", {}).get("min", ""),
                        offset_max=data.get("offset", {}).get("max", "")
                    )
                    
                    self.db_session.add(fitment_record)
                    fitment_records_added += 1
            
            # Update bolt pattern if provided
            if bolt_pattern:
                new_record.bolt_pattern = bolt_pattern
            
            # Commit the entire transaction
            self.db_session.commit()
            
            # Success message with tick mark for console visibility
            worker_info = f"Worker {self.worker_id}" if self.worker_id else "Main"
            year_range = f"({self.start_year}-{self.end_year})" if self.start_year and self.end_year else ""
            success_msg = f"✅ {worker_info} {year_range} - Data saved successfully! YMM ID: {ymm_id}, Fitment records: {fitment_records_added}"
            
            self.logger.info(success_msg)
            print(success_msg)  # Also print to console for visibility
            
            return True
            
        except ValueError as ve:
            # Validation error - this should trigger browser restart
            self.logger.error(f"✗ Vehicle data validation failed: {ve}")
            try:
                if ymm_id:  # Only rollback if we started a transaction
                    self.db_session.rollback()
                    self.logger.info(f"✓ Transaction rolled back due to validation failure")
            except Exception as rollback_error:
                self.logger.error(f"✗ Rollback failed: {rollback_error}")
            # Re-raise ValueError to trigger browser restart
            raise ve
            
        except Exception as e:
            self.logger.error(f"✗ Transaction failed: {e}")
            try:
                self.db_session.rollback()
                self.logger.info(f"✓ Transaction rolled back successfully")
            except Exception as rollback_error:
                self.logger.error(f"✗ Rollback failed: {rollback_error}")
            return False
    
    def save_fitment_data(self, ymm_id: int, fitment_json: dict) -> None:
        """Save fitment data for both front and rear positions."""
        try:
            for position in ["front", "rear"]:
                if position in fitment_json:
                    data = fitment_json[position]
                    
                    fitment_record = CustomWheelOffsetData(
                        ymm_id=ymm_id,
                        position=position,
                        diameter_min=data.get("diameter", {}).get("min", ""),
                        diameter_max=data.get("diameter", {}).get("max", ""),
                        width_min=data.get("width", {}).get("min", ""),
                        width_max=data.get("width", {}).get("max", ""),
                        offset_min=data.get("offset", {}).get("min", ""),
                        offset_max=data.get("offset", {}).get("max", "")
                    )
                    
                    self.db_session.add(fitment_record)
            
            self.db_session.commit()
            self.logger.info(f"Saved fitment data for YMM ID: {ymm_id}")
            
        except Exception as e:
            self.logger.error(f"Error saving fitment data: {e}")
            self.db_session.rollback()
    
    def update_bolt_pattern(self, ymm_id: int, bolt_pattern: str) -> None:
        """Update the bolt pattern for a YMM record."""
        try:
            ymm_record = self.db_session.query(CustomWheelOffsetYMM).filter_by(id=ymm_id).first()
            if ymm_record:
                ymm_record.bolt_pattern = bolt_pattern
                self.db_session.commit()
                self.logger.info(f"Updated bolt pattern for YMM ID {ymm_id}: {bolt_pattern}")
            else:
                self.logger.warning(f"YMM record with ID {ymm_id} not found")
                
        except Exception as e:
            self.logger.error(f"Error updating bolt pattern: {e}")
            self.db_session.rollback()
    
    def extract_url_parameters(self, url: str) -> dict:
        """Extract vehicle parameters from the URL."""
        try:
            parsed_url = urlparse(url)
            params = parse_qs(parsed_url.query)
            
            # Extract single values from query parameters
            vehicle_data = {}
            for key in ["year", "make", "model", "trim", "drive", "DRChassisID", "vehicle_type", "suspension", "modification", "rubbing"]:
                if key in params and params[key]:
                    vehicle_data[key] = params[key][0]  # Get first value
            
            self.logger.info(f"Extracted vehicle data from URL: {vehicle_data}")
            return vehicle_data
            
        except Exception as e:
            self.logger.error(f"Error extracting URL parameters: {e}")
            return {}
    
    async def inject_and_execute_js(self, js_file_path: Path, function_call: str = None) -> str:
        """Inject JavaScript file and optionally execute a function call."""
        try:
            # Read JavaScript file
            with open(js_file_path, 'r', encoding='utf-8') as f:
                js_content = f.read()
            
            # Execute the JavaScript and get the result
            result = await self.page.evaluate(js_content)
            self.logger.info(f"Executed JavaScript from {js_file_path.name}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error injecting/executing JavaScript: {e}")
            return None
    
    async def monitor_console_for_completion(self) -> bool:
        """Monitor console for completion messages."""
        completion_messages = ["[COMPLETE] No more data found.", "[STOP] Year limit is reached."]
        
        def handle_console(msg):
            message_text = msg.text
            self.logger.info(f"Console: {message_text}")
            
            for completion_msg in completion_messages:
                if completion_msg in message_text:
                    self.logger.info(f"Completion message detected: {completion_msg}")
                    return True
            return False
        
        # Set up console listener
        self.page.on("console", handle_console)
        return False
    
    async def detect_json_from_page(self) -> dict:
        """Detect and extract JSON object from page content before redirection."""
        try:
            # Wait a moment for the JSON to appear on the page
            await asyncio.sleep(1)
            
            # Try to extract JSON from page content using multiple methods
            json_data = None
            
            # Method 1: Look for JSON in page text content
            page_content = await self.page.content()
            
            # Look for JSON patterns in the page content
            import re
            json_patterns = [
                r'\{"year":\s*"[^"]+",\s*"make":\s*"[^"]+",\s*"model":\s*"[^"]+",\s*"trim":\s*"[^"]+",\s*"drive":\s*"[^"]+"[^}]*\}',
                r'\{[^{}]*"year"[^{}]*\}',
                r'\{[^{}]*"make"[^{}]*\}',
            ]
            
            for pattern in json_patterns:
                matches = re.findall(pattern, page_content, re.IGNORECASE)
                if matches:
                    for match in matches:
                        try:
                            json_data = json.loads(match)
                            if 'year' in json_data and 'make' in json_data and 'model' in json_data:
                                self.logger.info(f"Found JSON data via regex pattern: {json_data}")
                                return json_data
                        except json.JSONDecodeError:
                            continue
            
            # Method 2: Look for JSON in visible text elements
            json_text = await self.page.evaluate("""
                () => {
                    // Look for JSON in all text nodes
                    const walker = document.createTreeWalker(
                        document.body,
                        NodeFilter.SHOW_TEXT,
                        null,
                        false
                    );
                    
                    let node;
                    while (node = walker.nextNode()) {
                        const text = node.textContent.trim();
                        if (text.includes('"year"') && text.includes('"make"') && text.includes('"model"')) {
                            // Try to extract JSON from this text
                            const jsonMatch = text.match(/\\{[^{}]*"year"[^{}]*\\}/);
                            if (jsonMatch) {
                                try {
                                    const parsed = JSON.parse(jsonMatch[0]);
                                    if (parsed.year && parsed.make && parsed.model) {
                                        return jsonMatch[0];
                                    }
                                } catch (e) {
                                    // Continue searching
                                }
                            }
                        }
                    }
                    
                    // Also check for JSON in pre, code, or script tags
                    const elements = document.querySelectorAll('pre, code, script, div, span, p');
                    for (const element of elements) {
                        const text = element.textContent || element.innerText || '';
                        if (text.includes('"year"') && text.includes('"make"') && text.includes('"model"')) {
                            const jsonMatch = text.match(/\\{[^{}]*"year"[^{}]*\\}/);
                            if (jsonMatch) {
                                try {
                                    const parsed = JSON.parse(jsonMatch[0]);
                                    if (parsed.year && parsed.make && parsed.model) {
                                        return jsonMatch[0];
                                    }
                                } catch (e) {
                                    // Continue searching
                                }
                            }
                        }
                    }
                    
                    return null;
                }
            """)
            
            if json_text:
                try:
                    json_data = json.loads(json_text)
                    self.logger.info(f"Found JSON data via JavaScript evaluation: {json_data}")
                    return json_data
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Failed to parse JSON text: {json_text}, error: {e}")
            
            # Method 3: Check if JSON is in console logs
            console_logs = []
            def capture_console(msg):
                console_logs.append(msg.text)
            
            self.page.on("console", capture_console)
            await asyncio.sleep(0.5)  # Brief wait to capture any console output
            
            for log in console_logs:
                if '"year"' in log and '"make"' in log and '"model"' in log:
                    try:
                        # Extract JSON from console log
                        json_match = re.search(r'\{[^{}]*"year"[^{}]*\}', log)
                        if json_match:
                            json_data = json.loads(json_match.group())
                            self.logger.info(f"Found JSON data in console logs: {json_data}")
                            return json_data
                    except json.JSONDecodeError:
                        continue
            
            self.logger.warning("No JSON data found on page")
            return {}
            
        except Exception as e:
            self.logger.error(f"Error detecting JSON from page: {e}")
            return {}

    def save_initial_ymm_record(self, json_data: dict) -> int:
        """Save initial YMM record from JSON data and return the row ID."""
        try:
            # Validate required fields
            required_fields = ['year', 'make', 'model', 'trim', 'drive']
            for field in required_fields:
                if field not in json_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # Create YMM record with JSON data
            ymm_record = CustomWheelOffsetYMM(
                year=json_data.get('year', ''),
                make=json_data.get('make', ''),
                model=json_data.get('model', ''),
                trim=json_data.get('trim', ''),
                drive=json_data.get('drive', ''),
                suspension=json_data.get('suspension', ''),
                modification=json_data.get('modification', ''),
                rubbing=json_data.get('rubbing', ''),
                # Additional fields will be updated later from URL
                dr_chassis_id='',
                vehicle_type='',
                bolt_pattern=''
            )
            
            self.db_session.add(ymm_record)
            self.db_session.flush()  # Flush to get the ID without committing
            
            ymm_id = ymm_record.id
            self.logger.info(f"Created initial YMM record with ID: {ymm_id}, data: {json_data}")
            
            return ymm_id
            
        except Exception as e:
            self.logger.error(f"Error saving initial YMM record: {e}")
            self.db_session.rollback()
            raise e

    def update_ymm_record_with_url_data(self, ymm_id: int, url_data: dict, json_data: dict) -> bool:
        """Update existing YMM record with additional URL data, excluding JSON fields."""
        try:
            # Get the existing record
            ymm_record = self.db_session.query(CustomWheelOffsetYMM).filter_by(id=ymm_id).first()
            if not ymm_record:
                raise ValueError(f"YMM record with ID {ymm_id} not found")
            
            # Fields to exclude (already set from JSON)
            excluded_fields = {'year', 'make', 'model', 'trim', 'drive', 'suspension', 'modification', 'rubbing'}
            
            # Update only the fields not in JSON data
            updated_fields = []
            for key, value in url_data.items():
                if key not in excluded_fields and hasattr(ymm_record, key):
                    setattr(ymm_record, key, value)
                    updated_fields.append(f"{key}={value}")
            
            # Map URL parameter names to database field names if needed
            field_mapping = {
                'DRChassisID': 'dr_chassis_id',
                'vehicle_type': 'vehicle_type'
            }
            
            for url_key, db_field in field_mapping.items():
                if url_key in url_data and hasattr(ymm_record, db_field):
                    setattr(ymm_record, db_field, url_data[url_key])
                    updated_fields.append(f"{db_field}={url_data[url_key]}")
            
            self.db_session.commit()
            self.logger.info(f"Updated YMM record {ymm_id} with URL data: {updated_fields}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating YMM record with URL data: {e}")
            self.db_session.rollback()
            return False

    async def javascript_workflow(self) -> bool:
        """Execute the JavaScript injection workflow. Returns True if should continue, False if complete."""
        try:
            # Step 1: Inject navigateToNext.js
            await self.inject_and_execute_js(self.navigate_js_path)
            
            # Step 2: Detect and extract JSON data from page before redirection
            json_data = await self.detect_json_from_page()
            
            if not json_data:
                self.logger.warning("No JSON data detected on page - this may indicate an issue")
                # Continue with original workflow if no JSON detected
                return await self.original_javascript_workflow()
            
            # Step 3: Save initial YMM record with JSON data and get the row ID
            try:
                ymm_id = self.save_initial_ymm_record(json_data)
                self.logger.info(f"Saved initial YMM record with ID: {ymm_id}")
            except Exception as e:
                self.logger.error(f"Failed to save initial YMM record: {e}")
                return False
            
            # Check console for success message
            await asyncio.sleep(1)  # Brief wait for console message
            
            # Step 4: Get last YMM record and construct resumeFrom object
            last_record = self.get_last_ymm_record(self.start_year, self.end_year)
            resume_from_js = f"const resumeFrom = {json.dumps(last_record)};"
            print(f"resumeFrom: {resume_from_js}")
            
            # Step 5: Execute navigateAndApplyNext with resumeFrom and dynamic END_YEAR
            end_year_param = self.end_year if self.end_year else '2026'  # Fallback to default
            start_year_param = self.start_year if self.start_year else '2026'  # Fallback to default
            function_call = f"{resume_from_js} navigateAndApplyNext(resumeFrom, true, '{start_year_param}', '{end_year_param}');"
            await self.page.evaluate(function_call)
            self.logger.info(f"Executed navigateAndApplyNext with resumeFrom: {last_record}, START_YEAR: {start_year_param}, END_YEAR: {end_year_param}")
            
            # Step 6: Wait for page reload and handle captcha if needed
            await asyncio.sleep(3)  # Wait for navigation to start
            await self.page.wait_for_load_state('networkidle', timeout=30000)
            
            # Check for captcha after reload
            await self.wait_for_captcha_resolution()
            await self.wait_for_page_load()
            
            # Step 7: Extract vehicle data from URL (excluding JSON fields)
            current_url = self.page.url
            url_data = self.extract_url_parameters(current_url)
            
            if not url_data:
                error_msg = "No vehicle data extracted from URL - this indicates a navigation or page loading error"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)
            
            # Step 8: Update YMM record with URL data (excluding JSON fields)
            update_success = self.update_ymm_record_with_url_data(ymm_id, url_data, json_data)
            if not update_success:
                self.logger.error("Failed to update YMM record with URL data")
                return False
            
            # Step 9: Execute extract_fitment.js
            fitment_result = await self.inject_and_execute_js(self.extract_js_path)
            
            if fitment_result:
                try:
                    # Parse the JSON result
                    if isinstance(fitment_result, str):
                        fitment_data = json.loads(fitment_result)
                    else:
                        fitment_data = fitment_result
                    
                    # Extract bolt pattern from front data if available
                    bolt_pattern = None
                    if "front" in fitment_data and "boltPattern" in fitment_data["front"]:
                        bolt_pattern = fitment_data["front"]["boltPattern"].get("inch", "")
                    
                    # Step 10: Save fitment data and update bolt pattern
                    try:
                        self.save_fitment_data(ymm_id, fitment_data)
                        if bolt_pattern:
                            self.update_bolt_pattern(ymm_id, bolt_pattern)
                        
                        self.logger.info("✓ Successfully processed vehicle data and fitment information")
                        return True
                        
                    except Exception as e:
                        self.logger.error(f"Error saving fitment data: {e}")
                        return False
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"Error parsing fitment JSON: {e}")
                    return False
            else:
                self.logger.error("Failed to extract fitment data")
                return False
                
        except Exception as e:
            self.logger.error(f"Error in JavaScript workflow: {e}")
            # Check if this error should trigger restart using comprehensive logic
            if self.is_restart_required_error(str(e), type(e).__name__):
                self.logger.warning("Detected critical error that requires restart - will trigger restart")
                raise e  # Re-raise to trigger auto-restart
            else:
                # Non-critical error, continue workflow
                return False

    async def original_javascript_workflow(self) -> bool:
        """Original JavaScript workflow for fallback when JSON detection fails."""
        try:
            # Check console for success message
            await asyncio.sleep(1)  # Brief wait for console message
            
            # Step 2: Get last YMM record and construct resumeFrom object
            last_record = self.get_last_ymm_record(self.start_year, self.end_year)
            resume_from_js = f"const resumeFrom = {json.dumps(last_record)};"
            print(f"resumeFrom: {resume_from_js}")
            # Step 3: Execute navigateAndApplyNext with resumeFrom and dynamic END_YEAR
            end_year_param = self.end_year if self.end_year else '2026'  # Fallback to default
            start_year_param = self.start_year if self.start_year else '2026'  # Fallback to default
            function_call = f"{resume_from_js} navigateAndApplyNext(resumeFrom, true, '{start_year_param}', '{end_year_param}');"
            await self.page.evaluate(function_call)
            self.logger.info(f"Executed navigateAndApplyNext with resumeFrom: {last_record}, START_YEAR: {start_year_param}, END_YEAR: {end_year_param}")
            
            # Step 4: Wait for page reload and handle captcha if needed
            await asyncio.sleep(3)  # Wait for navigation to start
            await self.page.wait_for_load_state('networkidle', timeout=30000)
            
            # Check for captcha after reload
            await self.wait_for_captcha_resolution()
            await self.wait_for_page_load()
            
            # Step 5: Extract vehicle data from URL
            current_url = self.page.url
            vehicle_data = self.extract_url_parameters(current_url)
            
            if not vehicle_data:
                error_msg = "No vehicle data extracted from URL - this indicates a navigation or page loading error"
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)
            
            # Step 6: Execute extract_fitment.js
            fitment_result = await self.inject_and_execute_js(self.extract_js_path)
            
            if fitment_result:
                try:
                    # Parse the JSON result
                    if isinstance(fitment_result, str):
                        fitment_data = json.loads(fitment_result)
                    else:
                        fitment_data = fitment_result
                    
                    # Extract bolt pattern from front data if available
                    bolt_pattern = None
                    if "front" in fitment_data and "boltPattern" in fitment_data["front"]:
                        bolt_pattern = fitment_data["front"]["boltPattern"].get("inch", "")
                    
                    # Step 7: Save complete record with transaction
                    success = self.save_complete_record_with_transaction(
                        vehicle_data=vehicle_data,
                        fitment_data=fitment_data,
                        bolt_pattern=bolt_pattern
                    )
                    
                    if success:
                        self.logger.info("✓ Successfully processed vehicle data and fitment information")
                        return True
                    else:
                        self.logger.error("✗ Failed to save complete record - transaction rolled back")
                        return False
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"Error parsing fitment JSON: {e}")
                    return False
            else:
                self.logger.error("Failed to extract fitment data")
                return False
                
        except Exception as e:
            self.logger.error(f"Error in original JavaScript workflow: {e}")
            # Check if this error should trigger restart using comprehensive logic
            if self.is_restart_required_error(str(e), type(e).__name__):
                self.logger.warning("Detected critical error that requires restart - will trigger restart")
                raise e  # Re-raise to trigger auto-restart
            else:
                # Non-critical error, continue workflow
                return False

    def is_restart_required_error(self, error_msg: str, error_type: str = "") -> bool:
        """
        Determine if an error requires system restart.
        
        Args:
            error_msg: Error message to analyze
            error_type: Type of error (optional)
            
        Returns:
            True if system restart is required
        """
        restart_indicators = [
            "session expired",
            "invalid session", 
            "authentication failed",
            "unauthorized",
            "token expired",
            "captcha",
            "human verification",
            "aws waf",
            "forbidden",
            "access denied",
            "connection error",
            "timeout",
            "network error",
            "http error",
            "ssl error",
            "certificate error",
            "connection reset",
            # Browser/page errors
            "page closed",
            "browser closed",
            "context closed",
            "target closed",
            "target page",
            "navigation failed",
            # JavaScript navigation errors
            "not found in dropdown",
            "not found in drive dropdown",
            "not found in make dropdown", 
            "not found in model dropdown",
            "not found in trim dropdown",
            "not found in year dropdown",
            "item not found",
            "dropdown not found",
            "element not found",
            "selector not found",
            # Database/session binding errors
            "is not bound to a session",
            "attribute refresh operation cannot proceed",
            "object is not bound to a session",
            "session is closed",
            "session has been closed",
            "detached instance",
            "instance is not persistent",
            # Database constraint errors
            "notnullviolation",
            "integrityerror",
            "databaseerror",
            "operationalerror",
            "programmingerror",
            "database error",
            "constraint violation",
            "foreign key constraint",
            "unique constraint",
            "check constraint"
        ]
        
        error_lower = error_msg.lower()
        
        # Also check for specific exception types that should trigger restart
        if error_type:
            error_type_lower = error_type.lower()
            critical_exception_types = [
                "runtimeerror",
                "valueerror", 
                "connectionerror",
                "timeouterror",
                "sessionexpirederror"
            ]
            if any(exc_type in error_type_lower for exc_type in critical_exception_types):
                return True
        
        return any(indicator in error_lower for indicator in restart_indicators)

    async def restart_browser(self, force_fresh_profile: bool = False) -> bool:
        """Restart the browser and reinitialize the session."""
        try:
            if force_fresh_profile:
                self.logger.info("🔄 Attempting to restart browser with fresh profile...")
            else:
                self.logger.info("🔄 Attempting to restart browser...")
            
            # Mark this as a restart, not initial startup
            self.is_initial_startup = False
            
            # Clean up existing resources
            try:
                if hasattr(self, 'page') and self.page:
                    await self.page.close()
                if hasattr(self, 'context') and self.context:
                    await self.context.close()
                if hasattr(self, 'playwright') and self.playwright:
                    await self.playwright.stop()
            except Exception as cleanup_error:
                self.logger.warning(f"Error during cleanup: {cleanup_error}")
            
            # Reinitialize browser with fresh profile if requested
            await self.setup_browser(force_fresh_profile=force_fresh_profile)
            await self.navigate_to_target()
            await self.wait_for_captcha_resolution()
            await self.wait_for_page_load()
            await self.perform_human_activities()
            
            self.logger.info("✅ Browser restarted successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to restart browser: {e}")
            return False

    async def run_continuous_scraping(self) -> None:
        """Run continuous scraping workflow for this worker instance."""
        await self.run_scraper_with_auto_restart()
    
    async def run_scraper_with_auto_restart(self) -> None:
        """Main scraper execution with automatic browser restart on failures."""
        # Remove restart limits to allow unlimited restarts as requested
        restart_count = 0
        consecutive_failures = 0
        scraper_initialized = False
        
        try:
            # Setup browser
            await self.setup_browser()
            
            # Navigate to target
            await self.navigate_to_target()
            
            # Wait for captcha resolution if present
            await self.wait_for_captcha_resolution()
            
            # Wait for page to fully load after captcha resolution
            await self.wait_for_page_load()
            
            # Perform human activities if new profile
            await self.perform_human_activities()
            
            scraper_initialized = True
            
        except Exception as e:
            self.logger.error(f"Initial scraper setup failed: {e}")
            await self.cleanup()
            return
        
        # Set up console monitoring for completion messages
        completion_detected = False
        
        def handle_console(msg):
            nonlocal completion_detected
            message_text = msg.text
            
            completion_messages = ["[COMPLETE] No more data found.", "[STOP] Year limit is reached."]
            for completion_msg in completion_messages:
                if completion_msg in message_text:
                    self.logger.info(f"Completion detected: {completion_msg}")
                    completion_detected = True
                    return
        
        # Set up console listener
        self.page.on("console", handle_console)
        
        # Start infinite loop for JavaScript workflow with unlimited restart capability
        iteration_count = 0
        
        while not completion_detected:  # Removed restart limit check
            iteration_count += 1
            
            try:
                # Execute JavaScript workflow
                should_continue = await self.javascript_workflow()
                
                if not should_continue:
                    break
                
                # Check if completion was detected via console
                if completion_detected:
                    break
                
                # Reset failure counter on successful iteration
                consecutive_failures = 0
                
                # Brief pause between iterations
                await asyncio.sleep(2)
                
                # Safety check to prevent infinite loops (optional)
                if iteration_count >= 1000:  # Adjust as needed
                    self.logger.warning("Maximum iterations reached - stopping for safety")
                    break
                    
            except Exception as e:
                restart_count += 1
                consecutive_failures += 1
                error_type = type(e).__name__
                self.logger.error(f"JavaScript workflow failed with {error_type}: {e}")
                
                # Always attempt restart (no maximum limit)
                # Calculate backoff delay based on consecutive failures
                backoff_delay = min(30, 5 * consecutive_failures)
                
                # Wait with exponential backoff
                await asyncio.sleep(backoff_delay)
                
                # Clean up browser profile if too many consecutive failures
                if consecutive_failures >= 3:
                    self.logger.warning("Multiple consecutive failures detected - cleaning browser profile")
                    await self.clean_browser_profile()
                
                # Determine if we should force a fresh profile
                force_fresh = consecutive_failures >= 2 or "Target page, context or browser has been closed" in str(e)
                
                # Attempt to restart browser with fresh profile if needed
                restart_success = await self.restart_browser(force_fresh_profile=force_fresh)
                
                if not restart_success:
                    self.logger.error(f"Browser restart failed on attempt {restart_count}")
                    # Additional delay on restart failure
                    await asyncio.sleep(10)
                else:
                    # Re-setup console listener after restart
                    self.page.on("console", handle_console)
                    # Brief pause before resuming
                    await asyncio.sleep(5)
        
        if completion_detected:
            self.logger.info("Scraper completed successfully")
        else:
            self.logger.info("Scraper stopped due to other conditions")
        
        # Final cleanup
        await self.cleanup()

    async def clean_browser_profile(self) -> None:
        """Clean browser profile to resolve persistent issues."""
        try:
            profile_path, _ = self.profile_manager.get_profile_path()
            
            # Remove crash reports
            crashpad_path = os.path.join(profile_path, "Crashpad", "reports")
            if os.path.exists(crashpad_path):
                import shutil
                shutil.rmtree(crashpad_path, ignore_errors=True)
                self.logger.info("Removed crash reports from browser profile")
            
            # Remove lock files
            for lock_file in ["SingletonLock", "SingletonSocket", "SingletonCookie"]:
                lock_path = os.path.join(profile_path, lock_file)
                if os.path.exists(lock_path):
                    try:
                        os.remove(lock_path)
                    except:
                        pass
                        
        except Exception as e:
            self.logger.error(f"Error cleaning browser profile: {e}")

    async def run_scraper_core(self) -> None:
        """Core scraper logic separated for restart functionality."""
        
        # Setup browser
        await self.setup_browser()
        
        # Navigate to target
        await self.navigate_to_target()
        
        # Wait for captcha resolution if present
        await self.wait_for_captcha_resolution()
        
        # Wait for page to fully load after captcha resolution
        await self.wait_for_page_load()
        
        # Perform human activities if new profile
        await self.perform_human_activities()
        
        # Set up console monitoring for completion messages
        completion_detected = False
        
        def handle_console(msg):
            nonlocal completion_detected
            message_text = msg.text
            
            completion_messages = ["[COMPLETE] No more data found.", "[STOP] Year limit is reached."]
            for completion_msg in completion_messages:
                if completion_msg in message_text:
                    self.logger.info(f"Completion detected: {completion_msg}")
                    completion_detected = True
                    return
        
        # Set up console listener
        self.page.on("console", handle_console)
        
        # Start infinite loop for JavaScript workflow
        iteration_count = 0
        
        while not completion_detected:
            iteration_count += 1
            
            try:
                # Execute JavaScript workflow
                should_continue = await self.javascript_workflow()
                
                if not should_continue:
                    break
                
                # Check if completion was detected via console
                if completion_detected:
                    break
                
                # Brief pause between iterations
                await asyncio.sleep(2)
                
                # Safety check to prevent infinite loops (optional)
                if iteration_count >= 1000:  # Adjust as needed
                    self.logger.warning("Maximum iterations reached - stopping for safety")
                    break
                    
            except Exception as e:
                # If javascript_workflow raises an exception, it should trigger restart
                self.logger.error(f"JavaScript workflow failed with exception: {e}")
                raise e  # Re-raise to trigger auto-restart mechanism
    
    async def cleanup(self) -> None:
        """Clean up resources."""
        try:
            # Close database session
            if hasattr(self, 'db_session') and self.db_session:
                self.db_session.close()
            
            if hasattr(self, 'context') and self.context:
                await self.context.close()
            
            if hasattr(self, 'playwright') and self.playwright:
                await self.playwright.stop()
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


async def main():
    """Main function to run multi-browser instances in parallel."""
    from multi_browser_manager import MultiBrowserManager
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    manager = MultiBrowserManager()
    
    try:
        await manager.start_all_workers()
    except KeyboardInterrupt:
        logging.info("Received interrupt signal, stopping all workers...")
        await manager.stop_all_workers()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        await manager.stop_all_workers()
        raise


if __name__ == "__main__":
    asyncio.run(main())