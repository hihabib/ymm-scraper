from bs4 import BeautifulSoup
from typing import List, Optional
from urllib.parse import urlencode
import logging
import re
import asyncio
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# Import the HTTP client with DNS rotation support (kept for fallback)
from core.http import fetch_with_dns_rotation
from config.proxy import COOKIE_STRING


class VehicleDataExtractor:
    """
    Utility class to extract vehicle information from HTML select elements.
    Fetches data from customwheeloffset.com using Playwright browser automation.
    Uses comprehensive browser-like headers and behavior to exactly mimic Chrome browser requests.
    """
    
    def __init__(self, year: str = "", make: str = "", model: str = "", trim: str = ""):
        """
        Initialize the VehicleDataExtractor with optional vehicle parameters.
        
        Args:
            year: Vehicle year (optional)
            make: Vehicle make (optional)
            model: Vehicle model (optional)
            trim: Vehicle trim (optional)
        """
        self.year = year
        self.make = make
        self.model = model
        self.trim = trim
        self.soup = None
        self.logger = logging.getLogger(__name__)
        self.session_cookies = {}  # Store session cookies including PHPSESSID
        self.browser = None
        self.context = None
        self.page = None
        self._initialized = False
        
    async def initialize(self):
        """
        Initialize the extractor by fetching and parsing HTML data.
        This must be called before using any extraction methods.
        """
        if not self._initialized:
            html_data = await self._fetch_html_data_async()
            self.soup = BeautifulSoup(html_data, 'html.parser')
            self._initialized = True
    
    def _ensure_initialized(self):
        """
        Ensure the extractor is initialized. If not, raise an error for async context.
        """
        if not self._initialized:
            raise RuntimeError(
                "VehicleDataExtractor is not initialized. "
                "Please call 'await extractor.initialize()' before using extraction methods."
            )
    
    async def _setup_browser(self) -> None:
        """
        Set up Playwright browser with anti-bot detection measures.
        """
        if self.browser is None:
            playwright = await async_playwright().start()
            
            # Launch browser with realistic settings to avoid detection
            self.browser = await playwright.chromium.launch(
                headless=False,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--window-size=1920,1080',
                    '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                ]
            )
            
            # Create context with realistic settings
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                extra_http_headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Cache-Control': 'max-age=0'
                }
            )
            
            # Create page
            self.page = await self.context.new_page()
            
            # Add stealth measures
            await self.page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });
                
                window.chrome = {
                    runtime: {},
                };
            """)
    
    async def _cleanup_browser(self) -> None:
        """
        Clean up browser resources.
        """
        if self.page:
            await self.page.close()
            self.page = None
        if self.context:
            await self.context.close()
            self.context = None
        if self.browser:
            await self.browser.close()
            self.browser = None
    
    def __del__(self):
        """
        Cleanup browser resources when object is destroyed.
        """
        if self.browser:
            try:
                asyncio.run(self._cleanup_browser())
            except Exception:
                pass  # Ignore cleanup errors during destruction
    
    def _has_phpsessid(self) -> bool:
        """
        Check if PHPSESSID exists in current session cookies.
        
        Returns:
            True if PHPSESSID exists, False otherwise
        """
        return 'PHPSESSID' in self.session_cookies and self.session_cookies['PHPSESSID']
    
    async def _fetch_phpsessid_async(self) -> Optional[str]:
        """
        Fetch PHPSESSID from the ymm-temp.php endpoint using Playwright browser.
        Makes a browser navigation to get a fresh PHPSESSID from cookies.
        
        Returns:
            PHPSESSID value if successful, None otherwise
        """
        phpsessid_url = "https://www.customwheeloffset.com/api/ymm-temp.php?store=wheels&type=set&vehicle_type=truck&year=2023&make=Ford&model=Bronco%20Sport&trim=Big%20Bend&drive=AWD&chassis=90666"
        
        self.logger.info(f"Fetching PHPSESSID from: {phpsessid_url}")
        
        try:
            await self._setup_browser()
            
            # Navigate to the PHPSESSID endpoint
            response = await self.page.goto(phpsessid_url, wait_until='networkidle')
            
            if response and response.status == 200:
                # Get cookies from the browser context
                cookies = await self.context.cookies()
                
                # Find PHPSESSID cookie
                for cookie in cookies:
                    if cookie['name'] == 'PHPSESSID':
                        phpsessid = cookie['value']
                        self.logger.info(f"Successfully fetched PHPSESSID: {phpsessid}")
                        return phpsessid
                
                self.logger.warning("No PHPSESSID found in browser cookies")
                return None
            else:
                self.logger.error(f"Failed to load PHPSESSID endpoint, status: {response.status if response else 'No response'}")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to fetch PHPSESSID with browser: {e}")
            return None
    
    async def _fetch_html_data_async(self) -> str:
        """
        Fetch HTML data using Playwright with enhanced anti-bot measures.
        
        Returns:
            HTML content as string
            
        Raises:
            Exception: If fetching fails after all retry attempts
        """
        # Check if we have PHPSESSID, if not, fetch it first
        if not self._has_phpsessid():
            self.logger.info("No PHPSESSID found, fetching from ymm-temp.php endpoint")
            phpsessid = await self._fetch_phpsessid_async()
            if phpsessid:
                self.session_cookies['PHPSESSID'] = phpsessid
                self.logger.info(f"Successfully obtained PHPSESSID: {phpsessid}")
            else:
                self.logger.warning("Failed to obtain PHPSESSID, proceeding without it")
        else:
            self.logger.info(f"Using existing PHPSESSID: {self.session_cookies['PHPSESSID']}")
        
        # Build the API endpoint URL
        base_url = "https://www.customwheeloffset.com/makemodel/bp.php"
        
        # Build query parameters (only include non-empty values)
        params = {}
        if self.year:
            params['year'] = self.year
        if self.make:
            params['make'] = self.make
        if self.model:
            params['model'] = self.model
        if self.trim:
            params['trim'] = self.trim
        
        # Construct the full URL
        if params:
            url = f"{base_url}?{urlencode(params)}"
        else:
            url = base_url
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Fetching data from: {url} (attempt {attempt + 1}/{max_retries})")
                
                await self._setup_browser()
                
                # Set cookies if we have PHPSESSID
                if self._has_phpsessid():
                    await self.context.add_cookies([{
                        'name': 'PHPSESSID',
                        'value': self.session_cookies['PHPSESSID'],
                        'domain': '.customwheeloffset.com',
                        'path': '/'
                    }])
                
                # Add random delay to appear more human-like
                import random
                await asyncio.sleep(random.uniform(1, 3))
                
                # Navigate to the URL with extended timeout
                response = await self.page.goto(
                    url, 
                    wait_until='networkidle',
                    timeout=60000
                )
                
                if response and response.status == 200:
                    # Get the HTML content from the page
                    html_content = await self.page.content()
                    
                    if html_content and len(html_content) > 1000:  # Basic validation
                        self.logger.info(f"Successfully fetched vehicle data ({len(html_content)} characters)")
                        
                        # Clean up browser resources after successful fetch
                        await self._cleanup_browser()
                        
                        return html_content
                    else:
                        raise Exception("Received empty or invalid HTML content")
                        
                elif response:
                    self.logger.warning(f"Received status {response.status} from server")
                    if response.status == 403:
                        # Try with different approach for 403 errors
                        await self._handle_403_error()
                        continue
                    elif response.status >= 500:
                        # Server error, retry
                        await asyncio.sleep(retry_delay * (attempt + 1))
                        continue
                    else:
                        raise Exception(f"HTTP {response.status} error")
                else:
                    raise Exception("No response received from server")
                    
            except Exception as e:
                self.logger.error(f"Attempt {attempt + 1} failed: {e}")
                await self._cleanup_browser()
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to fetch vehicle data from {url}: {e}")
                
                # Wait before retry with exponential backoff
                await asyncio.sleep(retry_delay * (2 ** attempt))
        
        raise Exception(f"Failed to fetch vehicle data after {max_retries} attempts")
    
    async def _handle_403_error(self):
        """
        Handle 403 Forbidden errors by implementing additional anti-bot measures.
        """
        try:
            # Clear existing page and create new one
            await self.page.close()
            self.page = await self.context.new_page()
            
            # Add more aggressive stealth measures
            await self.page.add_init_script("""
                // Override webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Mock plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [
                        {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer'},
                        {name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai'},
                        {name: 'Native Client', filename: 'internal-nacl-plugin'}
                    ],
                });
                
                // Mock languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });
                
                // Mock chrome object
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                };
                
                // Mock permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """);
            
            # Add random mouse movements to appear more human
            await self.page.mouse.move(100, 100)
            await asyncio.sleep(0.5)
            await self.page.mouse.move(200, 200)
            
        except Exception as e:
            self.logger.warning(f"Failed to apply 403 error handling: {e}")
    
    def get_years(self) -> List[str]:
        """
        Extract all vehicle years from the year select element.
        
        Returns:
            List of year values (excluding empty placeholder)
        """
        self._ensure_initialized()
        year_select = self.soup.find('select', {'id': 'year', 'name': 'year'})
        if not year_select:
            return []
        
        years = []
        for option in year_select.find_all('option'):
            value = option.get('value', '').strip()
            if value:  # Skip empty placeholder option
                years.append(value)
        
        return years
    
    def get_models(self) -> List[str]:
        """
        Extract all vehicle models or makes from the appropriate select element.
        
        When only year is provided, returns makes from the make select element.
        When year and make are provided, returns models from the model select element.
        
        Returns:
            List of model/make values (excluding empty placeholder)
        """
        self._ensure_initialized()
        # If only year is provided, return makes
        if self.year and not self.make:
            make_select = self.soup.find('select', {'id': 'make', 'name': 'make'})
            if not make_select:
                return []
            
            makes = []
            for option in make_select.find_all('option'):
                value = option.get('value', '').strip()
                if value:  # Skip empty placeholder option
                    makes.append(value)
            
            return makes
        
        # If year and make are provided, return models
        model_select = self.soup.find('select', {'id': 'model', 'name': 'model'})
        if not model_select:
            return []
        
        models = []
        for option in model_select.find_all('option'):
            value = option.get('value', '').strip()
            if value:  # Skip empty placeholder option
                models.append(value)
        
        return models
    
    def get_trims(self) -> List[str]:
        """
        Extract all vehicle trims from the trim select element.
        
        Returns:
            List of trim values (excluding empty placeholder)
        """
        self._ensure_initialized()
        trim_select = self.soup.find('select', {'id': 'trim', 'name': 'trim'})
        if not trim_select:
            return []
        
        trims = []
        for option in trim_select.find_all('option'):
            value = option.get('value', '').strip()
            if value:  # Skip empty placeholder option
                trims.append(value)
        
        return trims
    
    def get_drives(self) -> List[str]:
        """
        Extract all vehicle drive trains from the drive select element.
        
        Returns:
            List of drive train values (excluding empty placeholder)
        """
        self._ensure_initialized()
        drive_select = self.soup.find('select', {'id': 'drive', 'name': 'drive'})
        if not drive_select:
            return []
        
        drives = []
        for option in drive_select.find_all('option'):
            value = option.get('value', '').strip()
            if value:  # Skip empty placeholder option
                drives.append(value)
        
        return drives

