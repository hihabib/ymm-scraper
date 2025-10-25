"""
Custom Wheel Offset Playwright Provider (Modular Orchestrator)
Refactored into smaller modules to keep files under 300 lines.
"""

import asyncio
import logging
import random
from typing import Optional
from playwright.async_api import BrowserContext, Page, Playwright
import json
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
import sys
from pathlib import Path

# Add the src directory to the path for imports
src_path = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(src_path))

from db.db import engine
from core.models import CustomWheelOffsetYMM
from services.repository_optimized import get_last_custom_wheel_offset_ymm
from config.worker import CUSTOM_WHEEL_OFFSET_YMM_SCRAPER_LIMIT

try:
    from .browser_setup import setup_browser as _setup_browser
    from .human_utils import (
        human_type as _human_type,
        human_delay as _human_delay,
        network_delay as _network_delay,
        human_mouse_movement as _human_mouse_movement,
        human_scroll as _human_scroll,
    )
    from .navigation import (
        visit_google_and_search as _visit_google_and_search,
        visit_wikipedia as _visit_wikipedia,
        visit_random_sites as _visit_random_sites,
        navigate_to_wheels_page as _navigate_to_wheels_page,
        navigate_to_dynamic_url as _navigate_to_dynamic_url,
    )
except ImportError:
    from browser_setup import setup_browser as _setup_browser
    from human_utils import (
        human_type as _human_type,
        human_delay as _human_delay,
        network_delay as _network_delay,
        human_mouse_movement as _human_mouse_movement,
        human_scroll as _human_scroll,
    )
    from navigation import (
        visit_google_and_search as _visit_google_and_search,
        visit_wikipedia as _visit_wikipedia,
        visit_random_sites as _visit_random_sites,
        navigate_to_wheels_page as _navigate_to_wheels_page,
        navigate_to_dynamic_url as _navigate_to_dynamic_url,
    )


class CustomWheelOffsetPlaywright:
    """
    Orchestrator class that delegates to modular helpers for browser setup,
    human-like behaviors, navigation, and cleanup.
    """

    def __init__(self, profile_name: str = "default", assigned_records: list = None):
        self.playwright: Optional[Playwright] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.profile_name = profile_name
        self.is_existing_profile = False
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Store assigned records and URLs for this instance
        self.assigned_records = assigned_records or []

    # Browser lifecycle
    async def setup_browser(self) -> None:
        """Initialize persistent Playwright context with enhanced stealth."""
        pw, context, page, is_existing = await _setup_browser(self.profile_name, self.logger)
        self.playwright, self.context, self.page, self.is_existing_profile = pw, context, page, is_existing

    async def cleanup(self, keep_profile: bool = True) -> None:
        """Clean up browser resources while optionally preserving profile data."""
        try:
            self.logger.info("Cleaning up browser resources...")
            if self.page:
                await self.page.close()
                self.page = None
            if self.context:
                await self.context.close()
                self.context = None
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
            if keep_profile:
                self.logger.info(f"Persistent profile data preserved for profile: {self.profile_name}")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    # Human-like helpers (wrappers)
    async def human_type(self, element, text: str, typing_speed: str = "normal") -> None:
        await _human_type(self.page, element, text, typing_speed)

    async def human_delay(self, min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
        await _human_delay(min_seconds, max_seconds)

    async def network_delay(self, request_type: str = "normal") -> None:
        await _network_delay(request_type)

    async def human_mouse_movement(self, page: Optional[Page] = None) -> None:
        await _human_mouse_movement(page or self.page)

    async def human_scroll(self, page: Optional[Page] = None) -> None:
        await _human_scroll(page or self.page)

    # Navigation flows (wrappers)
    async def visit_google_and_search(self) -> None:
        await _visit_google_and_search(self.page, self.logger)

    async def visit_wikipedia(self) -> None:
        await _visit_wikipedia(self.page, self.logger)

    async def visit_random_sites(self) -> None:
        await _visit_random_sites(self.page, self.logger)

    async def navigate_to_wheels_page(self) -> None:
        """Navigate to the wheels page (for preparation worker compatibility)."""
        if not self.page:
            raise RuntimeError("Browser not initialized. Call setup_browser() first.")
        await _navigate_to_wheels_page(self.page, self.logger)
    
    async def navigate_to_dynamic_url(self, url: str) -> None:
        """Navigate to a dynamically generated URL."""
        if not self.page:
            raise RuntimeError("Browser not initialized. Call setup_browser() first.")
        await _navigate_to_dynamic_url(self.page, self.logger, url)

    @contextmanager
    def get_db_session(self):
        """Get a database session with proper cleanup."""
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def parse_console_log_for_json(self, log_message: str) -> dict:
        """
        Parse console log message to extract JSON object.
        Handles both valid JSON and JavaScript object notation.
        Returns None if no valid JSON is found.
        """
        try:
            # Look for JSON-like patterns in the log message
            # Console logs might have prefixes like "console.log:" or timestamps
            message = log_message.strip()
            
            # Try to find JSON object in the message
            start_idx = message.find('{')
            end_idx = message.rfind('}')
            
            if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                json_str = message[start_idx:end_idx + 1]
                
                try:
                    # First try to parse as valid JSON
                    parsed_obj = json.loads(json_str)
                except json.JSONDecodeError:
                    # If JSON parsing fails, try to convert JS object notation to JSON
                    import re
                    
                    # Convert JavaScript object notation to JSON
                    # Replace unquoted property names with quoted ones
                    json_text = re.sub(r'(\w+):', r'"\1":', json_str)
                    
                    # Handle unquoted string values (but be careful not to quote numbers)
                    # This regex looks for values that are not numbers, booleans, or already quoted
                    json_text = re.sub(r':\s*([^"\d,\{\}\[\]][^,\}]*)', r': "\1"', json_text)
                    
                    # Clean up any trailing spaces in quoted values
                    json_text = re.sub(r'"\s*([^"]*?)\s*"', r'"\1"', json_text)
                    
                    parsed_obj = json.loads(json_text)
                
                # Validate that this looks like our expected object structure
                # Make validation more flexible - check for basic required fields
                basic_fields = ['year', 'make', 'model']
                if all(field in parsed_obj for field in basic_fields):
                    return parsed_obj
                    
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            # Not a valid JSON object or not our expected structure
            self.logger.debug(f"Failed to parse console log as JSON: {log_message}, error: {e}")
            
        return None

    def save_objects_to_database(self, objects_list: list) -> int:
        """
        Save collected objects to the CustomWheelOffsetYMM table.
        Returns the number of records saved.
        """
        if not objects_list:
            return 0
            
        saved_count = 0
        try:
            with self.get_db_session() as session:
                for obj in objects_list:
                    # Create new record with processed=False (0)
                    record = CustomWheelOffsetYMM(
                        year=obj.get('year', ''),
                        make=obj.get('make', ''),
                        model=obj.get('model', ''),
                        trim=obj.get('trim', ''),
                        drive=obj.get('drive', ''),
                        vehicle_type=obj.get('vehicle_type', ''),
                        dr_chassis_id=obj.get('dr_chassis_id', ''),
                        bolt_pattern=obj.get('bolt_pattern'),  # Optional field
                        suspension=obj.get('suspension'),      # Optional field
                        modification=obj.get('modification'),  # Optional field
                        rubbing=obj.get('rubbing'),           # Optional field
                        processed=False  # Set to False as requested
                    )
                    session.add(record)
                    saved_count += 1
                    
                self.logger.info(f"Successfully saved {saved_count} records to database")
                
        except Exception as e:
            self.logger.error(f"Error saving objects to database: {e}")
            raise
            
        return saved_count

    async def run_preparation_instance_with_monitoring(self) -> bool:
        """
        Run a single preparation instance that monitors for 'No more data left' message.
        Returns True if 'No more data left' is detected, False otherwise.
        """
        try:
            self.logger.info(f"Starting preparation instance with monitoring for {self.profile_name}")
            await self.setup_browser()
            self.logger.info(f"Preparation browser setup completed for {self.profile_name}")
            
            # Navigate directly to target site (no 20-second wait)
            self.logger.info("Preparation instance - navigating directly to target site")
            await self.navigate_to_wheels_page()
            
            # Inject the combination.js script
            self.logger.info("Injecting combination.js script...")
            js_file_path = Path(__file__).parent / "js" / "combination.js"
            with open(js_file_path, 'r', encoding='utf-8') as f:
                js_content = f.read()
            
            await self.page.evaluate(js_content)
            self.logger.info("JavaScript injection completed")
            
            # Set up console log monitoring
            console_logs = []
            collected_objects = []
            no_more_data_detected = False
            
            def handle_console(msg):
                nonlocal no_more_data_detected
                log_text = msg.text
                console_logs.append(log_text)
                self.logger.info(f"Console log: {log_text}")  # Debug: log all console messages
                
                # Check for 'No more data left' message
                if "No more data left" in log_text:
                    self.logger.info("Detected 'No more data left' message - no more data available")
                    no_more_data_detected = True
                    return
                
                # Try to parse JSON objects from console logs
                parsed_obj = self.parse_console_log_for_json(log_text)
                if parsed_obj:
                    collected_objects.append(parsed_obj)
                    self.logger.info(f"Captured object: {parsed_obj.get('year', '')} {parsed_obj.get('make', '')} {parsed_obj.get('model', '')} {parsed_obj.get('trim', '')} {parsed_obj.get('drive', '')} {parsed_obj.get('vehicle_type', '')} {parsed_obj.get('dr_chassis_id', '')}")
            
            self.page.on("console", handle_console)
            
            # Execute the scraping command
            self.logger.info("Executing startScraping with vehicle data...")
            
            # Get the last inserted YMM record to use as starting point
            last_record = get_last_custom_wheel_offset_ymm()
            if last_record:
                starts_data = {
                    "starts": {
                        "year": last_record.year,
                        "make": last_record.make,
                        "model": last_record.model,
                        "trim": last_record.trim,
                        "drive": last_record.drive
                    }
                }
                starts_json = json.dumps(starts_data)
                self.logger.info(f"Using starting data: {starts_json}")
                await self.page.evaluate(f"startScraping({starts_json}, false, {CUSTOM_WHEEL_OFFSET_YMM_SCRAPER_LIMIT});")
            else:
                self.logger.warning("No last record found, using empty object")
                await self.page.evaluate(f"startScraping({{}}, false, {CUSTOM_WHEEL_OFFSET_YMM_SCRAPER_LIMIT});")
            
            # Monitor for completion messages
            self.logger.info("Monitoring console logs for completion and collecting objects...")
            max_wait_time = 300  # 5 minutes max
            check_interval = 2   # Check every 2 seconds
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                await asyncio.sleep(check_interval)
                elapsed_time += check_interval
                
                # Check if 'No more data left' was detected
                if no_more_data_detected:
                    self.logger.info("No more data left detected - returning True")
                    return True
                
                # Check if we've reached the limit
                for log in console_logs:
                    if f"[LIMIT] Reached limit of {CUSTOM_WHEEL_OFFSET_YMM_SCRAPER_LIMIT} " in log:
                        self.logger.info(f"Preparation instance completed - limit reached! Collected {len(collected_objects)} objects")
                        
                        # Save all collected objects to database before closing
                        if collected_objects:
                            saved_count = self.save_objects_to_database(collected_objects)
                            self.logger.info(f"Saved {saved_count} objects to database")
                        else:
                            self.logger.warning("No objects were collected from console logs")
                        
                        return False  # Limit reached but more data may be available
                        
                # Also check for any error messages
                for log in console_logs:
                    if "[ERROR]" in log or "[FATAL]" in log:
                        self.logger.warning(f"Error detected in preparation: {log}")
                        
                # Log progress every 30 seconds
                if elapsed_time % 30 == 0:
                    self.logger.info(f"Preparation running... Collected {len(collected_objects)} objects so far")
            
            # Timeout reached
            self.logger.warning("Preparation instance timed out")
            if collected_objects:
                saved_count = self.save_objects_to_database(collected_objects)
                self.logger.info(f"Saved {saved_count} objects to database before timeout")
            
            return False  # Timeout, assume more data may be available
            
        except Exception as e:
            self.logger.error(f"Error in preparation instance with monitoring: {e}")
            return False
        finally:
            try:
                await self.cleanup()
            except Exception as cleanup_error:
                self.logger.error(f"Error during cleanup: {cleanup_error}")

    async def run_preparation_instance(self) -> None:
        """
        Run a single preparation instance that:
        1. Launches Chrome browser
        2. Navigates to target page
        3. Injects and executes combination.js
        4. Captures console logs containing JSON objects
        5. Saves collected data to database
        6. Closes when limit message appears
        """
        try:
            self.logger.info(f"Starting preparation instance for {self.profile_name}")
            await self.setup_browser()
            self.logger.info(f"Preparation browser setup completed for {self.profile_name}")
            
            # Navigate directly to target site (no 20-second wait)
            self.logger.info("Preparation instance - navigating directly to target site")
            await self.navigate_to_wheels_page()
            
            # Inject the combination.js script
            self.logger.info("Injecting combination.js script...")
            js_file_path = Path(__file__).parent / "js" / "combination.js"
            with open(js_file_path, 'r', encoding='utf-8') as f:
                js_content = f.read()
            
            await self.page.evaluate(js_content)
            self.logger.info("JavaScript injection completed")
            
            # Set up console log monitoring
            console_logs = []
            collected_objects = []
            
            def handle_console(msg):
                log_text = msg.text
                console_logs.append(log_text)
                self.logger.info(f"Console log: {log_text}")  # Debug: log all console messages
                
                # Try to parse JSON objects from console logs
                parsed_obj = self.parse_console_log_for_json(log_text)
                if parsed_obj:
                    collected_objects.append(parsed_obj)
                    self.logger.info(f"Captured object: {parsed_obj.get('year', '')} {parsed_obj.get('make', '')} {parsed_obj.get('model', '')} {parsed_obj.get('trim', '')} {parsed_obj.get('drive', '')} {parsed_obj.get('vehicle_type', '')} {parsed_obj.get('dr_chassis_id', '')}")
            
            self.page.on("console", handle_console)
            
            # Execute the scraping command
            self.logger.info("Executing startScraping with vehicle data...")
            
            # Get the last inserted YMM record to use as starting point
            last_record = get_last_custom_wheel_offset_ymm()
            if last_record:
                starts_data = {
                    "starts": {
                        "year": last_record.year,
                        "make": last_record.make,
                        "model": last_record.model,
                        "trim": last_record.trim,
                        "drive": last_record.drive
                    }
                }
                starts_json = json.dumps(starts_data)
                self.logger.info(f"Using starting data: {starts_json}")
                await self.page.evaluate(f"startScraping({starts_json}, false, {CUSTOM_WHEEL_OFFSET_YMM_SCRAPER_LIMIT});")
            else:
                self.logger.warning("No last record found, using empty object")
                await self.page.evaluate(f"startScraping({{}}, false, {CUSTOM_WHEEL_OFFSET_YMM_SCRAPER_LIMIT});")
            
            # Monitor for the completion message
            self.logger.info("Monitoring console logs for completion and collecting objects...")
            max_wait_time = 300  # 5 minutes max
            check_interval = 2   # Check every 2 seconds
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                await asyncio.sleep(check_interval)
                elapsed_time += check_interval
                
                # Check if we've reached the limit
                for log in console_logs:
                    if f"[LIMIT] Reached limit of {CUSTOM_WHEEL_OFFSET_YMM_SCRAPER_LIMIT} " in log:
                        self.logger.info(f"Preparation instance completed - limit reached! Collected {len(collected_objects)} objects")
                        
                        # Save all collected objects to database before closing
                        if collected_objects:
                            saved_count = self.save_objects_to_database(collected_objects)
                            self.logger.info(f"Saved {saved_count} objects to database")
                        else:
                            self.logger.warning("No objects were collected from console logs")
                        
                        return
                        
                # Also check for any error messages
                for log in console_logs:
                    if "[ERROR]" in log or "[FATAL]" in log:
                        self.logger.warning(f"Error detected in preparation: {log}")
                        
                # Log progress every 30 seconds
                if elapsed_time % 30 == 0:
                    self.logger.info(f"Preparation running... Collected {len(collected_objects)} objects so far")
            
            self.logger.warning(f"Preparation instance timed out after 5 minutes. Collected {len(collected_objects)} objects")
            
            # Save collected objects even if timed out
            if collected_objects:
                saved_count = self.save_objects_to_database(collected_objects)
                self.logger.info(f"Saved {saved_count} objects to database before timeout")
            
        except Exception as e:
            self.logger.error(f"Error in preparation instance for {self.profile_name}: {e}")
            raise
        finally:
            self.logger.info(f"Closing preparation instance for {self.profile_name}")
            await self.cleanup()

    async def run(self) -> None:
        """
        Main execution method for browser instances with assigned records.
        Navigates to dynamic URLs based on assigned database records and extracts fitment data.
        """
        try:
            self.logger.info(f"Starting run method for {self.profile_name}")
            await self.setup_browser()
            
            # Check if we have assigned records to process
            if not self.assigned_records:
                self.logger.warning(f"No assigned records for {self.profile_name} - skipping execution")
                return
            
            self.logger.info(f"{self.profile_name} has {len(self.assigned_records)} assigned records to process")
            
            # Initialize fitment extractor and database handler
            try:
                from .fitment_extractor import FitmentDataExtractor
                from .database_handler import FitmentDatabaseHandler
            except ImportError:
                from fitment_extractor import FitmentDataExtractor
                from database_handler import FitmentDatabaseHandler
            
            extractor = FitmentDataExtractor(self.logger)
            db_handler = FitmentDatabaseHandler(self.logger)
            
            # Process each assigned record
            for i, (record, url) in enumerate(self.assigned_records):
                self.logger.info(f"{self.profile_name} processing record {i+1}/{len(self.assigned_records)}: ID {record.id}")
                
                try:
                    # Navigate to the dynamic URL for this record
                    await self.navigate_to_dynamic_url(url)
                    self.logger.info(f"Successfully navigated to URL for record {record.id}")
                    
                    # Extract fitment data using extract_fitment.js
                    self.logger.info(f"Extracting fitment data for record {record.id}...")
                    raw_fitment_data = await extractor.extract_fitment_data(self.page, record.id)
                    
                    if raw_fitment_data:
                        # Parse and clean the fitment data
                        cleaned_fitment_data = extractor.parse_fitment_values(raw_fitment_data, record.id)
                        
                        if cleaned_fitment_data:
                            # Save to database
                            inserted_count = db_handler.insert_fitment_data(record.id, cleaned_fitment_data, record.id)
                            
                            if inserted_count > 0:
                                # Mark record as processed
                                db_handler.mark_record_as_processed(record.id)
                                self.logger.info(f"Successfully processed record {record.id}: {inserted_count} fitment records saved")
                            else:
                                self.logger.warning(f"No fitment data saved for record {record.id}")
                        else:
                            self.logger.warning(f"No valid fitment data extracted for record {record.id}")
                    else:
                        self.logger.warning(f"Failed to extract fitment data for record {record.id}")
                        
                except Exception as record_error:
                    self.logger.error(f"Error processing record {record.id}: {record_error}")
                    # Continue with next record instead of failing completely
                    continue
            
            self.logger.info(f"Completed processing all assigned records for {self.profile_name}")
            
        except Exception as e:
            self.logger.error(f"Error in run method for {self.profile_name}: {e}")
            raise
        finally:
            self.logger.info(f"Entering cleanup for {self.profile_name}")
            await self.cleanup()


async def main():
    scraper = CustomWheelOffsetPlaywright()
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())