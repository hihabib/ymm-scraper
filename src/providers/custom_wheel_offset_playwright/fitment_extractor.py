import asyncio
import json
import logging
import os
from typing import Dict, Any, Optional, Tuple
from playwright.async_api import Page


class FitmentDataExtractor:
    """Handles extraction of fitment data using extract_fitment.js and console monitoring."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.extract_js_path = os.path.join(
            os.path.dirname(__file__), 
            "js", "extract_fitment.js"
        )
    
    async def extract_fitment_data(self, page: Page, record_id: int) -> Optional[Dict[str, Any]]:
        """
        Execute extract_fitment.js and capture JSON output from console.
        
        Args:
            page: Playwright page instance
            record_id: Database record ID for logging
            
        Returns:
            Parsed JSON data or None if extraction failed
        """
        try:
            self.logger.info(f"Starting fitment data extraction for record {record_id}")
            
            # Set up console monitoring
            console_output = []
            
            def handle_console(msg):
                if msg.type == 'log':
                    console_output.append(msg.text)
                    self.logger.debug(f"Console output for record {record_id}: {msg.text}")
            
            page.on('console', handle_console)
            
            try:
                # Read and execute the JavaScript file
                if not os.path.exists(self.extract_js_path):
                    self.logger.error(f"extract_fitment.js not found at {self.extract_js_path}")
                    return None
                
                with open(self.extract_js_path, 'r', encoding='utf-8') as f:
                    js_content = f.read()
                
                self.logger.info(f"Executing extract_fitment.js for record {record_id}")
                
                # Execute the JavaScript - remove the console.log line
                js_content_without_console = js_content.replace('\nconsole.log(JSON.stringify(extractFitmentData()))', '')
                
                # Execute the JavaScript function definition and call it in one evaluation
                js_code = f"(function() {{ {js_content_without_console}; return JSON.stringify(extractFitmentData()); }})()"
                result_json = await page.evaluate(js_code)
                
                # Parse the JSON result
                import json
                result = json.loads(result_json) if result_json else None
                
                # Log the result to console for capture
                await page.evaluate('(result) => console.log(result)', result_json)
                
                # Wait a moment for the script to complete and output to console
                await asyncio.sleep(2)
                
                # Look for JSON output in console
                json_data = self._find_json_in_console(console_output, record_id)
                
                if json_data:
                    self.logger.info(f"Successfully extracted fitment data for record {record_id}")
                    return json_data
                else:
                    self.logger.warning(f"No valid JSON found in console output for record {record_id}")
                    return None
                    
            finally:
                # Remove console listener
                page.remove_listener('console', handle_console)
                
        except Exception as e:
            self.logger.error(f"Error extracting fitment data for record {record_id}: {e}")
            return None
    
    def _find_json_in_console(self, console_output: list, record_id: int) -> Optional[Dict[str, Any]]:
        """
        Search console output for valid JSON string and parse it.
        
        Args:
            console_output: List of console messages
            record_id: Database record ID for logging
            
        Returns:
            Parsed JSON data or None if not found
        """
        for output in console_output:
            try:
                # Try to parse each console message as JSON
                if output.strip().startswith('{') and output.strip().endswith('}'):
                    parsed_data = json.loads(output.strip())
                    
                    # Validate that it contains expected fitment data structure
                    if self._validate_fitment_data(parsed_data, record_id):
                        return parsed_data
                        
            except json.JSONDecodeError:
                continue
        
        return None
    
    def _validate_fitment_data(self, data: Dict[str, Any], record_id: int) -> bool:
        """
        Validate that the parsed JSON contains expected fitment data structure.
        
        Args:
            data: Parsed JSON data
            record_id: Database record ID for logging
            
        Returns:
            True if data is valid, False otherwise
        """
        if not isinstance(data, dict):
            return False
        
        # Check if at least 'front' or 'rear' exists
        if 'front' not in data and 'rear' not in data:
            self.logger.debug(f"Record {record_id}: No 'front' or 'rear' data found")
            return False
        
        # Validate each position that exists
        for position in ['front', 'rear']:
            if position in data:
                pos_data = data[position]
                if not isinstance(pos_data, dict):
                    self.logger.debug(f"Record {record_id}: Invalid {position} data structure")
                    return False
                
                # Check required fields
                required_fields = ['diameter', 'width', 'offset']
                for field in required_fields:
                    if field not in pos_data:
                        self.logger.debug(f"Record {record_id}: Missing {field} in {position} data")
                        return False
                    
                    field_data = pos_data[field]
                    if not isinstance(field_data, dict) or 'min' not in field_data or 'max' not in field_data:
                        self.logger.debug(f"Record {record_id}: Invalid {field} structure in {position} data")
                        return False
        
        self.logger.debug(f"Record {record_id}: Fitment data validation passed")
        return True
    
    def parse_fitment_values(self, data: Dict[str, Any], record_id: int) -> Dict[str, Any]:
        """
        Parse and clean fitment values from the extracted data.
        
        Args:
            data: Raw fitment data from JavaScript
            record_id: Database record ID for logging
            
        Returns:
            Cleaned data ready for database insertion
        """
        cleaned_data = {}
        
        for position in ['front', 'rear']:
            if position in data:
                pos_data = data[position]
                cleaned_pos_data = {}
                
                # Parse diameter (remove quotes and convert to int)
                diameter = pos_data.get('diameter', {})
                cleaned_pos_data['diameter_min'] = self._parse_numeric_value(diameter.get('min'), 'diameter_min', record_id)
                cleaned_pos_data['diameter_max'] = self._parse_numeric_value(diameter.get('max'), 'diameter_max', record_id)
                
                # Parse width (remove quotes and convert to float)
                width = pos_data.get('width', {})
                cleaned_pos_data['width_min'] = self._parse_numeric_value(width.get('min'), 'width_min', record_id, is_float=True)
                cleaned_pos_data['width_max'] = self._parse_numeric_value(width.get('max'), 'width_max', record_id, is_float=True)
                
                # Parse offset (remove 'mm' and convert to int)
                offset = pos_data.get('offset', {})
                cleaned_pos_data['offset_min'] = self._parse_numeric_value(offset.get('min'), 'offset_min', record_id, remove_suffix='mm')
                cleaned_pos_data['offset_max'] = self._parse_numeric_value(offset.get('max'), 'offset_max', record_id, remove_suffix='mm')
                
                # Only include if all values are valid
                if all(v is not None for v in cleaned_pos_data.values()):
                    cleaned_data[position] = cleaned_pos_data
                else:
                    self.logger.warning(f"Record {record_id}: Incomplete {position} data, skipping")
        
        return cleaned_data
    
    def _parse_numeric_value(self, value: str, field_name: str, record_id: int, 
                           is_float: bool = False, remove_suffix: str = None) -> Optional[float]:
        """
        Parse a numeric value from string, handling various formats.
        
        Args:
            value: String value to parse
            field_name: Field name for logging
            record_id: Record ID for logging
            is_float: Whether to return float instead of int
            remove_suffix: Suffix to remove (e.g., 'mm')
            
        Returns:
            Parsed numeric value or None if parsing failed
        """
        if not value:
            return None
        
        try:
            # Clean the value
            cleaned = str(value).strip()
            
            # Remove quotes (both single and double)
            if (cleaned.startswith('"') and cleaned.endswith('"')) or (cleaned.startswith("'") and cleaned.endswith("'")):
                cleaned = cleaned[1:-1]
            
            # Remove trailing quote character if present
            if cleaned.endswith('"'):
                cleaned = cleaned[:-1]
            
            # Remove suffix if specified
            if remove_suffix and cleaned.endswith(remove_suffix):
                cleaned = cleaned[:-len(remove_suffix)]
            
            # Parse as number
            if is_float:
                return float(cleaned)
            else:
                return int(float(cleaned))  # Convert to float first to handle decimal strings
                
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Record {record_id}: Failed to parse {field_name} value '{value}': {e}")
            return None