"""
Data format parsers for Schema Inference Plugin
"""

import csv
import json
import re
from abc import ABC, abstractmethod
from io import StringIO
from typing import Any, Dict, List, Optional, Union

from ..utils.exceptions import FormatDetectionError
from ..utils.logger import get_logger


class BaseParser(ABC):
    """Base class for data format parsers."""
    
    def __init__(self):
        """Initialize parser."""
        self.logger = get_logger(__name__)
    
    @abstractmethod
    def parse(self, message: bytes) -> Optional[Dict[str, Any]]:
        """
        Parse a message into a dictionary.
        
        Args:
            message: Raw message bytes
            
        Returns:
            Parsed data as dictionary or None if parsing fails
        """
        pass
    
    @abstractmethod
    def can_parse(self, message: bytes) -> bool:
        """
        Check if this parser can handle the message.
        
        Args:
            message: Raw message bytes
            
        Returns:
            True if parser can handle the message
        """
        pass
    
    def parse_batch(self, messages: List[bytes]) -> List[Dict[str, Any]]:
        """
        Parse a batch of messages.
        
        Args:
            messages: List of raw message bytes
            
        Returns:
            List of parsed data dictionaries
        """
        
        parsed_messages = []
        
        for message in messages:
            try:
                parsed = self.parse(message)
                if parsed is not None:
                    parsed_messages.append(parsed)
            except Exception as e:
                self.logger.debug(f"Failed to parse message: {e}")
                continue
        
        return parsed_messages


class JSONParser(BaseParser):
    """Parser for JSON format messages."""
    
    def parse(self, message: bytes) -> Optional[Dict[str, Any]]:
        """Parse JSON message."""
        
        try:
            text = message.decode('utf-8').strip()
            if not text:
                return None
            
            data = json.loads(text)
            
            # Convert to flat dictionary if it's a list
            if isinstance(data, list):
                if data and isinstance(data[0], dict):
                    # Merge all objects in the list
                    result = {}
                    for item in data:
                        if isinstance(item, dict):
                            result.update(item)
                    return result
                else:
                    return {"array": data}
            
            # Return as-is if it's already a dict
            if isinstance(data, dict):
                return data
            
            # Wrap primitive values
            return {"value": data}
            
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
            self.logger.debug(f"JSON parsing failed: {e}")
            return None
    
    def can_parse(self, message: bytes) -> bool:
        """Check if message is valid JSON."""
        
        try:
            text = message.decode('utf-8').strip()
            json.loads(text)
            return True
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            return False


class CSVParser(BaseParser):
    """Parser for CSV format messages."""
    
    def __init__(self, delimiter: str = ',', has_header: bool = True):
        """
        Initialize CSV parser.
        
        Args:
            delimiter: CSV delimiter character
            has_header: Whether CSV has header row
        """
        super().__init__()
        self.delimiter = delimiter
        self.has_header = has_header
        self.headers: Optional[List[str]] = None
    
    def parse(self, message: bytes) -> Optional[Dict[str, Any]]:
        """Parse CSV message."""
        
        try:
            text = message.decode('utf-8').strip()
            if not text:
                return None
            
            # Use StringIO for CSV parsing
            csv_reader = csv.reader(StringIO(text), delimiter=self.delimiter)
            rows = list(csv_reader)
            
            if not rows:
                return None
            
            # Handle header detection
            if self.has_header and self.headers is None:
                # First row is header
                self.headers = rows[0]
                if len(rows) > 1:
                    data_row = rows[1]
                else:
                    return None
            else:
                # Use existing headers or generate generic ones
                if self.headers:
                    data_row = rows[0]
                else:
                    # Generate generic headers
                    data_row = rows[0]
                    self.headers = [f"column_{i}" for i in range(len(data_row))]
            
            # Create dictionary from row data
            if len(self.headers) != len(data_row):
                # Pad or truncate as needed
                if len(self.headers) > len(data_row):
                    data_row.extend([''] * (len(self.headers) - len(data_row)))
                else:
                    data_row = data_row[:len(self.headers)]
            
            return dict(zip(self.headers, data_row))
            
        except (UnicodeDecodeError, csv.Error, ValueError) as e:
            self.logger.debug(f"CSV parsing failed: {e}")
            return None
    
    def can_parse(self, message: bytes) -> bool:
        """Check if message looks like CSV."""
        
        try:
            text = message.decode('utf-8').strip()
            if not text:
                return False
            
            # Basic CSV validation
            if self.delimiter not in text:
                return False
            
            # Try to parse as CSV
            csv_reader = csv.reader(StringIO(text), delimiter=self.delimiter)
            rows = list(csv_reader)
            
            return len(rows) > 0 and all(len(row) > 0 for row in rows)
            
        except (UnicodeDecodeError, csv.Error, ValueError):
            return False


class RawTextParser(BaseParser):
    """Parser for raw text messages that don't fit other formats."""
    
    def __init__(self):
        """Initialize raw text parser."""
        super().__init__()
    
    def parse(self, message: bytes) -> Optional[Dict[str, Any]]:
        """Parse raw text message as a single field."""
        
        try:
            text = message.decode('utf-8').strip()
            if not text:
                return None
            
            # Treat the entire message as a single text field
            return {
                "raw_content": text,
                "message_length": len(text),
                "is_binary": False
            }
            
        except UnicodeDecodeError:
            # Handle binary data
            return {
                "raw_content": message.hex(),  # Convert to hex string
                "message_length": len(message),
                "is_binary": True
            }
    
    def can_parse(self, message: bytes) -> bool:
        """Check if message can be parsed as raw text."""
        
        try:
            # Any message can be treated as raw text
            return len(message) > 0
        except Exception:
            return False
    
    def is_valid(self, message: bytes) -> bool:
        """Check if message can be parsed as raw text."""
        
        try:
            # Any message can be treated as raw text
            return len(message) > 0
        except Exception:
            return False


class TSVParser(CSVParser):
    """Parser for TSV (Tab-Separated Values) format messages."""
    
    def __init__(self, has_header: bool = True):
        """Initialize TSV parser."""
        super().__init__(delimiter='\t', has_header=has_header)


class KeyValueParser(BaseParser):
    """Parser for key-value format messages."""
    
    def __init__(self, pair_separator: str = ',', key_value_separator: str = '='):
        """
        Initialize key-value parser.
        
        Args:
            pair_separator: Separator between key-value pairs
            key_value_separator: Separator between key and value
        """
        super().__init__()
        self.pair_separator = pair_separator
        self.key_value_separator = key_value_separator
    
    def parse(self, message: bytes) -> Optional[Dict[str, Any]]:
        """Parse key-value message."""
        
        try:
            text = message.decode('utf-8').strip()
            if not text:
                return None
            
            # Validate that this looks like key-value data
            if not self._is_valid_key_value_text(text):
                return None
            
            # Split into pairs
            pairs = text.split(self.pair_separator)
            result = {}
            
            for pair in pairs:
                pair = pair.strip()
                if not pair:
                    continue
                
                # Split key and value
                if self.key_value_separator in pair:
                    key, value = pair.split(self.key_value_separator, 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Validate key and value are reasonable
                    if not key or not value or len(key) > 100 or len(value) > 1000:
                        continue
                    
                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    # Try to convert to appropriate type
                    result[key] = self._convert_value(value)
            
            return result if result else None
            
        except (UnicodeDecodeError, ValueError) as e:
            self.logger.debug(f"Key-value parsing failed: {e}")
            return None
    
    def _is_valid_key_value_text(self, text: str) -> bool:
        """Check if text looks like valid key-value data."""
        # Check for reasonable key-value patterns
        if self.key_value_separator not in text:
            return False
        
        # Check that it doesn't contain too many binary-like characters
        binary_chars = sum(1 for c in text if ord(c) < 32 and c not in '\t\n\r')
        if binary_chars > len(text) * 0.1:  # More than 10% binary chars
            return False
        
        return True
    
    def can_parse(self, message: bytes) -> bool:
        """Check if message looks like key-value format."""
        
        try:
            text = message.decode('utf-8').strip()
            if not text:
                return False
            
            # Check for key-value pattern
            if self.key_value_separator not in text:
                return False
            
            # Basic validation
            pairs = text.split(self.pair_separator)
            for pair in pairs:
                pair = pair.strip()
                if pair and self.key_value_separator not in pair:
                    return False
            
            return True
            
        except (UnicodeDecodeError, ValueError):
            return False
    
    def _convert_value(self, value: str) -> Union[str, int, float, bool, None]:
        """Convert string value to appropriate type."""
        
        if not value:
            return None
        
        # Try boolean
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Return as string
        return value


class DelimitedParser(BaseParser):
    """Parser for custom delimited format messages."""
    
    def __init__(self, delimiter: str, has_header: bool = True):
        """
        Initialize delimited parser.
        
        Args:
            delimiter: Custom delimiter character
            has_header: Whether data has header row
        """
        super().__init__()
        self.delimiter = delimiter
        self.has_header = has_header
        self.headers: Optional[List[str]] = None
    
    def parse(self, message: bytes) -> Optional[Dict[str, Any]]:
        """Parse delimited message."""
        
        try:
            text = message.decode('utf-8').strip()
            if not text:
                return None
            
            # Split by delimiter
            parts = text.split(self.delimiter)
            
            if not parts:
                return None
            
            # Handle headers
            if self.has_header and self.headers is None:
                self.headers = parts
                return None  # This is a header row
            elif self.headers is None:
                # Generate generic headers
                self.headers = [f"field_{i}" for i in range(len(parts))]
            
            # Create dictionary
            if len(self.headers) != len(parts):
                # Pad or truncate as needed
                if len(self.headers) > len(parts):
                    parts.extend([''] * (len(self.headers) - len(parts)))
                else:
                    parts = parts[:len(self.headers)]
            
            return dict(zip(self.headers, parts))
            
        except (UnicodeDecodeError, ValueError) as e:
            self.logger.debug(f"Delimited parsing failed: {e}")
            return None
    
    def can_parse(self, message: bytes) -> bool:
        """Check if message can be parsed with this delimiter."""
        
        try:
            text = message.decode('utf-8').strip()
            if not text:
                return False
            
            return self.delimiter in text
            
        except UnicodeDecodeError:
            return False


class ParserFactory:
    """Factory for creating appropriate parsers."""
    
    @staticmethod
    def create_parser(format_name: str, **kwargs) -> BaseParser:
        """
        Create a parser for the specified format.
        
        Args:
            format_name: Name of the format
            **kwargs: Additional parser-specific arguments
            
        Returns:
            Appropriate parser instance
        """
        
        parsers = {
            'json': JSONParser,
            'csv': CSVParser,
            'tsv': TSVParser,
            'key-value': KeyValueParser,
            'raw-text': RawTextParser,
        }
        
        if format_name not in parsers:
            raise FormatDetectionError(f"Unsupported format: {format_name}")
        
        parser_class = parsers[format_name]
        return parser_class(**kwargs)
    
    @staticmethod
    def create_delimited_parser(delimiter: str, **kwargs) -> BaseParser:
        """
        Create a delimited parser with custom delimiter.
        
        Args:
            delimiter: Custom delimiter
            **kwargs: Additional parser arguments
            
        Returns:
            DelimitedParser instance
        """
        return DelimitedParser(delimiter, **kwargs)
