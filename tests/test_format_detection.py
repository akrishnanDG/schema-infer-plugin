"""
Comprehensive unit tests for format detection and parsing
"""

import pytest
from typing import List, Dict, Any

from schema_infer.formats.detector import FormatDetector
from schema_infer.formats.parsers import (
    JSONParser, 
    CSVParser, 
    KeyValueParser, 
    RawTextParser,
    ParserFactory
)


class TestFormatDetection:
    """Comprehensive tests for format detection."""
    
    def setup_method(self):
        """Set up test data."""
        self.detector = FormatDetector()
    
    def test_json_detection(self):
        """Test JSON format detection."""
        json_messages = [
            b'{"name": "John", "age": 30}',
            b'{"id": 1, "active": true}',
            b'{"data": {"nested": "value"}}'
        ]
        
        format_name, confidence = self.detector.detect_format(json_messages)
        
        assert format_name == "json"
        assert confidence > 0.8
    
    def test_csv_detection(self):
        """Test CSV format detection."""
        csv_messages = [
            b'name,age,city\nJohn,30,New York',
            b'id,status,value\n1,active,100',
            b'product,price,quantity\nlaptop,999.99,1'
        ]
        
        format_name, confidence = self.detector.detect_format(csv_messages)
        
        assert format_name == "csv"
        assert confidence > 0.8
    
    def test_key_value_detection(self):
        """Test key-value format detection."""
        kv_messages = [
            b'name=John age=30 city=New York',
            b'id=1 status=active value=100',
            b'product=laptop price=999.99 quantity=1'
        ]
        
        format_name, confidence = self.detector.detect_format(kv_messages)
        
        assert format_name == "key-value"
        assert confidence > 0.8
    
    def test_raw_text_detection(self):
        """Test raw text format detection."""
        text_messages = [
            b'This is just plain text',
            b'Another line of text',
            b'No specific format here'
        ]
        
        format_name, confidence = self.detector.detect_format(text_messages)
        
        assert format_name == "raw-text"
        assert confidence > 0.5
    
    def test_mixed_format_detection(self):
        """Test detection with mixed format messages."""
        mixed_messages = [
            b'{"name": "John"}',  # JSON
            b'name,age\nJohn,30',  # CSV
            b'name=John age=30'    # Key-value
        ]
        
        format_name, confidence = self.detector.detect_format(mixed_messages)
        
        # Should detect the most common format
        assert format_name in ["json", "csv", "key-value"]
        assert confidence > 0.3
    
    def test_binary_data_handling(self):
        """Test handling of binary data."""
        binary_messages = [
            b'\x00\x01\x02\x03\x04',
            b'\xff\xfe\xfd\xfc',
            b'Some text with \x00 null bytes'
        ]
        
        format_name, confidence = self.detector.detect_format(binary_messages)
        
        # Should fall back to raw-text for binary data
        assert format_name == "raw-text"
        assert confidence < 0.5
    
    def test_empty_messages(self):
        """Test handling of empty messages."""
        empty_messages = [b'', b'', b'']
        
        format_name, confidence = self.detector.detect_format(empty_messages)
        
        # Should fall back to raw-text
        assert format_name == "raw-text"
        assert confidence < 0.3
    
    def test_confidence_scoring(self):
        """Test confidence scoring for different formats."""
        # High confidence JSON
        json_messages = [b'{"valid": "json"}' for _ in range(10)]
        _, json_confidence = self.detector.detect_format(json_messages)
        
        # Low confidence mixed
        mixed_messages = [b'{"json": "one"}', b'not json', b'{"another": "json"}']
        _, mixed_confidence = self.detector.detect_format(mixed_messages)
        
        assert json_confidence > mixed_confidence


class TestJSONParser:
    """Comprehensive tests for JSON parser."""
    
    def setup_method(self):
        """Set up test data."""
        self.parser = JSONParser()
    
    def test_valid_json_parsing(self):
        """Test parsing of valid JSON messages."""
        messages = [
            (None, b'{"name": "John", "age": 30}'),
            (None, b'{"id": 1, "active": true}'),
            (None, b'{"data": {"nested": "value"}}')
        ]
        
        parsed_data = self.parser.parse(messages)
        
        assert len(parsed_data) == 3
        assert parsed_data[0]["name"] == "John"
        assert parsed_data[0]["age"] == 30
        assert parsed_data[1]["id"] == 1
        assert parsed_data[1]["active"] == True
        assert parsed_data[2]["data"]["nested"] == "value"
    
    def test_invalid_json_handling(self):
        """Test handling of invalid JSON."""
        messages = [
            (None, b'{"invalid": json}'),  # Missing quotes
            (None, b'{"incomplete":'),      # Incomplete JSON
            (None, b'not json at all')      # Not JSON
        ]
        
        parsed_data = self.parser.parse(messages)
        
        # Should return empty list for invalid JSON
        assert len(parsed_data) == 0
    
    def test_json_with_keys(self):
        """Test parsing JSON with message keys."""
        messages = [
            (b'key1', b'{"value": 1}'),
            (b'key2', b'{"value": 2}'),
            (None, b'{"value": 3}')
        ]
        
        parsed_data = self.parser.parse(messages)
        
        assert len(parsed_data) == 3
        # Keys are not included in parsed data, only values
        assert all("value" in record for record in parsed_data)
    
    def test_can_parse_detection(self):
        """Test can_parse method."""
        assert self.parser.can_parse(b'{"valid": "json"}')
        assert not self.parser.can_parse(b'not json')
        assert not self.parser.can_parse(b'{"invalid": json}')


class TestCSVParser:
    """Comprehensive tests for CSV parser."""
    
    def setup_method(self):
        """Set up test data."""
        self.parser = CSVParser()
    
    def test_valid_csv_parsing(self):
        """Test parsing of valid CSV messages."""
        messages = [
            (None, b'name,age,city\nJohn,30,New York'),
            (None, b'id,status,value\n1,active,100'),
            (None, b'product,price,quantity\nlaptop,999.99,1')
        ]
        
        parsed_data = self.parser.parse(messages)
        
        assert len(parsed_data) == 3
        assert parsed_data[0]["name"] == "John"
        assert parsed_data[0]["age"] == "30"
        assert parsed_data[0]["city"] == "New York"
        assert parsed_data[1]["id"] == "1"
        assert parsed_data[1]["status"] == "active"
        assert parsed_data[2]["product"] == "laptop"
    
    def test_csv_with_different_separators(self):
        """Test CSV parsing with different separators."""
        messages = [
            (None, b'name;age;city\nJohn;30;New York'),  # Semicolon
            (None, b'name|age|city\nJohn|30|New York'),  # Pipe
            (None, b'name\tage\tcity\nJohn\t30\tNew York')  # Tab
        ]
        
        parsed_data = self.parser.parse(messages)
        
        # Should handle different separators
        assert len(parsed_data) >= 1
    
    def test_csv_with_headers(self):
        """Test CSV parsing with headers."""
        messages = [
            (None, b'name,age,city\nJohn,30,New York\nJane,25,Boston')
        ]
        
        parsed_data = self.parser.parse(messages)
        
        assert len(parsed_data) == 2
        assert parsed_data[0]["name"] == "John"
        assert parsed_data[1]["name"] == "Jane"
    
    def test_csv_without_headers(self):
        """Test CSV parsing without headers."""
        messages = [
            (None, b'John,30,New York\nJane,25,Boston')
        ]
        
        parsed_data = self.parser.parse(messages)
        
        # Should create generic column names
        assert len(parsed_data) == 2
        assert "column_0" in parsed_data[0]
        assert "column_1" in parsed_data[0]
    
    def test_can_parse_detection(self):
        """Test can_parse method."""
        assert self.parser.can_parse(b'name,age\nJohn,30')
        assert not self.parser.can_parse(b'not csv')
        assert not self.parser.can_parse(b'{"json": "data"}')


class TestKeyValueParser:
    """Comprehensive tests for key-value parser."""
    
    def setup_method(self):
        """Set up test data."""
        self.parser = KeyValueParser()
    
    def test_valid_key_value_parsing(self):
        """Test parsing of valid key-value messages."""
        messages = [
            (None, b'name=John age=30 city=New York'),
            (None, b'id=1 status=active value=100'),
            (None, b'product=laptop price=999.99 quantity=1')
        ]
        
        parsed_data = self.parser.parse(messages)
        
        assert len(parsed_data) == 3
        assert parsed_data[0]["name"] == "John"
        assert parsed_data[0]["age"] == "30"
        assert parsed_data[0]["city"] == "New York"
        assert parsed_data[1]["id"] == "1"
        assert parsed_data[1]["status"] == "active"
        assert parsed_data[2]["product"] == "laptop"
    
    def test_key_value_with_different_separators(self):
        """Test key-value parsing with different separators."""
        messages = [
            (None, b'name:John age:30 city:New York'),  # Colon
            (None, b'name|John age|30 city|New York'),  # Pipe
            (None, b'name John age 30 city New York')   # Space
        ]
        
        parsed_data = self.parser.parse(messages)
        
        # Should handle different separators
        assert len(parsed_data) >= 1
    
    def test_key_value_with_quoted_values(self):
        """Test key-value parsing with quoted values."""
        messages = [
            (None, b'name="John Doe" age=30 city="New York"'),
            (None, b'description="A long description with spaces" value=100')
        ]
        
        parsed_data = self.parser.parse(messages)
        
        assert len(parsed_data) == 2
        assert parsed_data[0]["name"] == "John Doe"
        assert parsed_data[0]["city"] == "New York"
        assert parsed_data[1]["description"] == "A long description with spaces"
    
    def test_key_value_with_special_characters(self):
        """Test key-value parsing with special characters."""
        messages = [
            (None, b'url=https://example.com path=/api/v1'),
            (None, b'email=user@domain.com phone=+1-555-1234')
        ]
        
        parsed_data = self.parser.parse(messages)
        
        assert len(parsed_data) == 2
        assert parsed_data[0]["url"] == "https://example.com"
        assert parsed_data[0]["path"] == "/api/v1"
        assert parsed_data[1]["email"] == "user@domain.com"
    
    def test_can_parse_detection(self):
        """Test can_parse method."""
        assert self.parser.can_parse(b'name=John age=30')
        assert not self.parser.can_parse(b'not key value')
        assert not self.parser.can_parse(b'{"json": "data"}')


class TestRawTextParser:
    """Comprehensive tests for raw text parser."""
    
    def setup_method(self):
        """Set up test data."""
        self.parser = RawTextParser()
    
    def test_raw_text_parsing(self):
        """Test parsing of raw text messages."""
        messages = [
            (None, b'This is plain text'),
            (None, b'Another line of text'),
            (None, b'No specific format here')
        ]
        
        parsed_data = self.parser.parse(messages)
        
        assert len(parsed_data) == 3
        assert parsed_data[0]["text"] == "This is plain text"
        assert parsed_data[1]["text"] == "Another line of text"
        assert parsed_data[2]["text"] == "No specific format here"
    
    def test_raw_text_with_special_characters(self):
        """Test raw text parsing with special characters."""
        messages = [
            (None, 'Text with émojis 🚀 and spëcial chars'.encode('utf-8')),
            (None, b'Numbers: 123, 456.789'),
            (None, b'Symbols: @#$%^&*()')
        ]
        
        parsed_data = self.parser.parse(messages)
        
        assert len(parsed_data) == 3
        assert "émojis" in parsed_data[0]["text"]
        assert "123" in parsed_data[1]["text"]
        assert "@#$%^&*()" in parsed_data[2]["text"]
    
    def test_raw_text_with_keys(self):
        """Test raw text parsing with message keys."""
        messages = [
            (b'key1', b'Text message 1'),
            (b'key2', b'Text message 2'),
            (None, b'Text message 3')
        ]
        
        parsed_data = self.parser.parse(messages)
        
        assert len(parsed_data) == 3
        assert all("text" in record for record in parsed_data)
    
    def test_can_parse_detection(self):
        """Test can_parse method."""
        assert self.parser.can_parse(b'Any text content')
        assert self.parser.can_parse(b'123 numbers')
        assert self.parser.can_parse(b'Special chars: @#$%')


class TestParserFactory:
    """Comprehensive tests for parser factory."""
    
    def test_parser_creation(self):
        """Test creation of parsers for different formats."""
        json_parser = ParserFactory.create_parser("json")
        assert isinstance(json_parser, JSONParser)
        
        csv_parser = ParserFactory.create_parser("csv")
        assert isinstance(csv_parser, CSVParser)
        
        kv_parser = ParserFactory.create_parser("key-value")
        assert isinstance(kv_parser, KeyValueParser)
        
        text_parser = ParserFactory.create_parser("raw-text")
        assert isinstance(text_parser, RawTextParser)
    
    def test_invalid_parser_format(self):
        """Test handling of invalid parser format."""
        with pytest.raises(ValueError, match="Unsupported format"):
            ParserFactory.create_parser("invalid-format")
    
    def test_parser_consistency(self):
        """Test that same format returns same parser type."""
        parser1 = ParserFactory.create_parser("json")
        parser2 = ParserFactory.create_parser("json")
        
        assert type(parser1) == type(parser2)


class TestFormatDetectionIntegration:
    """Integration tests for format detection and parsing."""
    
    def test_end_to_end_format_detection_and_parsing(self):
        """Test complete flow from detection to parsing."""
        detector = FormatDetector()
        
        # Test JSON flow
        json_messages = [
            (None, b'{"name": "John", "age": 30}'),
            (None, b'{"id": 1, "active": true}')
        ]
        
        format_name, confidence = detector.detect_format(json_messages)
        assert format_name == "json"
        
        parser = ParserFactory.create_parser(format_name)
        parsed_data = parser.parse(json_messages)
        
        assert len(parsed_data) == 2
        assert parsed_data[0]["name"] == "John"
        assert parsed_data[1]["id"] == 1
        
        # Test CSV flow
        csv_messages = [
            (None, b'name,age\nJohn,30'),
            (None, b'id,status\n1,active')
        ]
        
        format_name, confidence = detector.detect_format(csv_messages)
        assert format_name == "csv"
        
        parser = ParserFactory.create_parser(format_name)
        parsed_data = parser.parse(csv_messages)
        
        assert len(parsed_data) == 2
        assert parsed_data[0]["name"] == "John"
        assert parsed_data[1]["id"] == "1"
    
    def test_fallback_to_raw_text(self):
        """Test fallback to raw text when other formats fail."""
        detector = FormatDetector()
        
        # Binary/unparseable data
        binary_messages = [
            b'\x00\x01\x02\x03',
            b'Some text with \x00 null bytes',
            b'Not any recognizable format'
        ]
        
        format_name, confidence = detector.detect_format(binary_messages)
        assert format_name == "raw-text"
        assert confidence < 0.5
        
        parser = ParserFactory.create_parser(format_name)
        parsed_data = parser.parse(binary_messages)
        
        assert len(parsed_data) == 3
        assert all("text" in record for record in parsed_data)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
