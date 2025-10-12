"""
Data format detection for Schema Inference Plugin
"""

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from ..utils.exceptions import FormatDetectionError
from ..utils.logger import get_logger


class FormatDetector:
    """Detects the data format of messages."""
    
    def __init__(self, confidence_threshold: float = 0.8, sample_size: int = 100):
        """
        Initialize format detector.
        
        Args:
            confidence_threshold: Minimum confidence for format detection
            sample_size: Number of messages to sample for detection
        """
        
        self.confidence_threshold = confidence_threshold
        self.sample_size = sample_size
        self.logger = get_logger(__name__)
        
        # Format detection patterns
        self.patterns = {
            "json": [
                r'^\s*\{.*\}\s*$',  # JSON object
                r'^\s*\[.*\]\s*$',  # JSON array
            ],
            "csv": [
                r'^[^,]+(,[^,]+)+$',  # Contains commas, not just one
                r'^[^,\n]+(,[^,\n]+)+$',  # CSV with potential newlines
            ],
            "tsv": [
                r'^[^\t]+(\t[^\t]+)+$',  # Contains tabs
            ],
            "key-value": [
                r'^[^=]+=[^=]+(,[^=]+=[^=]+)*$',  # key=value,key=value
                r'^[^:]+:[^:]+(,[^:]+:[^:]+)*$',  # key:value,key:value
            ],
        }
    
    def detect_format(self, messages: List[bytes]) -> Tuple[str, float]:
        """
        Detect the format of messages.
        
        Args:
            messages: List of message bytes
            
        Returns:
            Tuple of (format_name, confidence_score)
        """
        
        if not messages:
            raise FormatDetectionError("No messages provided for format detection")
        
        # Sample messages for detection
        sample_messages = messages[:min(self.sample_size, len(messages))]
        
        # Convert bytes to strings
        text_messages = []
        for msg in sample_messages:
            try:
                text = msg.decode('utf-8').strip()
                if text:  # Only process non-empty messages
                    text_messages.append(text)
            except UnicodeDecodeError:
                # Skip non-UTF-8 messages
                continue
        
        if not text_messages:
            # All messages are binary - use raw-text format
            self.logger.warning("No valid text messages found - treating as binary/raw-text format")
            return "raw-text", 0.1
        
        # Score each format
        format_scores = {}
        
        for format_name, patterns in self.patterns.items():
            score = self._calculate_format_score(text_messages, patterns)
            format_scores[format_name] = score
        
        # Find the best format
        best_format = max(format_scores.items(), key=lambda x: x[1])
        format_name, confidence = best_format
        
        self.logger.info(f"Detected format: {format_name} (confidence: {confidence:.2f})")
        
        if confidence < self.confidence_threshold:
            self.logger.warning(f"Low confidence format detection: {confidence:.2f} < {self.confidence_threshold}")
            # Try to provide a fallback format
            if confidence < 0.3:
                self.logger.warning("Very low confidence - treating as raw text format")
                return "raw-text", 0.1
        
        return format_name, confidence
    
    def _calculate_format_score(self, messages: List[str], patterns: List[str]) -> float:
        """
        Calculate confidence score for a format based on pattern matching.
        
        Args:
            messages: List of text messages
            patterns: List of regex patterns for the format
            
        Returns:
            Confidence score between 0 and 1
        """
        
        if not messages:
            return 0.0
        
        total_messages = len(messages)
        matching_messages = 0
        
        for message in messages:
            for pattern in patterns:
                if re.match(pattern, message, re.DOTALL):
                    matching_messages += 1
                    break  # Count each message only once
        
        # Base score from pattern matching
        pattern_score = matching_messages / total_messages
        
        # Additional validation for specific formats
        validation_score = self._validate_format(messages, patterns)
        
        # Combine scores
        final_score = (pattern_score * 0.7) + (validation_score * 0.3)
        
        return min(final_score, 1.0)
    
    def _validate_format(self, messages: List[str], patterns: List[str]) -> float:
        """
        Additional validation for specific formats.
        
        Args:
            messages: List of text messages
            patterns: List of regex patterns
            
        Returns:
            Validation score between 0 and 1
        """
        
        if not messages:
            return 0.0
        
        # JSON validation
        if any('json' in str(pattern) for pattern in patterns):
            return self._validate_json(messages)
        
        # CSV validation
        if any('csv' in str(pattern) for pattern in patterns):
            return self._validate_csv(messages)
        
        # Key-value validation
        if any('key-value' in str(pattern) for pattern in patterns):
            return self._validate_key_value(messages)
        
        return 0.5  # Default validation score
    
    def _validate_json(self, messages: List[str]) -> float:
        """Validate JSON format."""
        
        valid_count = 0
        
        for message in messages:
            try:
                json.loads(message)
                valid_count += 1
            except (json.JSONDecodeError, ValueError):
                continue
        
        return valid_count / len(messages) if messages else 0.0
    
    def _validate_csv(self, messages: List[str]) -> float:
        """Validate CSV format."""
        
        if not messages:
            return 0.0
        
        # Check for consistent column count
        column_counts = []
        
        for message in messages:
            columns = message.count(',') + 1
            column_counts.append(columns)
        
        if not column_counts:
            return 0.0
        
        # Calculate consistency score
        most_common_count = max(set(column_counts), key=column_counts.count)
        consistent_count = column_counts.count(most_common_count)
        
        return consistent_count / len(column_counts)
    
    def _validate_key_value(self, messages: List[str]) -> float:
        """Validate key-value format."""
        
        if not messages:
            return 0.0
        
        valid_count = 0
        
        for message in messages:
            # Check for key=value or key:value pairs
            if '=' in message:
                pairs = message.split(',')
                if all('=' in pair and len(pair.split('=')) == 2 for pair in pairs):
                    valid_count += 1
            elif ':' in message:
                pairs = message.split(',')
                if all(':' in pair and len(pair.split(':')) == 2 for pair in pairs):
                    valid_count += 1
        
        return valid_count / len(messages)
    
    def detect_delimiter(self, messages: List[str]) -> Optional[str]:
        """
        Detect the delimiter used in structured data.
        
        Args:
            messages: List of text messages
            
        Returns:
            Detected delimiter or None
        """
        
        if not messages:
            return None
        
        # Common delimiters to check
        delimiters = [',', '\t', '|', ';', ' ']
        delimiter_scores = {}
        
        for delimiter in delimiters:
            scores = []
            
            for message in messages:
                if delimiter in message:
                    # Count occurrences and check consistency
                    parts = message.split(delimiter)
                    if len(parts) > 1:  # Must have at least 2 parts
                        scores.append(len(parts))
            
            if scores:
                # Calculate consistency (lower variance = higher score)
                avg_parts = sum(scores) / len(scores)
                variance = sum((x - avg_parts) ** 2 for x in scores) / len(scores)
                consistency = 1.0 / (1.0 + variance)  # Higher consistency = higher score
                delimiter_scores[delimiter] = consistency * len(scores) / len(messages)
        
        if delimiter_scores:
            best_delimiter = max(delimiter_scores.items(), key=lambda x: x[1])
            if best_delimiter[1] > 0.5:  # Minimum threshold
                return best_delimiter[0]
        
        return None
    
    def detect_encoding(self, messages: List[bytes]) -> str:
        """
        Detect the encoding of messages.
        
        Args:
            messages: List of message bytes
            
        Returns:
            Detected encoding
        """
        
        # Try common encodings
        encodings = ['utf-8', 'utf-16', 'latin-1', 'ascii']
        
        for encoding in encodings:
            try:
                for message in messages[:10]:  # Test first 10 messages
                    message.decode(encoding)
                return encoding
            except UnicodeDecodeError:
                continue
        
        # Default to utf-8 with error handling
        return 'utf-8'
