"""
Main schema inference engine that coordinates all components
"""

from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from ..config import Config
from ..formats.detector import FormatDetector
from ..formats.parsers import ParserFactory, BaseParser
from ..schemas.inference import SchemaInferrer as SchemaAnalyzer, InferredSchema, FieldType
from ..schemas.generators import SchemaGeneratorFactory, BaseSchemaGenerator
from ..utils.exceptions import InferenceError
from ..utils.logger import get_logger


class SchemaInferrer:
    """Main schema inference engine."""
    
    def __init__(self, config: Config):
        """
        Initialize schema inferrer.
        
        Args:
            config: Configuration object
        """
        
        self.config = config
        self.logger = get_logger(__name__)
        
        # Initialize components
        self.format_detector = FormatDetector(
            confidence_threshold=config.inference.confidence_threshold,
            sample_size=config.inference.sample_size
        )
        
        self.schema_analyzer = SchemaAnalyzer(
            confidence_threshold=config.inference.confidence_threshold,
            max_depth=config.inference.max_depth,
            array_handling=config.inference.array_handling,
            null_handling=config.inference.null_handling
        )
        
        self.logger.info("Initialized schema inferrer")
    
    def process_topics_parallel(
        self, 
        topic_messages: Dict[str, List[Tuple[Optional[bytes], bytes]]],
        output_format: str,
        output_path: Optional[str] = None,
        output_dir: Optional[str] = None,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        Process multiple topics in parallel for better performance.
        
        Args:
            topic_messages: Dictionary mapping topic names to their messages
            output_format: Output format (avro, protobuf, json)
            output_path: Single output file path (for single topic)
            output_dir: Output directory (for multiple topics)
            progress_callback: Optional callback function for progress updates
            
        Returns:
            Dictionary with processing results
        """
        
        results = {
            'successful': 0,
            'failed': 0,
            'total': len(topic_messages),
            'schemas': {}
        }
        
        def process_single_topic(topic_name, messages):
            """Process a single topic and return results."""
            try:
                start_time = time.time()
                schema = self.infer_schema(messages, topic_name)
                elapsed_time = time.time() - start_time
                
                if schema:
                    # Convert dictionary back to schema object for generator
                    schema_obj = self._dict_to_schema(schema)
                    
                    # Generate schema file
                    generator = SchemaGeneratorFactory.create_generator(output_format)
                    schema_content = generator.generate(schema_obj)
                    
                    # Determine output file path
                    if output_dir:
                        output_file = f"{output_dir}/{topic_name}.{output_format}"
                    elif output_path and len(topic_messages) == 1:
                        output_file = output_path
                    else:
                        output_file = f"{topic_name}.{output_format}"
                    
                    # Write schema to file
                    with open(output_file, 'w') as f:
                        f.write(schema_content)
                    
                    return {
                        'topic': topic_name,
                        'success': True,
                        'schema': schema,
                        'output_file': output_file,
                        'processing_time': elapsed_time,
                        'message_count': len(messages)
                    }
                else:
                    return {
                        'topic': topic_name,
                        'success': False,
                        'error': 'No schema generated',
                        'processing_time': elapsed_time,
                        'message_count': len(messages)
                    }
                    
            except Exception as e:
                return {
                    'topic': topic_name,
                    'success': False,
                    'error': str(e),
                    'processing_time': 0,
                    'message_count': len(messages)
                }
        
        # Process topics in parallel
        max_workers = min(self.config.performance.max_workers, len(topic_messages))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all topic processing tasks
            future_to_topic = {
                executor.submit(process_single_topic, topic_name, messages): topic_name
                for topic_name, messages in topic_messages.items()
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_topic):
                try:
                    result = future.result()
                    
                    if result['success']:
                        results['successful'] += 1
                        results['schemas'][result['topic']] = result['schema']
                        print(f"✅ {result['topic']}: Generated schema in {result['processing_time']:.2f}s ({result['message_count']} messages)")
                    else:
                        results['failed'] += 1
                        print(f"❌ {result['topic']}: {result['error']}")
                    
                    # Call progress callback if provided
                    if progress_callback:
                        progress_callback(results['successful'] + results['failed'], len(topic_messages))
                        
                except Exception as e:
                    topic_name = future_to_topic[future]
                    results['failed'] += 1
                    print(f"❌ {topic_name}: Processing failed - {e}")
                    
                    # Call progress callback even for exceptions
                    if progress_callback:
                        progress_callback(results['successful'] + results['failed'], len(topic_messages))
        
        return results
    
    def infer_schema(
        self, 
        messages: List[Tuple[Optional[bytes], bytes]], 
        topic_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Infer schema from Kafka messages.
        
        Args:
            messages: List of (key, value) tuples from Kafka
            topic_name: Name of the topic
            
        Returns:
            Inferred schema as dictionary or None if inference fails
        """
        
        if not messages:
            self.logger.warning("No messages provided for schema inference")
            print("⚠️  No messages found in topic - cannot generate schema")
            return None
        
        try:
            # Extract message values
            message_values = [value for _, value in messages if value is not None]
            
            if not message_values:
                self.logger.warning("No valid message values found")
                print("⚠️  No valid message values found - all messages may be empty or corrupted")
                return None
            
            self.logger.info(f"Processing {len(message_values)} messages for schema inference")
            
            # Detect data format
            if self.config.inference.auto_detect_format:
                detected_format, confidence = self.format_detector.detect_format(message_values)
                self.logger.info(f"Detected format: {detected_format} (confidence: {confidence:.2f})")
            else:
                detected_format = self.config.inference.forced_data_format or "json"
                self.logger.info(f"Using forced format: {detected_format}")
            
            # Create appropriate parser
            parser = self._create_parser(detected_format, message_values)
            
            # Parse messages
            parsed_data = parser.parse_batch(message_values)
            
            if not parsed_data:
                self.logger.warning("No data could be parsed")
                print(f"⚠️  Could not parse messages in {detected_format} format - trying fallback approach")
                
                # Try raw-text as fallback
                if detected_format != "raw-text":
                    self.logger.info("Attempting fallback to raw-text format")
                    fallback_parser = self._create_parser("raw-text", message_values)
                    parsed_data = fallback_parser.parse_batch(message_values)
                    
                    if parsed_data:
                        # Check if all messages are binary
                        all_binary = all(
                            isinstance(record, dict) and 
                            record.get("is_binary", False) and 
                            "raw_content" in record
                            for record in parsed_data
                        )
                        
                        if all_binary:
                            print("❌ All messages are in binary format - schema cannot be inferred")
                            print("💡 Binary messages require specific deserializers (Avro, Protobuf, etc.)")
                            print("💡 Consider using Schema Inference Schema Registry with proper serializers")
                            return None
                        
                        print("✅ Successfully parsed messages as raw text")
                        detected_format = "raw-text"
                    else:
                        print("❌ Failed to parse messages even as raw text")
                        return None
                else:
                    return None
            
            self.logger.info(f"Successfully parsed {len(parsed_data)} messages")
            
            # Check if all messages are binary (even if format detection worked)
            if detected_format == "raw-text":
                all_binary = all(
                    isinstance(record, dict) and 
                    record.get("is_binary", False) and 
                    "raw_content" in record
                    for record in parsed_data
                )
                
                if all_binary:
                    print("❌ All messages are in binary format - schema cannot be inferred")
                    print("💡 Binary messages require specific deserializers (Avro, Protobuf, etc.)")
                    print("💡 Consider using Schema Inference Schema Registry with proper serializers")
                    return None
            
            # Infer schema from parsed data
            inferred_schema = self.schema_analyzer.infer_schema(parsed_data, topic_name)
            
            # Convert to dictionary for return
            schema_dict = inferred_schema.to_dict()
            schema_dict["_metadata"] = {
                "format": detected_format,
                "message_count": len(message_values),
                "parsed_count": len(parsed_data),
                "confidence": confidence if self.config.inference.auto_detect_format else 1.0
            }
            
            return schema_dict
            
        except Exception as e:
            self.logger.error(f"Schema inference failed: {e}")
            raise InferenceError(f"Schema inference failed: {e}")
    
    def generate_schema(self, schema_dict: Dict[str, Any], schema_format: str) -> str:
        """
        Generate schema in the specified format.
        
        Args:
            schema_dict: Inferred schema dictionary
            schema_format: Target schema format (avro, protobuf, json-schema)
            
        Returns:
            Generated schema as string
        """
        
        try:
            # Create schema generator
            generator = SchemaGeneratorFactory.create_generator(schema_format)
            
            # Convert dictionary back to InferredSchema object
            inferred_schema = self._dict_to_schema(schema_dict)
            
            # Generate schema
            schema_content = generator.generate(inferred_schema)
            
            self.logger.info(f"Generated {schema_format} schema")
            return schema_content
            
        except Exception as e:
            self.logger.error(f"Schema generation failed: {e}")
            raise InferenceError(f"Schema generation failed: {e}")
    
    def _create_parser(self, format_name: str, messages: List[bytes]) -> BaseParser:
        """
        Create appropriate parser for the detected format.
        
        Args:
            format_name: Detected format name
            messages: Sample messages for parser configuration
            
        Returns:
            Configured parser instance
        """
        
        if format_name == "csv":
            # Detect delimiter
            delimiter = self.format_detector.detect_delimiter(
                [msg.decode('utf-8', errors='ignore') for msg in messages[:10]]
            )
            delimiter = delimiter or ","
            
            return ParserFactory.create_parser("csv", delimiter=delimiter)
        
        elif format_name == "tsv":
            return ParserFactory.create_parser("tsv")
        
        elif format_name == "key-value":
            # Detect separators
            sample_texts = [msg.decode('utf-8', errors='ignore') for msg in messages[:5]]
            
            # Check for = or : separator
            if any('=' in text for text in sample_texts):
                return ParserFactory.create_parser("key-value", key_value_separator='=')
            else:
                return ParserFactory.create_parser("key-value", key_value_separator=':')
        
        elif format_name == "raw-text":
            return ParserFactory.create_parser("raw-text")
        
        else:
            # Default to JSON
            return ParserFactory.create_parser("json")
    
    def _dict_to_schema(self, schema_dict: Dict[str, Any]) -> InferredSchema:
        """
        Convert schema dictionary back to InferredSchema object.
        
        Args:
            schema_dict: Schema dictionary
            
        Returns:
            InferredSchema object
        """
        
        from ..schemas.inference import InferredSchema, SchemaField, FieldType
        
        # Extract fields
        fields = []
        for field_dict in schema_dict.get("fields", []):
            # Parse field type
            type_str = field_dict.get("type", "string")
            field_type = self._parse_field_type(type_str)
            
            field = SchemaField(
                name=field_dict.get("name", ""),
                field_type=field_type,
                required=field_dict.get("required", True),
                default_value=field_dict.get("default_value"),
                description=field_dict.get("description"),
                examples=field_dict.get("examples", [])
            )
            fields.append(field)
        
        return InferredSchema(
            name=schema_dict.get("name", "UnknownSchema"),
            fields=fields,
            description=schema_dict.get("description"),
            namespace=schema_dict.get("namespace")
        )
    
    def _parse_field_type(self, type_str: str) -> FieldType:
        """
        Parse field type string into FieldType object.
        
        Args:
            type_str: Type string (e.g., "nullable<array<string>>")
            
        Returns:
            FieldType object
        """
        
        from ..schemas.inference import FieldType
        
        # Simple parsing for common patterns
        nullable = "nullable<" in type_str
        array = "array<" in type_str
        
        # Extract base type
        if array:
            # Extract type from array<type>
            start = type_str.find("array<") + 6
            end = type_str.rfind(">")
            base_type = type_str[start:end]
        else:
            base_type = type_str
        
        # Remove nullable wrapper
        if nullable and base_type.startswith("nullable<"):
            base_type = base_type[9:-1]  # Remove "nullable<" and ">"
        
        return FieldType(base_type, nullable=nullable, array=array)
    
    def get_supported_formats(self) -> List[str]:
        """
        Get list of supported data formats.
        
        Returns:
            List of supported format names
        """
        
        return ["json", "csv", "tsv", "key-value"]
    
    def get_supported_schema_formats(self) -> List[str]:
        """
        Get list of supported schema output formats.
        
        Returns:
            List of supported schema format names
        """
        
        return ["avro", "protobuf", "json-schema"]
