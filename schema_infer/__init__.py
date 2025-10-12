"""
Schema Inference Schema Inference Plugin

A powerful tool for automatically inferring schemas from Kafka topic data.
"""

__version__ = "0.1.0"
__author__ = "Schema Inference Plugin"
__email__ = "schema-infer@schema-infer.io"

from .core.inferrer import SchemaInferrer
from .core.consumer import KafkaConsumer
from .core.registry import SchemaRegistry
from .formats.detector import FormatDetector
from .formats.parsers import JSONParser, CSVParser, KeyValueParser
from .schemas.generators import AvroGenerator, ProtobufGenerator, JSONSchemaGenerator

__all__ = [
    "SchemaInferrer",
    "KafkaConsumer", 
    "SchemaRegistry",
    "FormatDetector",
    "JSONParser",
    "CSVParser", 
    "KeyValueParser",
    "AvroGenerator",
    "ProtobufGenerator",
    "JSONSchemaGenerator",
]
