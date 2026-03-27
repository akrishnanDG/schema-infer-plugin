"""
Schema Inference Plugin

A powerful tool for automatically inferring schemas from Kafka topic data.
"""

__version__ = "1.4.4"
__author__ = "Arun Krishnan"

from .core.consumer import KafkaConsumer
from .core.inferrer import SchemaInferrer
from .core.registry import SchemaRegistry
from .formats.detector import FormatDetector
from .formats.parsers import CSVParser, JSONParser, KeyValueParser
from .schemas.generators import AvroGenerator, JSONSchemaGenerator, ProtobufGenerator

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
