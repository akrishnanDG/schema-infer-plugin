"""
Pytest configuration and fixtures for comprehensive testing
"""

import pytest
import tempfile
import os
from typing import Dict, Any

from schema_infer.config import Config


@pytest.fixture
def sample_config():
    """Provide a sample configuration for testing."""
    config = Config()
    config.kafka.bootstrap_servers = "localhost:9092"
    config.kafka.auto_offset_reset = "latest"
    config.kafka.session_timeout_ms = 30000
    config.kafka.heartbeat_interval_ms = 10000
    config.schema_registry.url = "http://localhost:8081"
    config.schema_registry.verify_ssl = True
    config.schema_registry.auth = None
    config.inference.max_messages = 1000
    config.inference.timeout = 30
    config.performance.max_workers = 4
    config.performance.batch_size = 100
    return config


@pytest.fixture
def sample_data():
    """Provide comprehensive sample data for testing."""
    return [
        {
            "userId": "user123",
            "email": "test@example.com",
            "age": 25,
            "height": 5.9,
            "isActive": True,
            "lastLogin": None,
            "profile": {
                "firstName": "John",
                "lastName": "Doe",
                "age": 30,
                "salary": 75000.50,
                "isEmployed": True,
                "address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "zipCode": "10001",
                    "coordinates": {
                        "latitude": 40.7128,
                        "longitude": -74.0060
                    }
                },
                "phoneNumbers": [
                    {"type": "home", "number": "555-1234"},
                    {"type": "work", "number": "555-5678"}
                ],
                "tags": ["developer", "senior", "remote"],
                "scores": [95, 87, 92, 88],
                "ratings": [4.5, 4.2, 4.8, 4.1]
            },
            "orders": [
                {
                    "orderId": "order1",
                    "total": 99.99,
                    "items": [
                        {"name": "laptop", "price": 999.99, "quantity": 1},
                        {"name": "mouse", "price": 29.99, "quantity": 2}
                    ],
                    "shipping": {
                        "method": "express",
                        "cost": 15.99,
                        "estimatedDays": 2
                    }
                }
            ],
            "preferences": {
                "theme": "dark",
                "notifications": {
                    "email": True,
                    "sms": False,
                    "push": True
                },
                "languages": ["en", "es", "fr"],
                "settings": {
                    "autoSave": True,
                    "backupFrequency": 24,
                    "maxFileSize": 10485760.5
                }
            },
            "emptyObject": {},
            "emptyArray": [],
            "nullField": None,
            "booleanArray": [True, False, True],
            "numberArray": [1, 2.5, -3, 0, 100.99],
            "stringArray": ["a", "b", "c", "empty", ""],
            "metadata": {
                "createdAt": "2025-01-15T10:30:00Z",
                "updatedAt": "2025-01-16T14:22:00Z",
                "version": 2,
                "flags": {
                    "featureA": True,
                    "featureB": False,
                    "featureC": None
                },
                "tags": ["production", "v2", "stable"],
                "config": {
                    "timeout": 30,
                    "retries": 3,
                    "enabled": True,
                    "threshold": 0.95
                }
            }
        }
    ]


@pytest.fixture
def json_messages():
    """Provide JSON format messages for testing."""
    return [
        (None, b'{"name": "John", "age": 30}'),
        (None, b'{"id": 1, "active": true}'),
        (None, b'{"data": {"nested": "value"}}')
    ]


@pytest.fixture
def csv_messages():
    """Provide CSV format messages for testing."""
    return [
        (None, b'name,age,city\nJohn,30,New York'),
        (None, b'id,status,value\n1,active,100'),
        (None, b'product,price,quantity\nlaptop,999.99,1')
    ]


@pytest.fixture
def key_value_messages():
    """Provide key-value format messages for testing."""
    return [
        (None, b'name=John age=30 city=New York'),
        (None, b'id=1 status=active value=100'),
        (None, b'product=laptop price=999.99 quantity=1')
    ]


@pytest.fixture
def raw_text_messages():
    """Provide raw text messages for testing."""
    return [
        (None, b'This is plain text'),
        (None, b'Another line of text'),
        (None, b'No specific format here')
    ]


@pytest.fixture
def binary_messages():
    """Provide binary messages for testing."""
    return [
        (None, b'\x00\x01\x02\x03\x04'),
        (None, b'\xff\xfe\xfd\xfc'),
        (None, b'Some text with \x00 null bytes')
    ]


@pytest.fixture
def temp_config_file():
    """Provide a temporary configuration file for testing."""
    config_content = """
kafka:
  bootstrap_servers: "localhost:9092"
  auto_offset_reset: "latest"
  session_timeout_ms: 30000
  heartbeat_interval_ms: 10000

schema_registry:
  url: "http://localhost:8081"
  verify_ssl: true
  auth: null

inference:
  max_messages: 1000
  timeout: 30

performance:
  max_workers: 4
  batch_size: 100
  show_progress: true
  verbose_logging: false
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_file = f.name
    
    yield temp_file
    
    # Cleanup
    if os.path.exists(temp_file):
        os.unlink(temp_file)


@pytest.fixture
def mock_kafka_metadata():
    """Provide mock Kafka metadata for testing."""
    mock_metadata = {
        "topics": {
            "user-events": {"partitions": {0: {}, 1: {}}},
            "order-events": {"partitions": {0: {}}},
            "payment-events": {"partitions": {0: {}, 1: {}, 2: {}}},
            "_internal_topic": {"partitions": {0: {}}},
            "__consumer_offsets": {"partitions": {0: {}}},
            "_schema-infer-metrics": {"partitions": {0: {}}},
            "prod-user-events": {"partitions": {0: {}}},
            "prod-order-events": {"partitions": {0: {}}},
            "dev-user-events": {"partitions": {0: {}}},
            "test-order-events": {"partitions": {0: {}}},
            "temp-topic": {"partitions": {0: {}}},
            "backup-topic": {"partitions": {0: {}}},
            "system-logs": {"partitions": {0: {}}}
        }
    }
    return mock_metadata


@pytest.fixture
def mock_schema_registry_response():
    """Provide mock Schema Registry response for testing."""
    return {
        "id": 1,
        "version": 1,
        "subject": "test-topic-value",
        "schema": '{"type":"record","name":"TestRecord","fields":[]}'
    }


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test names."""
    for item in items:
        # Add slow marker to tests that might take time
        if "performance" in item.name or "large" in item.name:
            item.add_marker(pytest.mark.slow)
        
        # Add integration marker to integration tests
        if "integration" in item.name or "end_to_end" in item.name:
            item.add_marker(pytest.mark.integration)
        
        # Add unit marker to unit tests
        if "test_" in item.name and "integration" not in item.name:
            item.add_marker(pytest.mark.unit)
