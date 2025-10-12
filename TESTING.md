# Comprehensive Testing Guide

This document provides a comprehensive guide to testing the Schema Inference Schema Inference Plugin.

## Overview

The test suite includes comprehensive unit tests, integration tests, and performance tests covering all three schema formats (JSON Schema, Avro, Protobuf) with extensive data types and nested structures.

## Test Structure

```
tests/
├── conftest.py                    # Pytest configuration and fixtures
├── test_schema_generators.py      # Schema generator tests (JSON, Avro, Protobuf)
├── test_schema_inference.py       # Schema inference engine tests
├── test_format_detection.py       # Format detection and parsing tests
├── test_core_components.py        # Core component tests (consumer, registry, discovery)
└── run_tests.py                   # Test runner script
```

## Test Categories

### 1. Schema Generator Tests (`test_schema_generators.py`)

**Comprehensive coverage of all three schema formats:**

#### JSON Schema Generator Tests
- ✅ **Basic data types**: string, int, float, boolean, null
- ✅ **Nested object structures**: Deep nesting with proper `properties` hierarchy
- ✅ **Array handling**: Arrays of primitives and objects
- ✅ **Field validation**: Required fields, examples, descriptions
- ✅ **Edge cases**: Empty objects, null fields, mixed types

#### Avro Generator Tests
- ✅ **Record structures**: Proper Avro record type generation
- ✅ **Nested records**: Hierarchical record organization
- ✅ **Type mapping**: Correct Avro type conversions
- ✅ **Field sanitization**: Avro-compatible field names
- ✅ **Nullable fields**: Union types with null

#### Protobuf Generator Tests
- ✅ **Message structures**: Proper Protobuf message generation
- ✅ **Nested messages**: Hierarchical message organization
- ✅ **Field numbering**: Sequential field numbering
- ✅ **Type mapping**: Correct Protobuf type conversions
- ✅ **Array handling**: Repeated fields for arrays

#### Cross-Format Tests
- ✅ **Factory pattern**: Schema generator factory
- ✅ **File extensions**: Correct file extensions for each format
- ✅ **Edge cases**: Minimal data, large schemas
- ✅ **Performance**: Large schema handling
- ✅ **Validation**: Schema validity checks

### 2. Schema Inference Tests (`test_schema_inference.py`)

**Comprehensive inference engine testing:**

#### Data Type Inference
- ✅ **Primitive types**: string, int, float, boolean, null
- ✅ **Array types**: Arrays of primitives and objects
- ✅ **Object types**: Nested object structures
- ✅ **Union types**: Mixed data types
- ✅ **Nullable fields**: Optional vs required detection

#### Nested Structure Analysis
- ✅ **Deep nesting**: Multi-level object analysis
- ✅ **Array of objects**: Complex array structures
- ✅ **Field naming**: Dot-notation for nested fields
- ✅ **Depth limiting**: Max depth enforcement
- ✅ **Recursive analysis**: Proper recursive field analysis

#### Configuration Testing
- ✅ **Array handling strategies**: union, first, all
- ✅ **Null handling strategies**: optional, required, ignore
- ✅ **Confidence thresholds**: Type confidence scoring
- ✅ **Max depth settings**: Depth limitation testing

#### Edge Cases
- ✅ **Empty data**: Empty arrays and objects
- ✅ **Single records**: Single record inference
- ✅ **Mixed formats**: Inconsistent data types
- ✅ **Large datasets**: Performance with large data

### 3. Format Detection Tests (`test_format_detection.py`)

**Comprehensive format detection and parsing:**

#### Format Detection
- ✅ **JSON detection**: High confidence JSON format detection
- ✅ **CSV detection**: CSV format with headers and separators
- ✅ **Key-value detection**: Various key-value formats
- ✅ **Raw text detection**: Fallback for unrecognized formats
- ✅ **Mixed formats**: Detection with mixed message types
- ✅ **Binary data**: Handling of binary/unparseable data

#### Parser Testing
- ✅ **JSON parser**: Valid and invalid JSON handling
- ✅ **CSV parser**: Different separators and headers
- ✅ **Key-value parser**: Various separators and quoted values
- ✅ **Raw text parser**: Plain text and special characters
- ✅ **Error handling**: Graceful handling of parse errors

#### Integration Testing
- ✅ **End-to-end flow**: Detection to parsing workflow
- ✅ **Fallback handling**: Fallback to raw text
- ✅ **Factory pattern**: Parser factory testing

### 4. Core Component Tests (`test_core_components.py`)

**Core component functionality testing:**

#### Kafka Consumer Tests
- ✅ **Initialization**: Consumer setup and configuration
- ✅ **Topic listing**: Metadata retrieval and topic listing
- ✅ **Watermark offsets**: Offset range retrieval
- ✅ **Connection handling**: Consumer lifecycle management

#### Schema Registry Tests
- ✅ **Connection testing**: Registry connectivity validation
- ✅ **Schema registration**: Schema upload and management
- ✅ **Subject naming**: Different subject name strategies
- ✅ **Error handling**: Connection and registration failures

#### Topic Discovery Tests
- ✅ **Name-based discovery**: Exact topic name matching
- ✅ **List-based discovery**: Multiple topic selection
- ✅ **Prefix-based discovery**: Topic prefix filtering
- ✅ **Pattern-based discovery**: Regex pattern matching
- ✅ **Internal topic filtering**: Exclude internal topics
- ✅ **Custom filtering**: Additional exclude prefixes
- ✅ **Include patterns**: Pattern-based inclusion

#### Integration Tests
- ✅ **End-to-end workflow**: Complete component integration
- ✅ **Configuration validation**: Config object validation
- ✅ **Error propagation**: Error handling across components

## Test Data

### Comprehensive Test Dataset

The test suite uses a comprehensive dataset covering:

#### Data Types
- **Primitives**: string, int, float, boolean, null
- **Arrays**: Arrays of primitives, objects, and mixed types
- **Objects**: Nested objects with various depths
- **Special cases**: Empty arrays, null fields, mixed types

#### Nested Structures
- **Shallow nesting**: 2-3 levels deep
- **Deep nesting**: 4-5 levels deep
- **Complex arrays**: Arrays of objects with nested structures
- **Mixed nesting**: Objects and arrays combined

#### Edge Cases
- **Empty data**: Empty objects and arrays
- **Binary data**: Non-text data handling
- **Special characters**: Unicode, emojis, symbols
- **Large datasets**: Performance testing data

## Running Tests

### Quick Start

```bash
# Run all tests
python run_tests.py

# Run with coverage
python run_tests.py --coverage

# Run specific test type
python run_tests.py --type generators
python run_tests.py --type inference
python run_tests.py --type format
python run_tests.py --type core

# Run specific test file
python run_tests.py --file test_schema_generators.py

# Run specific test function
python run_tests.py --file test_schema_generators.py --function test_json_schema_generation
```

### Test Categories

```bash
# Unit tests only
python run_tests.py --type unit

# Integration tests only
python run_tests.py --type integration

# Fast tests (exclude slow tests)
python run_tests.py --type fast

# Slow tests only
python run_tests.py --type slow
```

### Advanced Options

```bash
# Verbose output
python run_tests.py --verbose

# Parallel execution
python run_tests.py --parallel

# Check dependencies
python run_tests.py --check-deps
```

### Direct Pytest Usage

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=schema_infer --cov-report=html

# Run specific test file
pytest tests/test_schema_generators.py -v

# Run specific test function
pytest tests/test_schema_generators.py::TestSchemaGenerators::test_json_schema_generation -v
```

## Test Coverage

### Coverage Goals

- **Schema Generators**: 100% coverage for all three formats
- **Schema Inference**: 100% coverage for inference engine
- **Format Detection**: 100% coverage for detection and parsing
- **Core Components**: 90%+ coverage with mocked external dependencies

### Coverage Reports

```bash
# Generate HTML coverage report
python run_tests.py --coverage

# View coverage report
open htmlcov/index.html
```

## Test Fixtures

### Available Fixtures

- `sample_config`: Complete configuration object
- `sample_data`: Comprehensive test dataset
- `json_messages`: JSON format test messages
- `csv_messages`: CSV format test messages
- `key_value_messages`: Key-value format test messages
- `raw_text_messages`: Raw text test messages
- `binary_messages`: Binary data test messages
- `temp_config_file`: Temporary configuration file
- `mock_kafka_metadata`: Mock Kafka metadata
- `mock_schema_registry_response`: Mock Schema Registry response

### Using Fixtures

```python
def test_with_fixture(sample_data, sample_config):
    # Use the fixtures in your tests
    schema = inferrer.infer_schema(sample_data, "test")
    assert len(schema.fields) > 0
```

## Performance Testing

### Performance Benchmarks

The test suite includes performance tests for:

- **Large schema generation**: 100+ field schemas
- **Deep nesting**: 5+ level nested structures
- **Array handling**: Large arrays with complex objects
- **Memory usage**: Memory consumption monitoring

### Running Performance Tests

```bash
# Run performance tests
python run_tests.py --type slow

# Run with benchmark
pytest tests/ --benchmark-only
```

## Continuous Integration

### GitHub Actions Integration

The test suite is designed to work with CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run Tests
  run: |
    pip install -r requirements-test.txt
    python run_tests.py --coverage --parallel
```

### Test Markers

Tests are marked for different execution contexts:

- `@pytest.mark.unit`: Unit tests
- `@pytest.mark.integration`: Integration tests
- `@pytest.mark.slow`: Performance tests
- `@pytest.mark.fast`: Quick tests

## Troubleshooting

### Common Issues

1. **Missing Dependencies**
   ```bash
   python run_tests.py --check-deps
   pip install -r requirements-test.txt
   ```

2. **Import Errors**
   ```bash
   # Ensure you're in the project root
   cd /path/to/project
   python run_tests.py
   ```

3. **Test Failures**
   ```bash
   # Run with verbose output
   python run_tests.py --verbose
   
   # Run specific failing test
   python run_tests.py --file test_schema_generators.py --function test_json_schema_generation
   ```

### Debug Mode

```bash
# Run with debug output
pytest tests/ -v -s --tb=long

# Run single test with debug
pytest tests/test_schema_generators.py::TestSchemaGenerators::test_json_schema_generation -v -s
```

## Contributing

### Adding New Tests

1. **Follow naming conventions**: `test_*.py` files
2. **Use descriptive test names**: `test_json_schema_generation_with_nested_objects`
3. **Add appropriate markers**: `@pytest.mark.unit` or `@pytest.mark.integration`
4. **Use fixtures**: Leverage existing fixtures for consistency
5. **Add docstrings**: Document what each test validates

### Test Requirements

- **Comprehensive coverage**: Test all code paths
- **Edge cases**: Include boundary conditions
- **Error handling**: Test error scenarios
- **Performance**: Include performance considerations
- **Documentation**: Clear test documentation

## Summary

This comprehensive test suite ensures:

- ✅ **All three schema formats** work correctly with nested structures
- ✅ **All data types** are properly handled and converted
- ✅ **Format detection** accurately identifies message formats
- ✅ **Core components** function correctly with proper error handling
- ✅ **Performance** is maintained with large and complex datasets
- ✅ **Edge cases** are handled gracefully
- ✅ **Integration** between components works seamlessly

The test suite provides confidence that the plugin will work correctly across all supported platforms and use cases.
