#!/usr/bin/env python3
"""
Basic usage examples for Schema Inference Plugin
"""

import asyncio
from pathlib import Path

from schema_infer import SchemaInferrer, KafkaConsumer, SchemaRegistry
from schema_infer.config import Config


def example_basic_inference():
    """Example of basic schema inference."""
    
    # Create configuration
    config = Config()
    config.bootstrap_servers = "localhost:9092"
    config.schema_registry_url = "http://localhost:8081"
    config.max_messages = 100
    
    # Initialize components
    consumer = KafkaConsumer(config)
    inferrer = SchemaInferrer(config)
    
    try:
        # Consume messages from a topic
        messages = consumer.consume_topic("my-topic", max_messages=100, timeout=30)
        
        # Infer schema
        schema_dict = inferrer.infer_schema(messages, "my-topic")
        
        if schema_dict:
            # Generate Avro schema
            avro_schema = inferrer.generate_schema(schema_dict, "avro")
            print("Generated Avro Schema:")
            print(avro_schema)
            
            # Save to file
            with open("my-topic.avsc", "w") as f:
                f.write(avro_schema)
    
    finally:
        consumer.close()


def example_multiple_formats():
    """Example of generating multiple schema formats."""
    
    config = Config()
    consumer = KafkaConsumer(config)
    inferrer = SchemaInferrer(config)
    
    try:
        # Consume messages
        messages = consumer.consume_topic("user-events", max_messages=200, timeout=30)
        
        # Infer schema
        schema_dict = inferrer.infer_schema(messages, "user-events")
        
        if schema_dict:
            # Generate different formats
            formats = ["avro", "protobuf", "json-schema"]
            
            for fmt in formats:
                schema_content = inferrer.generate_schema(schema_dict, fmt)
                
                # Save with appropriate extension
                extensions = {"avro": "avsc", "protobuf": "proto", "json-schema": "json"}
                filename = f"user-events.{extensions[fmt]}"
                
                with open(filename, "w") as f:
                    f.write(schema_content)
                
                print(f"Generated {fmt} schema: {filename}")
    
    finally:
        consumer.close()


def example_schema_registry():
    """Example of registering schemas to Schema Registry."""
    
    config = Config()
    consumer = KafkaConsumer(config)
    inferrer = SchemaInferrer(config)
    registry = SchemaRegistry(config)
    
    try:
        # Test connection
        if not registry.test_connection():
            print("Failed to connect to Schema Registry")
            return
        
        # Consume messages
        messages = consumer.consume_topic("orders", max_messages=150, timeout=30)
        
        # Infer schema
        schema_dict = inferrer.infer_schema(messages, "orders")
        
        if schema_dict:
            # Generate and register Avro schema
            avro_schema = inferrer.generate_schema(schema_dict, "avro")
            schema_id = registry.register_schema("orders", avro_schema, "avro")
            
            print(f"Registered schema with ID: {schema_id}")
    
    finally:
        consumer.close()


async def example_async_processing():
    """Example of asynchronous processing."""
    
    config = Config()
    config.background = True
    config.max_workers = 4
    
    consumer = KafkaConsumer(config)
    inferrer = SchemaInferrer(config)
    
    try:
        # Process multiple topics asynchronously
        topics = ["topic1", "topic2", "topic3"]
        
        async def process_topic(topic_name):
            messages = consumer.consume_topic(topic_name, max_messages=50, timeout=20)
            schema_dict = inferrer.infer_schema(messages, topic_name)
            
            if schema_dict:
                schema_content = inferrer.generate_schema(schema_dict, "avro")
                return {"topic": topic_name, "schema": schema_content}
            return None
        
        # Process all topics concurrently
        tasks = [process_topic(topic) for topic in topics]
        results = await asyncio.gather(*tasks)
        
        # Save results
        for result in results:
            if result:
                filename = f"{result['topic']}.avsc"
                with open(filename, "w") as f:
                    f.write(result["schema"])
                print(f"Processed {result['topic']}: {filename}")
    
    finally:
        consumer.close()


def example_custom_configuration():
    """Example with custom configuration."""
    
    # Load from file
    config_path = Path("schema-infer.yaml")
    if config_path.exists():
        from schema_infer.config import load_config
        config = load_config(config_path)
    else:
        # Create custom config
        config = Config()
        config.kafka.bootstrap_servers = "kafka-cluster:9092"
        config.kafka.security_protocol = "SASL_SSL"
        config.kafka.sasl_mechanism = "PLAIN"
        config.kafka.sasl_username = "user"
        config.kafka.sasl_password = "password"
        
        config.schema_registry.url = "https://schema-registry:8081"
        config.schema_registry.username = "user"
        config.schema_registry.password = "password"
        
        config.inference.max_messages = 2000
        config.inference.confidence_threshold = 0.9
        config.inference.auto_detect_format = True
        
        config.performance.max_workers = 8
        config.performance.batch_size = 200
    
    # Use the configuration
    consumer = KafkaConsumer(config)
    inferrer = SchemaInferrer(config)
    
    try:
        messages = consumer.consume_topic("production-topic", 
                                        max_messages=config.inference.max_messages, 
                                        timeout=config.inference.timeout)
        
        schema_dict = inferrer.infer_schema(messages, "production-topic")
        
        if schema_dict:
            print(f"Schema inference completed for {len(messages)} messages")
            print(f"Detected format: {schema_dict.get('_metadata', {}).get('format', 'unknown')}")
            print(f"Confidence: {schema_dict.get('_metadata', {}).get('confidence', 0):.2f}")
    
    finally:
        consumer.close()


if __name__ == "__main__":
    print("Schema Inference Plugin Examples")
    print("=" * 40)
    
    # Run examples
    try:
        print("\n1. Basic Inference Example:")
        example_basic_inference()
        
        print("\n2. Multiple Formats Example:")
        example_multiple_formats()
        
        print("\n3. Schema Registry Example:")
        example_schema_registry()
        
        print("\n4. Async Processing Example:")
        asyncio.run(example_async_processing())
        
        print("\n5. Custom Configuration Example:")
        example_custom_configuration()
        
    except Exception as e:
        print(f"Error running examples: {e}")
        print("Make sure Kafka and Schema Registry are running and accessible.")
