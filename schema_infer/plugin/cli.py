"""
CLI Plugin interface for Schema Inference
Version: 1.2.0
Build: 2025-10-12-10:55:00
"""

import sys
import os
from pathlib import Path
from typing import List, Optional

os.environ['KAFKA_LOG_LEVEL'] = '7'
os.environ['RDKAFKA_LOG_LEVEL'] = '7'

import click

from ..config import Config, load_config
from ..core.inferrer import SchemaInferrer
from ..core.registry import SchemaRegistry
from ..core.discovery import TopicDiscovery
from ..plugin.auth import AuthenticationManager
from ..plugin.optimistic import OptimisticProcessor, SuppressTelemetry
from ..utils.logger import setup_logging

# Plugin version information
PLUGIN_VERSION = "1.2.0"
PLUGIN_BUILD = "2025-10-12-10:55:00"


@click.group()
@click.option(
    "--bootstrap-servers",
    help="Kafka bootstrap servers (e.g., localhost:9092 or pkc-xxxxx.us-west-2.aws.schema-infer.cloud:9092)",
)
@click.option(
    "--schema-registry-url", 
    help="Schema Registry URL (e.g., http://localhost:8081 or https://psrc-xxxxx.us-west-2.aws.schema-infer.cloud)",
)
@click.option(
    "--log-level",
    default="WARNING",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Logging verbosity level (default: WARNING for clean output)",
)
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Path to YAML configuration file (recommended for complex setups)",
)
@click.version_option(version=PLUGIN_VERSION, prog_name="schema-infer")
@click.pass_context
def main(
    ctx: click.Context,
    bootstrap_servers: Optional[str],
    schema_registry_url: Optional[str], 
    log_level: str,
    config: Optional[Path],
) -> None:
    """
    🚀 Schema Inference Plugin
    
    Automatically infer and generate schemas from Kafka topic data.
    
    \b
    SUPPORTED FORMATS:
      • Avro (.avsc) - For Schema Inference Platform/Cloud integration
      • Protobuf (.proto) - For high-performance applications  
      • JSON Schema (.json) - For web APIs and validation
    
    \b
    SUPPORTED PLATFORMS:
      • Schema Inference Platform (SASL/SSL, PLAINTEXT)
      • Schema Inference Cloud (SASL_SSL with API keys)
      • Any Kafka cluster with Schema Registry
    
    \b
    QUICK START:
      schema-infer --config my-config.yaml infer --topic my-topic
      schema-infer --config my-config.yaml list-topics
    """
    
    # Load configuration
    cfg = load_config(config) if config else Config()
    
    # Override with CLI arguments
    if bootstrap_servers:
        cfg.bootstrap_servers = bootstrap_servers
    if schema_registry_url:
        cfg.schema_registry_url = schema_registry_url
    cfg.log_level = log_level
    
    # Setup logging with minimal verbosity by default
    setup_logging("WARNING", verbose=cfg.performance.verbose_logging)
    
    # Store config in context
    ctx.ensure_object(dict)
    ctx.obj["config"] = cfg


@main.command()
@click.option(
    "--topic",
    "-t",
    help="📝 Single topic name to process (e.g., 'user-events')",
)
@click.option(
    "--topics",
    help="📝 Comma-separated list of topic names (e.g., 'topic1,topic2,topic3')",
)
@click.option(
    "--topic-prefix",
    help="📝 Prefix to match multiple topics (e.g., 'user-' matches 'user-events', 'user-profiles')",
)
@click.option(
    "--topic-pattern",
    help="📝 Regex pattern to match topics (e.g., '^prod-.*' matches all topics starting with 'prod-')",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["avro", "protobuf", "json-schema"]),
    default="avro",
    help="📋 Output schema format (default: avro)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="💾 Output file path for single topic schema (e.g., 'schema.avsc')",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    help="📁 Output directory for multiple topic schemas (creates one file per topic)",
)
@click.option(
    "--register",
    is_flag=True,
    help="📤 Register generated schema to Schema Registry",
)
@click.option(
    "--max-messages",
    type=int,
    default=50,
    help="🔢 Maximum number of messages to sample for schema inference (default: 50)",
)
@click.option(
    "--timeout",
    type=int,
    default=30,
    help="⏱️  Consumer timeout in seconds (default: 30)",
)
@click.option(
    "--data-format",
    type=click.Choice(["json", "csv", "key-value", "auto"]),
    default="auto",
    help="🔍 Force specific data format detection (default: auto-detect)",
)
@click.option(
    "--exclude-internal",
    is_flag=True,
    default=None,
    help="🚫 Exclude internal topics (uses config default if not specified)",
)
@click.option(
    "--internal-prefix",
    help="🚫 Prefix for internal topics to exclude (overrides config, e.g., '_schema-infer-')",
)
@click.option(
    "--additional-exclude-prefixes",
    help="🚫 Comma-separated list of additional prefixes to exclude (e.g., '_kafka,__consumer_offsets')",
)
@click.option(
    "--show-auth-info",
    is_flag=True,
    help="🔐 Show authentication information for debugging",
)
@click.pass_context
def infer(
    ctx: click.Context,
    topic: Optional[str],
    topics: Optional[str],
    topic_prefix: Optional[str],
    topic_pattern: Optional[str],
    format: str,
    output: Optional[Path],
    output_dir: Optional[Path],
    register: bool,
    max_messages: int,
    timeout: int,
    data_format: str,
    exclude_internal: Optional[bool],
    internal_prefix: Optional[str],
    additional_exclude_prefixes: Optional[str],
    show_auth_info: bool,
) -> None:
    """
    🔍 Infer schemas from Kafka topic data
    
    Reads messages from Kafka topics and automatically generates schemas in your chosen format.
    Uses an optimistic approach to sample the latest messages for accurate schema inference.
    
    \b
    🎯 COMMON USE CASES:
      • Generate Avro schemas for Schema Inference Platform/Cloud integration
      • Create Protobuf schemas for high-performance applications
      • Build JSON schemas for API validation and documentation
      • Migrate from schemaless to schema-based data architecture
    
    \b
    📊 HOW IT WORKS:
      1. Connects to your Kafka cluster using provided authentication
      2. Samples the latest messages from specified topics
      3. Analyzes message structure and data types
      4. Generates schema in your chosen format
      5. Optionally registers schema to Schema Registry
    
    💡 EXAMPLES:
    
    \b
    # 🚀 Quick start - single topic with config file
    schema-infer --config cc-config.yaml infer --topic user-events --output user-events-schema.json
    
    \b
    # 📋 Generate Avro schema for Schema Inference Cloud
    schema-infer --config cc-config.yaml infer --topic orders --format avro --register
    
    \b
    # 🔄 Process multiple topics at once
    schema-infer --config cc-config.yaml infer --topics events,users,orders --output-dir ./schemas/
    
    \b
    # 🎯 Process all topics with a prefix
    schema-infer infer --topic-prefix prod- --format protobuf --output-dir ./protobuf-schemas/
    
    \b
    # ⚡ High-volume sampling for complex schemas
    schema-infer infer --topic complex-data --max-messages 5000 --timeout 120
    
    \b
    # 🔍 Force specific data format detection
    schema-infer infer --topic csv-data --data-format csv --format json-schema
    """
    
    config = ctx.obj["config"]
    
    # Update topic filter configuration from CLI parameters
    if internal_prefix is not None:
        config.topic_filter.internal_prefix = internal_prefix
    if additional_exclude_prefixes is not None:
        config.topic_filter.additional_exclude_prefixes = [p.strip() for p in additional_exclude_prefixes.split(",") if p.strip()]
    
    # Show authentication info if requested
    if show_auth_info:
        auth_manager = AuthenticationManager(config)
        auth_info = auth_manager.get_authentication_info()
        click.echo("Authentication Information:")
        for key, value in auth_info.items():
            click.echo(f"  {key}: {value}")
        click.echo()
    
    # Validate input
    if not any([topic, topics, topic_prefix, topic_pattern]):
        click.echo("Error: Must specify either --topic, --topics, --topic-prefix, or --topic-pattern", err=True)
        sys.exit(1)
    
    if not register and not output and not output_dir:
        click.echo("Error: Must specify either --register, --output, or --output-dir", err=True)
        sys.exit(1)
    
    # Validate input parameters
    if topics and any(pattern in topics for pattern in ['.*', '^', '$', '+', '*', '?', '[', ']', '(', ')']):
        click.echo("❌ Error: Regex patterns like '.*' should be used with --topic-pattern, not --topics", err=True)
        click.echo("💡 Try: --topic-pattern '.*' instead of --topics '.*'", err=True)
        sys.exit(1)
    
    # Discover topics to process
    discovery = TopicDiscovery(config)
    topic_list = discovery.discover_topics(
        topic=topic,
        topics=topics,
        topic_prefix=topic_prefix,
        topic_pattern=topic_pattern,
        exclude_internal=exclude_internal
    )
    
    if not topic_list:
        click.echo("Error: No topics found matching the specified criteria", err=True)
        sys.exit(1)
    
    click.echo(f"🔍 Found {len(topic_list)} topics to process")
    
    # Update config with CLI options
    config.max_messages = max_messages
    config.timeout = timeout
    config.auto_detect_format = (data_format == "auto")
    config.forced_data_format = data_format if data_format != "auto" else None
    
    # Initialize components with shared consumer for connection reuse
    with OptimisticProcessor(config) as processor:
        inferrer = SchemaInferrer(config)
        registry = SchemaRegistry(config) if register else None
        
        # Process topics - use shared consumer for both single and multiple topics
        if len(topic_list) > 1:
            # Shared consumer processing for multiple topics (avoids multiple consumer creation)
            click.echo(f"\n🚀 Inferring schemas for {len(topic_list)} topics...")
            
            # Read messages from all topics using shared consumer
            topic_messages = {}
            
            # Create enhanced progress bar with ETA
            from tqdm import tqdm
            import time
            start_time = time.time()
            
            progress_bar = tqdm(
                total=len(topic_list),
                desc="Processing topics",
                unit="topic",
                disable=not config.performance.show_progress,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                dynamic_ncols=True
            )
            
            for i, topic_name in enumerate(topic_list):
                topic_start_time = time.time()
                
                try:
                    if not config.performance.show_progress:
                        click.echo(f"  📝 Processing {topic_name}")
                    
                    # Use shared consumer for multiple topics (avoids multiple consumer creation)
                    messages = processor.read_messages_shared_consumer(topic_name, max_messages, timeout)
                    
                    topic_elapsed = time.time() - topic_start_time
                    
                    if messages:
                        topic_messages[topic_name] = messages
                        progress_bar.set_postfix({
                            'messages': len(messages),
                            'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                            'time': f'{topic_elapsed:.1f}s'
                        })
                    else:
                        if not config.performance.show_progress:
                            click.echo(f"  ⚠️  {topic_name} - no messages found")
                        progress_bar.set_postfix({
                            'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                            'time': f'{topic_elapsed:.1f}s',
                            'status': 'empty'
                        })
                except Exception as e:
                    topic_elapsed = time.time() - topic_start_time
                    if not config.performance.show_progress:
                        click.echo(f"  ❌ {topic_name} - failed to process")
                    progress_bar.set_postfix({
                        'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                        'time': f'{topic_elapsed:.1f}s',
                        'status': 'error'
                    })
                
                progress_bar.update(1)
            
            progress_bar.close()
            
            # Show overall timing
            total_elapsed = time.time() - start_time
            if config.performance.show_progress:
                click.echo(f"📊 Message reading completed in {total_elapsed:.1f}s")
        
            if topic_messages:
                # Process all topics in parallel with progress bar
                click.echo(f"\n🔄 Generating schemas for {len(topic_messages)} topics...")
                
                schema_start_time = time.time()
                schema_progress = tqdm(
                    total=len(topic_messages),
                    desc="Generating schemas",
                    unit="schema",
                    disable=not config.performance.show_progress,
                    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                    dynamic_ncols=True
                )
                
                results = inferrer.process_topics_parallel(
                    topic_messages=topic_messages,
                    output_format=format,
                    output_dir=output_dir,
                    progress_callback=lambda completed, total: schema_progress.update(1)
                )
                
                schema_progress.close()
                schema_elapsed = time.time() - schema_start_time
                
                if config.performance.show_progress:
                    click.echo(f"📊 Schema generation completed in {schema_elapsed:.1f}s")
                
                success_count = results['successful']
                error_count = results['failed']

                # Register schemas if requested
                if register and registry and results['schemas']:
                    click.echo(f"\n📤 Registering {len(results['schemas'])} schemas to Schema Registry...")
                    for topic_name, schema_dict in results['schemas'].items():
                        try:
                            schema_content = inferrer.generate_schema(schema_dict, format)
                            # Validate schema before registration
                            from ..utils.validators import validate_generated_schema
                            is_valid, validation_error = validate_generated_schema(schema_content, format)
                            if not is_valid:
                                click.echo(f"  ❌ {topic_name}: Generated schema is invalid: {validation_error}", err=True)
                                error_count += 1
                                success_count -= 1
                                continue
                            schema_id = registry.register_schema(topic_name, schema_content, format)
                            click.echo(f"  ✅ {topic_name}: Registered with ID {schema_id}")
                        except Exception as e:
                            click.echo(f"  ❌ {topic_name}: Registration failed - {e}", err=True)
                            error_count += 1
                            success_count -= 1
            else:
                success_count = 0
                error_count = len(topic_list)
        else:
            # Single topic processing with progress bar
            success_count = 0
            error_count = 0
            error_details = []  # Store detailed error information
            
            # Create progress bar for single topic processing
            from tqdm import tqdm
            import time
            start_time = time.time()
            
            progress_bar = tqdm(
                total=len(topic_list),
                desc="Processing topic",
                unit="topic",
                disable=True,  # No progress bar for single topic
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                dynamic_ncols=True
            )
            
            for topic_name in topic_list:
                try:
                    # Don't show this message when progress bar is enabled
                    # if not config.performance.show_progress:
                    #     click.echo(f"\n  📝 Processing {topic_name}")
                    
                    topic_start_time = time.time()
                    
                    # Read messages using shared consumer for better performance
                    messages = processor.read_messages_shared_consumer(topic_name, max_messages, timeout)
                    
                    topic_elapsed = time.time() - topic_start_time
                    
                    if not messages:
                        error_reason = "No messages found - topic may be empty or all messages expired"
                        error_details.append({
                            'topic': topic_name,
                            'reason': error_reason,
                            'type': 'empty'
                        })
                        if not config.performance.show_progress:
                            click.echo(f"  ⚠️  {topic_name}: {error_reason}")
                        progress_bar.set_postfix({
                            'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                            'time': f'{topic_elapsed:.1f}s',
                            'status': 'empty'
                        })
                        error_count += 1
                        progress_bar.update(1)
                        continue
                    
                    # Suppress this message when progress bar is enabled
                    # if not config.performance.show_progress:
                    #     click.echo(f"  📥 Read {len(messages)} messages")
                    
                    progress_bar.set_postfix({
                        'messages': len(messages),
                        'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                        'time': f'{topic_elapsed:.1f}s'
                    })
                    
                    # Infer schema
                    schema_dict = inferrer.infer_schema(messages, topic_name)
                    
                    if not schema_dict:
                        error_reason = "Could not infer schema - messages may be in unsupported format or corrupted"
                        error_details.append({
                            'topic': topic_name,
                            'reason': error_reason,
                            'type': 'schema_inference_failed'
                        })
                        if not config.performance.show_progress:
                            click.echo(f"  ❌ {topic_name}: {error_reason}")
                        progress_bar.set_postfix({
                            'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                            'time': f'{topic_elapsed:.1f}s',
                            'status': 'failed'
                        })
                        error_count += 1
                        progress_bar.update(1)
                        continue
                    
                    # Show inference metadata
                    metadata = schema_dict.get("_metadata", {})
                    detected_format = metadata.get("format", "unknown")
                    confidence = metadata.get("confidence", 0)
                    if not config.performance.show_progress:
                        click.echo(f"  📋 Format: {detected_format}")
                    
                    # Generate schema in requested format
                    schema_content = inferrer.generate_schema(schema_dict, format)
                
                    # Output schema
                    if register and registry:
                        # Validate schema before registration
                        from ..utils.validators import validate_generated_schema
                        is_valid, validation_error = validate_generated_schema(schema_content, format)
                        if not is_valid:
                            error_reason = f"Generated schema is invalid: {validation_error}"
                            error_details.append({
                                'topic': topic_name,
                                'reason': error_reason,
                                'type': 'schema_validation_error'
                            })
                            click.echo(f"  ❌ {topic_name}: {error_reason}", err=True)
                            error_count += 1
                            continue
                        try:
                            schema_id = registry.register_schema(topic_name, schema_content, format)
                            if not config.performance.show_progress:
                                click.echo(f"  ✅ Registered schema with ID: {schema_id}")
                        except Exception as e:
                            error_reason = f"Failed to register schema to Schema Registry: {str(e)}"
                            error_details.append({
                                'topic': topic_name,
                                'reason': error_reason,
                                'type': 'schema_registry_error'
                            })
                            if not config.performance.show_progress:
                                click.echo(f"  ❌ {topic_name}: {error_reason}", err=True)
                            error_count += 1
                            progress_bar.update(1)
                            continue
                    
                    if output:
                        output.write_text(schema_content)
                        if not config.performance.show_progress:
                            click.echo(f"  💾 Schema written to: {output}")
                    
                    if output_dir:
                        output_dir.mkdir(parents=True, exist_ok=True)
                        extensions = {"avro": "avsc", "protobuf": "proto", "json-schema": "json"}
                        schema_file = output_dir / f"{topic_name}.{extensions[format]}"
                        schema_file.write_text(schema_content)
                        if not config.performance.show_progress:
                            click.echo(f"  💾 Schema written to: {schema_file}")
                    
                    success_count += 1
                    progress_bar.set_postfix({
                        'messages': len(messages),
                        'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                        'time': f'{topic_elapsed:.1f}s',
                        'status': 'success'
                    })
                    progress_bar.update(1)
                    
                except KeyboardInterrupt:
                    click.echo("\n⚠️  Processing interrupted by user")
                    break
                except Exception as e:
                    # Determine error type and reason
                    error_str = str(e)
                    if "Failed to resolve" in error_str or "nodename nor servname provided" in error_str:
                        error_reason = "Network connectivity issue - cannot reach Kafka broker"
                        error_type = "network_error"
                    elif "Authentication" in error_str or "SASL" in error_str:
                        error_reason = "Authentication failed - check credentials and configuration"
                        error_type = "auth_error"
                    elif "Topic not found" in error_str or "UnknownTopicOrPartition" in error_str:
                        error_reason = "Topic does not exist or is not accessible"
                        error_type = "topic_not_found"
                    elif "Permission denied" in error_str or "Not authorized" in error_str:
                        error_reason = "Permission denied - insufficient access rights"
                        error_type = "permission_error"
                    else:
                        error_reason = f"Processing error: {error_str}"
                        error_type = "processing_error"
                    
                    error_details.append({
                        'topic': topic_name,
                        'reason': error_reason,
                        'type': error_type
                    })
                    
                    if not config.performance.show_progress:
                        click.echo(f"  ❌ {topic_name}: {error_reason}", err=True)
                    progress_bar.set_postfix({
                        'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                        'time': f'{topic_elapsed:.1f}s',
                        'status': 'error'
                    })
                    error_count += 1
                    progress_bar.update(1)
                    continue
            
            progress_bar.close()
            
            # Show overall timing
            total_elapsed = time.time() - start_time
            if config.performance.show_progress:
                click.echo(f"📊 Processing completed in {total_elapsed:.1f}s")
        
        # Summary
        click.echo(f"\n📊 Results:")
        click.echo(f"  ✅ Successful: {success_count}")
        click.echo(f"  ❌ Failed: {error_count}")
        click.echo(f"  📈 Total: {success_count + error_count}")
        
        # Show detailed error information if there are errors
        if error_count > 0 and error_details:
            click.echo(f"\n🔍 Error Details:")
            
            # Group errors by type for better organization
            error_groups = {}
            for error in error_details:
                error_type = error['type']
                if error_type not in error_groups:
                    error_groups[error_type] = []
                error_groups[error_type].append(error)
            
            # Display errors grouped by type
            for error_type, errors in error_groups.items():
                type_emoji = {
                    'empty': '📭',
                    'network_error': '🌐',
                    'auth_error': '🔐',
                    'topic_not_found': '❓',
                    'permission_error': '🚫',
                    'schema_inference_failed': '🧠',
                    'schema_registry_error': '📋',
                    'schema_validation_error': '🔎',
                    'processing_error': '⚙️'
                }.get(error_type, '❌')

                type_name = {
                    'empty': 'Empty Topics',
                    'network_error': 'Network Issues',
                    'auth_error': 'Authentication Issues',
                    'topic_not_found': 'Topic Not Found',
                    'permission_error': 'Permission Issues',
                    'schema_inference_failed': 'Schema Inference Failed',
                    'schema_registry_error': 'Schema Registry Issues',
                    'schema_validation_error': 'Schema Validation Errors',
                    'processing_error': 'Processing Errors'
                }.get(error_type, 'Other Errors')
                
                click.echo(f"\n  {type_emoji} {type_name} ({len(errors)}):")
                for error in errors:
                    click.echo(f"    • {error['topic']}: {error['reason']}")
            
            # Show suggestions based on error types
            click.echo(f"\n💡 Suggestions:")
            if any(e['type'] == 'network_error' for e in error_details):
                click.echo(f"  • Check your network connection and Kafka broker addresses")
                click.echo(f"  • Verify bootstrap servers are reachable from your network")
            if any(e['type'] == 'auth_error' for e in error_details):
                click.echo(f"  • Verify your API keys and secrets in the configuration file")
                click.echo(f"  • Check authentication method (SASL_SSL, etc.) matches your cluster")
            if any(e['type'] == 'empty' for e in error_details):
                click.echo(f"  • Topics may be empty or have expired messages")
                click.echo(f"  • Try increasing --max-messages or check topic retention settings")
            if any(e['type'] == 'schema_inference_failed' for e in error_details):
                click.echo(f"  • Messages may be in binary format or unsupported structure")
                click.echo(f"  • Try specifying --data-format explicitly")
            if any(e['type'] == 'schema_validation_error' for e in error_details):
                click.echo(f"  • The generated schema has structural issues")
                click.echo(f"  • Try a different --format or increase --max-messages for better inference")
    
    if error_count > 0:
        sys.exit(1)


@main.command()
@click.option(
    "--topic-prefix",
    help="📝 Filter topics by prefix (e.g., 'user-' shows only topics starting with 'user-')",
)
@click.option(
    "--topic-pattern",
    help="📝 Filter topics by regex pattern (e.g., '^prod-.*' shows topics starting with 'prod-')",
)
@click.option(
    "--exclude-internal",
    is_flag=True,
    default=None,
    help="🚫 Exclude internal topics (uses config default if not specified)",
)
@click.option(
    "--internal-prefix",
    help="🚫 Prefix for internal topics to exclude (overrides config, e.g., '_schema-infer-')",
)
@click.option(
    "--additional-exclude-prefixes",
    help="🚫 Comma-separated list of additional prefixes to exclude (e.g., '_kafka,__consumer_offsets')",
)
@click.option(
    "--show-metadata",
    is_flag=True,
    help="📊 Show detailed topic metadata (partitions, offsets, etc.)",
)
@click.pass_context
def list_topics(
    ctx: click.Context,
    topic_prefix: Optional[str],
    topic_pattern: Optional[str],
    exclude_internal: Optional[bool],
    internal_prefix: Optional[str],
    additional_exclude_prefixes: Optional[str],
    show_metadata: bool,
) -> None:
    """
    📋 List available Kafka topics
    
    Discover and list all topics in your Kafka cluster with powerful filtering options.
    Perfect for exploring your data landscape before schema inference.
    
    🎯 COMMON USE CASES:
      • Explore available topics in your cluster
      • Find topics matching specific patterns
      • Identify topics for schema inference
      • Debug topic accessibility and permissions
    
    💡 EXAMPLES:
    
    \b
    # 📋 List all topics (basic overview)
    schema-infer list-topics --config cc-config.yaml
    
    \b
    # 🎯 Find topics with specific prefix
    schema-infer list-topics --topic-prefix user- --config cc-config.yaml
    
    \b
    # 🔍 Advanced pattern matching
    schema-infer list-topics --topic-pattern ^prod-.* --config cc-config.yaml
    
    \b
    # 📊 Detailed topic information
    schema-infer list-topics --show-metadata --config cc-config.yaml
    
    \b
    # 🚫 Exclude internal topics
    schema-infer list-topics --exclude-internal --config cc-config.yaml
    
    \b
    # 🎯 Custom internal topic filtering
    schema-infer list-topics --internal-prefix _schema-infer- --config cc-config.yaml
    """
    
    config = ctx.obj["config"]
    
    # Update topic filter configuration from CLI parameters
    if internal_prefix is not None:
        config.topic_filter.internal_prefix = internal_prefix
    if additional_exclude_prefixes is not None:
        config.topic_filter.additional_exclude_prefixes = [p.strip() for p in additional_exclude_prefixes.split(",") if p.strip()]
    
    try:
        discovery = TopicDiscovery(config)
        
        # Discover topics
        topic_list = discovery.discover_topics(
            topic_prefix=topic_prefix,
            topic_pattern=topic_pattern,
            exclude_internal=exclude_internal
        )
        
        if not topic_list:
            click.echo("No topics found matching the specified criteria")
            return
        
        click.echo(f"Found {len(topic_list)} topics:")
        
        if show_metadata:
            # Get metadata for all topics
            metadata = discovery.get_topic_metadata(topic_list)
            
            for topic_name in sorted(topic_list):
                topic_meta = metadata.get(topic_name, {})
                partition_count = len(topic_meta.get("partition_info", {}))
                error = topic_meta.get("error")
                
                if error:
                    click.echo(f"  ❌ {topic_name} (Error: {error})")
                else:
                    click.echo(f"  ✅ {topic_name} ({partition_count} partitions)")
        else:
            # Simple list
            for topic_name in sorted(topic_list):
                click.echo(f"  {topic_name}")
                
    except Exception as e:
        click.echo(f"Error listing topics: {e}", err=True)
        sys.exit(1)


@main.command()
@click.option(
    "--topics",
    help="📝 Comma-separated list of topic names to validate (e.g., 'topic1,topic2,topic3')",
)
@click.option(
    "--topic-prefix",
    help="📝 Prefix to validate all matching topics (e.g., 'user-' validates all topics starting with 'user-')",
)
@click.pass_context
def validate_topics(
    ctx: click.Context,
    topics: Optional[str],
    topic_prefix: Optional[str],
) -> None:
    """
    ✅ Validate topic accessibility and permissions
    
    Test that topics exist and are accessible with your current authentication configuration.
    Perfect for troubleshooting connection issues before running schema inference.
    
    🎯 COMMON USE CASES:
      • Verify topic access before schema inference
      • Troubleshoot authentication and permission issues
      • Test connectivity to specific topics
      • Validate topic names and patterns
    
    💡 EXAMPLES:
    
    \b
    # ✅ Validate specific topics
    schema-infer validate-topics --topics user-events,orders,payments --config cc-config.yaml
    
    \b
    # 🎯 Validate all topics with prefix
    schema-infer validate-topics --topic-prefix prod- --config cc-config.yaml
    
    \b
    # 🔍 Quick connectivity test
    schema-infer validate-topics --topics test-topic --config cc-config.yaml
    """
    
    config = ctx.obj["config"]
    
    if not topics and not topic_prefix:
        click.echo("Error: Must specify either --topics or --topic-prefix", err=True)
        sys.exit(1)
    
    try:
        discovery = TopicDiscovery(config)
        
        # Get topics to validate
        if topics:
            topic_list = [t.strip() for t in topics.split(",")]
        else:
            topic_list = discovery.discover_topics(topic_prefix=topic_prefix)
        
        if not topic_list:
            click.echo("No topics found to validate")
            return
        
        click.echo(f"Validating {len(topic_list)} topics...")
        
        # Validate topics
        results = discovery.validate_topics(topic_list)
        
        # Display results
        if results["valid"]:
            click.echo(f"\n✅ Valid topics ({len(results['valid'])}):")
            for topic in results["valid"]:
                click.echo(f"  {topic}")
        
        if results["invalid"]:
            click.echo(f"\n❌ Invalid topic names ({len(results['invalid'])}):")
            for item in results["invalid"]:
                click.echo(f"  {item['topic']}: {item['error']}")
        
        if results["not_found"]:
            click.echo(f"\n🔍 Topics not found ({len(results['not_found'])}):")
            for topic in results["not_found"]:
                click.echo(f"  {topic}")
        
        if results["inaccessible"]:
            click.echo(f"\n🚫 Inaccessible topics ({len(results['inaccessible'])}):")
            for item in results["inaccessible"]:
                click.echo(f"  {item['topic']}: {item['error']}")
        
        if results["accessible"]:
            click.echo(f"\n🎯 Accessible topics ({len(results['accessible'])}):")
            for topic in results["accessible"]:
                click.echo(f"  {topic}")
        
        # Summary
        total_issues = len(results["invalid"]) + len(results["not_found"]) + len(results["inaccessible"])
        if total_issues > 0:
            click.echo(f"\n⚠️  Found {total_issues} issues with topic access")
            sys.exit(1)
        else:
            click.echo(f"\n🎉 All {len(results['accessible'])} topics are accessible!")
            
    except Exception as e:
        click.echo(f"Error validating topics: {e}", err=True)
        sys.exit(1)


@main.command()
def version() -> None:
    """
    📋 Show plugin version and build information
    
    Display the current version of the Schema Inference Schema Inference Plugin
    along with build information for debugging and support purposes.
    """
    click.echo(f"Schema Inference Schema Inference Plugin")
    click.echo(f"Version: {PLUGIN_VERSION}")
    click.echo(f"Build: {PLUGIN_BUILD}")
    click.echo(f"Python: {sys.version.split()[0]}")
    click.echo(f"Platform: {sys.platform}")


if __name__ == "__main__":
    main()
