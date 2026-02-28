"""
CLI Plugin interface for Schema Inference
Version: 1.2.0
Build: 2025-10-12-10:55:00
"""

import logging
import sys
import os
import time
from pathlib import Path
from typing import List, Optional, Set

os.environ['KAFKA_LOG_LEVEL'] = '3'
os.environ['RDKAFKA_LOG_LEVEL'] = '3'

import click

from ..config import Config, load_config
from ..core.inferrer import SchemaInferrer
from ..core.registry import SchemaRegistry
from ..core.discovery import TopicDiscovery
from ..plugin.auth import AuthenticationManager
from ..plugin.optimistic import OptimisticProcessor, SuppressTelemetry
from ..utils.logger import setup_logging

logger = logging.getLogger(__name__)

# Plugin version information
PLUGIN_VERSION = "1.4.3"
PLUGIN_BUILD = "2026-02-28"


def _extract_event_types(main_schema_json: str, topic_name: str) -> set:
    """Extract event type names from an existing oneOf main schema."""
    import json
    try:
        schema = json.loads(main_schema_json)
        types = set()
        for ref in schema.get("oneOf", []):
            ref_name = ref.get("$ref", "")
            if ref_name.startswith(f"{topic_name}-"):
                types.add(ref_name[len(f"{topic_name}-"):])
        return types
    except Exception as e:
        logger.debug("Failed to extract event types from schema for topic '%s': %s", topic_name, e)
        return set()


@click.group()
@click.option(
    "--bootstrap-servers",
    help="Kafka bootstrap servers (e.g., localhost:9092 or pkc-xxxxx.us-west-2.aws.confluent.cloud:9092)",
)
@click.option(
    "--schema-registry-url",
    help="Schema Registry URL (e.g., http://localhost:8081 or https://psrc-xxxxx.us-west-2.aws.confluent.cloud)",
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
    Schema Inference Plugin

    Automatically infer and generate schemas from Kafka topic data.

    \b
    SUPPORTED FORMATS:
      - Avro (.avsc) - For Schema Inference Platform/Cloud integration
      - Protobuf (.proto) - For high-performance applications
      - JSON Schema (.json) - For web APIs and validation

    \b
    SUPPORTED PLATFORMS:
      - Schema Inference Platform (SASL/SSL, PLAINTEXT)
      - Schema Inference Cloud (SASL_SSL with API keys)
      - Any Kafka cluster with Schema Registry

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
    help="Single topic name to process (e.g., 'user-events')",
)
@click.option(
    "--topics",
    help="Comma-separated list of topic names (e.g., 'topic1,topic2,topic3')",
)
@click.option(
    "--topic-prefix",
    help="Prefix to match multiple topics (e.g., 'user-' matches 'user-events', 'user-profiles')",
)
@click.option(
    "--topic-pattern",
    help="Regex pattern to match topics (e.g., '^prod-.*' matches all topics starting with 'prod-')",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["avro", "protobuf", "json-schema"]),
    default="json-schema",
    help="Output schema format (default: json-schema)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Output file path for single topic schema (e.g., 'schema.avsc')",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    help="Output directory for multiple topic schemas (creates one file per topic)",
)
@click.option(
    "--register",
    is_flag=True,
    help="Register generated schema to Schema Registry",
)
@click.option(
    "--max-messages",
    type=int,
    default=50,
    help="Maximum number of messages to sample for schema inference (default: 50)",
)
@click.option(
    "--timeout",
    type=int,
    default=30,
    help="Consumer timeout in seconds (default: 30)",
)
@click.option(
    "--data-format",
    type=click.Choice(["json", "csv", "key-value", "auto"]),
    default="auto",
    help="Force specific data format detection (default: auto-detect)",
)
@click.option(
    "--flatten",
    is_flag=True,
    default=False,
    help="Disable multi-event detection, merge all records into one flat schema",
)
@click.option(
    "--discriminator",
    help="Override auto-detected discriminator field for multi-event schemas (e.g., 'event_type')",
)
@click.option(
    "--exclude-internal",
    is_flag=True,
    default=None,
    help="Exclude internal topics (uses config default if not specified)",
)
@click.option(
    "--internal-prefix",
    help="Prefix for internal topics to exclude (overrides config, e.g., '_schema-infer-')",
)
@click.option(
    "--additional-exclude-prefixes",
    help="Comma-separated list of additional prefixes to exclude (e.g., '_kafka,__consumer_offsets')",
)
@click.option(
    "--context",
    help="Schema Registry context for subject name prefixing (e.g., 'my-context' registers as ':.my-context:topic-value')",
)
@click.option(
    "--show-auth-info",
    is_flag=True,
    help="Show authentication information for debugging",
)
@click.option(
    "--message",
    help="Infer schema from a JSON string instead of Kafka (e.g., '{\"id\": 1, \"name\": \"test\"}')",
)
@click.option(
    "--data-file",
    type=click.Path(exists=True, path_type=Path),
    help="Infer schema from a file containing JSON objects (one per line or a JSON array)",
)
@click.option(
    "--schema-name",
    default="inferred",
    help="Schema name when using --message or --data-file (default: 'inferred')",
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
    flatten: bool,
    discriminator: Optional[str],
    exclude_internal: Optional[bool],
    internal_prefix: Optional[str],
    additional_exclude_prefixes: Optional[str],
    context: Optional[str],
    show_auth_info: bool,
    message: Optional[str],
    data_file: Optional[Path],
    schema_name: str,
) -> None:
    """
    Infer schemas from Kafka topic data

    Reads messages from Kafka topics and automatically generates schemas in your chosen format.
    Uses an optimistic approach to sample the latest messages for accurate schema inference.

    Topics with multiple event types are automatically detected and split into
    per-type schemas with a main oneOf schema using Schema Registry references.
    Use --flatten to disable this and produce a single merged schema.

    \b
    COMMON USE CASES:
      - Generate Avro schemas for Schema Inference Platform/Cloud integration
      - Create Protobuf schemas for high-performance applications
      - Build JSON schemas for API validation and documentation
      - Migrate from schemaless to schema-based data architecture
      - Split multi-event topics into per-type schemas with references

    \b
    HOW IT WORKS:
      1. Connects to your Kafka cluster using provided authentication
      2. Samples the latest messages from specified topics
      3. Analyzes message structure and data types
      4. Generates schema in your chosen format
      5. Optionally registers schema to Schema Registry

    EXAMPLES:

    \b
    # Quick start - single topic with config file
    schema-infer --config cc-config.yaml infer --topic user-events --output user-events-schema.json

    \b
    # Generate Avro schema for Schema Inference Cloud
    schema-infer --config cc-config.yaml infer --topic orders --format avro --register

    \b
    # Process multiple topics at once
    schema-infer --config cc-config.yaml infer --topics events,users,orders --output-dir ./schemas/

    \b
    # Process all topics with a prefix
    schema-infer infer --topic-prefix prod- --format protobuf --output-dir ./protobuf-schemas/

    \b
    # High-volume sampling for complex schemas
    schema-infer infer --topic complex-data --max-messages 5000 --timeout 120

    \b
    # Force specific data format detection
    schema-infer infer --topic csv-data --data-format csv --format json-schema

    \b
    # Infer schema from a JSON string (no Kafka required)
    schema-infer infer --message '{"user_id": "123", "name": "John", "age": 30}' --output user.json

    \b
    # Infer schema from a file (JSON array or one JSON object per line)
    schema-infer infer --data-file sample-data.json --output schema.json --format avro
    """

    config = ctx.obj["config"]

    # Update topic filter configuration from CLI parameters
    if internal_prefix is not None:
        config.topic_filter.internal_prefix = internal_prefix
    if additional_exclude_prefixes is not None:
        config.topic_filter.additional_exclude_prefixes = [p.strip() for p in additional_exclude_prefixes.split(",") if p.strip()]
    if context is not None:
        config.schema_registry.context = context

    # Show authentication info if requested
    if show_auth_info:
        auth_manager = AuthenticationManager(config)
        auth_info = auth_manager.get_authentication_info()
        click.echo("Authentication Information:")
        for key, value in auth_info.items():
            click.echo(f"  {key}: {value}")
        click.echo()

    # Warn if multi-event options used with non-JSON Schema format
    if format != "json-schema" and discriminator:
        click.echo(
            f"Warning: --discriminator is only supported with --format json-schema. "
            f"Using flat schema for {format}."
        )
        discriminator = None

    # Local inference from --message or --data-file (no Kafka required)
    if message or data_file:
        import json as _json

        records = []
        if message:
            try:
                parsed = _json.loads(message)
                records = [parsed] if isinstance(parsed, dict) else parsed
            except _json.JSONDecodeError as e:
                click.echo(f"Error: Invalid JSON in --message: {e}", err=True)
                sys.exit(1)
        elif data_file:
            try:
                content = data_file.read_text()
                try:
                    parsed = _json.loads(content)
                    records = [parsed] if isinstance(parsed, dict) else parsed
                except _json.JSONDecodeError:
                    # Try JSONL (one JSON object per line)
                    records = []
                    for line in content.strip().splitlines():
                        line = line.strip()
                        if line:
                            records.append(_json.loads(line))
            except Exception as e:
                click.echo(f"Error: Failed to read {data_file}: {e}", err=True)
                sys.exit(1)

        if not records:
            click.echo("Error: No records found in input", err=True)
            sys.exit(1)

        # Convert to Kafka message format: List[Tuple[Optional[bytes], bytes]]
        messages_list = [(None, _json.dumps(r).encode("utf-8")) for r in records]

        inferrer = SchemaInferrer(config)
        schema_dict = inferrer.infer_schema(messages_list, schema_name)

        if not schema_dict:
            click.echo("Error: Failed to infer schema from input", err=True)
            sys.exit(1)

        schema_content = inferrer.generate_schema(schema_dict, format)

        if output:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(schema_content)
            click.echo(f"Schema written to {output}")
        else:
            click.echo(schema_content)

        if register:
            registry = SchemaRegistry(config)
            try:
                schema_id = registry.register_schema(schema_name, schema_content, format)
                click.echo(f"Registered schema with ID: {schema_id}")
            except Exception as e:
                click.echo(f"Error registering schema: {e}", err=True)
                sys.exit(1)

        return

    # Validate input — Kafka mode requires topic specification
    if not any([topic, topics, topic_prefix, topic_pattern]):
        click.echo("Error: Must specify either --topic, --topics, --topic-prefix, --topic-pattern, --message, or --data-file", err=True)
        sys.exit(1)

    if not register and not output and not output_dir:
        click.echo("Error: Must specify either --register, --output, or --output-dir", err=True)
        sys.exit(1)

    # Validate input parameters
    if topics and any(pattern in topics for pattern in ['.*', '^', '$', '+', '*', '?', '[', ']', '(', ')']):
        click.echo("Error: Regex patterns like '.*' should be used with --topic-pattern, not --topics", err=True)
        click.echo("Hint: Try --topic-pattern '.*' instead of --topics '.*'", err=True)
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

    click.echo(f"Found {len(topic_list)} topics to process")

    # Update config with CLI options
    config.max_messages = max_messages
    config.timeout = timeout
    config.auto_detect_format = (data_format == "auto")
    config.forced_data_format = data_format if data_format != "auto" else None

    error_details = []  # Track detailed error information

    # Initialize components with shared consumer for connection reuse
    with OptimisticProcessor(config) as processor:
        inferrer = SchemaInferrer(config)
        registry = SchemaRegistry(config) if register else None

        # Process topics - use shared consumer for both single and multiple topics
        if len(topic_list) > 1:
            # Parallel consumer processing for multiple topics
            click.echo(f"\nInferring schemas for {len(topic_list)} topics...")

            from tqdm import tqdm
            import time
            start_time = time.time()

            # Scale processing workers based on topic count
            num_workers = min(config.performance.max_workers, len(topic_list))
            if len(topic_list) > 20:
                num_workers = max(num_workers, 8)
            if len(topic_list) > 100:
                num_workers = max(num_workers, 16)

            # Reader pool: small fixed number of consumer connections
            # to avoid broker saturation (independent of processing workers)
            num_readers = min(10, len(topic_list))

            progress_bar = tqdm(
                total=len(topic_list),
                desc=f"Reading messages ({num_readers} readers)",
                unit="topic",
                disable=not config.performance.show_progress,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                dynamic_ncols=True
            )

            def _progress(completed, total):
                progress_bar.update(1)

            # Read from all topics using a small reader pool (few consumers)
            # Processing workers (num_workers) are used later for inference/registration
            topic_messages = processor.read_topics_parallel(
                topic_list, max_messages, timeout,
                max_readers=num_readers,
                progress_callback=_progress,
            )

            progress_bar.close()

            total_elapsed = time.time() - start_time
            empty_count = len(topic_list) - len(topic_messages)
            if config.performance.show_progress:
                click.echo(
                    f"Message reading completed in {total_elapsed:.1f}s "
                    f"({len(topic_messages)} with data, {empty_count} empty)"
                )

            if topic_messages:
                # Process all topics in parallel with progress bar
                click.echo(f"\nGenerating schemas for {len(topic_messages)} topics...")

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
                    progress_callback=lambda completed, total: schema_progress.update(1),
                    flatten=flatten,
                    discriminator=discriminator,
                )

                schema_progress.close()
                schema_elapsed = time.time() - schema_start_time

                if config.performance.show_progress:
                    multi_count = len(results.get('multi_event', {}))
                    flat_count = len(results.get('schemas', {}))
                    click.echo(
                        f"Schema generation completed in {schema_elapsed:.1f}s"
                        + (f" ({multi_count} multi-event, {flat_count} flat)" if multi_count else "")
                    )

                success_count = results['successful']
                error_count = results['failed']

                # Register schemas if requested
                if register and registry:
                    from concurrent.futures import ThreadPoolExecutor, as_completed
                    from ..utils.validators import validate_generated_schema
                    import threading

                    # Count total registrations needed
                    flat_schemas = results.get('schemas', {})
                    multi_event_topics = results.get('multi_event', {})
                    total_reg = len(flat_schemas) + len(multi_event_topics)

                    if total_reg > 0:
                        reg_workers = min(config.performance.max_workers, total_reg)
                        click.echo(f"\nRegistering schemas to Schema Registry ({reg_workers} workers)...")

                        reg_start = time.time()
                        reg_success = 0
                        reg_fail = 0

                        from ..core.merger import SchemaMerger
                        merger = SchemaMerger()

                        # Register flat schemas in parallel (with merge)
                        if flat_schemas:
                            def _register_one(topic_name, schema_dict):
                                schema_content = inferrer.generate_schema(schema_dict, format)
                                # Merge with existing SR schema (JSON Schema only)
                                if format == "json-schema":
                                    try:
                                        subject = registry._generate_subject_name(topic_name, format)
                                        existing = registry.get_latest_schema(subject)
                                        if existing and "schema" in existing:
                                            schema_content = merger.merge_flat_schemas(
                                                existing["schema"], schema_content
                                            )
                                    except Exception as e:
                                        logger.debug("Skipping schema merge for '%s': %s", topic_name, e)
                                is_valid, validation_error = validate_generated_schema(schema_content, format)
                                if not is_valid:
                                    return (topic_name, False, f"Generated schema is invalid: {validation_error}")
                                schema_id = registry.register_schema(topic_name, schema_content, format)
                                return (topic_name, True, schema_id)

                            reg_progress = tqdm(
                                total=len(flat_schemas),
                                desc=f"Registering flat schemas",
                                unit="schema",
                                disable=not config.performance.show_progress,
                                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                                dynamic_ncols=True,
                            )

                            with ThreadPoolExecutor(max_workers=reg_workers) as reg_executor:
                                future_to_topic = {
                                    reg_executor.submit(_register_one, tn, sd): tn
                                    for tn, sd in flat_schemas.items()
                                }
                                for future in as_completed(future_to_topic):
                                    reg_progress.update(1)
                                    try:
                                        topic_name, ok, result_val = future.result()
                                        if ok:
                                            reg_success += 1
                                        else:
                                            click.echo(f"  FAIL {topic_name}: {result_val}", err=True)
                                            error_count += 1
                                            success_count -= 1
                                            reg_fail += 1
                                    except Exception as e:
                                        tn = future_to_topic[future]
                                        click.echo(f"  FAIL {tn}: Registration failed - {e}", err=True)
                                        error_count += 1
                                        success_count -= 1
                                        reg_fail += 1

                            reg_progress.close()

                        # Register multi-event schemas (with merge)
                        for topic_name, me_result in multi_event_topics.items():
                            try:
                                schema_files = me_result['schema_files']
                                event_types = list(me_result['multi_event_data']['event_schemas'].keys())

                                sub_contents = {
                                    et: schema_files[f"{topic_name}.{et}"]
                                    for et in event_types
                                }
                                main_content = schema_files[topic_name]

                                # Merge with existing SR schemas
                                try:
                                    main_subject = registry._generate_subject_name(topic_name, format)
                                    existing_main = registry.get_latest_schema(main_subject)
                                    if existing_main and "schema" in existing_main:
                                        existing_sub = merger.fetch_existing_sub_schemas(
                                            registry, topic_name,
                                            list(set(event_types) | _extract_event_types(existing_main["schema"], topic_name))
                                        )
                                        merged = merger.merge_multi_event_schemas(
                                            existing_main["schema"], sub_contents,
                                            main_content, topic_name, existing_sub
                                        )
                                        main_content = merged[topic_name]
                                        sub_contents = {
                                            et: merged[f"{topic_name}.{et}"]
                                            for et in sorted(set(sub_contents.keys()) | set(existing_sub.keys()))
                                            if f"{topic_name}.{et}" in merged
                                        }
                                except Exception as e:
                                    logger.debug("Skipping multi-event schema merge for '%s': %s", topic_name, e)

                                reg_result = registry.register_multi_event_schemas(
                                    topic_name, sub_contents, main_content, format
                                )
                                reg_success += len(reg_result)
                                click.echo(
                                    f"  OK {topic_name}: Registered {len(reg_result)} schemas "
                                    f"({len(sub_contents)} sub + 1 main with references)"
                                )
                            except Exception as e:
                                click.echo(f"  FAIL {topic_name}: Multi-event registration failed - {e}", err=True)
                                reg_fail += 1

                        reg_elapsed = time.time() - reg_start
                        click.echo(f"Registration completed in {reg_elapsed:.1f}s ({reg_success} registered, {reg_fail} failed)")
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
                            click.echo(f"  WARN {topic_name}: {error_reason}")
                        progress_bar.set_postfix({
                            'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                            'time': f'{topic_elapsed:.1f}s',
                            'status': 'empty'
                        })
                        error_count += 1
                        progress_bar.update(1)
                        continue

                    progress_bar.set_postfix({
                        'messages': len(messages),
                        'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                        'time': f'{topic_elapsed:.1f}s'
                    })

                    # Try multi-event inference first (JSON Schema only, unless --flatten)
                    multi_event_result = None
                    if not flatten and format == "json-schema":
                        multi_event_result = inferrer.infer_multi_event(
                            messages, topic_name, discriminator_field=discriminator
                        )

                    if multi_event_result:
                        # Multi-event path
                        disc_field = multi_event_result["discriminator_field"]
                        event_schemas = multi_event_result["event_schemas"]
                        event_counts = multi_event_result["event_counts"]
                        event_types = list(event_schemas.keys())

                        click.echo(
                            f"  Detected {len(event_types)} event types via '{disc_field}': "
                            f"{', '.join(f'{t} ({event_counts.get(t, 0)})' for t in event_types)}"
                        )

                        # Generate sub-schemas + main oneOf schema
                        from ..schemas.generators import JSONSchemaGenerator
                        json_gen = JSONSchemaGenerator()

                        # Convert event schema dicts back to InferredSchema objects
                        event_schema_objs = {}
                        for et, sd in event_schemas.items():
                            event_schema_objs[et] = inferrer._dict_to_schema(sd)

                        schema_files = json_gen.generate_multi_event(
                            topic_name, event_schema_objs, disc_field
                        )

                        # Write schema files
                        if output_dir:
                            output_dir.mkdir(parents=True, exist_ok=True)
                            for file_key, content in schema_files.items():
                                schema_file = output_dir / f"{file_key}.json"
                                schema_file.write_text(content)

                        # Register with references (merge with existing if present)
                        if register and registry:
                            try:
                                from ..core.merger import SchemaMerger
                                merger = SchemaMerger()

                                sub_contents = {
                                    et: schema_files[f"{topic_name}.{et}"]
                                    for et in event_types
                                }
                                main_content = schema_files[topic_name]

                                # Check for existing main schema and merge
                                try:
                                    main_subject = registry._generate_subject_name(topic_name, format)
                                    existing_main = registry.get_latest_schema(main_subject)
                                    if existing_main and "schema" in existing_main:
                                        existing_sub = merger.fetch_existing_sub_schemas(
                                            registry, topic_name,
                                            list(set(event_types) | _extract_event_types(existing_main["schema"], topic_name))
                                        )
                                        merged = merger.merge_multi_event_schemas(
                                            existing_main["schema"], sub_contents,
                                            main_content, topic_name, existing_sub
                                        )
                                        # Update with merged results
                                        main_content = merged[topic_name]
                                        sub_contents = {
                                            et: merged[f"{topic_name}.{et}"]
                                            for et in sorted(set(sub_contents.keys()) | set(existing_sub.keys()))
                                            if f"{topic_name}.{et}" in merged
                                        }
                                except Exception as e:
                                    logger.debug("Skipping multi-event schema merge for '%s': %s", topic_name, e)

                                reg_result = registry.register_multi_event_schemas(
                                    topic_name, sub_contents, main_content, format
                                )
                                click.echo(
                                    f"  Registered {len(reg_result)} schemas "
                                    f"({len(sub_contents)} sub-schemas + 1 main with references)"
                                )
                            except Exception as e:
                                click.echo(f"  FAIL {topic_name}: Multi-event registration failed - {e}", err=True)
                                error_count += 1
                                progress_bar.update(1)
                                continue

                        success_count += 1

                    else:
                        # Single flat schema path (original behavior)
                        schema_dict = inferrer.infer_schema(messages, topic_name)

                        if not schema_dict:
                            error_reason = "Could not infer schema - messages may be in unsupported format or corrupted"
                            error_details.append({
                                'topic': topic_name,
                                'reason': error_reason,
                                'type': 'schema_inference_failed'
                            })
                            if not config.performance.show_progress:
                                click.echo(f"  FAIL {topic_name}: {error_reason}")
                            progress_bar.set_postfix({
                                'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                                'time': f'{topic_elapsed:.1f}s',
                                'status': 'failed'
                            })
                            error_count += 1
                            progress_bar.update(1)
                            continue

                        # Generate schema in requested format
                        schema_content = inferrer.generate_schema(schema_dict, format)

                        # Output schema (merge with existing if present, JSON Schema only)
                        if register and registry:
                            if format == "json-schema":
                                try:
                                    from ..core.merger import SchemaMerger
                                    subject = registry._generate_subject_name(topic_name, format)
                                    existing = registry.get_latest_schema(subject)
                                    if existing and "schema" in existing:
                                        merger = SchemaMerger()
                                        schema_content = merger.merge_flat_schemas(
                                            existing["schema"], schema_content
                                        )
                                except Exception as e:
                                    logger.debug("Skipping schema merge for '%s': %s", topic_name, e)

                            from ..utils.validators import validate_generated_schema
                            is_valid, validation_error = validate_generated_schema(schema_content, format)
                            if not is_valid:
                                error_reason = f"Generated schema is invalid: {validation_error}"
                                error_details.append({
                                    'topic': topic_name,
                                    'reason': error_reason,
                                    'type': 'schema_validation_error'
                                })
                                click.echo(f"  FAIL {topic_name}: {error_reason}", err=True)
                                error_count += 1
                                continue
                            try:
                                schema_id = registry.register_schema(topic_name, schema_content, format)
                                if not config.performance.show_progress:
                                    click.echo(f"  OK Registered schema with ID: {schema_id}")
                            except Exception as e:
                                error_reason = f"Failed to register schema to Schema Registry: {str(e)}"
                                error_details.append({
                                    'topic': topic_name,
                                    'reason': error_reason,
                                    'type': 'schema_registry_error'
                                })
                                if not config.performance.show_progress:
                                    click.echo(f"  FAIL {topic_name}: {error_reason}", err=True)
                                error_count += 1
                                progress_bar.update(1)
                                continue

                        if output:
                            output.write_text(schema_content)
                            if not config.performance.show_progress:
                                click.echo(f"  Schema written to: {output}")

                        if output_dir:
                            output_dir.mkdir(parents=True, exist_ok=True)
                            extensions = {"avro": "avsc", "protobuf": "proto", "json-schema": "json"}
                            schema_file = output_dir / f"{topic_name}.{extensions[format]}"
                            schema_file.write_text(schema_content)
                            if not config.performance.show_progress:
                                click.echo(f"  Schema written to: {schema_file}")

                        success_count += 1

                    success_count += 1
                    progress_bar.set_postfix({
                        'messages': len(messages),
                        'topic': topic_name[:20] + '...' if len(topic_name) > 20 else topic_name,
                        'time': f'{topic_elapsed:.1f}s',
                        'status': 'success'
                    })
                    progress_bar.update(1)

                except KeyboardInterrupt:
                    click.echo("\nProcessing interrupted by user")
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
                        click.echo(f"  FAIL {topic_name}: {error_reason}", err=True)
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
                click.echo(f"Processing completed in {total_elapsed:.1f}s")

        # Summary
        click.echo(f"\nResults:")
        click.echo(f"  Successful: {success_count}")
        click.echo(f"  Failed: {error_count}")
        click.echo(f"  Total: {success_count + error_count}")

        # Show detailed error information if there are errors
        if error_count > 0 and error_details:
            click.echo(f"\nError Details:")

            # Group errors by type for better organization
            error_groups = {}
            for error in error_details:
                error_type = error['type']
                if error_type not in error_groups:
                    error_groups[error_type] = []
                error_groups[error_type].append(error)

            # Display errors grouped by type
            for error_type, errors in error_groups.items():
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

                click.echo(f"\n  {type_name} ({len(errors)}):")
                for error in errors:
                    click.echo(f"    - {error['topic']}: {error['reason']}")

            # Show suggestions based on error types
            click.echo(f"\nSuggestions:")
            if any(e['type'] == 'network_error' for e in error_details):
                click.echo(f"  - Check your network connection and Kafka broker addresses")
                click.echo(f"  - Verify bootstrap servers are reachable from your network")
            if any(e['type'] == 'auth_error' for e in error_details):
                click.echo(f"  - Verify your API keys and secrets in the configuration file")
                click.echo(f"  - Check authentication method (SASL_SSL, etc.) matches your cluster")
            if any(e['type'] == 'empty' for e in error_details):
                click.echo(f"  - Topics may be empty or have expired messages")
                click.echo(f"  - Try increasing --max-messages or check topic retention settings")
            if any(e['type'] == 'schema_inference_failed' for e in error_details):
                click.echo(f"  - Messages may be in binary format or unsupported structure")
                click.echo(f"  - Try specifying --data-format explicitly")
            if any(e['type'] == 'schema_validation_error' for e in error_details):
                click.echo(f"  - The generated schema has structural issues")
                click.echo(f"  - Try a different --format or increase --max-messages for better inference")

    if error_count > 0:
        sys.exit(1)


@main.command()
@click.option(
    "--topic-prefix",
    help="Filter topics by prefix (e.g., 'user-' shows only topics starting with 'user-')",
)
@click.option(
    "--topic-pattern",
    help="Filter topics by regex pattern (e.g., '^prod-.*' shows topics starting with 'prod-')",
)
@click.option(
    "--exclude-internal",
    is_flag=True,
    default=None,
    help="Exclude internal topics (uses config default if not specified)",
)
@click.option(
    "--internal-prefix",
    help="Prefix for internal topics to exclude (overrides config, e.g., '_schema-infer-')",
)
@click.option(
    "--additional-exclude-prefixes",
    help="Comma-separated list of additional prefixes to exclude (e.g., '_kafka,__consumer_offsets')",
)
@click.option(
    "--show-metadata",
    is_flag=True,
    help="Show detailed topic metadata (partitions, offsets, etc.)",
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
    List available Kafka topics

    Discover and list all topics in your Kafka cluster with powerful filtering options.
    Perfect for exploring your data landscape before schema inference.

    COMMON USE CASES:
      - Explore available topics in your cluster
      - Find topics matching specific patterns
      - Identify topics for schema inference
      - Debug topic accessibility and permissions

    EXAMPLES:

    \b
    # List all topics (basic overview)
    schema-infer list-topics --config cc-config.yaml

    \b
    # Find topics with specific prefix
    schema-infer list-topics --topic-prefix user- --config cc-config.yaml

    \b
    # Advanced pattern matching
    schema-infer list-topics --topic-pattern ^prod-.* --config cc-config.yaml

    \b
    # Detailed topic information
    schema-infer list-topics --show-metadata --config cc-config.yaml

    \b
    # Exclude internal topics
    schema-infer list-topics --exclude-internal --config cc-config.yaml

    \b
    # Custom internal topic filtering
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
                    click.echo(f"  FAIL {topic_name} (Error: {error})")
                else:
                    click.echo(f"  OK   {topic_name} ({partition_count} partitions)")
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
    help="Comma-separated list of topic names to validate (e.g., 'topic1,topic2,topic3')",
)
@click.option(
    "--topic-prefix",
    help="Prefix to validate all matching topics (e.g., 'user-' validates all topics starting with 'user-')",
)
@click.pass_context
def validate_topics(
    ctx: click.Context,
    topics: Optional[str],
    topic_prefix: Optional[str],
) -> None:
    """
    Validate topic accessibility and permissions

    Test that topics exist and are accessible with your current authentication configuration.
    Perfect for troubleshooting connection issues before running schema inference.

    COMMON USE CASES:
      - Verify topic access before schema inference
      - Troubleshoot authentication and permission issues
      - Test connectivity to specific topics
      - Validate topic names and patterns

    EXAMPLES:

    \b
    # Validate specific topics
    schema-infer validate-topics --topics user-events,orders,payments --config cc-config.yaml

    \b
    # Validate all topics with prefix
    schema-infer validate-topics --topic-prefix prod- --config cc-config.yaml

    \b
    # Quick connectivity test
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
            click.echo(f"\nValid topics ({len(results['valid'])}):")
            for topic in results["valid"]:
                click.echo(f"  {topic}")

        if results["invalid"]:
            click.echo(f"\nInvalid topic names ({len(results['invalid'])}):")
            for item in results["invalid"]:
                click.echo(f"  {item['topic']}: {item['error']}")

        if results["not_found"]:
            click.echo(f"\nTopics not found ({len(results['not_found'])}):")
            for topic in results["not_found"]:
                click.echo(f"  {topic}")

        if results["inaccessible"]:
            click.echo(f"\nInaccessible topics ({len(results['inaccessible'])}):")
            for item in results["inaccessible"]:
                click.echo(f"  {item['topic']}: {item['error']}")

        if results["accessible"]:
            click.echo(f"\nAccessible topics ({len(results['accessible'])}):")
            for topic in results["accessible"]:
                click.echo(f"  {topic}")

        # Summary
        total_issues = len(results["invalid"]) + len(results["not_found"]) + len(results["inaccessible"])
        if total_issues > 0:
            click.echo(f"\nFound {total_issues} issues with topic access")
            sys.exit(1)
        else:
            click.echo(f"\nAll {len(results['accessible'])} topics are accessible!")

    except Exception as e:
        click.echo(f"Error validating topics: {e}", err=True)
        sys.exit(1)


@main.command()
def version() -> None:
    """
    Show plugin version and build information

    Display the current version of the Schema Inference Plugin
    along with build information for debugging and support purposes.
    """
    click.echo(f"Schema Inference Plugin")
    click.echo(f"Version: {PLUGIN_VERSION}")
    click.echo(f"Build: {PLUGIN_BUILD}")
    click.echo(f"Python: {sys.version.split()[0]}")
    click.echo(f"Platform: {sys.platform}")


@main.command()
@click.option(
    "--topic",
    "-t",
    help="Single topic name to monitor",
)
@click.option(
    "--topics",
    help="Comma-separated list of topic names",
)
@click.option(
    "--topic-prefix",
    help="Prefix to match multiple topics",
)
@click.option(
    "--topic-pattern",
    help="Regex pattern to match topics",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["avro", "protobuf", "json-schema"]),
    default="json-schema",
    help="Output schema format (default: json-schema)",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Output directory for schema files",
)
@click.option(
    "--register",
    is_flag=True,
    help="Register/update schemas in Schema Registry",
)
@click.option(
    "--context",
    help="Schema Registry context for subject name prefixing",
)
@click.option(
    "--consumer-group",
    default=None,
    help="Consumer group ID (default: from config or 'schema-infer-live')",
)
@click.option(
    "--batch-size",
    type=int,
    default=None,
    help="Messages per batch before re-inferring (default: 100)",
)
@click.option(
    "--batch-timeout",
    type=float,
    default=None,
    help="Seconds to wait for batch (default: 60.0)",
)
@click.option(
    "--state-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="State persistence directory (default: ~/.schema-infer/state/)",
)
@click.option(
    "--no-persist-state",
    is_flag=True,
    help="Disable state persistence",
)
@click.option(
    "--exclude-internal",
    is_flag=True,
    default=True,
    help="Exclude internal topics (default: True)",
)
@click.option(
    "--data-format",
    type=click.Choice(["json", "csv", "key-value", "auto"]),
    default="auto",
    help="Force specific data format detection (default: auto)",
)
@click.option(
    "--on-incompatible",
    type=click.Choice(["skip", "log", "force", "fail"]),
    default=None,
    help="Behavior when schema is incompatible (default: skip)",
)
@click.option(
    "--from-beginning",
    is_flag=True,
    help="Start consuming from the earliest offset (useful for initial bootstrap). "
         "Only applies when no committed offsets exist for the consumer group.",
)
@click.pass_context
def live(
    ctx: click.Context,
    topic: Optional[str],
    topics: Optional[str],
    topic_prefix: Optional[str],
    topic_pattern: Optional[str],
    format: str,
    output_dir: Optional[Path],
    register: bool,
    context: Optional[str],
    consumer_group: Optional[str],
    batch_size: Optional[int],
    batch_timeout: Optional[float],
    state_dir: Optional[Path],
    no_persist_state: bool,
    exclude_internal: bool,
    data_format: str,
    on_incompatible: Optional[str],
    from_beginning: bool,
) -> None:
    """
    Continuously consume Kafka topics and update schemas as data evolves.

    Unlike 'infer' (which samples once and exits), 'live' continuously reads
    new messages from specified topics, incrementally builds schemas, detects
    schema evolution, and re-registers updated schemas to Schema Registry.
    When using --topic-prefix or --topic-pattern, new topics matching the
    filters are automatically discovered and added to the subscription.

    Consumer offsets are tracked via Kafka consumer groups, so the command
    can resume from where it left off after restart.

    \b
    EXAMPLES:

    \b
    # Monitor a single topic and register schemas
    schema-infer --config config.yaml live --topic orders --register

    \b
    # Monitor multiple topics with custom batch settings
    schema-infer --config config.yaml live \\
      --topics "orders,payments,users" \\
      --register --format avro \\
      --batch-size 200 --batch-timeout 60

    \b
    # Monitor topics by pattern with output to files
    schema-infer --config config.yaml live \\
      --topic-pattern "^prod-.*" \\
      --output-dir ./schemas --register

    \b
    # Force registration on incompatible changes
    schema-infer --config config.yaml live \\
      --topic orders --register --on-incompatible force
    """

    config = ctx.obj["config"]

    # Override persist_state if --no-persist-state is set
    if no_persist_state:
        config.live.persist_state = False

    # Override initial_offset if --from-beginning is set
    if from_beginning:
        config.live.initial_offset = "earliest"

    # Apply CLI overrides to live config
    effective_consumer_group = consumer_group or config.live.consumer_group
    effective_batch_size = batch_size if batch_size is not None else config.live.batch_size
    effective_batch_timeout = batch_timeout if batch_timeout is not None else config.live.batch_timeout_seconds
    effective_on_incompatible = on_incompatible or config.live.on_incompatible

    # Validate input
    if not any([topic, topics, topic_prefix, topic_pattern]):
        click.echo(
            "Error: Must specify either --topic, --topics, --topic-prefix, or --topic-pattern",
            err=True,
        )
        sys.exit(1)

    if not register and not output_dir:
        click.echo(
            "Error: Must specify either --register or --output-dir (or both)",
            err=True,
        )
        sys.exit(1)

    # Discover topics to process
    discovery = TopicDiscovery(config)
    topic_list = discovery.discover_topics(
        topic=topic,
        topics=topics,
        topic_prefix=topic_prefix,
        topic_pattern=topic_pattern,
        exclude_internal=exclude_internal,
    )

    if not topic_list:
        click.echo("Error: No topics found matching the specified criteria", err=True)
        sys.exit(1)

    # Configure inference settings
    config.auto_detect_format = data_format == "auto"
    config.forced_data_format = data_format if data_format != "auto" else None

    # Build discovery kwargs for periodic re-discovery in live mode
    topic_discovery_kwargs = None
    if topic_prefix or topic_pattern:
        topic_discovery_kwargs = {
            "topic": topic,
            "topics": topics,
            "topic_prefix": topic_prefix,
            "topic_pattern": topic_pattern,
            "exclude_internal": exclude_internal,
        }

    # Create and run the orchestrator
    from ..plugin.live import LiveModeOrchestrator

    orchestrator = LiveModeOrchestrator(
        config=config,
        topics=topic_list,
        schema_format=format,
        register=register,
        output_dir=output_dir,
        state_dir=state_dir,
        batch_size=effective_batch_size,
        batch_timeout=effective_batch_timeout,
        consumer_group=effective_consumer_group,
        context=context,
        on_incompatible=effective_on_incompatible,
        data_format=data_format,
        topic_discovery_kwargs=topic_discovery_kwargs,
    )

    orchestrator.run()


if __name__ == "__main__":
    main()
