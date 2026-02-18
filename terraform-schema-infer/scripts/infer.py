#!/usr/bin/env python3
"""
Terraform external data source wrapper for schema-infer.

Supports two configuration modes:
  A) config_file: path to an existing YAML config file
  B) Inline variables: bootstrap_servers, kafka_api_key, etc.
     (generates a temporary config file automatically)

Reads JSON query from stdin, ensures schema-infer is installed,
runs inference, and returns the schema as JSON on stdout.
"""

import json
import os
import subprocess
import sys
import shutil
import tempfile


PACKAGE_NAME = "schema-infer"
PACKAGE_SOURCE = "git+https://github.com/akrishnanDG/schema-infer-plugin.git"


def ensure_installed():
    """Install schema-infer if not already available."""
    if shutil.which("schema-infer") is not None:
        return

    sys.stderr.write(f"schema-infer not found. Installing from {PACKAGE_SOURCE}...\n")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", PACKAGE_SOURCE],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as e:
        json.dump({"error": f"Failed to install {PACKAGE_NAME}: {e}"}, sys.stdout)
        sys.exit(1)

    if shutil.which("schema-infer") is None:
        json.dump(
            {"error": f"{PACKAGE_NAME} installed but 'schema-infer' command not found on PATH"},
            sys.stdout,
        )
        sys.exit(1)

    sys.stderr.write("schema-infer installed successfully.\n")


def build_config_yaml(query):
    """Build a YAML config string from inline Terraform variables."""
    bootstrap_servers = query.get("bootstrap_servers", "")
    kafka_api_key = query.get("kafka_api_key", "")
    kafka_api_secret = query.get("kafka_api_secret", "")
    security_protocol = query.get("security_protocol", "")
    schema_registry_url = query.get("schema_registry_url", "")
    sr_api_key = query.get("sr_api_key", "")
    sr_api_secret = query.get("sr_api_secret", "")

    # Auto-detect security protocol from bootstrap server hostname
    if not security_protocol:
        if "confluent.cloud" in bootstrap_servers.lower():
            security_protocol = "SASL_SSL"
        else:
            security_protocol = "PLAINTEXT"

    lines = [
        "kafka:",
        f'  bootstrap_servers: "{bootstrap_servers}"',
        f'  security_protocol: "{security_protocol}"',
    ]

    if kafka_api_key:
        lines.append(f'  cloud_api_key: "{kafka_api_key}"')
    if kafka_api_secret:
        lines.append(f'  cloud_api_secret: "{kafka_api_secret}"')

    if schema_registry_url:
        lines.append("")
        lines.append("schema_registry:")
        lines.append(f'  url: "{schema_registry_url}"')
        if sr_api_key:
            lines.append(f'  cloud_api_key: "{sr_api_key}"')
        if sr_api_secret:
            lines.append(f'  cloud_api_secret: "{sr_api_secret}"')

    lines.append("")
    lines.append("inference:")
    lines.append(f"  max_messages: {query.get('max_messages', '100')}")
    lines.append("  timeout: 30")

    return "\n".join(lines)


def resolve_config(query):
    """
    Resolve the config file path.

    If config_file is provided, use it directly.
    Otherwise, generate a temporary config from inline variables.

    Returns (config_path, temp_file_or_None).
    """
    config_file = query.get("config_file", "")
    if config_file:
        return config_file, None

    # Validate that inline variables are provided
    bootstrap_servers = query.get("bootstrap_servers", "")
    if not bootstrap_servers:
        json.dump(
            {"error": "Either config_file or bootstrap_servers must be provided"},
            sys.stdout,
        )
        sys.exit(1)

    # Generate temporary config file
    yaml_content = build_config_yaml(query)
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="schema-infer-", delete=False
    )
    tmp.write(yaml_content)
    tmp.close()
    return tmp.name, tmp.name


def main():
    query = json.load(sys.stdin)
    topic = query["topic"]
    fmt = query["format"]
    max_messages = query["max_messages"]

    ensure_installed()

    config_path, temp_path = resolve_config(query)

    # Write schema to a temp file to avoid mixing with CLI progress output on stdout
    schema_tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".schema", prefix="schema-infer-out-", delete=False
    )
    schema_tmp.close()
    schema_output_path = schema_tmp.name

    try:
        result = subprocess.run(
            [
                "schema-infer",
                "--config", config_path,
                "infer",
                "--topic", topic,
                "--format", fmt,
                "--max-messages", str(max_messages),
                "--output", schema_output_path,
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            stderr = result.stderr.strip()
            stdout = result.stdout.strip()
            error_msg = stderr or stdout or "Unknown error"
            json.dump({"error": f"schema-infer failed for topic '{topic}': {error_msg}"}, sys.stdout)
            sys.exit(1)

        # Read schema from the temp file
        if not os.path.exists(schema_output_path) or os.path.getsize(schema_output_path) == 0:
            json.dump({"error": f"No schema generated for topic '{topic}'"}, sys.stdout)
            sys.exit(1)

        with open(schema_output_path, "r") as f:
            schema = f.read().strip()

        if not schema:
            json.dump({"error": f"No schema generated for topic '{topic}'"}, sys.stdout)
            sys.exit(1)

        json.dump({"schema": schema}, sys.stdout)

    except FileNotFoundError:
        json.dump({"error": "schema-infer command not found"}, sys.stdout)
        sys.exit(1)
    except Exception as e:
        json.dump({"error": str(e)}, sys.stdout)
        sys.exit(1)
    finally:
        # Clean up temporary files
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
        if os.path.exists(schema_output_path):
            os.unlink(schema_output_path)


if __name__ == "__main__":
    main()
