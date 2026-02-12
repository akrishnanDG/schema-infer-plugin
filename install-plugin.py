#!/usr/bin/env python3
"""
Installation script for Schema Inference CLI Schema Inference Plugin
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path


def get_plugin_directory():
    """Get the directory where Schema Inference CLI plugins should be installed."""
    
    # Check common plugin directories
    home = Path.home()
    possible_dirs = [
        home / ".schema-infer" / "plugins",
        home / "schema-infer" / "plugins", 
        home / ".local" / "bin",
        home / "bin",
        Path("/usr/local/bin"),
        Path("/opt/schema-infer/bin"),
    ]
    
    # Find existing Schema Inference CLI installation
    schema_infer_path = shutil.which("schema-infer")
    if schema_infer_path:
        schema_infer_dir = Path(schema_infer_path).parent
        possible_dirs.insert(0, schema_infer_dir)
    
    # Check which directories exist and are writable
    for plugin_dir in possible_dirs:
        if plugin_dir.exists() and os.access(plugin_dir, os.W_OK):
            return plugin_dir
    
    # Default to user's local bin
    default_dir = home / ".local" / "bin"
    default_dir.mkdir(parents=True, exist_ok=True)
    return default_dir


def install_requirements():
    """Install required dependencies."""
    
    print("Installing dependencies...")
    
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-r", str(Path(__file__).parent / "requirements.txt")
        ])
        print("✓ Dependencies installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to install dependencies: {e}")
        return False
    
    return True


def install_package():
    """Install the package in development mode."""
    
    print("Installing package in development mode...")
    
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-e", "."
        ])
        print("✓ Package installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to install package: {e}")
        return False
    
    return True


def install_plugin():
    """Install the plugin to the Schema Inference CLI plugins directory."""
    
    print("Installing Schema Inference CLI plugin...")
    
    # Get plugin directory
    plugin_dir = get_plugin_directory()
    print(f"Plugin directory: {plugin_dir}")
    
    # Source and destination paths
    source_plugin = Path("schema-infer-schema")
    dest_plugin = plugin_dir / "schema-infer-schema"
    source_schema_infer = Path("schema_infer")
    dest_schema_infer = plugin_dir / "schema_infer"
    
    if not source_plugin.exists():
        print(f"✗ Plugin file not found: {source_plugin}")
        return False
    
    if not source_schema_infer.exists():
        print(f"✗ Schema infer module not found: {source_schema_infer}")
        return False
    
    try:
        # Copy plugin file
        shutil.copy2(source_plugin, dest_plugin)
        
        # Copy schema_infer module
        if dest_schema_infer.exists():
            shutil.rmtree(dest_schema_infer)
        shutil.copytree(source_schema_infer, dest_schema_infer)
        
        # Make executable
        os.chmod(dest_plugin, 0o755)
        
        print(f"✓ Plugin installed to: {dest_plugin}")
        print(f"✓ Schema infer module installed to: {dest_schema_infer}")
        print(f"✓ Plugin will be available as: schema-infer schema")
        
        # Check if directory is in PATH
        if str(plugin_dir) not in os.environ.get("PATH", ""):
            print(f"\n⚠️  Warning: {plugin_dir} is not in your PATH")
            print(f"Add this line to your shell configuration file (.bashrc, .zshrc, etc.):")
            print(f"export PATH=\"$PATH:{plugin_dir}\"")
            print("\nThen restart your shell or run: source ~/.bashrc")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to install plugin: {e}")
        return False


def test_plugin():
    """Test if the plugin is working."""
    
    print("Testing plugin installation...")
    
    try:
        # Test if schema-infer command exists
        result = subprocess.run(
            ["schema-infer", "--version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0:
            print("✗ Schema Inference CLI not found or not working")
            return False
        
        print(f"✓ Schema Inference CLI found: {result.stdout.strip()}")
        
        # Test if our plugin is discoverable
        result = subprocess.run(
            ["schema-infer", "schema", "--help"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode != 0:
            print("✗ Plugin not discoverable by Schema Inference CLI")
            print("Make sure the plugin directory is in your PATH")
            return False
        
        print("✓ Plugin is discoverable by Schema Inference CLI")
        return True
        
    except subprocess.TimeoutExpired:
        print("✗ Plugin test timed out")
        return False
    except FileNotFoundError:
        print("✗ Schema Inference CLI not found. Please install it first:")
        print("  curl -sL --http1.1 https://cnfl.io/cli | sh -s -- -b /usr/local/bin")
        return False
    except Exception as e:
        print(f"✗ Plugin test failed: {e}")
        return False


def main():
    """Main installation function."""

    print("Schema Inference CLI Plugin Installation")
    print("=" * 45)

    # Check Python version
    import sys
    if sys.version_info < (3, 9):
        print(f"✗ Python 3.9+ required (found {sys.version})")
        sys.exit(1)

    project_root = Path(__file__).parent

    # Install package with pip
    print("Installing package and dependencies...")
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-e", str(project_root)
        ])
        print("✓ Package installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"✗ Installation failed: {e}")
        sys.exit(1)

    # Verify
    try:
        result = subprocess.run(
            [sys.executable, "-m", "schema_infer.plugin.cli", "--help"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print("✓ Plugin verified successfully")
        else:
            print("⚠ Plugin installed but verification failed")
    except Exception as e:
        print(f"⚠ Could not verify: {e}")

    print("\n" + "=" * 45)
    print("Installation complete!")
    print("\nUsage:")
    print("  schema-infer --help")
    print("  schema-infer --bootstrap-servers localhost:9092 list-topics")


if __name__ == "__main__":
    main()
