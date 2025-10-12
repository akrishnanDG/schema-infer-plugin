#!/usr/bin/env python3
"""
Comprehensive test runner for the Schema Inference Schema Inference Plugin
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path


def run_tests(test_type="all", verbose=False, coverage=False, parallel=False):
    """
    Run comprehensive tests for the schema inference plugin.
    
    Args:
        test_type: Type of tests to run (all, unit, integration, format, generators, inference, core)
        verbose: Enable verbose output
        coverage: Enable coverage reporting
        parallel: Run tests in parallel
    """
    
    # Ensure we're in the project root
    project_root = Path(__file__).parent
    os.chdir(project_root)
    
    # Base pytest command
    cmd = ["python", "-m", "pytest"]
    
    # Add test directory
    cmd.append("tests/")
    
    # Add verbosity
    if verbose:
        cmd.append("-v")
    else:
        cmd.append("-q")
    
    # Add coverage if requested
    if coverage:
        cmd.extend(["--cov=schema_infer", "--cov-report=html", "--cov-report=term"])
    
    # Add parallel execution if requested
    if parallel:
        cmd.extend(["-n", "auto"])
    
    # Select specific test types
    if test_type == "unit":
        cmd.extend(["-m", "unit"])
    elif test_type == "integration":
        cmd.extend(["-m", "integration"])
    elif test_type == "format":
        cmd.append("tests/test_format_detection.py")
    elif test_type == "generators":
        cmd.append("tests/test_schema_generators.py")
    elif test_type == "inference":
        cmd.append("tests/test_schema_inference.py")
    elif test_type == "core":
        cmd.append("tests/test_core_components.py")
    elif test_type == "slow":
        cmd.extend(["-m", "slow"])
    elif test_type == "fast":
        cmd.extend(["-m", "not slow"])
    
    # Add additional pytest options
    cmd.extend([
        "--tb=short",  # Short traceback format
        "--strict-markers",  # Strict marker checking
        "--disable-warnings",  # Disable warnings for cleaner output
    ])
    
    print(f"Running tests: {' '.join(cmd)}")
    print("=" * 60)
    
    # Run the tests
    try:
        result = subprocess.run(cmd, check=True)
        print("\n" + "=" * 60)
        print("✅ All tests passed!")
        return 0
    except subprocess.CalledProcessError as e:
        print("\n" + "=" * 60)
        print(f"❌ Tests failed with exit code {e.returncode}")
        return e.returncode
    except FileNotFoundError:
        print("❌ pytest not found. Please install it with: pip install pytest")
        return 1


def run_specific_test(test_file, test_function=None, verbose=False):
    """
    Run a specific test file or function.
    
    Args:
        test_file: Path to test file
        test_function: Specific test function to run
        verbose: Enable verbose output
    """
    
    project_root = Path(__file__).parent
    os.chdir(project_root)
    
    cmd = ["python", "-m", "pytest"]
    
    if verbose:
        cmd.append("-v")
    else:
        cmd.append("-q")
    
    # Add test file
    cmd.append(f"tests/{test_file}")
    
    # Add specific test function if provided
    if test_function:
        cmd.append(f"::{test_function}")
    
    print(f"Running specific test: {' '.join(cmd)}")
    print("=" * 60)
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n" + "=" * 60)
        print("✅ Test passed!")
        return 0
    except subprocess.CalledProcessError as e:
        print("\n" + "=" * 60)
        print(f"❌ Test failed with exit code {e.returncode}")
        return e.returncode


def check_dependencies():
    """Check if all required dependencies are installed."""
    required_packages = [
        "pytest",
        "pytest-cov",
        "pytest-xdist",
        "pytest-mock"
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print("❌ Missing required packages:")
        for package in missing_packages:
            print(f"  - {package}")
        print("\nInstall them with:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    print("✅ All required packages are installed")
    return True


def main():
    """Main entry point for the test runner."""
    parser = argparse.ArgumentParser(
        description="Comprehensive test runner for Schema Inference Schema Inference Plugin",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_tests.py                    # Run all tests
  python run_tests.py --type unit        # Run only unit tests
  python run_tests.py --type integration # Run only integration tests
  python run_tests.py --type format      # Run format detection tests
  python run_tests.py --type generators  # Run schema generator tests
  python run_tests.py --coverage         # Run with coverage report
  python run_tests.py --parallel         # Run tests in parallel
  python run_tests.py --verbose          # Verbose output
  python run_tests.py --file test_schema_generators.py --function test_json_schema_generation
        """
    )
    
    parser.add_argument(
        "--type", "-t",
        choices=["all", "unit", "integration", "format", "generators", "inference", "core", "slow", "fast"],
        default="all",
        help="Type of tests to run (default: all)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    parser.add_argument(
        "--coverage", "-c",
        action="store_true",
        help="Enable coverage reporting"
    )
    
    parser.add_argument(
        "--parallel", "-p",
        action="store_true",
        help="Run tests in parallel"
    )
    
    parser.add_argument(
        "--file", "-f",
        help="Run specific test file"
    )
    
    parser.add_argument(
        "--function", "-fn",
        help="Run specific test function"
    )
    
    parser.add_argument(
        "--check-deps",
        action="store_true",
        help="Check if all required dependencies are installed"
    )
    
    args = parser.parse_args()
    
    # Check dependencies if requested
    if args.check_deps:
        if not check_dependencies():
            return 1
        return 0
    
    # Check dependencies before running tests
    if not check_dependencies():
        print("\nRun with --check-deps to see missing packages")
        return 1
    
    # Run specific test file/function
    if args.file:
        return run_specific_test(args.file, args.function, args.verbose)
    
    # Run tests by type
    return run_tests(args.type, args.verbose, args.coverage, args.parallel)


if __name__ == "__main__":
    sys.exit(main())
