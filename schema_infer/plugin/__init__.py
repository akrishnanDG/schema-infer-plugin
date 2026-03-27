"""
CLI Plugin modules for Schema Inference
"""

from .auth import AuthenticationManager
from .cli import main
from .optimistic import OptimisticProcessor

__all__ = [
    "main",
    "AuthenticationManager",
    "OptimisticProcessor",
]
