"""
CLI Plugin modules for Schema Inference
"""

from .cli import main
from .auth import AuthenticationManager
from .optimistic import OptimisticProcessor

__all__ = [
    "main",
    "AuthenticationManager", 
    "OptimisticProcessor",
]
