# Re-export from src.utils.logging for backwards compatibility.
# Use src.utils.logging as the canonical import going forward.
from src.utils.logging import get_logger

__all__ = ["get_logger"]
