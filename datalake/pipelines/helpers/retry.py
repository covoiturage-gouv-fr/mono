import time
from typing import Callable, TypeVar

T = TypeVar("T")


def retry(fn: Callable[[], T], attempts: int = 4, base_delay: float = 2.0, label: str = "") -> T:
  """Réessaie fn() avec backoff exponentiel ; relance la dernière exception si tout échoue."""
  for i in range(attempts):
    try:
      return fn()
    except Exception as e:
      if i == attempts - 1:
        raise
      delay = base_delay * (2 ** i)
      print(f"  ⚠️  {label or 'opération'} échouée ({type(e).__name__}), essai {i + 2}/{attempts} dans {delay:.0f}s")
      time.sleep(delay)
