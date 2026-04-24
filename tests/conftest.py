import multiprocessing
import os
import sys
from pathlib import Path


def _running_under_mutmut() -> bool:
    if "MUTANT_UNDER_TEST" in os.environ:
        return True

    if Path.cwd().name == "mutants":
        return True

    return any(Path(path).name == "mutants" for path in sys.path if path)


if _running_under_mutmut():
    _original_set_start_method = multiprocessing.set_start_method

    def _safe_set_start_method(method: str | None = None, force: bool = False):
        try:
            return _original_set_start_method(method, force=force)
        except RuntimeError as exc:
            if str(exc) == "context has already been set":
                return None
            raise

    multiprocessing.set_start_method = _safe_set_start_method
