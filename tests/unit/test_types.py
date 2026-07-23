import pytest
from mypy import api

def test_mypy_type_checking_python_source():
    """Verify that python/pysync passes mypy static type checking with zero errors."""
    stdout, stderr, exit_code = api.run(["python/pysync"])
    assert exit_code == 0, f"Mypy type check failed on python/pysync:\n{stdout}\n{stderr}"

def test_mypy_strict_type_stubs():
    """Verify that python/pysync/__init__.pyi passes mypy --strict validation."""
    stdout, stderr, exit_code = api.run(["--strict", "python/pysync/__init__.pyi"])
    assert exit_code == 0, f"Mypy --strict failed on __init__.pyi:\n{stdout}\n{stderr}"
