"""Unit tests for _helpers module (no emulator required)."""
import re

import sqlalchemy

from sqlalchemy_datastore._helpers import (
    SCOPES,
    USER_AGENT_TEMPLATE,
    google_client_info,
    substitute_re_method,
    substitute_string_re_method,
)

# ---------------------------------------------------------------------------
# google_client_info
# ---------------------------------------------------------------------------

def test_google_client_info_default_user_agent():
    info = google_client_info()
    expected = USER_AGENT_TEMPLATE.format(sqlalchemy.__version__)
    assert info.user_agent == expected


def test_google_client_info_custom_user_agent():
    info = google_client_info(user_agent="custom-agent/1.0")
    assert info.user_agent == "custom-agent/1.0"


# ---------------------------------------------------------------------------
# SCOPES constant
# ---------------------------------------------------------------------------

def test_scopes():
    assert "https://www.googleapis.com/auth/datastore" in SCOPES
    assert "https://www.googleapis.com/auth/cloud-platform" in SCOPES
    assert "https://www.googleapis.com/auth/drive" in SCOPES


# ---------------------------------------------------------------------------
# substitute_re_method (deferred decorator pattern)
# ---------------------------------------------------------------------------

def test_substitute_re_method_deferred():
    """When repl is None, returns a decorator that wraps the function."""

    class Processor:
        @substitute_re_method(r"(\d+)")
        def double_numbers(self, m):
            return str(int(m.group(1)) * 2)

    proc = Processor()
    result = proc.double_numbers("abc 5 def 10 ghi")
    assert result == "abc 10 def 20 ghi"


def test_substitute_re_method_deferred_no_match():
    class Processor:
        @substitute_re_method(r"\d+")
        def replace_nums(self, m):
            return "X"

    proc = Processor()
    result = proc.replace_nums("no numbers here")
    assert result == "no numbers here"


def test_substitute_re_method_returns_callable():
    decorator = substitute_re_method(r"\d+")
    assert callable(decorator)

    def my_repl(self, m):
        return "NUM"

    method = decorator(my_repl)
    assert callable(method)


# ---------------------------------------------------------------------------
# substitute_string_re_method
# ---------------------------------------------------------------------------

def test_substitute_string_re_method():
    class Processor:
        transform = substitute_string_re_method(r"\bfoo\b", repl="bar")

    proc = Processor()
    result = proc.transform("I have foo and foobar")
    assert result == "I have bar and foobar"


def test_substitute_string_re_method_no_match():
    class Processor:
        transform = substitute_string_re_method(r"\bxyz\b", repl="abc")

    proc = Processor()
    result = proc.transform("no match here")
    assert result == "no match here"


def test_substitute_string_re_method_with_flags():
    class Processor:
        transform = substitute_string_re_method(r"hello", repl="HI", flags=re.IGNORECASE)

    proc = Processor()
    result = proc.transform("Hello World")
    assert result == "HI World"
