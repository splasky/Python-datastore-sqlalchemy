# Copyright (c) 2025 hychang <hychang.1997.tw@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
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
