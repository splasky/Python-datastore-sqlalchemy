# Copyright (c) 2017 The sqlalchemy-bigquery Authors
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

import base64
import functools
import json
import os
import re
from typing import Optional, Tuple

import google.auth
import sqlalchemy
from google.api_core import client_info
from google.auth.credentials import Credentials
from google.cloud import datastore
from google.oauth2 import service_account

USER_AGENT_TEMPLATE = "sqlalchemy/{}"
SCOPES = (
    "https://www.googleapis.com/auth/datastore",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
)


def google_client_info(
    user_agent: Optional[str] = None,
) -> client_info.ClientInfo:
    """
    Return a client_info object, with an optional user agent
    string.  If user_agent is None, use a default value.
    """

    if user_agent is None:
        user_agent = USER_AGENT_TEMPLATE.format(sqlalchemy.__version__)
    return client_info.ClientInfo(user_agent=user_agent)


def create_datastore_client(
    credentials_info: Optional[dict] = None,
    credentials_path: Optional[str] = None,
    credentials_base64: Optional[str] = None,
    project_id: Optional[str] = None,
    user_agent: Optional[client_info.ClientInfo] = None,
    database: Optional[str] = None
) -> Tuple[datastore.Client, Optional[Credentials]]:
    """Construct a BigQuery client object.

    Args:
        credentials_info Optional[dict]:
        credentials_path Optional[str]:
        credentials_base64 Optional[str]:
        location (Optional[str]):
            Default location for jobs / datasets / tables.
        project_id (Optional[str]):
            Project ID for the project which the client acts on behalf of.
        user_agent (Optional[google.api_core.client_info.ClientInfo]):
            The client info used to send a user-agent string along with API
            requests. If ``None``, then default info will be used. Generally,
            you only need to set this if you're developing your own library
            or partner tool.
    """

    default_project = None
    database = database if database != "(default)" else None
    if os.getenv("DATASTORE_EMULATOR_HOST") is not None:
        client = datastore.Client(project=project_id)
        return client, None

    if credentials_base64:
        credentials_info = json.loads(base64.b64decode(credentials_base64))

    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        credentials = credentials.with_scopes(SCOPES)
        default_project = credentials.project_id
    elif credentials_info:
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info
        )
        credentials = credentials.with_scopes(SCOPES)
        default_project = credentials.project_id
    else:
        credentials, default_project = google.auth.default(scopes=SCOPES)

    if project_id is None:
        project_id = default_project

    info = google_client_info(user_agent=user_agent.to_user_agent() if user_agent is not None else None)

    return datastore.Client(
        client_info=info,
        project=project_id,
        credentials=credentials,
        database=database
    ), credentials


def substitute_re_method(r, flags=0, repl=None):
    if repl is None:
        return lambda f: substitute_re_method(r, flags, f)

    r = re.compile(r, flags)

    @functools.wraps(repl)
    def sub(self, s, *args, **kw):
        def repl_(m):
            return repl(self, m, *args, **kw)

        return r.sub(repl_, s)

    return sub


def substitute_string_re_method(r, *, repl, flags=0):
    r = re.compile(r, flags)
    return lambda self, s: r.sub(repl, s)
