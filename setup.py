# Copyright (c) 2025 The sqlalchemy-datastore Authors
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

import os
import re

from setuptools import setup, find_packages

v = open(
    os.path.join(os.path.dirname(__file__), "sqlalchemy_datastore", "__init__.py")
)
version_match = re.compile(r'.*__version__ = "(.*?)"', re.S).match(v.read())
if not version_match:
    raise RuntimeError("Unable to find version string in __init__.py.")
VERSION = version_match.group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), "README.md")


setup(
    name="python-datastore-sqlalchemy",
    version=VERSION,
    description="SQLAlchemy dialect for google cloud datastore",
    long_description=open(readme).read(),
    long_description_content_type="text/x-rst",
    url="https://github.com/splasky/python-datastore-sqlalchemy",
    author="HY Chang(splasky)",
    author_email="hychang.1997.tw@gmail.com",
    license="MIT",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database :: Front-Ends",
        "Operating System :: POSIX :: Linux",
    ],
    keywords="SQLAlchemy GCP Datastore",
    python_requires=">=3.9",
    project_urls={
        "Documentation": "https://github.com/splasky/python-datastore-sqlalchemy/wiki",
        "Source": "https://github.com/splasky/python-datastore-sqlalchemy",
        "Tracker": "https://github.com/splasky/python-datastore-sqlalchemy/issues",
    },
    packages=find_packages(include=["sqlalchemy_datastore"]),
    include_package_data=True,
    install_requires=["SQLAlchemy>=2.0.0",],
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects": ["datastore = sqlalchemy_datastore:CloudDatastoreDialect"]
    },
)
