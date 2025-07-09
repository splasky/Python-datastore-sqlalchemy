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
import pytest
import os
import signal
import subprocess
import requests
import time
import logging
from datetime import datetime, timezone
from google.cloud import datastore
from google.cloud.datastore.helpers import GeoPoint

from sqlalchemy.dialects import registry
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base

registry.register("datastore", "sqlalchemy_datastore", "CloudDatastoreDialect")

TEST_PROJECT = "python-datastore-sqlalchemy"

# Fixture example (add this to your conftest.py)
@pytest.fixture
def conn():
    """Database connection fixture - implement according to your setup"""
    os.environ["DATASTORE_EMULATOR_HOST"]="localhost:8081"
    engine = create_engine(f'datastore://{TEST_PROJECT}', echo=True)
    conn = engine.connect()
    return conn


@pytest.fixture(scope="session")
def datastore_client():
    """
    Create a local datastore emulator for testing.
    This is a session-scoped fixture, so it will only be run once.
    separate from the development config, open data simulator at port from DATASTORE_EMULATOR_HOST.
    """
    # Start the emulator.
    os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8081"
    result = subprocess.Popen(
        [
            "gcloud",
            "beta",
            "emulators",
            "datastore",
            "start",
            "--no-store-on-disk",
            "--quiet",
        ]
    )

    # Wait for the emulator to start.
    while True:
        time.sleep(1)
        try:
            requests.get(f"http://{os.environ['DATASTORE_EMULATOR_HOST']}/")
            break
        except requests.exceptions.ConnectionError:
            logging.info("Waiting for emulator to spin up...")
    # Create a client that points to the emulator.
    client = datastore.Client(project=TEST_PROJECT)

    yield client

    # Stop the emulator.
    result.terminate()
    # Wait for child process to terminate.
    result.wait(timeout=2)

    # If the process hasn't terminated, kill it.
    if result.poll() is None:
        os.kill(result.pid, signal.SIGKILL)

    # Teardown Reset the emulator.
    requests.post(f"http://{os.environ['DATASTORE_EMULATOR_HOST']}/reset")

    # Clear the environment variables.
    del os.environ["DATASTORE_EMULATOR_HOST"]

def clear_existing_data(client):
    for kind in ["users", "tasks", "assessment"]:
        query = client.query(kind=kind)
        keys = [entity.key for entity in query.fetch()]
        if keys:
            client.delete_multi(keys)

@pytest.fixture(scope="session", autouse=True)
def test_datasets(datastore_client):
    client = datastore_client
    clear_existing_data(client)

    # user1
    user1 = datastore.Entity(client.key("users"))
    user1["name"] = "Elmerulia Frixell"
    user1["age"] = 16
    user1["country"] = "Arland"
    user1["create_time"] = datetime(2025, 1, 1, 1, 2, 3, 4, tzinfo=timezone.utc)
    user1["description"] = "An aspiring alchemist and daughter of Rorona, aiming to surpass her mother and become the greatest alchemist in Arland. Cheerful, hardworking, and full of curiosity."
    user1["settings"] = None

    # user2
    user2 = datastore.Entity(client.key("users"))
    user2["name"] = "Virginia Robertson"
    user2["age"] = 14
    user2["country"] = "Britannia"
    user2["create_time"] = datetime(2025, 1, 1, 1, 2, 3, 4, tzinfo=timezone.utc)
    user2["description"] = (
        "Nicknamed 'Ginny', Virginia is a natural-born Night Witch from rural Britannia. "
        "Cheerful, pure-hearted and full of energy, she leads the Music Squadron as Sergeant, "
        "handling vocals and musical arrangements. She possesses the 'Magic Antenna' ability "
        "to detect Neuroi via sound/magic waves, and fights alongside her familiar Moffy."
    )
    user2["settings"] = None

    # user3
    user3 = datastore.Entity(client.key("users"))
    user3["name"] = "Travis 'Ghost' Hayes"
    user3["age"] = 28
    user3["country"] = "Los Santos, San Andreas"
    user3["create_time"] = datetime(2008, 11, 27, 23, 15, 30, 0, tzinfo=timezone.utc)
    user3["description"] = (
        "Known on the street as 'Ghost', Travis grew up rough in the dusty, forgotten towns of Blaine County, "
        "before making his mark in the grimy districts of South Los Santos. "
        "Once a promising voice in the local underground rap scene, his music quickly devolved into raw, "
        "aggressive tracks after a stint in Bolingbroke Penitentiary for grand theft auto and aggravated assault. "
        "He now funnels that volatile energy into running a small, but ruthless, chop shop operation out of Blaine County. "
        "His uncanny ability to 'hear' trouble brewing – often described as a sixth sense for bad deals and approaching heat – "
        "has kept him alive through countless shootouts. He's rarely seen without his heavily-modified, "
        "bulletproof 'Declasse Stallion' – a steel companion more loyal than any human."
    )
    user3["settings"] = None

    with client.batch() as batch:
        batch.put(user1)
        batch.put(user2)
        batch.put(user3)
        batch.commit()

    # Tasks
    ## task1
    task1 = datastore.Entity(client.key("tasks"))
    task1["task"] = "Collect Sea Urchins in Atelier"
    task1["content"] = {"description": "採集高品質海膽"}
    task1["is_done"] = False
    task1["tag"] = "house"
    task1["location"] = GeoPoint(25.047472, 121.517167)
    task1["assign_user"] = user1.key
    task1["reward"] = 22000.5
    task1["equipment"] = ["bomb", "healing salve", "nectar"]

    secret_recipe_bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\xda\xed\xc1\x01\x01\x00\x00\x00\xc2\xa0\xf7Om\x00\x00\x00\x00IEND\xaeB`\x82"
    task1["encrypted_formula"] = secret_recipe_bytes

    quality_assessment_entity = datastore.Entity(client.key("assessment"))
    quality_assessment_entity["min_grade"] = "A+"
    quality_assessment_entity["inspector"] = "Pia Naruse"
    quality_assessment_entity["comment"] = "海洋的氣息!?"
    task1["additional_notes"] = quality_assessment_entity

    ## task2
    task2 = datastore.Entity(client.key("tasks"))
    task2["task"] = "Luminous Witches – Main Mission"
    task2["content"] = {"description": "✈️ 空中巡演（航空慰問演出）"}
    task2["is_done"] = False
    task2["tag"] = "Wild"
    task2["location"] = GeoPoint(33.58916415889631, 130.39491658285505)
    task2["assign_user"] = user2.key
    task2["reward"] = 500000.45
    task2["equipment"] = ["Magic Antenna", "Moffy", "AT-6 Texan "]
    task2["additional_notes"] = None
    task2["encrypted_formula"] = b"\x00\x00\x00\x00"

    ## task3 
    task3 = datastore.Entity(client.key("tasks"))
    task3["task"] = (
        "Successful hostage rescue, defeating the kidnappers, with survival ensured"
    )
    task3["content"] = {
        "description": "Successful hostage rescue, defeating the kidnappers, with survival ensured",
        "important": "You need to bring your own weapons🔫, ammunition and vehicles 🚗"
    }
    task3["is_done"] = False
    task3["tag"] = "Apartment"
    task3["location"] = GeoPoint(34.79068656806868, -120.18356345391084)
    task3["assign_user"] = user3.key
    task3["reward"] = 1000000.99999
    task3["equipment"] = ["A 20-year-old used pickup truck.", "AR-16"]
    task3["additional_notes"] = None
    task3["encrypted_formula"] = b"\x00\x00\x00\x00"

    with client.batch() as batch:
        batch.put(task1)
        batch.put(task2)
        batch.put(task3)
        batch.commit()

    time.sleep(3) # wait for batch finish
    query =  client.query(kind="users")
    users = list(query.fetch())
    assert len(users) == 3

    time.sleep(3) # wait for batch finish
    query = client.query(kind="tasks")
    tasks = list(query.fetch())
    assert len(tasks) == 3

@pytest.fixture(scope="session")
def engine():
    engine = create_engine(f"datastore://{TEST_PROJECT}", echo=True)
    Base.metadata.create_all(engine)  # Create tables (kinds)
    return engine

@pytest.fixture(scope="function")
def session(engine):
    Session = sessionmaker(bind=engine)
    sess = Session()
    yield sess
    sess.rollback()  # For test isolation
    sess.close()
