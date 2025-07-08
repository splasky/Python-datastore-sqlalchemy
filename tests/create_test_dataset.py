from datetime import datetime
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./test_credentials.json"

# Create test dataset
from google.cloud import datastore
from google.cloud.datastore.helpers import GeoPoint

client = datastore.Client(project="python-datastore-sqlalchemy")

# user1
user1 = datastore.Entity(client.key("users"))
user1["name"] = "Elmerulia Frixell"
user1["age"] = 16
user1["country"] = "Arland"
user1["create_time"] = datetime(2025, 1, 1, 1, 2, 3, 4)
user1["description"] = "An aspiring alchemist and daughter of Rorona, aiming to surpass her mother and become the greatest alchemist in Arland. Cheerful, hardworking, and full of curiosity."
user1["settings"] = None

# user2
user2 = datastore.Entity(client.key("users"))
user2["name"] = "Virginia Robertson"
user2["age"] = 14
user2["country"] = "Britannia"
user2["create_time"] = datetime(2025, 1, 1, 1, 2, 3, 4)
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
user3["create_time"] = datetime(2008, 11, 27, 23, 15, 30, 0)
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
