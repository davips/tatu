import json

from aiuna.file import File
from tatu.okast import OkaSt
from aiuna.content.specialdata import UUIDData
from aiuna.creation import read_arff

with open("token.txt", "r") as f:
    token = json.load(f)["token"]

storage = OkaSt(post=True, token=token)

print("Reading file...")
data = File("iris.arff")

print("Storing...")
storage.store(data)  # TODO: it is always sending the file, even when not needed

print("Fetching...")
d = storage.fetch(UUIDData(data.id))
print(d)
