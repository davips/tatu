import json

from tatu.okaserver import OkaServer
from aiuna.content.specialdata import UUIDData
from aiuna.creation import read_arff

with open("token.txt", "r") as f:
    token = json.load(f)["token"]

storage = OkaServer(post=True, token=token)

print("Reading file...")
data = read_arff("iris.arff")[1]

print("Storing...")
storage.store(data)  # TODO: it is always sending the file, even when not needed

print("Fetching...")
d = storage.fetch(UUIDData(data.id))
print(d)
