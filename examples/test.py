# Listar *iris*
import json
from zipfile import ZipFile
from tatu.storageinterface import DuplicateEntryException
from tatu.pickle_ import Pickle
from tatu.sql.mysql import MySQL
from aiuna.content.creation import read_arff

lst = Pickle().list_by_name('iris')
for phantom in lst:
    print(phantom)

# Armazenar dataset, sem depender do tatu.
from tatu.pickle_ import Pickle

print('Storing iris...')
data = 0
try:
    data = read_arff('iris.arff')
    ???
    Pickle().store(data)
except DuplicateEntryException:
    print('Duplicate! Ignored.')
d = Pickle().fetch(data.uuid)
print('ok!', data.id, data.history)
print('ok!', d.id, d.history)

lst = Pickle().list_by_name('iris')
for phantom in lst:
    print(phantom)
    print('Fetching Xd...')
    data = Pickle().fetch(phantom)
    print(data.Xd)

# Testes            ############################
# data = Data(X=np.array([[1, 2, 3, 4], [5, 6, 7, 8]]),
#         Y=np.array([[1, 2, 3, 4]]),
#         name='flowers', desc='Beautiful description.')
# # Xd={'length': 'R', 'width': 'R'}, Yd={'class': ['M', 'F']}

# Teste de gravação ############################
print('Storing Data object...')
test = Pickle()
try:
    test.store(data)
    print('ok!')
except DuplicateEntryException:
    print('Duplicate! Ignored.')

print("fetch", test.fetch(data).id)

# # Teste de leitura ############################
# print('Getting Data information-only objects...')
# lista = test.list_by_name('flo')
# print([d.name for d in lista])
#
# print('Getting a complete Data object...')
# data = test.fetch(lista[0])
# print(data.X)

# # Resgatar por UUID ###########################
byuuid = Pickle().fetch(data.uuid)
print("byuuid", byuuid)

uuid = "ĹЇЖȡfĭϹƗͶэգ8Ƀű"
data = Pickle().fetch(uuid)
print("dddddddddddd", data.field_funcs_m)
storage = MySQL(db="user:senha@143.107.183.114/base")
# storage.store(data)
print("------------", data)
if data is None:
    raise Exception("Download failed: " + uuid + " not found!")

print("arffing...")
arff = data.arff("No name", "No description")

print("zipping...")
zipped_file = ZipFile("/tmp/lixo.zip", 'w')
print("add...")
zipped_file.writestr(uuid, arff)
zipped_file.close()

storage = Pickle()
uuid = "ĹЇЖȡfĭϹƗͶэգ8Ƀű"
vhist = storage.visual_history(uuid, "/tmp")
print(json.dumps(vhist, indent=4))
