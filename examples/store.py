from aiuna.compression import pack
from aiuna.file import File

from tatu.sql.mysql import MySQL

#
# f = open("/tmp/a", "bw")
# print([int(x) for x in integers2bytes(31243)])
# f.write(float2bytes(31111243))
# f.close()
#
# exit()

data = File("iris.arff").data
# print(data.X)
storage = MySQL()
storage.store(data)
