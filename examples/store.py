from time import sleep

from aiuna.step.file import File

from tatu.sql.mysql import MySQL


#
# f = open("/tmp/a", "bw")
# print([int(x) for x in integers2bytes(31243)])
# f.write(float2bytes(31111243))
# f.close()
#
# exit()
def f():
    print("1111111111\n", 111111111111111100000000000000000000)
    File("airlines200k.arff").data
    print("1111111111\n", 2222222222222222200000000000000000000)
    # print(data.X)

def g():
    print("1111111111\n", 10000)
    data = File("airlines200k.arff").data
    print("1111111111\n", 1000000000000000)
    storage = MySQL("tatu:kururu@localhost/tatu")
    print("1111111111\n", 1111111111111111)
    storage.store(data)
    print("1111111111\n", 22222222222222222)


f()
sleep(50)
g()
sleep(5000)
