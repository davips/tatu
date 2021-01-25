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


def f1():
    print("1111111111\n", 111111111111111100000000000000000000)
    File("airlines400k.arff").data
    print("1111111111\n", 2222222222222222200000000000000000000)


def f2():
    print("1111111111\n", 111111111111111100000000000000000000)
    File("airlines250k.arff").data
    print("1111111111\n", 2222222222222222200000000000000000000)


def f3():
    print("1111111111\n", 111111111111111100000000000000000000)
    File("airlines300k.arff").data
    print("1111111111\n", 2222222222222222200000000000000000000)


def g():
    print("1111111111\n", 10000)
    data = File("airlines200k.arff").data
    print("1111111111\n", 1000000000000000)
    storage = MySQL("tatu:kururu@localhost/tatu", threaded=False)
    print("1111111111\n", 1111111111111111)
    storage.store(data)
    print("1111111111\n", 22222222222222222)


def g1():
    print("1111111111\n", 10000)
    data = File("airlines400k.arff").data
    print("1111111111\n", 1000000000000000)
    storage = MySQL("tatu:kururu@localhost/tatu", threaded=False)
    print("1111111111\n", 1111111111111111)
    storage.store(data)
    print("1111111111\n", 22222222222222222)


def g2():
    print("1111111111\n", 10000)
    data = File("airlines300k.arff").data
    print("1111111111\n", 1000000000000000)
    storage = MySQL("tatu:kururu@localhost/tatu", threaded=False)
    print("1111111111\n", 1111111111111111)
    storage.store(data)
    print("1111111111\n", 22222222222222222)


def g3():
    print("1111111111\n", 10000)
    data = File("airlines250k.arff").data
    print("1111111111\n", 1000000000000000)
    storage = MySQL("tatu:kururu@localhost/tatu", threaded=False)
    print("1111111111\n", 1111111111111111)
    storage.store(data)
    print("1111111111\n", 22222222222222222)


f()
sleep(30)
g()
sleep(30)

f1()
sleep(30)
g1()
sleep(30)

f2()
sleep(30)
g2()
sleep(30)

f3()
sleep(30)
g3()
sleep(30)
