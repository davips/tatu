import random

from tatu.sql.mysql import MySQL
import requests

def user(username=None, password=None, email=None, base_url="http://localhost:5000"):
    """Create a new user."""

    username = username or ("username" + str(random.randint(1, 100000)))
    password = password or ("password" + str(random.randint(1, 100000)))
    email = email or ("email@" + str(random.randint(1, 100000)) + ".com")

    url_createuser = base_url + '/api/users'
    data_createuser = {"username": username, "password": password, "name": "Teste", "email": email}
    response_createuser = requests.post(url_createuser, json=data_createuser)
    print(response_createuser.text)
    return {"username": username, "password": password, "email": email}


# Only tatu
tatu = MySQL(db='tatu:kururu@localhost/tatu', threaded=False)

def get_data():
    data = tatu.fetch("3l9bSwFwL0TsSztkDb0iuVQ", lazy=False)
    attrs = data.Xd
    print("ATTRSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS", attrs)

# Only SQLALchemy
response_login = requests.post('http://localhost:5000/api/auth/login', json={"username": "fill", "password": "fill"})
access_token = response_login.json()['access_token']
headers = {'Authorization': 'Bearer ' + access_token}

# for i in range(10000):
#     response_users = requests.get('http://localhost:5000/api/users/rabizao', headers=headers)  
#     print(i, response_users.json()['username'])


# Tatu and SQLAlchemy
for i in range(10000):
    # get_data()
    response = requests.get('http://localhost:5000/api/posts/1', headers=headers)  
    print(i, response.json()['attrs'])
