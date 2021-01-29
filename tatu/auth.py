from getpass import getpass

from tatu.okast import OkaSt, j


def get_token(url):
    tries = 0
    ntries = 3

    while tries < ntries:
        tries += 1
        username = input("Username to connect to OKA: ")
        password = getpass("Password to connect to OKA: ")
        data = {"username": username, "password": password}
        okast = OkaSt(token=None, url=url, close_when_idle=True)
        response = okast.request(f"/api/auth/login", "post", json=data)
        if response and 'access_token' in j(response):
            return j(response)['access_token']
        else:
            if tries < ntries:
                print("[Error] Authentication error. Please try again.")
    print("[Error] Authentication failed.")
    exit()
