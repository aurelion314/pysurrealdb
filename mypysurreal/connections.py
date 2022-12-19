from .clients.http_client import HttpClient


class Cursor:
    """
    This is to simulate a pymysql Cursor object.
    """
    def __init__(self, client):
        self.client = client

    def execute(self, sql):
        self.data = self.client.query(sql)

    @property
    def description(self):
        keys = self.data[0].keys()
        return [(key, None, None, None, None, None, None) for key in keys]

    def fetchall(self):
        return self.data

    def close(self):
        self.client.close()


class Connection(HttpClient):
    """
    This is to simulate a pymysql Connection object.
    """
    def __init__(self, client):
        self.client = client

    def cursor(self) -> Cursor:
        return Cursor(self.client)

    # any other methods should just be passed to the client
    def __getattr__(self, name):
        return getattr(self.client, name)
