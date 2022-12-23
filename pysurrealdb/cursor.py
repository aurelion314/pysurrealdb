from .clients.http_client import HttpClient


class Cursor:
    """
    This is to simulate a pymysql Cursor object. This allows the use of libraries like pandas that expect a cursor object.

    This is instantiated by the connection object. You should not need to instantiate this directly.
    """
    def __init__(self, client: HttpClient):
        self.client = client

    def execute(self, sql):
        self.data = self.client.query(sql)

    @property
    def description(self):
        if self.data:
            keys = self.data[0].keys()
            return [(key, None, None, None, None, None, None) for key in keys]
        return []

    def fetchall(self):
        return self.data

    def close(self):
        self.client.close()