import requests

from .clients.http_client import HttpClient
from .connections import Connection


def connect(host='localhost', port=8000, user=None, password=None, database=None, namespace=None) -> Connection:
    # Connect to the SurrealDB server.
    return Connection(HttpClient(host, port, user, password, database, namespace))