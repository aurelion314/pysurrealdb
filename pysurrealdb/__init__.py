import requests

from .clients.http_client import HttpClient
from .connections import Connection
from .model import Model
from .query_builder import QueryBuilder


def connect(host=None, port=None, user=None, password=None, database=None, namespace=None) -> Connection:
    # Connect to the SurrealDB server.
    if isinstance(host, dict):
        # if host is a dict, assume it's a config object
        return Connection(HttpClient(**host))
    return Connection(HttpClient(host, port, user, password, database, namespace))

def connection(name=None) -> Connection:
    # Connect to the SurrealDB server. Similar to connect(), but uses a connection name from the config file.
    return Connection(name)