import json
import socket

from requests.auth import HTTPBasicAuth


class WSClient:
    """Representation of a socket connection with a surrealdb server
    
    This is a work in progress. It's not currently functional.
    """

    def __init__(self, host='localhost', port='8000', user=None, password=None, database=None, namespace=None, **kwargs):
        self.host = host
        self.port = port
        self.database = database
        self.namespace = namespace
        self._auth = HTTPBasicAuth(user, password)
        self._headers = {
            'Authorization': self._auth,
            'Content-Type': 'application/json',
            'Accept':'application/json',
            'ns': namespace,
            'db': database,
         }
        self._write_timeout = kwargs.get('write_timeout', 5)
        
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        #Include headers and auth
        self.sock = socket.create_connection((self.host, self.port))


    def close(self):
        self.sock.close()

    def _send(self, method, *params):
        R = {
            'method': method,
            'params': params,
        }
        print('sending', R)
        self.sock.settimeout(self._write_timeout)
        self.sock.send(json.dumps(R).encode('utf-8'))
        return self._recv()

    def _recv(self):
        # receive any data from the socket
        return self.sock.recv(1024)

    def ping(self):
        self._send('ping')
        return self._recv()

    def query(self, query):
        self._send('query', query)
        return self._recv()


if __name__ == '__main__':
    with WSClient(host='localhost', port=8000, user='testuser', password='passicus') as client:
        print(client.ping())
        print(client.query('select * from test'))
        