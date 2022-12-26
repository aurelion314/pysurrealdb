import requests
from requests.auth import HTTPBasicAuth


class HttpClient:
    """Representation of a connection with a SurrealDB server"""
    def __init__(self, host=None, port=None, user=None, password=None, database=None, namespace=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.namespace = namespace
        self.session = requests.Session()
        self.auth = HTTPBasicAuth(user, password)
        self._set_headers()

        if not host:
            print("SurrealDB: No host specified. Using localhost.")
            self.host = 'localhost'
        if not port:
            print("SurrealDB: No port specified. Using 8000.")
            self.port = 8000
        if not namespace:
            print("SurrealDB: No namespace specified. Using default.")
            self.namespace = 'default'
        if not database:
            print("SurrealDB: No database specified. Using default.")
            self.database = 'default'

    def _set_headers(self, headers=None):
        if not headers:
            headers = {
                'Content-Type': 'application/json',
                'Accept':'application/json',
                'ns': self.namespace,
                'db': self.database,
            }

        self.session.headers.update(headers)
        
    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def _send(self, data, method='POST', endpoint='sql'):
        """Send a request to SurrealDB and return the response."""
        url = f"http://{self.host}:{self.port}/{endpoint}"
        if isinstance(data, str):
            response = self.session.request(method, url, data=data, auth=self.auth)
        else:
            response = self.session.request(method, url, json=data, auth=self.auth)
        if not response.ok:
            raise ConnectionError("Request to SurrealDB failed.", response.content)
        
        r = response.json()
        if len(r) > 1:
            results = []
            for row in r:
                if row['status'] != 'OK':
                    raise ConnectionError("Query failed.", row['result'])
            results.append(row['result'])
            return results
                
        if r[0]['status'] != 'OK':
            raise ConnectionError("Query failed.", r[0])
        return r[0]['result']

    def close(self):
        """Close the connection to SurrealDB."""
        self.session.close()

    def select_db(self, database):
        """Select a database."""
        self.database = database
        self._set_headers()

    def select_namespace(self, namespace):
        """Select a namespace."""
        self.namespace = namespace
        self._set_headers()

    def query(self, sql):
        """Execute an SQL query and return the result."""
        return self._send(sql)

    def select(self, sql):
        """Execute an SQL query and return the result."""
        return self.query(sql)

    def create(self, table, data=None):
        """Create one or many records in a SurrealDB table."""
        if isinstance(data, list):
            return self.create_many(table, data)
        return self.create_one(table, data)

    def insert(self, table, data=None):
        """Insert one or many records in a SurrealDB table."""
        return self.create(table, data)

    def relate(self, *args, **kwargs):
        """Create a relationship between two records."""
        # Relate is not actually a method of the surreal API, but its a useful and somewhat fundamental method so we include it here. Use the QueryBuilder to create a query.
        from ..query_builder import QueryBuilder
        return QueryBuilder(self).relate(*args, **kwargs)




    def create_one(self, table, data=None):
        """Create a new record in a SurrealDB table."""
        if 'id' in data:
            id = str(data['id'])
            if ':' in id:
                id_table, id = id.split(':')
                if id_table != table:
                    raise ValueError("Cannot create a record with an ID that doesn't match the table.")
            return self._send(data, method='POST', endpoint=f'key/{table}/{id}')

        return self._send(data, method='POST', endpoint=f'key/{table}')

    def create_many(self, table, data=None):
        """Create a new record in a SurrealDB table."""
        #TODO: Currently the SurrealDB API doesn't support multiple records, so we loop through and create them one at a time. However if the API is updated we should update this too for efficiency.
        results = []
        for row in data:
            r = self.create_one(table, row)
            results.append(r)
        return results

    def update(self, table, data):
        """Update a record in a SurrealDB table."""
        # we need an ID to update
        if 'id' not in data:
            raise ValueError("Cannot update a record without an ID.")
        id = str(data['id'])
        return self._send(data, method='PUT', endpoint=f'key/{table}/{id}')

    def delete(self, table, id=None):
        """Delete a record in a SurrealDB table."""
        if ':' in table:
            table, id = table.split(':')
        if not id:
            raise ValueError("Cannot delete a record without an ID. If you meant to delete the entire table, use the drop() method.")
        return self._send(None, method='DELETE', endpoint=f'key/{table}/{id}')

    def drop(self, table):
        """Drop a SurrealDB table."""
        return self._send(None, method='DELETE', endpoint=f'key/{table}')

    def get(self, table, id=None):
        """Get a record from a SurrealDB table."""
        if ':' in table:
            table, id = table.split(':')
        if not id:
            return self._send(None, method='GET', endpoint=f'key/{table}')
        return self._send(None, method='GET', endpoint=f'key/{table}/{id}')

    