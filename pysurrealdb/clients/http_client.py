import requests, json, sys
from requests.auth import HTTPBasicAuth


class HttpClient:
    request_size_limit = 14000
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

        if host:
            if 'http:' not in self.host and 'https:' not in self.host:
                self.host = 'http://' + self.host
        else:
            print("SurrealDB: No host specified. Using localhost.")
            self.host = 'http://localhost'
        if not port:
            if host and 'https:' in host:
                self.port = 443
            else:
                self.port = 8000
            print("SurrealDB: No port specified. Using", self.port)
        if not namespace:
            print("SurrealDB: No namespace specified. Using main.")
            self.namespace = 'main'
        if not database:
            print("SurrealDB: No database specified. Using main.")
            self.database = 'main'

        self._set_headers()

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
        url = f"{self.host}:{self.port}/{endpoint}"
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

    def _send_chunks(self, data, method='POST', endpoint='sql'):
        """Send a request to SurrealDB and return the response."""
        # Note: Not currently used, but may be useful in the future
        url = f"{self.host}:{self.port}/{endpoint}"
        if isinstance(data, str):
            r = self.session.request(method, url, data=data, auth=self.auth, stream=True, timeout=5)
        else:
            r = self.session.request(method, url, json=data, auth=self.auth, stream=True, timeout=5)
        r.raise_for_status()

        receive_timeout = 5
        import time
        content = ''
        start = time.time()
        for chunk in r.iter_content(1024):
            if time.time() - start > receive_timeout:
                raise ConnectionError('SurrealDB timeout reached')

            if chunk:
                content += chunk.decode('utf-8')
        
        content = json.loads(content)
        return content[0]['result']

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

    def use(self, ns, db):
        """Select a namespace and database."""
        self.select_namespace(ns)
        self.select_db(db)

    def query(self, sql):
        """Execute an SQL query and return the result."""
        return self._send(sql)

    def select(self, sql):
        """Execute an SQL query and return the result."""
        return self.query(sql)

    def create(self, table, data):
        """Create one or many records in a SurrealDB table."""
        if isinstance(data, list):
            return self.create_many(table, data)
        return self.create_one(table, data)

    def insert(self, table, data):
        """Insert one or many records in a SurrealDB table."""
        return self.create(table, data)

    def create_large(self, table, data):
        """Create a record in a SurrealDB table that is larger than Surreal's request limit."""
        # we have to use query method for this, since it has a larger limit than they key endpoints.
        # format needs to be like this: INSERT INTO company {name: 'SurrealDB',founded: "2021-09-10",founders: [person:tobie, person:jaime], tags: ['big data', 'database']}
        data_string = ','.join([f"{k}: {json.dumps(v, default=str)}" for k, v in data.items()])
        return self.query(f"INSERT INTO {table} " + "{"+data_string +"}")

    def relate(self, *args, **kwargs):
        """Create a relationship between two records."""
        # Relate is not actually a method of the surreal API, but its a useful and somewhat fundamental method so we include it here. Use the QueryBuilder to create a query.
        from ..query_builder import QueryBuilder
        return QueryBuilder(self).relate(*args, **kwargs)

    def create_one(self, table, data=None):
        """Create a new record in a SurrealDB table."""
        # if the size of data is over 16kib, we must use create_large
        print('data size', self.getsizeof(data))
        if self.getsizeof(data) > self.request_size_limit: # use 14000 to be safe
            return self.create_large(table, data)

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

    def update(self, table, data=None):
        """Update a record in a SurrealDB table."""
        # we need an ID to update. It could be in either the table or data argument.
        id = None
        if ':' in table:
            table, id = table.split(':')

        if 'id' not in data and not id:
            raise ValueError("Cannot update a record without an ID.")
        
        id = str(data['id']) if id is None else id
        if ':' in id:
            id_table, id = id.split(':')
            if id_table != table:
                raise ValueError("Cannot update a record with an ID that doesn't match the table.", id_table, table)

        # check if the size of data is over 16kib, if so we must use update_large
        if self.getsizeof(data) > self.request_size_limit: # use 14000 to be safe
            return self.update_large(table, id, data)

        return self._send(data, method='PUT', endpoint=f'key/{table}/{id}')

    def update_large(self, table, id, data):
        """Update a record containing over 16kb of data."""
        # we have to use query method for this, since the surreal key endpoints all have a 16kb limit
        # format needs to be like this: UPDATE company SET name = 'SurrealDB',founded = "2021-09-10",founders = [person:tobie, person:jaime], tags = ['big data', 'database'] WHERE id = company:1
        data_string = ','.join([f"{k} = {json.dumps(v, default=str)}" for k, v in data.items()])
        return self.query(f"UPDATE {table} SET " + data_string + f" WHERE id = {table}:{id}")

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

    def getsizeof(self, data):
        """Get the size of a dictionary in bytes."""
        size = 0

        if isinstance(data, list):
            for item in data:
                size += self.getsizeof(item)
            return size
        if isinstance(data, dict):
            for k, v in data.items():
                size += self.getsizeof(k)
                size += self.getsizeof(v)
            return size
        
        return sys.getsizeof(data)
            