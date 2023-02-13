from .clients.http_client import HttpClient
from .clients.ws_client import WSClient
from .cursor import Cursor
from .query_builder import QueryBuilder
from .config import config
from .utils import verify_table_and_id

class Connection:
    """
    This is the main object that users will interact with. 
    It is a wrapper around the client objects with additional functionality.
    
    Includes methods to emulate a pymysql connection object. This allows the use of libraries like pandas. 
    """
    connections = {}
    client: HttpClient = None
    _cursor = None
    def __init__(self, **kwargs):
        """
            Attempt to build a connection. If a client is passed, use that. 

            Args:
                client: Specify client or client type. Can be a string or a client object. If a string, it must be 'http' or 'ws'. If a client object, it must be an instance of HttpClient or WSClient.
                host: The host of the server. 
                port: The port of the server. 
                user: The user to login with. 
                password: The password to login with. 
                database: The database to use. 
                namespace: The namespace to use.
        """
        client = kwargs.pop('client', None)
        
        # Set client type based on config.
        Client = HttpClient if config.default_client.lower() in ['http', 'https'] else WSClient
        # If they specify a client type explicitly, use that instead.
        if isinstance(client, str):
            Client = HttpClient if client.lower() in ['http', 'https'] else WSClient

        # If they pass a client object, use that. Otherwise, create a new client.
        if isinstance(client, HttpClient) or isinstance(client, WSClient):
            self.client = client
        elif kwargs:
            self.client = Client(**kwargs)
        else:
            self.client = Client()


    def cursor(self) -> Cursor:
        if not self._cursor:
            self._cursor = Cursor(self.client)
        return self._cursor

    def commit(self):
        pass

    # allow entry into query builder.
    def table(self, table) -> QueryBuilder:
        return QueryBuilder(self.client).table(table)

    def relate(self, noun1, verb, noun2, data=None):
        """Create a relationship between two records."""
        return QueryBuilder(self.client).relate(noun1, verb, noun2, data)

    def upsert(self, table, data=None, keys=['id']):
        """Update or create a record in the specified table"""
        if not data:
            data = table
            table, id = verify_table_and_id(None, data.get('id'))

        if ':' in table:
            if keys != ['id']:
                raise ValueError("Cannot upsert a record with a key that is not 'id' when using the table:id syntax.")
            table, id = verify_table_and_id(table, data.get('id'))
            r = self.get(table, id)
            if r:
                return self.update(table, data)
            else:
                return self.create(table, data)
            
        if isinstance(keys, str):
            keys = [keys]

        for k in keys:
            if k not in data:
                raise ValueError(f"Cannot upsert a record without a key. Key {k} not found in data.")

        conditions = []
        for key in keys:
            if key == 'id':
                table, id = verify_table_and_id(table, data.get('id'))
                conditions.append([key, f"{table}:{id}"])
            else:
                conditions.append([key, data[key]])
        existing = QueryBuilder(self).table(table).where(conditions).exists()
        if existing:
            return self.update(table, data)
        else:
            return self.create(table, data)

    # These methods are just wrappers around the client methods. They are here for autocomplete. If anyone knows a better way to do this, please let me know.
    def select(self, sql):
        return self.client.select(sql)

    def query(self, sql):
        return self.client.query(sql)

    def get(self, table, id=None):
        return self.client.get(table, id)

    def insert(self, table, data):
        return self.client.insert(table, data)

    def create(self, table, data):
        return self.client.create(table, data)

    def update(self, table, data=None):
        return self.client.update(table, data)

    def delete(self, table, id=None):
        return self.client.delete(table, id)

    def drop(self, table):
        return self.client.drop(table)

    # any other methods should just be passed to the client
    def __getattr__(self, name):
        return getattr(self.client, name)

    
    


    

    
    
