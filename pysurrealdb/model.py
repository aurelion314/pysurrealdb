from .connections import Connection
from .query_builder import QueryBuilder


class Model:
    """
    A class to represent a model that can be used to load and store data from the database.

    Extend this to create your own models. Requires table name.
    """
    _table = None
    _data = {}
    _client = None
    _id = None

    def __init__(self):
        if not self._table:
            raise Exception('Table name not set.')
        
        self._connection = Connection()

    @property
    def _query_builder(self):
        return QueryBuilder(self._connection)

    def get(self):
        """
        Get a single row from the database.
        """
        res = self._query_builder.table(self._table).first()
        if res:
            self._data = res
            self._id = res['id']
        return self
        

    def all(self):
        """
        Get all rows from the database.
        """
        res = self._query_builder.table(self._table).get()
        all = []
        for row in res:
            model = self.__class__()
            model._data = row
            model._id = row['id']
            all.append(model)
        return all

    def find(self, id):
        """
        Find a row by id.
        """
        res = self._query_builder.table(self._table).find(id)
        if res:
            self._data = res
            self._id = res['id']
        return self

    def where(self, *args):
        """
        Find a row by column and value.

        TODO: This should return a new model instance. 
        """
        return self._query_builder.table(self._table).where(*args)

    def save(self):
        """
        Save the model to the database.
        """
        if self._id:
            self._data['id'] = self._id
            return self._connection.update(self._table, self._data)
        else:
            return self._connection.insert(self._table, self._data)