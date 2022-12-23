
class QueryBuilder:
    """
    This class is used to build a query to send to the database. 
    Similar to Laravel and Orator's query builder.
    """
    client = None
    _where = None
    _limit = None
    _order_by = None
    _group_by = None
    _select = None
    _table = None

    def __init__(self, client):
        self.client = client


    def where(self, *args):
        """
            Add a where clause to the query.

            You can pass an array of where clauses, or a series of arguments.
            If you pass an array, each element should be in the format ['column', 'operator', 'value'].
            If you pass a series of strings, they should be in the format 'column', 'operator' ,'value'. If no operator is specified, '=' is used.
        """
        if len(args) == 1 and isinstance(args[0], list):
            self._where = args[0]
        elif len(args) == 3:
            self._where = [args]
        elif len(args) == 2:
            self._where = [[args[0], '=', args[1]]]

        return self

    def select(self, *args):
        """
        Set the columns to select.
        """
        self._select = args
        return self

    def table(self, table):
        """
        Set the table to query.
        """
        self._table = table
        return self

    def limit(self, limit):
        """
        Set the limit.
        """
        self._limit = limit
        return self

    def order_by(self, column, direction='ASC'):
        """
        Set the order by.
        """
        self._order_by = [column, direction]
        return self

    def group_by(self, column):
        """
        Set the group by.
        """
        self._group_by = column
        return self

    def get(self):
        """
        Execute the query and return the result.
        """
        return self.client.query(self._build_query())

    def first(self):
        """
        Execute the query and return the first result.
        Return None if no results are found.
        """
        self._limit = 1
        results = self.get()
        if results:
            return results[0]
        return None

    def count(self):
        """
        Execute the query and return the number of results.
        """
        self._select = ['COUNT(*)']
        return self.first()['COUNT(*)']

    def to_sql(self):
        """
        Return the query as a string.
        """
        return self._build_query()

    def _quote(self, value):
        """
        Quote a value for use in a query.
        """
        if isinstance(value, str):
            return f"'{value}'"
        return str(value)

    def _build_query(self):
        """
        Build the query.
        """
        query = 'SELECT '
        if self._select:
            query += ', '.join(self._select)
        else:
            query += '*'
        query += f' FROM {self._table}'
        if self._where:
            query += ' WHERE '
            for where in self._where:
                query += f'{where[0]} {where[1]} {self._quote(where[2])} AND '
            query = query[:-5]            
        if self._group_by:
            query += f' GROUP BY {self._group_by}'
        if self._order_by:
            query += f' ORDER BY {self._order_by[0]} {self._order_by[1]}'
        if self._limit:
            query += f' LIMIT {self._limit}'

        return query
