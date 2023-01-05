
class QueryBuilder:
    """
    This class is used to build a query to send to the database. 
    Similar to Laravel and Orator's query builder.
    """
    client = None
    _where = []
    _limit = None
    _order_by = None
    _group_by = None
    _select = None
    _table = None
    _type = 'select'
    _relate = None
    _fetch = []
    _data = None
    _escape_table = None

    def __init__(self, client):
        self.client = client
        self._where = []
        self._limit = None
        self._order_by = None
        self._group_by = None
        self._select = None
        self._table = None
        self._type = 'select'
        self._relate = None
        self._fetch = []
        self._data = None
        self._escape_table = None

    def where(self, *args) -> 'QueryBuilder':
        """
            Add a where clause to the query.

            You can pass an array of where clauses, or a series of arguments.
            If you pass an array, each element should be in the format ['column', 'operator', 'value'].
            If you pass a series of strings, they should be in the format 'column', 'operator' ,'value'. If no operator is specified, '=' is used.
        """
        self._where.append(args)

        return self

    def where_in(self, column, values) -> 'QueryBuilder':
        """
        Add a where in clause to the query. 
        """
        self._where.append([values, 'CONTAINS', column])
        return self

    def where_contains(self, column, value) -> 'QueryBuilder':
        return self.where_in(column, value)

    def where_not_in(self, column, values) -> 'QueryBuilder':
        """
        Add a where not in clause to the query. 
        """
        self._where.append([values, 'CONTAINSNOT', column])
        return self

    def where_contains_not(self, column, value) -> 'QueryBuilder':
        return self.where_not_in(column, value)

    def or_where(self, *args) -> 'QueryBuilder':
        """
        Add an or where clause to the query.
        """
        self._where.append(['OR'])
        return self.where(*args)

    def select(self, *args) -> 'QueryBuilder':
        """
        Set the columns to select.
        """
        self._select = args
        return self

    def insert(self, data):
        """
        Insert one or many rows.
        """
        return self.client.insert(self._table, data)

    def update(self, data):
        """
        Update fields.
        """
        self._type = 'update'
        self._data = data
        # we use the query method because the update api only supports 1 row at a time.
        return self.client.query(self._build_query())

    def relate(self, noun1, verb, noun2, data=None):
        """
        Relate 2 objects. Noun->Verb->Noun. You can also set data to be stored with the relationship.
        """
        self._type = 'relate'
        self._relate = [noun1, verb, noun2]
        self._data = data
        return self.client.query(self._build_query())

    def table(self, table) -> 'QueryBuilder':
        """
        Set the table to query.
        """
        self._table = table
        return self

    def limit(self, limit) -> 'QueryBuilder':
        """
        Set the limit.
        """
        self._limit = limit
        return self

    def order_by(self, column, direction='ASC') -> 'QueryBuilder':
        """
        Set the order by.
        """
        self._order_by = [column, direction]
        return self

    def group_by(self, column) -> 'QueryBuilder':
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

    def exists(self):
        """
        Execute the query and return True if any results are found.
        """
        self._select = ['id']
        self._limit = 1
        return bool(self.get())

    def fetch(self, column):
        """
        Add a fetch clause to the query.
        """
        # there is currently a bug that fetch sometimes fails without an order by. Add one in if not there.
        if not self._order_by:
            self._order_by = ['id', 'ASC']
        self._fetch.append(column)
        return self

    def count(self):
        """
        Execute the query and return the number of results.
        """
        self._select = ['count()']
        if not self._group_by:
            self._group_by = 'all'
        return self.get()[0]['count']

    def sum(self, column):
        """
        Execute the query and return the sum of a column.
        """
        self._select = [f'math::sum({column})']
        if not self._group_by:
            self._group_by = 'all'
        return self.get()[0][f'math::sum']

    def avg(self, column):
        """
        Execute the query and return the average of a column.
        """
        self._select = [f'math::mean({column})']
        if not self._group_by:
            self._group_by = 'all'
        return self.get()[0][f'math::mean']

    def mean(self, column):
        return self.avg(column)

    def max(self, column):
        """
        Execute the query and return the max of a column.
        """
        self._select = [f'math::max({column})']
        if not self._group_by:
            self._group_by = 'all'
        return self.get()[0][f'math::max']

    def min(self, column):
        """
        Execute the query and return the min of a column.
        """
        self._select = [f'math::min({column})']
        if not self._group_by:
            self._group_by = 'all'
        return self.get()[0][f'math::min']

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
            return f"'{self._escape_string(value)}'"
        return str(value)

    def _escape_string(self, value):
        """escapes *value* without adding quote.
        Value should be unicode
        """
        escape_table = self._build_escape_table()        
        return value.translate(escape_table)

    def _build_escape_table(self):
        if not self._escape_table:
            self._escape_table = [chr(x) for x in range(128)]
            self._escape_table[0] = "\\0"
            self._escape_table[ord("\\")] = "\\\\"
            self._escape_table[ord("\n")] = "\\n"
            self._escape_table[ord("\r")] = "\\r"
            self._escape_table[ord("\032")] = "\\Z"
            self._escape_table[ord('"')] = '\\"'
            self._escape_table[ord("'")] = "\\'"

        return self._escape_table

    def _build_query(self):
        """
        Build the query.
        """
        if self._type == 'select':
            return self._build_select()
        elif self._type == 'update':
            return self._build_update()
        elif self._type == 'relate':
            return self._build_relate()

    def _build_select(self):
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
            query += self._build_where(self._where)
        if self._group_by:
            query += f' GROUP BY {self._group_by}'
        if self._order_by:
            query += f' ORDER BY {self._order_by[0]} {self._order_by[1]}'
        if self._limit:
            query += f' LIMIT {self._limit}'
        if self._fetch:
            query += ' FETCH '
            query += ', '.join(self._fetch)

        return query

    def _build_where(self, _where):
        """
        Build the where clause.
        """
        query = ''
        for where in _where:
            # Check for an OR statement
            if len(where) == 1 and isinstance(where[0], str):
                query = query[:-4] # remove the previous 'AND'
                query += f' {where[0]} '
                continue
            # if all where elements are lists, it's a nested where
            if all(isinstance(x, list) for x in where):
                query += '('
                query += self._build_where(where)
                query += ')'
                query += ' AND '
                continue

            # if we only have 2 arguments, assume equals
            if len(where) == 2:
                where = [where[0], '=', where[1]]
            
            # if the first position is a list, its probably a contains clause. They are in reverse order in surreal. 
            if isinstance(where[0], list):
                query += '['
                for value in where[0]:
                    if isinstance(value, str):
                        query += f'{self._quote(value)}, '
                    else:
                        query += f'{value}, '
                query = query[:-2]
                query += f'] {where[1]} {where[2]}'
            elif isinstance(where[2], str):
                query += f'{where[0]} {where[1]} {self._quote(where[2])}'
            else:
                query += f'{where[0]} {where[1]} {where[2]}'
            query += ' AND '
            
        query = query[:-5]   
        return query     

    def _build_update(self):
        """
        Build the query.
        """
        query = f'UPDATE {self._table} SET '
        for key, value in self._data.items():
            if isinstance(value, str):
                query += f'{key}={self._quote(value)}, '
            else:
                query += f'{key}={value}, '
        query = query[:-2]
        if self._where:
            query += ' WHERE '
            query += self._build_where(self._where)         
        return query

    def _build_relate(self):
        """
        Build the query.
        """
        query = f'RELATE {self._relate[0]}->{self._relate[1]}->{self._relate[2]} '
        if self._data:
            query += ' SET '
            for key, value in self._data.items():
                if isinstance(value, str):
                    query += f'{key}={self._quote(value)}, '
                else:
                    query += f'{key}={value}, '
            query = query[:-2]

        return query

