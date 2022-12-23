# add parent directory to path
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import pysurrealdb as surreal

config = {
            "host": "localhost",
            "port": 8000,
            "user": "test",
            "password": "test",
            "database": "test",
            "namespace": "test"
        }
conn = surreal.connect(config)

def basic_example():
    conn.delete('test:test')
    conn.create('test', [{'id': 'test', 'name': 'test'}, {'name': 'Mike', 'age': 30}])
    records = conn.query('SELECT * FROM test')
    print(records)

def pandas_example():
    import pandas as pd

    df = pd.read_sql('SELECT * FROM test', conn)
    print(df)

def builder_example():
    conn.drop('person') # clear person table
    conn.insert('person', [{'name': 'Mike', 'age': 31}, {'name':'Mr P', 'age': 20}])

    first_person = conn.table('person').where('name', 'Mike').first()
    print(first_person)
    adults = conn.table('person').where('age', '>=', 18).order_by('age', 'desc').limit(10).get()
    print(adults)



if __name__ == '__main__':
    # basic_example()
    builder_example()