import mypysurreal


def get_connection():
    host = 'localhost'
    port = 8000
    user = 'testuser'
    password = 'passicus'
    database = 'test'
    namespace = 'test'
    
    return mypysurreal.connect(host, port, user, password, database, namespace)

def run_example():
    # Create a connection. This connection object includes a cursor method to simulate a pymysql connection object for use with pandas or similar libraries. 
    conn = get_connection()
    # You can also create the client directly. This is preferred if you don't need to simulate a cursor object. Either one can be used to call query() and similar methods.
    client = mypysurreal.HttpClient('localhost', 8000, 'testuser', 'passicus', 'test', 'test')

    new_record = conn.create('test', {'id': 'test', 'name': 'test'})
    records = conn.query('SELECT * FROM test')

    print(records)

    conn.delete('test:test')

def pandas_example():
    import pandas as pd
    conn = get_connection()

    df = pd.DataFrame('SELECT * FROM test', conn)
    print(df)

if __name__ == '__main__':
    run_example()
    pandas_example()