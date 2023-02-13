import pytest
from pysurrealdb import connect

conn = connect('localhost', 8000, 'test', 'test', 'test', 'test', 'http')

def test_connect():
    assert conn is not None

def test_query():
    assert conn.query('SELECT * FROM emptytable') == []

def test_create():
    conn.drop('test')
    records = conn.create('test', {'id': 'test', 'name': 'test'})
    assert records == [{'id': 'test:test', 'name': 'test'}]

def test_delete():
    conn.upsert('test', {'id': 'test', 'name': 'test'})
    records = conn.delete('test:test')
    assert records == []

def test_update():
    conn.drop('test')
    conn.insert('test', {'id': 'test', 'name': 'test'})
    records = conn.update('test:test', {'name': 'test2'})
    assert records == [{'id': 'test:test', 'name': 'test2'}]

def test_query_builder():
    conn.drop('test')
    records = conn.table('test').insert([{ 'id': 'test', 'name': 'test' }, { 'id': 'test2', 'name': 'test2' }])
    records = conn.table('test').where('id', 'test:test').get()
    assert records == [{'id': 'test:test', 'name': 'test'}]

def test_escaping():
    conn.drop('test')
    records = conn.table('test').insert({'name': "'test'" })
    records = conn.table('test').where('name', "'test'").get()
    assert records[0]['name'] == "'test'"

def test_where():
    conn.drop('test')
    records = conn.table('test').insert([{'name': 'test', 'age': 2 }, {'name': 'test2', 'age': 12}, {'name': 'test', 'age': 42}])

    records = conn.table('test').where('name', 'contains', "test").get()
    assert len(records) == 3
    records = conn.table('test').where('age', 2).where('name', 'test').get()
    assert len(records) == 1
    records = conn.table('test').where([['age', 2], ['name', 'test']]).get()
    assert len(records) == 1
    records = conn.table('test').where_in('age', [2, 12]).get()
    assert len(records) == 2
    records = conn.table('test').where('age', 2).or_where([['age', 42], ['name', '=', 'test']]).get()
    assert len(records) == 2
