import pytest
from pysurrealdb import connect

conn = connect('localhost', 8000, 'test', 'test', 'test', 'test')

def test_connect():
    assert conn is not None

def test_query():
    assert conn.query('SELECT * FROM emptytable') == []

def test_delete():
    records = conn.delete('test:test')
    assert records == []

def test_create():
    records = conn.create('test', {'id': 'test', 'name': 'test'})
    assert records == [{'id': 'test:test', 'name': 'test'}]

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