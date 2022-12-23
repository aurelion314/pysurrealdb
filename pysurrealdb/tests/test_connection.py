import pytest
from pysurrealdb import connect


def test_connect():
    conn = connect('localhost', 8000, 'testuser', 'passicus', 'test', 'test')
    assert conn is not None

def test_query():
    conn = connect('localhost', 8000, 'testuser', 'passicus', 'test', 'test')
    assert conn.query('SELECT * FROM emptytable') == []

def test_delete():
    conn = connect('localhost', 8000, 'testuser', 'passicus', 'test', 'test')
    records = conn.delete('test:test')
    assert records == []

def test_create():
    conn = connect('localhost', 8000, 'testuser', 'passicus', 'test', 'test')
    records = conn.create('test', {'id': 'test', 'name': 'test'})
    assert records == [{'id': 'test:test', 'name': 'test'}]