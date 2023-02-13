def verify_table_and_id(table=None, id=None):
    """
    Verifies the table and id parameters and returns them seperately.

    Args:
        table (str): May take the form of 'table' or 'table:id'. 
        id (str): May take the form of 'id' or 'table:id'.

    Returns:
        table (str): The table name.
        id (str): The id.

    Raises:
        ValueError: If the table and id conflict.
    """

    table_table, table_id = None, None
    if table is not None:
        if ':' in table:
            table_table, table_id = table.split(':')
        else:
            table_table = table

    id_table, id_id = None, None
    if id is not None:
        if ':' in id:
            id_table, id_id = id.split(':')
        else:
            id_id = id

    if table_table is not None and id_table is not None and table_table != id_table:
        raise ValueError("table from ID doesn't match the given table", table_table, id_table)
    if table_id is not None and id_id is not None and table_id != id_id:
        raise ValueError("id from table doesn't match the given id", table_id, id_id)

    table = table_table or id_table
    id = table_id or id_id

    return table, id



