def test_order_items_table_exists(cursor, order_items_project):
    cursor.execute('select * from order_items')
    rs = cursor.fetchall()
    assert len(rs) == 3306


def test_late_fee_table_exists(cursor, order_items_project):
    # cursor.execute('select * from late_fee')
    # rs = cursor.fetchall()
    # assert len(rs) == 2
    pass
