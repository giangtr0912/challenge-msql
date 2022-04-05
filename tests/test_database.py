from src import utils

TABLE_NAME_1 = 'order_item_main_infos'
TABLE_NAME_2 = 'order_late_fee_infos'
TABLE_NAME_3 = 'order_merchant_infos'


def test_order_items_table_exists(cursor, order_items_project):
    cursor.execute('select * from {}'.format(utils.TABLE_NAME_1))
    rs = cursor.fetchall()
    assert len(rs) == 3306


def test_late_fee_table_exists(cursor, late_fee_project):
    # cursor.execute('select * from {}'.format(utils.TABLE_NAME_2))
    # rs = cursor.fetchall()
    # assert len(rs) == 2
    pass


def test_merchant_table_exists(cursor, merchant_project):
    # cursor.execute('select * from {}'.format(utils.TABLE_NAME_3))
    # rs = cursor.fetchall()
    # assert len(rs) == 2
    pass
