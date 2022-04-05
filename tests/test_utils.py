from mock_db import MockDB
from src import utils

db_testconfig = utils.DB_config['test']


class TestUtils(MockDB):

    def test_db_write(self):
        with self.mock_db_config:
            self.assertEqual(
                utils.db_write(
                    """INSERT INTO `test_table` (`id`, `text`, `int`) VALUES
                            ('3', 'test_text_3', 3)""", db_testconfig), True)
            self.assertEqual(
                utils.db_write(
                    """INSERT INTO `test_table` (`id`, `text`, `int`) VALUES
                            ('1', 'test_text_3', 3)""", db_testconfig), False)
            self.assertEqual(
                utils.db_write("""DELETE FROM `test_table` WHERE id='1' """,
                               db_testconfig), True)
            self.assertEqual(
                utils.db_write("""DELETE FROM `test_table` WHERE id='4' """,
                               db_testconfig), True)
