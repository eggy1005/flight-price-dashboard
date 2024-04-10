import unittest
from unittest.mock import patch, MagicMock
from processor import flink_processor


class TestMainFunction(unittest.TestCase):
    def assert_sql_contains(self, sql, arg):
        self.assertTrue(sql in arg[0][0])

    @patch('processor.flink_processor.StreamExecutionEnvironment.get_execution_environment')
    @patch('processor.flink_processor.StreamTableEnvironment.create')
    def test_main(self, mock_table_env_create, mock_env):
        mock_env_instance = MagicMock()
        mock_env.return_value = mock_env_instance

        mock_table_env_instance = MagicMock()
        mock_table_env_create.return_value = mock_table_env_instance

        flink_processor.consume_data()

        mock_table_env_create.assert_called_once()
        mock_table_env_instance.execute_sql.assert_called()
        args = mock_table_env_instance.execute_sql.call_args_list

        self.assert_sql_contains("earliest-offset", args[0])
        self.assert_sql_contains("CREATE TABLE FLIGHTPRICE", args[0])
        self.assert_sql_contains("DEPARTURE", args[1])
        self.assert_sql_contains("AVG(PRICE_USD) AS AVERAGE_PRICE", args[1])



if __name__ == '__main__':
    unittest.main()