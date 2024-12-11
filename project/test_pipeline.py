import unittest
import os
import pandas as pd
import sqlite3

class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        # Get the absolute path to the directory containing this script
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Construct file paths relative to the script directory
        self.downloaded_US_Chronic_path = os.path.join(script_dir, "../data/downloaded_US_Chronic.csv")
        self.downloaded_NCHS_path = os.path.join(script_dir, "../data/downloaded_NCHS.csv")
        self.processed_data_path = os.path.join(script_dir, "../data/processed_data.db")
        # self.downloaded_US_Chronic_path = "../data/downloaded_US_Chronic.csv"
        # self.downloaded_NCHS_path = "../data/downloaded_NCHS.csv"
        # self.processed_data_path = "../data/processed_data.db"

    def test_data_download(self):
        self.assertTrue(os.path.isfile(self.downloaded_US_Chronic_path), "Chronic dataset not downloaded")
        self.assertTrue(os.path.isfile(self.downloaded_NCHS_path), "NCHS dataset not downloaded")

    def test_sql_file(self):
        self.assertTrue(os.path.isfile(self.processed_data_path), "SQLite database file does not exist")


    def test_processed_data(self):
        conn = sqlite3.connect(self.processed_data_path)

        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()

        self.assertTrue(('Merged_Data',) in tables, "Table does not exist in SQLite database")
        query = f"SELECT COUNT(*) FROM Merged_Data;"
        rows_count = conn.execute(query).fetchone()[0]
        self.assertGreater(rows_count, 0, f"No rows in the Merged_Data table.")

        conn.close()

    def test_table_columns(self):
        conn = sqlite3.connect(self.processed_data_path)

        expected_columns = ['Year', 'State', 'Deaths', 'Age_Adjusted_Death_Rate', 'ChronicDiseaseValue', 'GeoLocation']
        query = "PRAGMA table_info(Merged_Data);"
        columns = [col[1] for col in conn.execute(query).fetchall()]
        
        for column in expected_columns:
            self.assertIn(column, columns, f"Column {column} is missing from Merged_Data table")

        conn.close()

    def test_column_data_types(self):
        conn = sqlite3.connect(self.processed_data_path)

        query = "PRAGMA table_info(Merged_Data);"
        columns_info = {col[1]: col[2] for col in conn.execute(query).fetchall()}

        expected_data_types = {
            'Year': 'INTEGER',
            'State': 'TEXT',
            'Deaths': 'INTEGER',
            'Age_Adjusted_Death_Rate': 'REAL',
            'ChronicDiseaseValue': 'REAL',
            'GeoLocation': 'TEXT'
        }

        for column, expected_type in expected_data_types.items():
            self.assertEqual(columns_info.get(column), expected_type, f"The Column {column} has an incorrect data type")
        conn.close()

    def test_no_missing_values(self):
        conn = sqlite3.connect(self.processed_data_path)
        
        query = "SELECT * FROM Merged_Data WHERE Year IS NULL OR State IS NULL OR Deaths IS NULL OR Age_Adjusted_Death_Rate IS NULL OR ChronicDiseaseValue IS NULL;"
        result = conn.execute(query).fetchall()

        self.assertEqual(len(result), 0, "There are missing values in critical columns")

        conn.close()

if __name__ == '__main__':
    unittest.main()
