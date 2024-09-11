import csv
import os


class DataTransfer:
    def __init__(self, pg_conn, vertica_hook):
        self.pg_conn = pg_conn
        self.vertica_hook = vertica_hook

    def export_data_to_csv(self, table_name, date):
        csv_file_path = f'/lessons/{table_name}_{date}.csv'
        if table_name == "transactions":
            select_query = f"SELECT * FROM {table_name} WHERE transaction_dt::date = %s;"
        else:
            select_query = f"SELECT * FROM {table_name} WHERE date_update::date = %s;"
        # Connecting to PostgreSQL
        with self.pg_conn as pg_connection:
            with pg_connection.cursor() as pg_cursor:
                pg_cursor.execute(select_query, (date,))
                rows = pg_cursor.fetchall()

        # Writing data to a CSV file
        with open(csv_file_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([desc[0] for desc in pg_cursor.description])
            csv_writer.writerows(rows)

        print(f"Data successfully exported to file {csv_file_path}")

    def import_data_to_vertica(self, table_name, date):
        csv_file_path = f'/lessons/{table_name}_{date}.csv'
        # Query to insert data into Vertica
        vertica_copy_query = f'''
            COPY STV202311131__STAGING.{table_name} 
            FROM LOCAL '{csv_file_path}' DELIMITER ',' NULL AS 'null';
        '''
        # Connecting to Vertica
        with self.vertica_hook as vertica_connection:
            with vertica_connection.cursor() as vertica_cursor:
                vertica_cursor.execute(vertica_copy_query)
                
        print("Data successfully inserted into Vertica table")

    def read_sql_script(self, filepath):
        with open(filepath, 'r') as file:
            return file.read()
    
    def insert_into_global_metrics(self, date):
        sql_script = self.read_sql_script('global_metrics_insert.sql')
        formatted_script = sql_script.format(date=date)
        print('formatted_script', formatted_script)
