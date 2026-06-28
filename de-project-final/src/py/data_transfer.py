import csv
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.models import Variable


class DataTransfer:
    def __init__(self, pg_conn_id='postgres_conn', vertica_conn_id='vertica_conn'):
        self.pg_conn_id = pg_conn_id
        self.vertica_conn_id = vertica_conn_id

    def export_data_to_csv(self, table_name, date):
        csv_file_path = f'/lessons/{table_name}_{date}.csv'
        os.makedirs('/lessons', exist_ok=True)
        
        if table_name == "transactions":
            select_query = f"SELECT * FROM {table_name} WHERE transaction_dt::date = %s;"
        else:
            select_query = f"SELECT * FROM {table_name} WHERE date_update::date = %s;"
            
        # Connecting to PostgreSQL
        pg_hook = PostgresHook(self.pg_conn_id)
        with pg_hook.get_conn() as pg_connection:
            with pg_connection.cursor() as pg_cursor:
                pg_cursor.execute(select_query, (date,))
                rows = pg_cursor.fetchall()
                columns = [desc[0] for desc in pg_cursor.description]

        # Writing data to a CSV file
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(columns)
            csv_writer.writerows(rows)

        print(f"Data successfully exported to file {csv_file_path}")

    def import_data_to_vertica(self, table_name, date):
        csv_file_path = f'/lessons/{table_name}_{date}.csv'
        schema_prefix = Variable.get("VERTICA_SCHEMA_PREFIX", "stv202311131").lower()
        
        # Ensure idempotency by deleting any existing records for this date
        if table_name == "transactions":
            delete_query = f"DELETE FROM {schema_prefix}__STAGING.{table_name} WHERE transaction_dt::date = %s;"
        else:
            delete_query = f"DELETE FROM {schema_prefix}__STAGING.{table_name} WHERE date_update::date = %s;"

        # Query to insert data into Vertica
        vertica_copy_query = f'''
            COPY {schema_prefix}__STAGING.{table_name} 
            FROM LOCAL '{csv_file_path}' DELIMITER ',' NULL AS 'null';
        '''
        # Connecting to Vertica
        vertica_hook = VerticaHook(self.vertica_conn_id)
        with vertica_hook.get_conn() as vertica_connection:
            with vertica_connection.cursor() as vertica_cursor:
                vertica_cursor.execute(delete_query, (date,))
                vertica_cursor.execute(vertica_copy_query)
            vertica_connection.commit()
                
        print(f"Data successfully inserted into Vertica table {schema_prefix}__STAGING.{table_name}")

    def read_sql_script(self, filepath):
        with open(filepath, 'r', encoding='utf-8') as file:
            return file.read()
    
    def insert_into_global_metrics(self, date):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sql_filepath = os.path.join(current_dir, '../sql/global_metrics_insert.sql')
        
        sql_script = self.read_sql_script(sql_filepath)
        schema_prefix = Variable.get("VERTICA_SCHEMA_PREFIX", "stv202311131").lower()
        
        # Deletion query to clear existing metrics for the date to ensure idempotency
        delete_query = f"DELETE FROM {schema_prefix}__DWH.global_metrics WHERE date_update = %s;"
        
        # Replace hardcoded schema prefix (both cases)
        formatted_script = sql_script.replace('stv202311131', schema_prefix).replace('STV202311131', schema_prefix)
        formatted_script = formatted_script.format(date=date)
        
        print('Executing formatted script:', formatted_script)
        
        # Executing target showcase update in Vertica
        vertica_hook = VerticaHook(self.vertica_conn_id)
        with vertica_hook.get_conn() as vertica_connection:
            with vertica_connection.cursor() as vertica_cursor:
                vertica_cursor.execute(delete_query, (date,))
                vertica_cursor.execute(formatted_script)
            vertica_connection.commit()
                
        print("Successfully updated global_metrics in Vertica")

    def run_ddl_script(self, force_recreate=False):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sql_filepath = os.path.join(current_dir, '../sql/DE_final_ddl.sql')
        sql_script = self.read_sql_script(sql_filepath)
        
        schema_prefix = Variable.get("VERTICA_SCHEMA_PREFIX", "stv202311131").lower()
        
        # Create schemas first if they don't exist
        create_schemas_query = f"""
            CREATE SCHEMA IF NOT EXISTS {schema_prefix}__STAGING;
            CREATE SCHEMA IF NOT EXISTS {schema_prefix}__DWH;
        """
        
        if not force_recreate:
            # Strip out DROP TABLE statements if we're not forcing recreation
            lines = sql_script.split('\n')
            sql_script = '\n'.join([line for line in lines if not line.strip().upper().startswith('DROP TABLE')])

        formatted_script = sql_script.replace('stv202311131', schema_prefix).replace('STV202311131', schema_prefix)
        # Safe drop table with IF EXISTS (in case force_recreate is True)
        formatted_script = formatted_script.replace('DROP TABLE ', 'DROP TABLE IF EXISTS ')
        
        # Vertica syntax doesn't always support CREATE PROJECTION IF NOT EXISTS in all versions, 
        # so we will execute each statement inside a try-except block so it doesn't crash if projection/table already exists.
        vertica_hook = VerticaHook(self.vertica_conn_id)
        with vertica_hook.get_conn() as vertica_connection:
            with vertica_connection.cursor() as vertica_cursor:
                # Execute schema creation
                vertica_cursor.execute(create_schemas_query)
                # Split and execute DDL script
                for query in formatted_script.split(';'):
                    clean_query = query.strip()
                    if clean_query:
                        try:
                            vertica_cursor.execute(clean_query)
                        except Exception as e:
                            # Log and continue if table/projection already exists and force_recreate is False
                            print(f"Statement skipped or failed: {clean_query}. Error: {e}")
        print("Successfully executed Vertica DDL script")
