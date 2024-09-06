from  src.utils.app_logger import app_logger
from functools import wraps
from typing import Callable

def db_metadata_analysis():
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            def calculate_row_counts(db):
                cursor = db.conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                    row_count = cursor.fetchone()[0]
                    yield table_name, row_count

            def print_row_counts(row_counts, message):
                app_logger.info(f"========================================================================================================")
                app_logger.info(message)
                for table, count in row_counts.items():
                    app_logger.info(f"{table}: {count} rows")
                app_logger.info(f"========================================================================================================")
            db = args[0]['osint_database']
            row_counts_before = dict(calculate_row_counts(db))
            result = func(*args, **kwargs)
            row_counts_after = dict(calculate_row_counts(db))
            print_row_counts(row_counts_before, "Row counts before function execution:")
            print_row_counts(row_counts_after, "Row counts after function execution:")
            return result
        return wrapper
    return decorator


'''
Can only be used on modules
'''
def db_metadata_analysis_module():
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            def calculate_row_count_single_table(data_struct):
                cursor =  data_struct['db'].conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM { data_struct['table_name']};")
                row_count = cursor.fetchone()[0]
                return {data_struct['table_name']: row_count}

            def calculate_distinct_counts(data_struct):
                cursor = data_struct['db'].conn.cursor()
                columns = ', '.join(data_struct['column_list'])

                # Construct the SQL query to count distinct combinations of values
                query = f"""SELECT COUNT(*) FROM 
                            (
                                SELECT DISTINCT {columns} 
                                FROM {data_struct['table_name']}
                            )"""
                cursor.execute(query)
                row_count = cursor.fetchone()
                return {data_struct['table_name']: row_count[0]}
            
            
            def prepare_split_input_output_struct(data_struct):
                input_table = data_struct.copy()
                output_table = data_struct.copy()
                if 'module_table_name' in input_table:
                    input_table['table_name'] = input_table.pop('module_table_name')
                    input_table.pop('target_table_name')
                if 'target_table_name' in output_table:
                    output_table['table_name'] = output_table.pop('target_table_name')
                    output_table.pop('module_table_name')
                return input_table, output_table
                
            def generator_print_results(row_counts, distinct_counts, message):
                app_logger.info(f"========================================================================================================")
                app_logger.info(message)
                for table, count in row_counts.items():
                    app_logger.info(f"{table}: {count} rows")
                app_logger.info(f"Distinct counts for {table}:")
                for table, count in distinct_counts.items():
                    app_logger.info(f"{table}: {count} rows")
                app_logger.info(f"========================================================================================================")

            def generator_calculate_and_print(data_struct, message=''):
                input_table_struct, output_table_struct = prepare_split_input_output_struct(data_struct)
                
                row_counts = {}
                row_counts = calculate_row_count_single_table(input_table_struct)
                row_counts = calculate_row_count_single_table(output_table_struct)
                
                distinct_counts = {}
                distinct_counts = calculate_distinct_counts(input_table_struct)
                distinct_counts = calculate_distinct_counts(output_table_struct)
         
                generator_print_results(row_counts, distinct_counts, message)
                
            def create_db_metadata_analysis_struct():
                return {
                    'db': self.db,
                    'module_table_name': self.tablename,
                    'target_table_name': self.target_table,
                    'column_list' : self.column_mappings_rated_lists['primary'] 
                    }   

            self = args[0] 
            data_struct = create_db_metadata_analysis_struct()
            generator_before = generator_calculate_and_print(data_struct)
            result = func(*args, **kwargs)
            generator_after = generator_calculate_and_print(data_struct)
            return result
        return wrapper
    return decorator
