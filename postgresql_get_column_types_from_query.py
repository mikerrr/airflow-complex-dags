# -*- coding: utf-8 -*-




'''

returns a dict with column names and their respective types 

'''

def get_column_types_from_query(postgres_hook, sql_query):
    
    # get column types: create temp view -> get types from it -> delete temp view
    query = f'CREATE OR REPLACE VIEW tmp_view_python{postfix} AS(\n' + sql_query
    query = query.replace(';', ');')
    postgres_hook.run(query)
    
    CUR_DIR = os.path.abspath(os.path.dirname(__file__)) 
    with open(CUR_DIR+'/sql/get_column_types.sql', "r") as file:
        get_column_types_sql = file.read()
        get_column_types_sql = get_column_types_sql.replace('tablename', f'tmp_view_python{postfix}')
    column_types = dict(postgres_hook.get_records(get_column_types_sql))    
    
    postgres_hook.run(f'DROP VIEW tmp_view_python{postfix};')
    return column_types



'''

why it is needed?

1. Timestamp with timezone Pandas processing

You load pandas dataframe from query, like this:
df = pgsql.get_pandas_df(query)

Timestamp with timezone columns processed incorrectly and automatically converted to UTC.

to not allow this add:
'''    
# process timestamp with timezone, column_types is from get_column_types_from_query() function

param_dict = {col:{'utc':False} for col in column_types if 'timestamp with time zone' in column_types[col].lower()}    

df = pgsql.get_pandas_df(query, parse_dates = param_dict)

'''
timezone will be processed correctly

2. Integer columns with NULLs Pandas processing

You load pandas dataframe from query, like this:
df = pgsql.get_pandas_df(query)

All INTEGER columns with NULLS will be converted to FLOAT (NUMERIC).

to save corect CSV add:
'''

# process int types
need_to_convert_types = ['bigint', 'integer', 'smallint', 'smallserial', 'serial', 'bigserial']
convert_to_int_columns = [col for col in column_types if column_types[col] in need_to_convert_types]
for col in convert_to_int_columns:
    df[col] = df[col].apply(lambda x: '' if pd.isna(x) else str(int(x))) 

'''

3. Automatic creation of CREATE TABLE script from query... see separate file

'''


