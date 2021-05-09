from re import sub
from datetime import datetime

import dbase
import dextract

def ingestion():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def is_awesome(genre):
    return genre == 'Games'

def normalize(title):
    res = title.replace(' ', '_').lower()
    return sub('[\W]+', '', res)

types = {
    'app': ('apps', 'is_awesome'), 
    'movie': ('movies', 'original_title_normalized'), 
    'song': ('songs', 'ingestion_time')
    }

def execute_insert(record, file_name):

    #determine record type
    rtype = record['type']
    
    #generate value for extra column
    if rtype == 'app':
        extra_value = is_awesome(record['data']['genre'])
    elif rtype == 'movie':
        extra_value = normalize(record['data']['original_title'])
    elif rtype == 'song':
        extra_value = ingestion()
    else: 
        return

    #determine table name and extra column name
    table_name = types[rtype][0]
    extra_column = types[rtype][1]
    
    #build and execute query
    columns = ','.join(record['data'].keys()) + ',' + extra_column
    values = list(record['data'].values())
    values.append(extra_value)
    placeholders = ','.join('?' * len(values))
    query = "INSERT INTO "+table_name+" ("+columns+") VALUES (" + placeholders + ")"

    dbase.cur.execute(query, values)

    if dbase.REGISTER_FILENAMES:
        values_register = [file_name, table_name, dbase.cur.lastrowid]
        query_register = "INSERT INTO register (file_name, table_name, row_id) VALUES (?, ?, ?)"
        dbase.cur.execute(query_register, values_register)

def main():

    #get new files
    new_files = dextract.get_new_files()

    #iterate over new files, execute insert query for each json record 
    for f in new_files:
        print('Processing ' + f)
        data = dextract.get_json_file_local(f)
        for record in data:
            execute_insert(record, f)

    dbase.conn.commit()

if __name__ == "__main__":
   main()
        
