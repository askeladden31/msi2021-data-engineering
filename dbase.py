import sqlite3

REGISTER_FILENAMES = False

conn = sqlite3.connect('msi2021-data-engineering.db')
cur = conn.cursor()

cur.execute(''' 
    CREATE TABLE IF NOT EXISTS songs (
        artist_name varchar,
        title varchar,
        year integer,
        release varchar,
        ingestion_time datetime
    )
    ''')

cur.execute(''' 
    CREATE TABLE IF NOT EXISTS movies (
        original_title	varchar,
        original_language varchar,
        budget	integer,
        is_adult	boolean,
        release_date	date,
        original_title_normalized	varchar
    )
    ''')

cur.execute(''' 
    CREATE TABLE IF NOT EXISTS apps (
        name	varchar,
        genre	varchar,
        rating	float,
        version	varchar,
        size_bytes	integer,
        is_awesome	boolean
    )
    ''')

if REGISTER_FILENAMES:
    cur.execute(''' 
        CREATE TABLE IF NOT EXISTS register (
            file_name varchar,
            table_name varchar,
            row_id integer
        )
        ''')
