import os

import dotenv
import mysql.connector
import psycopg2

os.environ['TZ'] = 'UTC'

dotenv.load_dotenv()

event_source_id = os.environ['EVENT_SOURCE_ID']
course_record_page_size = os.getenv('COURSE_RECORD_PAGE_SIZE', 200000)

# DB

batch_size = 1000


def get_mysql_connection():
    return mysql.connector.connect(
        database='learner_record',
        host=os.environ['MYSQL_HOST'],
        user=os.environ['MYSQL_USER'],
        password=os.environ['MYSQL_PASSWORD']
    )


def get_pg_connection():
    return psycopg2.connect(
        dbname='reporting',
        host=os.environ['PG_HOST'],
        password=os.environ['PG_PASSWORD'],
        port=5432,
        user=os.environ['PG_USER']
    )
