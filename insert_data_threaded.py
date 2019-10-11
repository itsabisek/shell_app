import pymysql
import optparse
import sys
import os
import csv
import logging
import thread
import time

# from concurrent.futures import ThreadPoolExecutor, as_completed

formatter = logging.Formatter('[%(asctime)s] [%(filename)s] [%(levelname)s] [%(name)s] - %(message)s')
file_handler = logging.FileHandler('table_data.log')
file_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

HOST = 'localhost'
USERNAME = 'root'
PASSWORD = "Admin@1234"


# Opens file and gets an iterator to iterate over each row of the file
def get_csv_data(data_path):
    data = []
    empty_entries = []
    with open(data_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if validate_row(row):
                data.append(row)
                logger.info("Will be inserting row %s" % row)
            else:
                empty_entries.append(row)
                logger.warning("Empty cell in row %s. Excluding it." % row)
    return data, empty_entries


# Checks for rows with empty strings as empty strings are not
# considered as NULL in sql
def validate_row(row):
    for cell in row:
        if len(cell) == 0:
            return False
    return True


# Inserts data into the database using the connection passed
# data is a list of the rows where each row is of the form - ['Name':str, 'Department':str, Salary:float]
def insert_data(connection, data, lock, thread_name):

    logger.info("Inserting %d rows using thread %s" % (len(data), thread_name))
    insert_stmt = '''

    INSERT INTO %(table_name)s(name,department,salary)
    VALUES(\"%(name)s\",\"%(department)s\",%(salary)f);

    '''
    if lock.locked():
        logger.warning("Lock is alreay acquired!")

    logger.info('Acquiring lock over connection')
    lock.acquire()
    with connection.cursor() as cur:
        for entry in data:
            stmt = insert_stmt % {"table_name": table, "name": entry[0], "department": entry[1], "salary": float(entry[-1])}
            cur.execute(stmt)
    logger.info("Releasing lock over connection")
    lock.release()


exit_code = 1

parser = optparse.OptionParser()

parser.add_option('-d', '--db', dest='db', help='Give the database name to be connected to')
parser.add_option('-t', '--table', dest='table', help='Give the table name to be operated on')
parser.add_option('-r', '--data', dest='data', help='Give the path the file from which data is to be populated')

(options, args) = parser.parse_args()

if not options.db:
    parser.error("Please enter database name")
    sys.exit(exit_code)

if not options.table:
    parser.error('Please enter table name')
    sys.exit(exit_code)

if not options.data:
    parser.error("Please enter path to data")
    sys.exit(exit_code)

db, table = options.db, options.table
data_path = os.path.join(os.path.abspath(os.curdir), options.data)


try:
    lock = thread.allocate_lock()
    conn = pymysql.connect(host=HOST, user=USERNAME, password=PASSWORD, db=db)
    logger.info("Connection established")
    logger.info("Using database %(db)s Table %(table)s" % {"db": db, "table": table})
    logger.info("Will be importing data from path %s" % (data_path))

    data, empty_entries = get_csv_data(data_path)
    logger.info("Will be inserting %d data rows" % len(data))
    logger.warning("Skipping %d data rows. They have empty values" % len(empty_entries))

    thread_1 = thread.start_new_thread(insert_data, (conn, data[:len(data) // 2], lock, "Thread 1"))
    thread_2 = thread.start_new_thread(insert_data, (conn, data[len(data) // 2:], lock, "Thread 2"))

    time.sleep(2)
    exit_code = 0


except IOError, e:
    logger.error("IO Error: %s" % e, exc_info=True)

except pymysql.MySQLError, e:
    logger.error("MySQL Error: %s" % e, exc_info=True)

except pymysql.DatabaseError, e:
    logger.error("Database Error: %s" % e, exc_info=True)

except pymysql.ProgrammingError, e:
    logger.error("Programming Error: %s" % e, exc_info=True)

except pymysql.InternalError, e:
    logger.error("Internal Error occured: " % e, exc_info=True)

except pymysql.DataError, e:
    logger.error("Invalid data: %s" % e, exc_info=True)

except Exception, e:
    logger.error("Exception caught: %s" % e, exc_info=True)

finally:
    conn.commit()
    conn.close()

logger.info("Exiting with exit code %d" % exit_code)
sys.exit(exit_code)
