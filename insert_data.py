import pymysql
import optparse
import sys
import os
import csv
import traceback as tb
import logging

# from concurrent.futures import ThreadPoolExecutor, as_completed
# import thread

formatter = logging.Formatter('%(filename)s:%(levelname)s:%(name)s:%(message)s')
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
# data is a list of the form - ['Name':str, 'Department':str, Salary:float]
def insert_data(connection, data):
    insert_stmt = '''

    INSERT INTO %(table_name)s(name,department,salary)
    VALUES(\"%(name)s\",\"%(department)s\",%(salary)f);

    ''' % {"table_name": table, "name": data[0], "department": data[1], "salary": float(data[2])}

    # print insert_stmt
    with connection.cursor() as cur:
        return cur.execute(insert_stmt)


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
    conn = pymysql.connect(host=HOST, user=USERNAME, password=PASSWORD, db=db)
    logger.info("Connection established")
    logger.info("Using database %(db)s Table %(table)s" % {"db": db, "table": table})
    logger.info("Will be importing data from path %s" % (data_path))

    data, empty_entries = get_csv_data(data_path)
    logger.info("Will be inserting %d data rows" % len(data))
    logger.warning("Skipping %d data rows. They have empty values" % len(empty_entries))

    insert_count = 0
    for entry in data:
        insert_count += insert_data(conn, entry)

    exit_code = 0
    logger.info("Inserted %d rows in db" % insert_count)

except IOError, e:
    logger.critical("IO Error: %s" % e)
    tb.print_exc()

except pymysql.MySQLError, e:
    logger.critical("MySQL Error: %s" % e)
    # print "MySQL error: ", e
    tb.print_exc()

except pymysql.DatabaseError, e:
    logger.critical("Database Error: %s" % e)
    # print "Database Error occured: ", e
    tb.print_exc()

except pymysql.ProgrammingError, e:
    logger.critical("Programming Error: %s" % e)
    # print "Programming Error: ", e
    tb.print_exc()

except pymysql.InternalError, e:
    logger.critical("Internal Error occured: " % e)
    # print "Internal Error occured: ", e
    tb.print_exc()

except pymysql.DataError, e:
    logger.critical("Invalid data: %s" % e)
    # print "Invalid data: ", e
    tb.print_exc()

except Exception, e:
    logger.critical("Exception caught: %s" % e)
    # print "Exception caught: ", e
    tb.print_exc()

finally:
    conn.commit()
    conn.close()

logger.info("Exiting with exit code %d" % exit_code)
sys.exit(exit_code)
