import pymysql
import optparse
import sys
import os
import csv
import traceback as tb
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import thread

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
            else:
                empty_entries.append(row)
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
        cur.execute(insert_stmt)


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

    data, empty_entries = get_csv_data(data_path)
    for entry in data:
        insert_data(conn, entry)

    exit_code = 0

except IOError, e:
    print "IO Error: ", e
    tb.print_exc()

except pymysql.MySQLError, e:
    print "MySQL error: ", e
    tb.print_exc()

except pymysql.DatabaseError, e:
    print "Database Error occured: ", e
    tb.print_exc()

except pymysql.ProgrammingError, e:
    print "Programming Error: ", e
    tb.print_exc()

except pymysql.InternalError, e:
    print "Internal Error occured: ", e
    tb.print_exc()

except pymysql.DataError, e:
    print "Invalid data: ", e
    tb.print_exc()

except Exception, e:
    print "Exception caught: ", e
    tb.print_exc()

finally:
    conn.commit()
    conn.close()

sys.exit(exit_code)
