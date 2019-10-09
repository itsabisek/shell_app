import pymysql
import sys
import optparse

HOST = 'localhost'
USERNAME = 'root'
PASSWORD = "Admin@1234"

exit_code = 1

parser = optparse.OptionParser()

parser.add_option('-d', '--db', dest='db', help='Give the database name to be connected to')
parser.add_option('-t', '--table', dest='table', help='Give the table name to be operated on')

(options, args) = parser.parse_args()

if not options.db:
    parser.error("Please enter database name")
    sys.exit(exit_code)

if not options.table:
    parser.error('Please enter table name')
    sys.exit(exit_code)

db, table = options.db, options.table
conn = pymysql.connect(host='localhost', user='root', password='Admin@1234', db=db)


create_stmt = '''

CREATE TABLE IF NOT EXISTS %(table_name)s(
id INT(5) NOT NULL AUTO_INCREMENT,
name VARCHAR(255) NOT NULL,
department VARCHAR(255) NOT NULL,
salary FLOAT(9,2) NOT NULL,
PRIMARY KEY(id));

''' % {"table_name": table}

try:
    with conn.cursor() as cur:
        cur.execute(create_stmt)
    conn.commit()

    exit_code = 0

except pymysql.MySQLError, e:
    print "MySQL error: ", e

except pymysql.DatabaseError, e:
    print "Database Error occured: ", e

except pymysql.ProgrammingError, e:
    print "Programming Error: ", e

except pymysql.InternalError, e:
    print "Internal Error occured: ", e

except Exception, e:
    print "Exception caught: ", e

finally:
    sys.exit(int(exit_code))
