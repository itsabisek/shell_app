import pymysql
import sys
import optparse
import logging
import traceback as tb

formatter = logging.Formatter('%(filename)s:%(levelname)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('table_data.log')
file_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

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

try:
    conn = pymysql.connect(host='localhost', user='root', password='Admin@1234', db=db)
    logger.info("Connection established")
    logger.info("Using database %(db)s Table %(table)s" % {"db": db, "table": table})

    create_stmt = '''

    CREATE TABLE IF NOT EXISTS %(table_name)s(
    id INT(5) NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    department VARCHAR(255) NOT NULL,
    salary FLOAT(9,2) NOT NULL,
    PRIMARY KEY(id));

    ''' % {"table_name": table}

    with conn.cursor() as cur:
        cur.execute(create_stmt)
    conn.commit()

    exit_code = 0

    logger.info("Created table %s" % table)
    logger.info("Create statement is - %s" % create_stmt)

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

except Exception, e:
    logger.critical("Exception caught: %s" % e)
    # print "Exception caught: ", e
    tb.print_exc()

finally:
    sys.exit(int(exit_code))
