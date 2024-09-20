##N.D'Angelo 9/20/2024
"""
Synopsis:
This script reads teller transaction data from a text file, processes the data, and inserts it into a SQL Server database. 
It uses SQLAlchemy for database operations and logging to track the process. The script ensures data integrity by using transactions.

Modules:
- pyodbc: For ODBC driver management.
- sqlalchemy: For database connection and operations.
- urllib: For URL encoding.
- pathlib: For file path operations.
- logging: For logging operations.
- datetime: For date and time operations.

Classes:
- Record: A class to represent a teller transaction record.
- db: A class to manage database connections and operations.

Functions:
- db.__init__: Initializes the database connection.
- db.Insert: Inserts a record into the database.

Usage:
- Reads data from a specified text file.
- Processes each line to create Record objects.
- Inserts records into the database using a transaction.
"""
from pyodbc import drivers
import sqlalchemy as sa
import urllib
from pathlib import Path
import logging
from logging.handlers import TimedRotatingFileHandler
from logging import Formatter
import datetime
from datetime import datetime


class Record:
    def __init__(self, processdate, br, user, tx_count):
        self.processdate = processdate
        self.Br = br
        self.User = user
        self.Tx_count = tx_count
    def __str__(self):
        return f"Record(processdate={self.processdate}, Br={self.Br}, User={self.User}, Tx_count={self.Tx_count})"

LOG_FILENAME = "c:\\KFCU_SSIS\\logs\\ImportTellerTotals2.log"

 # Create a named logger
logger = logging.getLogger(LOG_FILENAME)

# Configure the TimedRotatingFileHandler
handler = TimedRotatingFileHandler(
    filename=LOG_FILENAME,  # Specify your log file name
    when="D",  # Rotate daily
    interval=1,  # Create a new backup every day
    backupCount=90,  # Keep up to 90 backups (adjust as needed)
    encoding="utf-8",  # Specify the encoding
    delay=False,  # Write logs immediately (not buffered)
)

# Create a formatter and add it to the handler
formatter = Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

# Add the handler to the named logger
logger.addHandler(handler)

# Set the logging level (e.g., INFO, DEBUG, ERROR)
logger.setLevel(logging.INFO)

# from collections import namedtuple
# Record = namedtuple('Record', ['processdate', 'Br', 'User', 'Txcount'])

class db:
    def __init__(self):
        self.server = "VSARCU02"
        self.db = "kRAP"

        try:
            if 'ODBC Driver 17 for SQL Server' in drivers():
                odbcDriver = 'ODBC Driver 17 for SQL Server'
            elif 'ODBC Driver 13.1 for SQL Server' in drivers():
                odbcDriver = 'ODBC Driver 13.1 for SQL Server'
            elif 'ODBC Driver 13 for SQL Server' in drivers():
                odbcDriver = 'ODBC Driver 13 for SQL Server'
            else:
                raise ConnectionError('Missing ODBC driver')

            connstrARCU = f"Driver={odbcDriver}; Server={self.server}; Database={self.db}; Trusted_Connection=yes;"
            self.engine = sa.create_engine(
                f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connstrARCU)}"
            )

        except ConnectionError as Err:
            print(f"ConnectionError: {Err}")
            logger.log(logging.ERROR, f"ConnectionError: {Err}")
            raise


    def Insert(self, conn, Record:Record):
        result = conn.execute(
            sa.text("EXEC [dbo].[KFCU_sp_Insert_TellerTotals_TEST] :ProcessDate, :Branch, :User, :Txcount"),
            {
                'ProcessDate': Record.processdate, 
                'Branch': Record.Br, 
                'User': Record.User,
                'Txcount': Record.Tx_count
            }
        )
        return result.rowcount


Path1 = "\\\\kfcu\\share\\PR\\MIS\\ASD Area\\TellerTotals\\"

rows = []
FullPath = Path(Path1 + "TellerTransactions.txt")
if FullPath.is_file():
    with open(FullPath, 'r') as file:
        for line in file:
            Br, User, Txcount, TxDate = line.strip().split(",")
            Br = int(Br)
            User = int(User)
            Txcount = int(Txcount)
            TxDate = TxDate.strip()

            date_obj = datetime.strptime(TxDate, "%m/%d/%Y")

            # Format the datetime object into the desired format
            processdate = int(date_obj.strftime("%Y%m%d"))
            row = Record(processdate, Br, User, Txcount)
            rows.append(row)

db1 = db()
with db1.engine.connect() as conn:
    trans = conn.begin()  # Start a transaction
    try:
        for record in rows:
            rows_affected = db1.Insert(conn, record)
            if rows_affected > 0:
                print(f"Record inserted successfully: {record}")
            else:
                logger.log(logging.ERROR, record)
                trans.rollback()  # Rollback the transaction if an error occurs
                break
        else:
            trans.commit()  # Commit the transaction if all records are inserted successfully
    except Exception as e:
        trans.rollback()  # Rollback the transaction in case of an exception
        logger.log(logging.ERROR, f"Transaction failed: {e}")


logger.info("Succesfully imported " + str(FullPath) + "  ProcessDate = " + str(processdate))
