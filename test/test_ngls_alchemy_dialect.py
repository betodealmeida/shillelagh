import os
import sys
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect

print("Starting test...")

if not os.getenv('NGLS_API_KEY'):
    print("The environment variable NGLS_API_KEY is not set")
    sys.exit(1)

ngls_api_key = os.getenv('NGLS_API_KEY')

if not os.getenv('NGLS_SERVER'):
    os.environ['NGLS_SERVER'] = 'ngls.mshome.net'

ngls_server = os.getenv('NGLS_SERVER')
print(f"Using server={ngls_server}")

if not os.getenv('CA_CERT_FILE'):
    os.environ['CA_CERT_FILE'] = 'certs/ca.crt'
ca_certs = os.getenv('CA_CERT_FILE')
if not os.path.exists(ca_certs):
    print(f"The CA certificate {ca_certs} does not exist")
    sys.exit(1)

url = f"ngls://{ngls_server}:4443/api/reporting"
print(f"Creating engine with URL: {url}")
engine = create_engine(url)

print("Connecting the database engine")
connection = engine.connect()

print("list tables")
inspector = inspect(engine)
print(inspector.get_table_names())

print("get data")
with engine.connect() as connection:
    result = connection.execute(text("SELECT * FROM busiest_hour WHERE call_type IN ('911', 'admin')"))
    for row in result:
        print(row)

print("Test successfully completed")
