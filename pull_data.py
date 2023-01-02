# imports
import psycopg2 as postgres
import yaml
import requests

# functions


def url_to_list(url):
    url_response = requests.get(url).iter_lines()
    result = [tuple(str(line).strip('b').strip("'").split(','))
              for line in url_response]
    return result


# URLs
customers = "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_customers.csv"
orders = "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv"
payments = "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_payments.csv"

# pulling data from the URLs
customers_data_raw = url_to_list(customers)
customers_columns = customers_data_raw.pop(0)
orders_data_raw = url_to_list(orders)
orders_columns = orders_data_raw.pop(0)
payments_data_raw = url_to_list(payments)
payments_columns = payments_data_raw.pop(0)

# database credentials
with open('database_config.yml', 'r+') as db_config_file:
    db_config = yaml.safe_load(db_config_file)
    db_config_file.close()

# connecting to database
conn_str = f"host={db_config['host']} dbname={db_config['database']} user={db_config['username']} password={db_config['password']}"
pg_connection = postgres.connect(conn_str)
pg_cursor = pg_connection.cursor()

# queries
create_schema = """CREATE SCHEMA IF NOT EXISTS "jaffleshop";"""
create_customers = """DROP TABLE IF EXISTS "jaffleshop"."customers" CASCADE; CREATE TABLE "jaffleshop"."customers" (id BIGINT PRIMARY KEY, first_name TEXT, last_name TEXT);"""
create_orders = """DROP TABLE IF EXISTS "jaffleshop"."orders" CASCADE; CREATE TABLE "jaffleshop"."orders" (id BIGINT PRIMARY KEY, user_id BIGINT, order_date DATE, status TEXT, CONSTRAINT fk_user_id FOREIGN KEY(user_id) REFERENCES "jaffleshop"."customers"(id));"""
create_payments = """DROP TABLE IF EXISTS "jaffleshop"."payments" CASCADE; CREATE TABLE "jaffleshop"."payments" (id BIGINT PRIMARY KEY, order_id BIGINT, payment_method TEXT, amount NUMERIC, CONSTRAINT fk_order_id FOREIGN KEY(order_id) REFERENCES "jaffleshop"."orders"(id));"""

pg_cursor.execute(create_schema)
pg_connection.commit()
print("..........Created Schema..........")
pg_cursor.execute(create_customers)
pg_connection.commit()
print("..........Created Customers Table..........")
pg_cursor.execute(create_orders)
pg_connection.commit()
print("..........Created Orders Table..........")
pg_cursor.execute(create_payments)
pg_connection.commit()
print("..........Creted Payments Table..........")


customers_values = ', '.join(pg_cursor.mogrify(
    '(%s, %s, %s)', x).decode('utf-8') for x in customers_data_raw)
pg_cursor.execute(
    'INSERT INTO "jaffleshop"."customers" VALUES ' + customers_values)
pg_connection.commit()
print("..........Inserted records in customers table..........")

orders_values = ','.join(pg_cursor.mogrify(
    "(%s, %s, %s, %s)", row).decode('utf-8') for row in orders_data_raw)
pg_cursor.execute('INSERT INTO "jaffleshop"."orders" VALUES ' + orders_values)
pg_connection.commit()
print("..........Inserted records in orders table..........")

payments_values = ','.join(pg_cursor.mogrify(
    "(%s, %s, %s, %s)", row).decode('utf-8') for row in payments_data_raw)
pg_cursor.execute(
    'INSERT INTO "jaffleshop"."payments" VALUES ' + payments_values)
pg_connection.commit()
print("..........Inserted records in payments table..........")

pg_connection.close()
