import psycopg2

"""Postgres is run from the Docker image.
This command `docker-compose up run` needs run to get it set up, but after that
found it easier to run the container using docker desktop.  Could run from a separate
terminal if desired, just not same one testing python
"""

CREATE_TABLE_SQL = """
DROP TABLE IF EXISTS account;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS transaction;

CREATE TABLE IF NOT EXISTS account (
    customer_id INTEGER NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    address_1 VARCHAR(100) NOT NULL,
    address_2 VARCHAR(100),
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    zip_code CHAR(10) NOT NULL,
    join_date TIMESTAMP NOT NULL
);

ALTER TABLE account ADD CONSTRAINT pk_account PRIMARY KEY (customer_id);
CLUSTER account USING pk_account;

CREATE TABLE IF NOT EXISTS product ( 
    product_id INTEGER NOT NULL,
    product_code CHAR(3) NOT NULL,
    product_description varchar(100) NOT NULL);

ALTER TABLE product ADD CONSTRAINT pk_product PRIMARY KEY (product_id, product_code);
CLUSTER product USING pk_product;

CREATE TABLE IF NOT EXISTS transaction ( 
    transaction_id VARCHAR(50) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    product_id INTEGER NOT NULL,
    product_code CHAR(3) NOT NULL,
    product_description VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    CONSTRAINT fk_account
        FOREIGN KEY (account_id)
        REFERENCES account(customer_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_product
        FOREIGN KEY (product_id, product_code)
        REFERENCES product(product_id, product_code)
        ON DELETE CASCADE);

ALTER TABLE transaction ADD CONSTRAINT pk_transaction PRIMARY KEY (transaction_id);
CLUSTER transaction USING pk_transaction;

SELECT CURRENT_TIMESTAMP;
"""


def create_tables(cur):
    try:
        cur.execute(CREATE_TABLE_SQL)
        result = cur.fetchone()
        return result
    except Exception as e:
        print("Table creation failed: %s", e)
        raise


def load_csv_to_postgres(cur,csv_file, table_name):
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:    
            cur.copy_expert( f"COPY {table_name} FROM STDIN WITH CSV HEADER", f )
        print(f"CSV data loaded into '{table_name}' successfully.")
    except Exception as e:
        print(f"Error loading CSV: {e}")

def main():
    # Create connection - for real connection, would use Environment Variables
    host = "localhost"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cur = conn.cursor()
    
    result = create_tables(cur)
    print("Current timestamp:", result[0])
    load_csv_to_postgres(cur,'Exercises/Exercise-5/data/accounts.csv','account')
    load_csv_to_postgres(cur,'Exercises/Exercise-5/data/products.csv','product')
    load_csv_to_postgres(cur,'Exercises/Exercise-5/data/transactions.csv','transaction')


    # Close the cursor and connection
    cur.close()
    conn.close()



if __name__ == "__main__":
    main()
