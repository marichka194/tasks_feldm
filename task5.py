import sqlalchemy as db
import argparse


def get_connection_string():
    parser = argparse.ArgumentParser()
    parser.add_argument("connection_string",
                        help="Enter dialect[+driver] and database name")
    args = parser.parse_args()
    connection_string = args.connection_string

    if connection_string.startswith("sqlite") or connection_string.startswith("postgresql"):
        return connection_string
    else:
        return None


def upload_transactions(connection_string):
    try:
        engine = db.create_engine(connection_string)
        conn = engine.connect()
        metadata = db.MetaData()
        transactions = db.Table('transactions', metadata, autoload=True, autoload_with=engine)
        return conn, transactions
    except db.exc.NoSuchTableError:
        print("Can't connect to database")


def execute_query(conn, transactions):
    query = db.select([transactions.columns.datetime,
                       db.func.sum(transactions.columns.revenue).label('revenue_by_day')
                       ])\
        .where(transactions.columns.device_type == '3')\
        .group_by(transactions.columns.datetime)\
        .order_by(db.desc('revenue_by_day'))

    result_proxy = conn.execute(query)
    result = result_proxy.fetchone()

    print(result)
    result_proxy.close()


def main():
    connection_string = get_connection_string()
    if connection_string:
        try:
            conn, transactions = upload_transactions(connection_string)
            execute_query(conn, transactions)
        except TypeError:
            print("No connection returned")
    else:
        print("Connection string invalid")


if __name__ == '__main__':
    main()
