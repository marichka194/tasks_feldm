import sqlite3
import csv
from itertools import zip_longest


def get_combined_content(cursor):
    cursor.execute('''SELECT Transactions.id, datetime, visitor_id, device_type, device_name, revenue, tax
        From Transactions 
        JOIN Devices
        ON transactions.device_type == Devices.id''')
    transactions = cursor.fetchall()
    return transactions


def create_csv(column_names, transactions):
    with open('transactions.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=column_names)
        writer.writeheader()

        for row in transactions:
            writer.writerow(dict(zip_longest(column_names, row)))


def main():
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()

    transactions = get_combined_content(c)
    column_names = [name[0] for name in c.description]
    create_csv(column_names, transactions)

    conn.close()


if __name__ == '__main__':
    main()
