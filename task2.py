import sqlite3


def main():
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()

    c.execute('''WITH transactions_by_day AS ( 
            SELECT datetime, SUM(revenue) AS revenue_by_day 
            FROM Transactions WHERE device_type == 3 GROUP BY datetime
        ) 
        SELECT datetime, MAX(revenue_by_day) FROM transactions_by_day''')
    print(c.fetchall())

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
