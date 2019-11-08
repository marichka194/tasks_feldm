import sqlite3


def main():
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()

    c.execute('''SELECT visitor_id, SUM(revenue) AS visitor_revenue
            FROM Transactions 
            GROUP BY visitor_id
            ORDER BY visitor_revenue DESC 
            LIMIT 1''')
    print(c.fetchall())

    conn.close()


if __name__ == '__main__':
    main()
