import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta


def get_usd_rates():
    tree = ET.parse('eurofxref-hist-90d.xml')
    root = tree.getroot()
    usd_rates = dict()
    for child in root[2]:
        time = child.attrib['time'] + " 00:00:00"
        usd_rates[time] = 1
        for cube in child.iter():
            if cube.get('currency') == "USD":
                usd_rates[time] = float(cube.get('rate'))
    return usd_rates


def get_transactions_days(cursor):
    cursor.execute("SELECT DISTINCT datetime FROM Transactions")
    days = [day[0] for day in cursor.fetchall()]
    return days


def update_revenue(cursor, usd_rates, days):
    for day in days:
        if day not in usd_rates.keys():
            delta_days = 0
            last_day_with_rate = day
            last_date_with_rate = datetime.strptime(last_day_with_rate, "%Y-%m-%d %H:%M:%S")

            while last_day_with_rate not in usd_rates.keys():
                delta_days += 1
                delta = timedelta(days=delta_days)
                last_date_with_rate -= delta
                last_day_with_rate = last_date_with_rate.strftime("%Y-%m-%d %H:%M:%S")

            usd_rates[day] = usd_rates[last_day_with_rate]

        cursor.execute("UPDATE transactions SET revenue=revenue/? WHERE datetime == ?", (usd_rates[day], day))


def show_updated_table(cursor):
    cursor.execute("SELECT * FROM Transactions LIMIT 10")
    print(cursor.fetchall())


def main():
    conn = sqlite3.connect('transactions.db')
    c = conn.cursor()

    usd_rates = get_usd_rates()
    transactions_days = get_transactions_days(c)

    update_revenue(c, usd_rates, transactions_days)
    show_updated_table(c)

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
