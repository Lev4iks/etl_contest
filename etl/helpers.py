import datetime

from pymysql import connect


def get_data_from_source_db(src_db: connect, last_download_date):
    """Receiving 1-hour interval data from the source - DB by 'last_download_date'"""

    with src_db:
        with src_db.cursor() as c:
            if not last_download_date:
                c.execute("""SELECT dt FROM transactions ORDER BY dt ASC LIMIT 1""")
                last_download_date = c.fetchone()
            else:
                last_download_date["dt"] += datetime.timedelta(hours=1)

            c.execute(
                """SELECT tr.id, tr.dt, tr.idoper, tr.move, tr.amount, ot.name
                FROM transactions tr
                INNER JOIN operation_types ot ON ot.id = tr.idoper
                WHERE dt BETWEEN %s AND %s""",
                (last_download_date["dt"], last_download_date["dt"] + datetime.timedelta(hours=1)))  # noqa

            data = c.fetchall()
            return [[*i.values()] for i in data]  # noqa


def get_last_download_date(dst_db: connect):
    """Receiving last downloading date from destination - DB"""

    with dst_db:
        with dst_db.cursor() as c:
            c.execute("""SELECT dt FROM transactions_denormalized ORDER BY dt DESC LIMIT 1""")
            date = c.fetchone()
    return date  # noqa


def load_data_to_destination_db(dst_db: connect, data):
    """Downloading data to the destination - DB"""

    with dst_db:
        with dst_db.cursor() as c:
            c.executemany(
                """INSERT INTO transactions_denormalized (id, dt, idoper, move, amount, name_oper)
                VALUES(%s, %s, %s, %s, %s, %s)""", data)

            dst_db.commit()

            c.execute("""SELECT * FROM transactions_denormalized""")

            [print(i) for i in c.fetchall()]
