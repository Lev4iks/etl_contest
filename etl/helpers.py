from pymysql import connect


def get_data_from_source_db(src_db: connect, last_download_date):
    """Получаем данные из БД - источника"""

    with src_db:
        with src_db.cursor() as c:
            # if not last_download_date:
            #     last_download_date = c.execute("""SELECT dt FROM transactions ORDER BY dt ASC""")

            c.execute("""
            SELECT tr.id, tr.dt, tr.idoper, tr.move, tr.amount, op.name
            FROM transactions tr
            INNER JOIN operation_types op ON op.id = tr.idoper
            WHERE dt = VALUES(%s)
            """, last_download_date)
            [print(i) for i in c.fetchall()]


def get_last_download_date(dst_db: connect):
    """Получаем дату последней загрузки данных из БД - назначения"""
    with dst_db:
        with dst_db.cursor() as c:
            c.execute("""SELECT dt FROM transactions_denormalized ORDER BY dt DESC""")

            return c.fetchone()


def load_data_to_destination_db(dst_db: connect, data):
    with dst_db:
        with dst_db.cursor() as c:
            c.execute("""SELECT dt FROM transactions_denormalized ORDER BY dt DESC""")
