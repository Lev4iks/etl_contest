from pymysql import connect

from etl.helpers import get_last_download_date, get_data_from_source_db, load_data_to_destination_db


def data_transfer(src_db: connect, dst_db: connect):
    """PASS"""

    last_download_date = get_last_download_date(dst_db)

    data = get_data_from_source_db(src_db, last_download_date)

    load_data_to_destination_db(dst_db, data)
