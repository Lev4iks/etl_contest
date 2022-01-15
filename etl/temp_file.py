from etl.helpers import get_last_download_date, get_data_from_source_db, load_data_to_destination_db

import pymysql


def data_transfer(mysql_source_image, mysql_destination_image):
    """
    Transferring data from two tables of the source - DB
    to the one denormalized table of the destination - DB
    """

    i = 1
    batch = [1]
    while batch:
        print('-' * 50, i, '-' * 50)
        src_db = _connect_to_db(mysql_source_image)
        dst_db = _connect_to_db(mysql_destination_image)

        last_download_date = get_last_download_date(dst_db)
        batch = get_data_from_source_db(src_db, last_download_date)

        # Creating new connection because previous was closed
        dst_db = _connect_to_db(mysql_destination_image)
        load_data_to_destination_db(dst_db, batch)

        print('-' * 50, i, '-' * 50, '\n')
        i += 1


def _connect_to_db(image):
    """Establishing a connection with DB via 'image'"""
    return pymysql.connect(**image, cursorclass=pymysql.cursors.DictCursor) # noqa
