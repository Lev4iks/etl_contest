import pymysql

from etl.temp_file import data_transfer
from .helpers import ping_container


def test_container_is_alive(mysql_source_image):
    assert ping_container(mysql_source_image)


def test_containers_assets_is_ready(mysql_source_image,
                                    mysql_destination_image):
    src_conn = pymysql.connect(**mysql_source_image,
                               cursorclass=pymysql.cursors.DictCursor)

    with src_conn:
        with src_conn.cursor() as c:
            src_query = """
                SELECT 
                    COUNT(*) AS total 
                FROM transactions t
                    JOIN operation_types ot ON t.idoper = ot.id
            """

            c.execute(src_query)
            src_result = c.fetchone()

    dst_conn = pymysql.connect(**mysql_destination_image,
                               cursorclass=pymysql.cursors.DictCursor)

    with dst_conn:
        with dst_conn.cursor() as c:
            dst_query = """
                SELECT 
                    COUNT(*) AS total 
                FROM transactions_denormalized t
            """

            c.execute(dst_query)
            dst_result = c.fetchone()

    assert src_result['total'] > 0
    assert dst_result['total'] == 0


def test_data_transfer(mysql_source_image,
                       mysql_destination_image):
    """

    :param mysql_source_image: Контейнер mysql-источника с исходными данными
    :param mysql_destination_image: Контейнер mysql-назначения
    :return:
    """

    #   put your code for testing here!

    src_db = pymysql.connect(**mysql_source_image,
                             cursorclass=pymysql.cursors.DictCursor)

    dst_db = pymysql.connect(**mysql_destination_image,
                             cursorclass=pymysql.cursors.DictCursor)

    # data_transfer(src_db, dst_db)
    # assert get_ids_from_db(src_db) == get_ids_from_db(dst_db)
