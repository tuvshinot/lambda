import sys
import os
import tempfile
import xml
import xml.etree.ElementTree as ET

import boto3
import pymysql

import database_helper as db_helper

DB_HOST = os.environ['DB_HOST']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_NAME = os.environ['DB_NAME']

s3 = boto3.client('s3')


def lambda_handler(event, context):
    # Connection setup
    try:
        conn = pymysql.connect(DB_HOST, user=DB_USERNAME,
                               passwd=DB_PASSWORD, db=DB_NAME,
                               connect_timeout=5, charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)

    except pymysql.MySQLError as e:
        print("FAIL: Unexpected error: Could not connect to MySQL instance.")
        print(e)
        sys.exit()

    print("SUCCESS: Connection to RDS MySQL instance succeeded")

    # Creating table in case, no tables
    tables_exist = [db_helper.table_exists(conn, DB_NAME, table_name)
                    for table_name in ('filmArrayTest', 'resultGroup', 'result')]

    if not all(tables_exist):
        print('INFO: No tables, creating.')
        db_helper.create_film_array_tables(conn)
        print('SUCCESS: Tables created.')

    # S3 event info
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print('SUCCESS : Object was uploaded: {}'.format(key))

    # Temporary location for storing s3 object
    with tempfile.TemporaryDirectory() as tmpdir:
        file_name = key.split('/')[-1]

        download_path = os.path.join(tmpdir, file_name)
        print('SUCCESS: TEMPDIR has been created')

        s3.download_file(source_bucket, key, download_path)
        print('SUCCESS: S3 Object has been downloaded')

        # Parsing xml file
        try:
            tree = ET.parse(download_path)
            root = tree.getroot()
        except FileNotFoundError as e:
            print('FAIL: File not found!')
            print(e)
        except xml.etree.ElementTree.ParseError as e:
            print('FAIL: Error when parsing xml')
            print(e)

        print('SUCCESS: XML has been parsed.')

        # Writing to filmArrayTest
        film_array_test_id = write_to_film_array_test(root, conn)
        print('SUCCESS: filmArrayTest is written.')

        # Writing Result and Result Group
        test = root.find('requestResult').find('testOrder').find('test')
        result_groups = test.findall('resultGroup')

        for result_group in result_groups:
            write_to_result_group(result_group, conn, film_array_test_id)

        print('SUCCESS: ResultGroup and Result has been written.')

    print('SUCCESS: DONE')

    conn.close()


def write_to_film_array_test(root, conn):
    """ Writes to filmArrayTest
        Args: root(XML element), conn(DB connection)
        Returns: id inserted to film_array_test
    """

    header = root.find('header')
    request_result = root.find('requestResult')
    test_order = request_result.find('testOrder')
    test = test_order.find('test')

    specimen_identifier = test_order.find('specimen').find('specimenIdentifier').text
    test_identifier = test.find('universalIdentifier').find('testIdentifier').text
    test_name = test.find('universalIdentifier').find('testName').text
    test_version = test.find('universalIdentifier').find('testVersion').text
    test_instrument_type = test.find('instrumentType').text
    test_instrument_serial_number = test.find('instrumentSerialNumber').text

    disposable_data = test.find('disposableData').find('disposable')
    disposable_identifier = disposable_data.find('disposableIdentifier').text
    disposable_reference = disposable_data.find('reference').text
    disposable_type = disposable_data.find('disposableType').text
    disposable_lot_number = disposable_data.find('lotNumber').text

    header_info_sender_name = header.find('senderName').text
    header_info_processing_identifier = header.find('processingIdentifier').text
    header_info_version = header.find('version').text
    header_info_date_time = header.find('dateTime').text
    header_info_message_type = header.find('messageType').text
    request_status = request_result.find('requestStatus').text

    dataset = (specimen_identifier, test_identifier, test_name, test_version,
               test_instrument_type, test_instrument_serial_number, disposable_identifier,
               disposable_reference, disposable_type, disposable_lot_number,
               header_info_sender_name, header_info_processing_identifier, header_info_version,
               header_info_date_time, header_info_message_type, request_status)

    sql = """INSERT INTO filmArrayTest 
              (specimen_identifier, test_identifier, test_name, test_version, test_instrument_type, 
              test_instrument_serial_number, disposable_identifier, disposable_reference, disposable_type, 
              disposable_lot_number, header_info_sender_name, header_info_processing_identifier, 
              header_info_version, header_info_date_time, header_info_message_type, request_status) 
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    with conn.cursor() as cursor:
        cursor.execute(sql, dataset)
        conn.commit()
        return cursor.lastrowid


def write_to_result_group(result_group, conn, film_array_test_id):
    """ Writes to result_group table
        Args: result_group(XML element), conn(DB connection), film_array_test_id
        Returns: id inserted to result_group table
    """
    result_group_code = result_group.find('resultGroupCode').text
    result_group_name = result_group.find('resultGroupName').text
    result_group_coding_system = result_group.find('resultGroupCodingSystem').text

    sql = """INSERT INTO resultGroup (result_group_code, result_group_name, result_group_coding_system, test_id)
             VALUES (%s, %s, %s, %s)"""

    with conn.cursor() as cursor:
        cursor.execute(sql, (result_group_code, result_group_name, result_group_coding_system, film_array_test_id))
        conn.commit()
        result_group_id = cursor.lastrowid

    # Writing Result
    results = result_group.findall('result')
    write_to_result(results, conn, result_group_id)


def write_to_result(results, conn, result_group_id):
    """ Writes to result table
        Args: result(XML element), conn(DB connection), result_group_id
        Returns: id inserted to result table
    """
    datasets = []
    for result in results:
        result_test_code = result.find('resultID').find('resultTestCode').text
        result_test_name = result.find('resultID').find('resultTestName').text
        result_coding_system = result.find('resultID').find('resultCodingSystem').text

        value_type = result.find('value').find('testResult').find('valueType').text
        observation_value = result.find('value').find('testResult').find('observationValue').text
        observation_name = result.find('value').find('testResult').find('observationName').text

        operator_name = result.find('operatorName').text
        result_date_time = result.find('resultDateTime').text

        datasets.append((result_test_code, result_test_name, result_coding_system, value_type, observation_value,
                         observation_name, operator_name, result_date_time, result_group_id))

    sql = """INSERT INTO result (result_test_code, result_test_name, result_coding_system, value_type, 
             observation_value, observation_name, operator_name, result_date_time, result_group_id)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    with conn.cursor() as cursor:
        cursor.executemany(sql, datasets)
        conn.commit()
