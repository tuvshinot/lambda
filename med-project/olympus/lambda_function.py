import sys
import os
import tempfile

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
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)
        sys.exit()

    print("SUCCESS: Connection to RDS MySQL instance succeeded")

    # Creating table in case, No table
    if not db_helper.table_exists(conn, DB_NAME, 'olympus'):
        print('INFO: No table, creating.')
        db_helper.create_olympus_table(conn)
        print('SUCCESS: Table created.')

    # Getting event info
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print('SUCCESS : Object was uploaded: {}'.format(key))

    # Temporary location for storing s3 object
    with tempfile.TemporaryDirectory() as tmpdir:
        file_name = key.split('/')[-1]
        file_type = file_name.split('.')[-1]

        if file_type.lower() != 'log':
            print('FAIL: File type is not supported!')
            raise TypeError('Not supported file!')

        download_path = os.path.join(tmpdir, file_name)
        print('SUCCESS: TEMPDIR has been created')

        s3.download_file(source_bucket, key, download_path)
        print('SUCCESS: S3 Object has been downloaded')

        # Getting data by set 10k rows or less
        query_data_generator = get_optimized_query_data(download_path)

        sql = """INSERT INTO olympus 
                      (accession_number, specimen_type, patient_name, amphetamine, 
                      barbiturates, benzodiazepine, cocaine, methadone, 
                      opiates, oxycodone, phencyclidine_pcp, thc_cooh, ecstacy_mdma) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        # MySQL cursor
        with conn.cursor() as cursor:

            for data_set in query_data_generator:
                cursor.executemany(sql, data_set)
                conn.commit()
                print('SUCCESS: Committing...')

    print('SUCCESS: DONE')


def get_optimized_query_data(s3_file):
    """
        s3_file : File temp location to s3 file
        Yield : Slice file lines by 10k or less
    """

    try:
        s3_file_object = open(s3_file, 'r')
        print('SUCCESS : File has been read')
    except FileNotFoundError:
        print('FAIL : Specified file not found.')
        raise

    data_set = []

    for line in s3_file_object.readlines():
        splitted_line = line.split()

        # Removes accession_number and type from line
        number_and_type = splitted_line[0]
        accession_number = number_and_type[:-1]
        specimen_type = number_and_type[-1]
        new_line_list = splitted_line[1:]

        # Building string back to split by 01
        new_line = ' '.join(new_line_list)

        # Splitting string back
        splitted_parts = new_line.split('01', 1)
        name_part = splitted_parts[0]
        concentration_part = splitted_parts[1]

        # Building patient name
        patient_name = name_part.replace(',', '').rstrip()

        data = [accession_number, specimen_type, patient_name]

        # Concentration values and their indexes from machine
        concentration_values = concentration_part.split()

        # Picking up concentration values
        for i in range(0, len(concentration_values), 2):
            data.append(concentration_values[i])

        data_set.append(tuple(data))

        if len(data_set) == 10000:
            yield data_set
            data_set = []

    s3_file_object.close()
    yield data_set
