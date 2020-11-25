import sys
import os
import tempfile

import boto3
import pymysql


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

    # Loop Through S3-events
    for record in event['Records']:
        source_bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print('SUCCESS : Object was uploaded: {}'.format(key))

        # Temporary location for storing s3 object
        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = key.split('/')[-1]
            file_type = file_name.split('.')[-1]

            download_path = os.path.join(tmpdir, file_name)
            print('SUCCESS: TEMPDIR has been created')

            s3.download_file(source_bucket, key, download_path)
            print('SUCCESS: S3 Object has been downloaded')

            # Getting data by set 100 rows or less
            query_data_generator = get_optimized_query_data(download_path, file_type)
            sql = 'INSERT INTO sciex (sample_name, component_name, actual_concentration, calculated_concentration) VALUES (%s, %s, %s, %s)'

            # MySQL cursor
            with conn.cursor() as cursor:

                for data in query_data_generator:
                    cursor.executemany(sql, data)
                    conn.commit()
                    print('SUCCESS: Committing...')

    print('SUCCESS: DONE')

    conn.close()


def get_optimized_query_data(s3_file, s3_file_type):
    """
        s3_file : File temp locationto s3 file
        s3_file_type : file type to recognize (ONLY TXT or CSV)

        Yield : Slice file lines by 50k or less
    
    """

    try:
        s3_file_object = open(s3_file, 'r')
        print('SUCCESS : File has been read')
    except FileNotFoundError:
        print('FAIL : Specified file not found.')
        raise

    if s3_file_type == 'txt':
        split_sign = '\t'
    elif s3_file_type == 'csv':
        split_sign = ','
    else:
        print('FAIL : Not supported file type')
        raise TypeError

    data_set = []
    header = s3_file_object.readline().split(split_sign)

    # Check if expected format file is: TXT (tab spaced), CSV (comma spaced)
    if len(header) < 4:
        print('FAIL: TXT/CSV file must be formated -> (TXT tab separeted), (CSV comma separated)' )
        raise TypeError

    for line in s3_file_object.readlines():
        formated_line = line.rstrip().split(split_sign)

        # In case empthy field
        if len(formated_line) < 4:
            while len(formated_line) < 4:
                formated_line.append('Blank')

        data_set.append(tuple(formated_line))

        if len(data_set) == 80000:
            yield data_set
            data_set = []

    s3_file_object.close()
    yield data_set
    