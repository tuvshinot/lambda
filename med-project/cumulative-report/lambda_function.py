import sys
import os
import tempfile
from time import strftime, gmtime, time

import boto3
import pymysql
from xhtml2pdf import pisa

from helper_methods import __get_olympus_spec, __get_sciex_spec, __get_film_array_spec, \
                    source_html, css_style, __get_header, __get_title_date, __get_patient_info, __get_footer
                    

DB_HOST = os.environ['DB_HOST']
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_NAME = os.environ['DB_NAME']
PATIENT_ID = os.environ['PATIENT_ID']
BUCKET_NAME = os.environ['BUCKET_NAME']
LAB_NAME = os.environ['LAB_NAME']

s3 = boto3.client('s3')


def lambda_handler(event, context):
    gmt_time = gmtime()
    date_time_reported = strftime('%m/%d/%Y at %H:%M', gmt_time)
    
    # Connection setup
    try:
        conn = pymysql.connect(host=DB_HOST, user=DB_USERNAME,
                               passwd=DB_PASSWORD, db=DB_NAME,
                               connect_timeout=5, charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)

    except pymysql.MySQLError as e:
        print("FAIL: Unexpected error: Could not connect to MySQL instance.")
        print(e)
        sys.exit()

    print("SUCCESS: Connection to RDS MySQL instance succeeded")
    
    # Get list of results for given patient
    with conn.cursor() as cursor:
        sql = """
            select * from patient p
            left join service_request r on r.patient_id = p.patient_id
            left join specimen s on s.accession_number = r.accession_number
            left join specimen_result sr on sr.specimen_id = s.specimen_id
            left join medical_machine m on sr.machine_id = m.machine_id
            where p.patient_id = %s;
        """
        cursor.execute(sql, (PATIENT_ID, ))
        patient_results = cursor.fetchall()
        
        if len(patient_results) == 0:
            raise ValueError('FAIL: No patient with given PATIENT_ID is found.!')
            print('FAIL: No patient with given PATIENT_ID is found.!')


    # Building results specs
    specs = ''
    for result in patient_results:
        print('INFO: Building specs with accession number: {} and machine table: {}'.format(result['accession_number'], result['results_table']))
        specs += build_spec(conn, result['results_table'], result['accession_number'], date_time_reported, result['type'], s_request_time='04/06/2021')
        # TODO -> replace s_request_time = real data

    # Tmpdir for generating PDF
    with tempfile.TemporaryDirectory() as tmpdir:
        
        # Report generated time
        random_seconds = str(time()).split('.')[1]
        datetime_iso_combined = strftime('%Y%m%d-%H%M%S', gmt_time)

        # Building file name
        file_name = '{patient_id}_{timestamp}_{seconds}.pdf'.format(patient_id=PATIENT_ID, timestamp=datetime_iso_combined, seconds=random_seconds)

        # Openning file at location
        output_filename = os.path.join(tmpdir, file_name)
        result_file = open(output_filename, "w+b")
        print('SUCCESS: File is opened! at location {}'.format(output_filename))
        
        # Formatting PDF
        patient = patient_results[0]
        header = __get_header()
        title_date = __get_title_date(date_time_reported)
        footer = __get_footer(date_time_reported, patient_id=PATIENT_ID, patient_name=patient['first_name'] + ' ' + patient['last_name'] + '.')
        patient_info=__get_patient_info(patient['first_name'], patient['last_name'], patient['gender'], patient_id=PATIENT_ID)
        

        source_html_formatted = source_html.format(style=css_style, header=header, title_date=title_date, 
                                                    patient_info=patient_info, specs=specs, footer=footer)
        print('SUCCESS: SPEC is formmated!.')

        # Converting HTML to PDF
        pisa_status = pisa.CreatePDF(source_html_formatted, dest=result_file)
        print('SUCCESS: PDF is generated!.')

        # close output file
        result_file.close() 
        print('INFO: Status {}'.format(pisa_status.err))

        s3.upload_file(output_filename, BUCKET_NAME, '{}/cumulative_report/{}'.format(LAB_NAME, file_name))
        print('SUCCESS: PDF is uploaded to Bucket!')
        
        # Insert into report_cumulative
        date_report_time_table = strftime('%Y-%m-%d-%H-%M', gmt_time)

        report_data = (PATIENT_ID, patient_results[0]['created_by'], date_report_time_table, file_name)
        
        with conn.cursor() as cursor:
            sql = """INSERT INTO report_cumulative (patient_id, created_by, created_at, filepath) 
                      values (%s, %s, %s, %s);"""
            cursor.execute(sql, report_data)
            conn.commit()
        print("SUCCESS: Report is written to DB!")
        

def build_spec(conn, machine_result_table_name, accession_number, date_time_reported, specimen_type, s_request_time):
    """ Cases where each machine result must be formmatted differently """

    spec = ''

    # Olympus spec
    if machine_result_table_name == 'result_machine_olympus':
        with conn.cursor() as cursor:
            sql = """
                select amphetamine, barbiturates, benzodiazepine, cocaine, 
                    methadone, opiates, oxycodone,phencyclidine_pcp, 
                    thc_cooh, ecstacy_mdma from result_machine_olympus where accession_number=%s;"""
            
            cursor.execute(sql, (accession_number, ))
            olympus_results = cursor.fetchall()

            if len(olympus_results) == 0:
                raise ValueError('FAIL: No olympus results with given accession number is found.!')
                print('FAIL: No olympus results with given accession number is found.!')

            for result in olympus_results:
                spec += __get_olympus_spec(result, accession_number, specimen_type, s_request_time, date_time_reported)

    
    elif machine_result_table_name == 'result_machine_sciex':
        with conn.cursor() as cursor:
            sql = """
                select component_name, actual_concentration, calculated_concentration from result_machine_sciex 
                where sample_name=%s;"""
            
            cursor.execute(sql, (accession_number, ))
            sciex_results = cursor.fetchall()

            if len(sciex_results) == 0:
                raise ValueError('FAIL: No sciex results with given accession number is found.!')
                print('FAIL: No sciex results with given accession number is found.!')

            for result in sciex_results:
                spec += __get_sciex_spec(result, accession_number, specimen_type, s_request_time, date_time_reported)
    
    elif machine_result_table_name == 'result_machine_film_array':
        
        with conn.cursor() as cursor:
            sql = """
                select test_id, test_name, test_identifier from result_machine_film_array 
                where specimen_identifier=%s;"""
            
            cursor.execute(sql, (accession_number, ))
            film_array_results = cursor.fetchall()

            if len(film_array_results) == 0:
                raise ValueError('FAIL: No film_array results with given accession number is found.!')
                print('FAIL: No film_array results with given accession number is found.!')

            # In case, there are multiple film_array with same accession number
            for film_array in film_array_results:
                # This list of groups with its results
                results_data = []

                # Getting result groups
                sql = """select * from result_machine_film_array_group where test_id=%s;"""
                cursor.execute(sql, (film_array['test_id'], ))
                film_array_group_results = cursor.fetchall()

                for group in film_array_group_results:
                    # Building group with its results
                    results_with_group = {}
                    results_with_group['result_group'] = group
                    
                    # Fethcing results of each group
                    sql = """select * from result_machine_film_array_group_item where result_group_id=%s;"""
                    cursor.execute(sql, (group['result_group_id'], ))
                    film_array_group_items = cursor.fetchall()
                    results_with_group['results'] = film_array_group_items

                    results_data.append(results_with_group)
                
                # Building up spec
                spec += __get_film_array_spec(film_array, results_data, accession_number, specimen_type, s_request_time, date_time_reported)
        
    
    else:
        raise ValueError(
            'FAIL: Given unsupported machine result table name!. {}'.format(machine_result_table_name))

    return spec
