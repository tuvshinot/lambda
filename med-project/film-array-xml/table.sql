CREATE TABLE film-array-test (
             test_id int NOT NULL AUTO_INCREMENT,specimen_identifier varchar(150),
             test_identifier varchar(150),test_name varchar(150), test_version varchar(30),
             test_instrument_type varchar(80),test_instrument_serial_number varchar(80),
             disposable_identifier varchar(80),disposable_reference varchar(80),
             disposable_type varchar(70),disposable_lot_number varchar(70),
             header_info_sender_name varchar(50),header_info_processing_identifier varchar(30),
             header_info_version varchar(30),header_info_date_time varchar(75),
             header_info_message_type varchar(30),request_status varchar(20),PRIMARY KEY (test_id));

CREATE TABLE result-group (
             result-group-id int NOT NULL AUTO_INCREMENT,result_group_code varchar(100),
             result_group_name varchar(100),result_group_coding_system varchar(100),
             test_id int, PRIMARY KEY (result-group-id),
             FOREIGN KEY (test_id) REFERENCES film-array-test(test_id));

CREATE TABLE result (
             result-id int NOT NULL AUTO_INCREMENT, result_test_code varchar(100),
             result_test_name varchar(100), result_coding_system varchar(100), value_type varchar(100),
             observation_value varchar(100), observation_name varchar(100), operator_name varchar(100),
             result_date_time varchar(100), result-group-id int, PRIMARY KEY (result-id),
             FOREIGN KEY (result-group-id) REFERENCES result-group(result-group-id));