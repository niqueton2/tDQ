CREATE APPLICATION ROLE app_public;
CREATE SCHEMA IF NOT EXISTS core;
GRANT ALL ON SCHEMA core TO APPLICATION ROLE app_public;




CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT ALL ON SCHEMA code_schema TO APPLICATION ROLE app_public;

------------ Sequences ------------------------------------
create sequence core.SEQ_TRY
    start=-1
    increment=-1;

create sequence core.SEQ_RTBD
    start=1
    increment=1;

create sequence core.SEQ_DRBD
    start=1
    increment=1;

create sequence core.SEQ_RTCS
    start=1
    increment=1;

create sequence core.SEQ_DRCS
    start=1
    increment=1;

create sequence core.SEQ_RTCR
    start=1
    increment=1;

create sequence core.SEQ_DRCR
    start=1
    increment=1;

create sequence core.SEQ_AD
    start=1
    increment=1;

----------------- Tables and views ------------------------

CREATE OR REPLACE TABLE core.on_off (
    si INT
);

GRANT INSERT,UPDATE,DELETE,SELECT on TABLE core.on_off to APPLICATION ROLE app_public;


CREATE OR REPLACE TABLE core.maintenance_period (
    cron VARCHAR,
    last_update TIMESTAMP DEFAULT current_timestamp
);

GRANT INSERT,UPDATE,DELETE,SELECT on TABLE core.maintenance_period to APPLICATION ROLE app_public;



CREATE OR REPLACE TABLE core.REFINED_TEST_BAD_ROWS (
    id INT,
    test_name VARCHAR,
    test_query VARCHAR NOT NULL,
    cron_schedule VARCHAR,  
    max_error_count VARCHAR,
    test_comment VARCHAR,
    is_active VARCHAR DEFAULT 'YES',
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

GRANT INSERT,UPDATE,DELETE,SELECT on TABLE core.REFINED_TEST_BAD_ROWS to APPLICATION ROLE app_public;

CREATE OR REPLACE TABLE core.REFINED_TEST_COUNTS_SUMS (
    id INT,
    test_name VARCHAR,
    test_query VARCHAR,
    cron_schedule VARCHAR,  
    expected_range VARCHAR,
    test_comment VARCHAR,
    is_active VARCHAR DEFAULT 'YES',
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

GRANT INSERT,UPDATE,DELETE,SELECT on TABLE core.REFINED_TEST_COUNTS_SUMS to APPLICATION ROLE app_public;


CREATE OR REPLACE TABLE core.REFINED_TEST_CUSTOM_RESULTS (
    id INT ,
    test_name VARCHAR,
    test_query VARCHAR,
    cron_schedule VARCHAR,  
    expected_result VARCHAR,
    test_comment VARCHAR,
    is_active VARCHAR DEFAULT 'YES',
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

GRANT INSERT,UPDATE,DELETE,SELECT on TABLE core.REFINED_TEST_CUSTOM_RESULTS to APPLICATION ROLE app_public;

create or replace view core.TEST_NAME_TYPE
as 
select test_name, 'BAD_ROWS' as test_type
from core.REFINED_TEST_BAD_ROWS
union
select test_name, 'COUNTS_SUMS'
from core.REFINED_TEST_COUNTS_SUMS
union 
select test_name, 'CUSTOM_RESULTS'
from core.REFINED_TEST_CUSTOM_RESULTS;

GRANT SELECT ON VIEW core.TEST_NAME_TYPE to APPLICATION ROLE app_public;

CREATE OR REPLACE TABLE core.ALERT_DEFINITION (
    alert_id INT ,
    test_bad_rows_id INT ,
    test_counts_sums_id INT ,
    test_custom_results_id INT ,
    email VARCHAR,
    message VARCHAR,
    creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP

    );

GRANT INSERT,UPDATE,DELETE,SELECT on TABLE core.ALERT_DEFINITION to APPLICATION ROLE app_public;

CREATE OR REPLACE TABLE core.DIRECT_RESULTS_BAD_ROWS (
    result_id INT,
    test_bad_rows_id INT ,
    BoolResult VARCHAR,
    result VARCHAR,
    query_executed VARCHAR,
    test_name VARCHAR,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

GRANT INSERT,UPDATE,DELETE,SELECT on TABLE core.DIRECT_RESULTS_BAD_ROWS to APPLICATION ROLE app_public;

CREATE OR REPLACE TABLE core.DIRECT_RESULTS_COUNTS_SUMS(
    result_id INT ,
    test_counts_sums_id INT,
    BoolResult VARCHAR,
    result VARCHAR,
    query_executed VARCHAR,
    test_name VARCHAR,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

GRANT INSERT,UPDATE,DELETE,SELECT on TABLE core.DIRECT_RESULTS_COUNTS_SUMS to APPLICATION ROLE app_public;

CREATE OR REPLACE TABLE core.DIRECT_RESULTS_CUSTOM_RESULTS (
    result_id INT,
    test_custom_results_id INT,
    BoolResult VARCHAR,
    result VARCHAR,
    query_executed VARCHAR,
    test_name VARCHAR,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

GRANT INSERT,UPDATE,DELETE,SELECT on TABLE core.DIRECT_RESULTS_CUSTOM_RESULTS to APPLICATION ROLE app_public;

create or replace view core.COMPLETE_RESULTS
as 
select 
dbr.id as ID,
'BAD_ROWS' as Test_type,
dbr.test_name as Name,
dbr.test_comment as Comment,
rbr.BOOLRESULT as Test_result,
rbr.execution_time
from core.DIRECT_RESULTS_BAD_ROWS rbr
left join core.REFINED_TEST_BAD_ROWS dbr
on rbr.test_bad_rows_id=dbr.id

union all

select 
dcs.id as ID,
'COUNTS_SUMS' as Test_type,
dcs.test_name as Name,
dcs.test_comment as Comment,
rcs.BOOLRESULT as Test_result,
rcs.execution_time
from core.DIRECT_RESULTS_COUNTS_SUMS rcs
left join core.REFINED_TEST_COUNTS_SUMS dcs
on rcs.test_counts_sums_id=dcs.id

union all

select 
dcr.id as ID,
'CUSTOM_RESULTS' as Test_type,
dcr.test_name as Name,
dcr.test_comment as Comment,
rcr.BOOLRESULT as Test_result,
rcr.execution_time
from core.DIRECT_RESULTS_CUSTOM_RESULTS rcr
left join core.REFINED_TEST_CUSTOM_RESULTS dcr
on rcr.test_custom_results_id=dcr.id;

GRANT SELECT on VIEW core.COMPLETE_RESULTS to APPLICATION ROLE app_public;

---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.EXECUTE_TEST_BAD_ROWS(id_test VARCHAR, try_mode VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var id_test = ID_TEST;
    var try_mode = TRY_MODE;
    var results = [];
    var statement1 = snowflake.createStatement({
        sqlText: "SELECT id, test_query, max_error_count, test_name, IS_ACTIVE FROM core.REFINED_TEST_BAD_ROWS where id=?",
        binds: [id_test]
    });
    var resultSet1 = statement1.execute();
    

    try{

        if (resultSet1.next()) {
            var id = resultSet1.getColumnValue(1);
            var test_query = resultSet1.getColumnValue(2);
            var max_error_count = resultSet1.getColumnValue(3);
            var test_name = resultSet1.getColumnValue(4);
            var is_active = resultSet1.getColumnValue(5);

            if (is_active=="NO" && try_mode == 'NO') {
                return "Unactive test"
            }

            // Siempre ajustar la consulta para contar las filas
            test_query = "SELECT COUNT(*) FROM (" + test_query + ") t";

            var statement2 = snowflake.createStatement({sqlText: test_query});
            var resultSet2 = statement2.execute();
            resultSet2.next();
            var result = resultSet2.getColumnValue(1);

            var is_correct;
            // Si el número de filas erróneas es menor o igual al valor esperado, el test pasa
            is_correct = (result <= max_error_count) ;
            is_correct_str=is_correct.toString();

            // Alertas
            if (try_mode == 'NO'){
                var sqlText = "SELECT email,message FROM core.ALERT_DEFINITION WHERE test_bad_rows_id ="+ id;
                var statement3 = snowflake.createStatement({sqlText: sqlText});
                var resultSet3 = statement3.execute();
                if (resultSet3.next() && is_correct_str == 'false') {
                    var email = resultSet3.getColumnValue(1);
                    var message = resultSet3.getColumnValue(2);

                    if (try_mode == 'NO'){
                        var call_alert = "CALL SEND_ALERT_EMAIL('"+ result +"' , 'BAD_ROWS','"+ id +"','"+ email +"','"+ message +"') ";
                        var statement4 = snowflake.createStatement({sqlText: call_alert});
                        statement4.execute();
                    }

                }

                var statement5 = snowflake.createStatement({
                sqlText: "INSERT INTO core.DIRECT_RESULTS_BAD_ROWS (result_id,test_bad_rows_id,BoolResult, result,query_executed,test_name) VALUES (core.SEQ_DRBD.nextval,?, ?, ?, ?, ?)",
                binds: [id_test, is_correct_str, result, test_query,test_name]
                });
            } else if (try_mode == 'YES') {
                return is_correct_str+":"+result
            }
            statement5.execute();
            results.push(is_correct_str, result);
        }
        return "Tests executed. Results: " + results.join(", ");
    } catch(err) {

        var rollbackStatement = snowflake.createStatement({sqlText: "ROLLBACK"});
        rollbackStatement.execute();

        return "Error:" + err;  // Retorna el mensaje de error
    }
$$;

GRANT USAGE ON PROCEDURE core.EXECUTE_TEST_BAD_ROWS(VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;
-----------------------  EXECUTE TEST PROCEDURES -------------------------------------------

CREATE OR REPLACE PROCEDURE core.EXECUTE_TEST_CUSTOM_RESULTS(id_test VARCHAR, try_mode VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var try_mode = TRY_MODE;
    var id_test = ID_TEST;
    var results = [];
    var statement1 = snowflake.createStatement({
        sqlText: "SELECT id, test_query, expected_result, test_name, IS_ACTIVE FROM core.REFINED_TEST_CUSTOM_RESULTS where id=?",
        binds: [id_test]
    });
    var resultSet1 = statement1.execute();
    
    try{
        if (resultSet1.next()) {
            var id = resultSet1.getColumnValue(1);
            var test_query = resultSet1.getColumnValue(2);
            var expected_result = resultSet1.getColumnValue(3);
            var test_name = resultSet1.getColumnValue(4);
            var is_active = resultSet1.getColumnValue(5);

            if (is_active=="NO") {
                return "Unactive test"
            }

            var statement2 = snowflake.createStatement({sqlText: test_query});
            var resultSet2 = statement2.execute();
            resultSet2.next();
            var result = resultSet2.getColumnValue(1);

            var is_correct;
            // Si el número de filas erróneas es menor o igual al valor esperado, el test pasa
            is_correct = (result == expected_result) ;
            is_correct_str=is_correct.toString();

            // Alertas

            if (try_mode == 'NO'){
                var sqlText = "SELECT email,message FROM core.ALERT_DEFINITION WHERE test_custom_results_id ="+ id;
                var statement3 = snowflake.createStatement({sqlText: sqlText});
                var resultSet3 = statement3.execute();
                if (resultSet3.next() && is_correct_str == 'false') {
                    var email = resultSet3.getColumnValue(1);
                    var message = resultSet3.getColumnValue(2);

                    var call_alert = "CALL SEND_ALERT_EMAIL('"+ result +"' , 'CUSTOM_RESULTS','"+ id +"','"+ email +"','"+ message +"') ";
                    var statement4 = snowflake.createStatement({sqlText: call_alert});
                    statement4.execute();

                }
                var statement5 = snowflake.createStatement({
                    sqlText: "INSERT INTO core.DIRECT_RESULTS_CUSTOM_RESULTS (result_id,test_custom_results_id, BoolResult, result,query_executed, test_name) VALUES (core.SEQ_DRCR.nextval,?, ?, ?, ?, ?)",
                    binds: [id_test, is_correct_str, result, test_query, test_name]
                });
                statement5.execute();
                results.push(is_correct_str);    
            } else if (try_mode == 'YES') {
                return is_correct_str+":"+result
            }

        }
        return call_alert;
        //"Tests executed. Results: " + results.join(", ");
    } catch(err) {

        var rollbackStatement = snowflake.createStatement({sqlText: "ROLLBACK"});
        rollbackStatement.execute();

        return "Error:" + err;  // Retorna el mensaje de error
    }
$$;

GRANT USAGE ON PROCEDURE core.EXECUTE_TEST_CUSTOM_RESULTS(VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;
-------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.EXECUTE_TEST_COUNTS_SUMS(id_test VARCHAR,try_mode VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var try_mode = TRY_MODE;
    var id_test = ID_TEST;
    var results = [];
    var statement1 = snowflake.createStatement({
        sqlText: "SELECT id, test_query, expected_range, test_name, IS_ACTIVE FROM core.REFINED_TEST_COUNTS_SUMS where id=?",
        binds: [id_test]
    });
    var resultSet1 = statement1.execute();
    try {
        if (resultSet1.next()) {
            var id = resultSet1.getColumnValue(1);
            var test_query = resultSet1.getColumnValue(2);
            var expected_range = resultSet1.getColumnValue(3);
            var test_name = resultSet1.getColumnValue(4);
            var is_active = resultSet1.getColumnValue(5);

            if (is_active=="NO") {
                return "Unactive test"
            }

            var statement2 = snowflake.createStatement({sqlText: test_query});
            var resultSet2 = statement2.execute();
            resultSet2.next();
            var result = resultSet2.getColumnValue(1);

            var limits = expected_range.split('-');
            var lower_limit, upper_limit;
            var is_correct;

            if (limits[0] !== "") {
                lower_limit = parseFloat(limits[0]);
            }

            if (limits[1] !== "") {
                upper_limit = parseFloat(limits[1]);
            }

            if (lower_limit !== undefined && upper_limit !== undefined) {
                is_correct = result >= lower_limit && result <= upper_limit;
            } else if (lower_limit !== undefined) {
                is_correct = result >= lower_limit;
            } else if (upper_limit !== undefined) {
                is_correct = result <= upper_limit;
            } else {
                // Si no hay límites definidos, consideramos 'true' por defecto
                is_correct = true;
            }


            is_correct_str=is_correct.toString();
            // Alertas

            if (try_mode == 'NO') {
                var sqlText = "SELECT email,message FROM core.ALERT_DEFINITION WHERE test_counts_sums_id ="+ id;
                var statement3 = snowflake.createStatement({sqlText: sqlText});
                var resultSet3 = statement3.execute();
                if (resultSet3.next() && is_correct_str == 'false') {
                    var email = resultSet3.getColumnValue(1);
                    var message = resultSet3.getColumnValue(2);

                    var call_alert = "CALL SEND_ALERT_EMAIL('"+ result +"' , 'COUNTS_SUMS','"+ id +"','"+ email +"','"+ message +"') ";
                    var statement4 = snowflake.createStatement({sqlText: call_alert});
                    statement4.execute();

                }

                var statement5 = snowflake.createStatement({
                    sqlText: "INSERT INTO core.DIRECT_RESULTS_COUNTS_SUMS (result_id,test_counts_sums_id,BoolResult, result, query_executed,test_name) VALUES (core.SEQ_DRCS.nextval,?, ?, ?, ?, ?)",
                    binds: [id_test, is_correct_str, result, test_query, test_name]
                });
                statement5.execute();
                results.push(is_correct_str);
            } else if (try_mode == 'YES'){
                return is_correct_str+":"+result
            }

        }
        return "Tests executed. Results: " + results.join(", ");
    } catch(err) {

        var rollbackStatement = snowflake.createStatement({sqlText: "ROLLBACK"});
        rollbackStatement.execute();

        return "Error:" + err;  // Retorna el mensaje de error
    }
$$;

GRANT USAGE ON PROCEDURE core.EXECUTE_TEST_COUNTS_SUMS(VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;
-------------------------------------------------------------
-- Triguers creation 

CREATE OR REPLACE PROCEDURE core.TASK_CREATION(test_type VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$

    // Repetir bloque para cada tipo de test
    
    // Obtenemos los test de nueva creación desde el último lanzamiento de la task

    var test_type = TEST_TYPE;

    var sqlText = `SELECT cron_schedule, id, IS_ACTIVE FROM core.REFINED_TEST_${test_type} WHERE last_update > TIMESTAMPADD( hour , -24 , current_timestamp )`
    var statement1 = snowflake.createStatement({
        sqlText: sqlText});

    var resultSet = statement1.execute();
    
    while (resultSet.next()) {
        var cron_schedule = resultSet.getColumnValue(1);
        var id = resultSet.getColumnValue(2);
        var is_active  = resultSet.getColumnValue(3);

        if (is_active == "YES") {

            // Crear la tarea
            var create_task_sql = `
                CREATE OR REPLACE TASK core.DQ_${test_type}_${id}
                SCHEDULE = 'USING CRON ${cron_schedule}'
                AS
                CALL core.EXECUTE_TEST_${test_type}('${id}','NO');
            `;

            var statement2 = snowflake.createStatement({sqlText: create_task_sql});
            statement2.execute();

            var sqlTexto = "ALTER TASK core.DQ_"+ test_type +"_" + id + " RESUME";
            var statement3 = snowflake.createStatement({sqlText: sqlTexto});

            statement3.execute();
        }
    }
        return "Task created successfully!";

$$;


GRANT USAGE ON PROCEDURE core.TASK_CREATION(VARCHAR) TO APPLICATION ROLE app_public;
--------------------------------------------------------------------------------------
-- Send email

CREATE OR REPLACE PROCEDURE core.send_alert_email(test_result STRING, test_type VARCHAR, test_id VARCHAR, email VARCHAR, message VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var test_id = TEST_ID;
    var test_type = TEST_TYPE;
    var test_result = TEST_RESULT;
    var email = EMAIL;
    var message = MESSAGE;

    // Recuperar info necesaria de source tables 
    var sqlText = "SELECT test_name, test_query, test_comment ,$5  FROM core.REFINED_TEST_"+ test_type +" where id="+ test_id;
    var statement1 = snowflake.createStatement({
        sqlText: sqlText,

    });
    var resultSet1 = statement1.execute();
    
    if (resultSet1.next()) {
        var test_name = resultSet1.getColumnValue(1);
        var test_query = resultSet1.getColumnValue(2);
        var test_comment = resultSet1.getColumnValue(3);
        var expected = resultSet1.getColumnValue(4);

        // Creación de expected result text
        if (test_type == 'BAD_ROWS') {
            var expected_result_text = "This test was expecting less than "+ expected +" bad rows but it found "+ test_result;
        } else if (test_type == 'COUNTS_SUMS') {
            var expected_result_text = "This test was expecting a result in the range "+ expected +" but it found "+ test_result;
        } else if (test_type == 'CUSTOM_RESULTS'){
            var expected_result_text = "This test was expecting "+ expected +" but if found "+ test_result;
        } else {
            var expected_result_text = 'Something went wrong'
        }

   
        
        var execution_timestamp = new Date();
    
     // Verificar si el mensaje es NULL o indefinido y asignar un mensaje predeterminado si es necesario
         
    if (!message || message == "undefined" || message == 'null') {
        message = 
            "Hello,\n\n" +
            "Please find below the alert details :\n\n" +
            "Test name : " + test_name + "\n\n" +
            "Test details : " + test_comment + "\n\n" +
            expected_result_text + "\n\n" +
            "At " + execution_timestamp + "\n\n" +
            "Test query : " + test_query + "\n\n" +
            "Best regards,\n\n" +
            "Snowflake Monitoring";
    }
    
        // Llamar a la función SYSTEM$SEND_EMAIL para enviar el correo
        var sqlTexto = "CALL SYSTEM$SEND_EMAIL('Emails','"+ email +"', 'Snowflake Monitoring: "+ test_name +" Alert', \$\$"+ message +"\$\$)";
        var send_email_statement = snowflake.createStatement({
            sqlText: sqlTexto,
        });
    
        send_email_statement.execute();
    }
    return "Correo enviado con éxito.";
$$;

GRANT USAGE ON PROCEDURE core.send_alert_email(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;
---------------------------------------------------------------------------------------

---- General insert 


CREATE OR REPLACE PROCEDURE core.general_test(
    test_type STRING,
    test_name STRING,
    test_query STRING,
    expected STRING,
    test_comment STRING,
    cron_schedule STRING,
    alert_email STRING,
    alert_message STRING,
    mode_try STRING
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    try {
        var beginStatement = snowflake.createStatement({sqlText: "BEGIN"});
        beginStatement.execute();
        
        var mode_try = MODE_TRY;
        var test_type = TEST_TYPE;
        var test_name = TEST_NAME;
        var test_query = TEST_QUERY;
        var cron_schedule = CRON_SCHEDULE;
        var expected = EXPECTED;
        var test_comment = TEST_COMMENT;
        var alert_email = ALERT_EMAIL;
        var alert_message = ALERT_MESSAGE;
        var test_types = ['BAD_ROWS','COUNTS_SUMS','CUSTOM_RESULTS'];

        // Comprobar si ya existe un test con ese nombre en alguna tabla de definición de test
        for (var i = 0; i < test_types.length; i++) {
        var checkSql = "SELECT COUNT(*) FROM core.REFINED_TEST_" + test_types[i] + " WHERE test_name = '"+ test_name +"'";
        var checkStatement = snowflake.createStatement({sqlText: checkSql});
        var checkResultSet = checkStatement.execute();
        checkResultSet.next();
        
        if (checkResultSet.getColumnValue(1) > 0) {
        
            var rollbackStatement = snowflake.createStatement({sqlText: "ROLLBACK"});
            rollbackStatement.execute();
            
            return "The test name already exists. Please use another name.";
        }
        }

        if (test_type === 'BAD_ROWS') {
            var expected_column_name = 'max_error_count';
            var seq = 'core.SEQ_RTBD';
        } else if (test_type === 'COUNTS_SUMS') {
            var expected_column_name = 'expected_range';
            var seq = 'core.SEQ_RTCS';
        } else if (test_type === 'CUSTOM_RESULTS') {
            var expected_column_name = 'expected_result';
            var seq = 'core.SEQ_RTCR';
        } else {
            return "Invalid test_type provided.";
        }
        // Try_test mode
        if (mode_try === 'YES' ) {
            var seq = 'core.SEQ_TRY';
        }
        var insert_sql = "INSERT INTO core.REFINED_TEST_"+ test_type +"(id,test_name, test_query, cron_schedule, "+ expected_column_name +", test_comment) VALUES ("+ seq +".nextval,'"+ test_name +"', \$\$"+ test_query +"\$\$, '"+ cron_schedule +"', '"+ expected +"', '"+ test_comment +"') ";
        
    
        var statement1 = snowflake.createStatement({sqlText: insert_sql});
        var resultSet = statement1.execute();
        resultSet.next();
    
        var id_return = "select id from core.REFINED_TEST_"+ test_type +" where test_name = '"+ test_name +"' and cron_schedule = '"+ cron_schedule +"' and "+ expected_column_name +" = '"+  expected+"' and test_comment = '"+ test_comment+"'";
        var statement2 = snowflake.createStatement({sqlText: id_return})
        var resultSet2 = statement2.execute();
        resultSet2.next();
        var id_test = resultSet2.getColumnValue(1);
    
        var test_type_lower = test_type.toLowerCase();
        // Si se proporciona alert_email, insertar en ALERT_DEFINITION
        if (alert_email) {
            var alert_sql = "INSERT INTO core.ALERT_DEFINITION(test_"+ test_type_lower +"_id, email, message) VALUES ('"+ id_test +"', '"+ alert_email +"', '"+ alert_message +"')";
            var statement2 = snowflake.createStatement({sqlText: alert_sql});
            statement2.execute();
        }
        var commitStatement = snowflake.createStatement({sqlText: "COMMIT"});
        commitStatement.execute();
        
    return "Test and alert (if applicable) successfully created!";
    } catch(err) {

        var rollbackStatement = snowflake.createStatement({sqlText: "ROLLBACK"});
        rollbackStatement.execute();

        return "Error: " + err;  // Retorna el mensaje de error
    }
$$;

GRANT USAGE ON PROCEDURE core.general_test(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;
----------------------------------------------------------------------------------------------------------------------
----- General delete


CREATE OR REPLACE PROCEDURE core.TEST_DELETION(test_name VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var test_name = TEST_NAME;
    var test_types = ['BAD_ROWS','COUNTS_SUMS','CUSTOM_RESULTS'];
    
    
    for (var i = 0; i < test_types.length; i++) {

        var check = "SELECT ID FROM core.REFINED_TEST_"+ test_types[i] +" WHERE TEST_NAME = '"+ test_name +"'";
        var statement2 = snowflake.createStatement({sqlText: check});
        var resultSet2 = statement2.execute();
        if (resultSet2.next()) {

            var id_test = resultSet2.getColumnValue(1);
            
            var check = "DELETE FROM core.REFINED_TEST_"+ test_types[i] +" WHERE TEST_NAME = '"+ test_name +"'";
            var statement2 = snowflake.createStatement({sqlText: check});
            var resultSet2 = statement2.execute();
            
            
            var checko = "DELETE FROM core.ALERT_DEFINITION WHERE TEST_"+ test_types[i] +"_ID = '"+ id_test +"'";
            var statement2 = snowflake.createStatement({sqlText: checko});
            var resultSet2 = statement2.execute();
            
            return test_types[i] +" type test was deleted."
        }
    }
    
    return " No test called '"+ test_name +"' was found.";
$$;

GRANT USAGE ON PROCEDURE core.TEST_DELETION(VARCHAR) TO APPLICATION ROLE app_public;
-----------------------------------------------------------------------------------------------------------------------
----- Drop unnecessary triguers

CREATE OR REPLACE PROCEDURE core.TRIGUERS_DELETION(dq_database VARCHAR, dq_schema VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var db = DQ_DATABASE;
    var schema = DQ_SCHEMA;
    var test_types1 = ['DQ_BAD_ROWS_%','DQ_COUNTS_SUMS_%','DQ_CUSTOM_RESULTS_%'];
    var test_types2 = ['BAD_ROWS','COUNTS_SUMS','CUSTOM_RESULTS'];
    
    var cont = 0;
    
    for (var i = 0; i < test_types1.length; i++) {
    
        var show_tasks = "SHOW TASKS LIKE '"+ test_types1[i] +"' IN "+ db +"."+ schema;
        var statement = snowflake.createStatement({sqlText: show_tasks});
        var resultSet = statement.execute();
        while (resultSet.next()) {

            var task_name = resultSet.getColumnValue(2);
            var state = resultSet.getColumnValue(10);
            var aux = task_name.split('_');
            var id_test = aux[3];

            var check = "SELECT * FROM core.REFINED_TEST_"+ test_types2[i] +" WHERE ID = '"+ id_test +"' and IS_ACTIVE = 'YES'";
            var statement2 = snowflake.createStatement({sqlText: check});
            var resultSet2 = statement2.execute();
            if (resultSet2.next()) {
            } else {

                var drop_task = "DROP TASK "+ db +"."+ schema +"."+ task_name ;
                var statement3 = snowflake.createStatement({sqlText: drop_task});
                statement3.execute();
                var cont = cont +1;
            }
            
        }
    }
    
    return cont+" Unused Test Triguers dropped succesfully";
$$;

GRANT USAGE ON PROCEDURE core.TRIGUERS_DELETION(VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;
---------------------------------------------------------------------------------
--- Predefined test


--CREATE OR REPLACE PROCEDURE core.predefined_string_patterns_test(
--    table_name STRING,
--    column_names  ARRAY,
--    test_name STRING,
--    cron_schedule STRING,
--    test_comment STRING,
--    alert_email STRING,
--    alert_message STRING,
--    mode_get_query STRING
--)
--    RETURNS STRING
--    LANGUAGE JAVASCRIPT
--    EXECUTE AS OWNER
--AS
--$$
--    var column_names = COLUMN_NAMES.join(", ");
--    var mode_get_query = MODE_GET_QUERY;
--    var table_name = TABLE_NAME;
--    var test_name = TEST_NAME;
--    var cron_schedule = CRON_SCHEDULE;
--    var test_comment = TEST_COMMENT;
--    var alert_email = ALERT_EMAIL;
--    var alert_message = ALERT_MESSAGE;
--
--    var test_query = `
--    With base as (
--    SELECT COUNT(*) as TC FROM ${table_name}
--    ), key as (
--    select ${column_names} FROM ${table_name} group by ${column_names}
--    ), Aux as (
--    select count(*) as KC FROM key
--    )
--    SELECT 
--        case 
--            when a.KC = b.TC then TRUE
--        else FALSE
--        end as is_unique
--    from Aux a 
--    join base b
--    `;
--
--    if (mode_get_query){
--        return test_query;
--    }
--    var call_sql = `
--        CALL CALL QUALITY.INTERNAL.general_test(
--            'CUSTOM_RESULTS',
--            '${test_name}',
--            '${test_query}',
--            '${cron_schedule}',
--            'TRUE',
--            '${test_comment}',
--            '${alert_email}',
--            '${alert_message || ''}'
--        );
--    `;
--
--    try {
--        var statement = snowflake.createStatement({sqlText: call_sql});
--        ResultSeto = statement.execute();
--        ResultSeto.next();
--        var already = ResultSeto.getColumnValue(1)
--        if (already == "The test name already exists. Please use another name."){
--            return already;
--        }
--        return "Predefined volumetry test created successfully!";
--    } catch (err) {
--        return `Error: ${err.message}`;
--    }
--    return 'String_patterns test implemented succesfully!';
--$$;
--
--GRANT USAGE ON PROCEDURE core.predefined_uniqueness_test(VARCHAR,ARRAY,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;
                    
CREATE OR REPLACE PROCEDURE core.predefined_uniqueness_test(
    table_name STRING,
    test_name STRING,
    column_names  ARRAY,
    test_comment STRING,
    cron_schedule STRING,
    alert_email STRING,
    alert_message STRING,
    mode_try STRING
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var column_names = COLUMN_NAMES.join(", ");
    var mode_try = MODE_TRY;
    var table_name = TABLE_NAME;
    var test_name = TEST_NAME;
    var cron_schedule = CRON_SCHEDULE;
    var test_comment = TEST_COMMENT;
    var alert_email = ALERT_EMAIL;
    var alert_message = ALERT_MESSAGE;

    var test_query = `
    With base as (
    SELECT COUNT(*) as TC FROM ${table_name}
    ), key as (
    select ${column_names} FROM ${table_name} group by ${column_names}
    ), Aux as (
    select count(*) as KC FROM key
    )
    SELECT 
        case 
            when a.KC = b.TC then 1
        else 0
        end as is_unique
    from Aux a 
    join base b
    `;

    var call_sql = `
        CALL core.general_test(
            'CUSTOM_RESULTS',
            '${test_name}',
            '${test_query}',
            '1',
            '${test_comment}',
            '${cron_schedule}',
            '${alert_email}',
            '${alert_message || ''}',
            '${mode_try}'
        );
    `;

    try {
        var statement = snowflake.createStatement({sqlText: call_sql});
        ResultSeto = statement.execute();
        ResultSeto.next();
        var already = ResultSeto.getColumnValue(1)
        if (already == "The test name already exists. Please use another name."){
            return already;
        }
        return "Predefined Uniqueness test created successfully!";
    } catch (err) {
        return `Error: ${err.message}`;
    }
    return 'Uniqueness test implemented succesfully!';
$$;

GRANT USAGE ON PROCEDURE core.predefined_uniqueness_test(VARCHAR,VARCHAR,ARRAY,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;
    
CREATE OR REPLACE PROCEDURE core.predefined_volumetry_test(
    table_name STRING,
    test_name STRING,
    expected_range STRING,
    condition STRING,
    test_comment STRING,
    cron_schedule STRING,
    alert_email STRING,
    alert_message STRING,
    mode_try STRING
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var mode_try = MODE_TRY;
    var table_name = TABLE_NAME;
    var test_name = TEST_NAME;
    var cron_schedule = CRON_SCHEDULE;
    var test_comment = TEST_COMMENT;
    var alert_email = ALERT_EMAIL;
    var alert_message = ALERT_MESSAGE;
    var expected_range = EXPECTED_RANGE;
    var condition = CONDITION;

    var test_query = `SELECT count(*) as RowCount FROM ${table_name}`;
    if (condition && condition.trim() !== "") {
        test_query += ` WHERE ${condition}`;
    }


    var call_sql = `
        CALL core.general_test(
            'COUNTS_SUMS',
            '${test_name}',
            \$\$ ${test_query} \$\$,
            '${expected_range}',
            '${test_comment}',
            '${cron_schedule}',
            '${alert_email}',
            '${alert_message || ''}',
            '${mode_try}'
        );
    `;

    try {
        var statement = snowflake.createStatement({sqlText: call_sql});
        ResultSeto = statement.execute();
        ResultSeto.next();
        var already = ResultSeto.getColumnValue(1)
        if (already == "The test name already exists. Please use another name."){
            return already;
        }        
        return "Predefined volumetry test created successfully!";
    } catch (err) {
        return `Error: ${err.message}`;
    }
    return "Predefined volumetry test created successfully!"
$$;

GRANT USAGE ON PROCEDURE core.predefined_volumetry_test(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;

CREATE OR REPLACE PROCEDURE core.predefined_null_test(
    table_name STRING,
    column_name STRING,
    test_name STRING,
    max_error_count STRING,
    max_percentage STRING,
    test_comment STRING,
    cron_schedule STRING,
    alert_email STRING,
    alert_message STRING,
    mode_try STRING
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$

    var mode_try = MODE_TRY;
    var table_name = TABLE_NAME;
    var column_name = COLUMN_NAME;
    var test_name = TEST_NAME;
    var cron_schedule = CRON_SCHEDULE;
    var max_error_count = MAX_ERROR_COUNT;
    var test_comment = TEST_COMMENT;
    var alert_email = ALERT_EMAIL;
    var alert_message = ALERT_MESSAGE;
    var max_percentage = MAX_PERCENTAGE;

    if (max_error_count) {

        var test_query = `SELECT * FROM ` + table_name + ` WHERE ` + column_name + ` IS NULL`;

        var call_sql = `
            CALL core.general_test(
                'BAD_ROWS',
                '` + test_name + `',
                '` + test_query + `',
                '` + max_error_count + `',
                '` + test_comment + `',
                '` + cron_schedule + `',
                '` + alert_email + `',
                '` + (alert_message || '') + `',
                '` + mode_try + `'
            );
        `;

        var statement = snowflake.createStatement({sqlText: call_sql});
        statement.execute();
        
    } else if (max_percentage) {
        
        var test_query = "WITH one AS (SELECT count(*) AS TOTAL FROM "+ table_name +"), " + 
                         "due AS (SELECT count(*) AS NULLEO FROM " + table_name + 
                         " WHERE " + column_name + " IS NULL) " +
                         "SELECT case when TOTAL=0 then 0 else round(100*NULLEO/TOTAL, 0) "+
                         "end as Null_Percentage FROM one JOIN due";

        var call_sql_2 = "CALL core.general_test('COUNTS_SUMS','" + test_name + "',\$\$" + test_query + "\$\$,'-" + max_percentage + "','" + test_comment + "','" + cron_schedule + "','" + alert_email + "','" + (alert_message || ' ') + "','" + mode_try + "')";
        var statement2 = snowflake.createStatement({sqlText: call_sql_2});
        ResultSeto = statement2.execute();
        ResultSeto.next();
        var already = ResultSeto.getColumnValue(1)
        if (already == "The test name already exists. Please use another name."){
            return already;
        }

    } else {
        return "Introduce max_row_count or max_percentage please";
    }

    return "Predefined null test created successfully!";
$$;

GRANT USAGE ON PROCEDURE core.predefined_null_test(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;

CREATE OR REPLACE PROCEDURE core.activate(cron VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var cron = CRON;
    var createTaskSql = `
    CREATE OR REPLACE TASK core.TEST_ACTIVATION
      SCHEDULE = 'USING CRON ${cron}'
    AS
        BEGIN
          CALL core.TRIGUERS_DELETION('HELLO_SNOWFLAKE_APP','CORE');
          CALL core.TASK_CREATION('BAD_ROWS');
          CALL core.TASK_CREATION('COUNTS_SUMS');
          CALL core.TASK_CREATION('CUSTOM_RESULTS');
        END;
    `;

    var alterTaskSql = `
    ALTER TASK core.TEST_ACTIVATION RESUME;
    `;

    var InsertOn = `
    INSERT INTO core.on_off 
    select 1;
    `;

    try {
        var statement1 = snowflake.createStatement({sqlText: createTaskSql});
        statement1.execute();

        var statement2 = snowflake.createStatement({sqlText: alterTaskSql});
        statement2.execute();

        var statement3 = snowflake.createStatement({sqlText: InsertOn});
        statement3.execute();

        return "Application initialised, enjoy!";

    } catch (err) {
        return `Error: ${err.message}`;
    }

$$;

GRANT USAGE ON PROCEDURE core.activate(VARCHAR) TO APPLICATION ROLE app_public;

CREATE OR REPLACE PROCEDURE core.shutdown()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var dropTaskSql = `
   DROP TASK if exists CORE.TEST_ACTIVATION;
    `;

    var TruncateOff =`
   TRUNCATE TABLE IF EXISTS CORE.on_off;
    `;
    try {
        var statement1 = snowflake.createStatement({sqlText: dropTaskSql});
        statement1.execute();

        var statement2 = snowflake.createStatement({sqlText: TruncateOff});
        statement2.execute();

        return "Application off";

    } catch (err) {
        return `Error: ${err.message}`;
    }

$$;

GRANT USAGE ON PROCEDURE core.shutdown() TO APPLICATION ROLE app_public;



CREATE STREAMLIT IF NOT EXISTS code_schema.dq_snowflake_streamlit
  FROM  '/streamlit'
  MAIN_FILE = '/hello_snowflake.py'
;


GRANT USAGE ON STREAMLIT code_schema.dq_snowflake_streamlit TO APPLICATION ROLE app_public;