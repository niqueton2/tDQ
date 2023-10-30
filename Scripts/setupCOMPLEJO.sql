-- Setup script for the DQ application.

CREATE APPLICATION ROLE app_public;

CREATE  SCHEMA IF NOT exists internal;

-- Priviledges over internal



-- Test_names view 
--
--create or replace view internal.TEST_NAMES
--as 
--select test_name, 'BAD_ROWS' as Type
--from QUALITY.INTERNAL.REFINED_TEST_BAD_ROWS
--union
--select test_name, 'COUNTS_SUMS'
--from QUALITY.internal.REFINED_TEST_COUNTS_SUMS
--union 
--select test_name, 'CUSTOM_RESULTS'
--from QUALITY.internal.REFINED_TEST_CUSTOM_RESULTS;

-- Load Timezones

--CREATE OR REPLACE STAGE PUERTO;
--create or replace table TimeZone (
--Identifier VARCHAR,
--Country_code varchar(3),
--Abbreviation varchar,
--time_Start varchar,
--gmt_offset varchar,
--dst varchar
--
--);


--copy into TimeZone from @PUERTO;

-- Refresh
CREATE OR REPLACE PROCEDURE internal.refresh_ddl()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var tables = [`
        CREATE OR REPLACE TABLE REFINED_TEST_BAD_ROWS (
    id INT AUTOINCREMENT PRIMARY KEY,
    test_name VARCHAR,
    test_query VARCHAR NOT NULL,
    cron_schedule VARCHAR,  
    max_error_count VARCHAR,
    test_comment VARCHAR,
    creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`,`

CREATE OR REPLACE TABLE REFINED_TEST_COUNTS_SUMS (
    id INT AUTOINCREMENT PRIMARY KEY,
    test_name VARCHAR,
    test_query VARCHAR NOT NULL,
    cron_schedule VARCHAR,  
    expected_range VARCHAR,
    test_comment VARCHAR,
    creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`,`

CREATE OR REPLACE TABLE REFINED_TEST_CUSTOM_RESULTS (
    id INT AUTOINCREMENT PRIMARY KEY,
    test_name VARCHAR,
    test_query VARCHAR NOT NULL,
    cron_schedule VARCHAR,  
    expected_result VARCHAR,
    test_comment VARCHAR,
    creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`,`

CREATE OR REPLACE TABLE ALERT_DEFINITION (
    alert_id INT AUTOINCREMENT PRIMARY KEY,
    test_bad_rows_id INT REFERENCES REFINED_TEST_BAD_ROWS(id),
    test_counts_sums_id INT REFERENCES REFINED_TEST_COUNTS_SUMS(id),
    test_custom_results_id INT REFERENCES REFINED_TEST_CUSTOM_RESULTS(id),
    email VARCHAR NOT NULL,
    message VARCHAR,
    creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP

    )`,`

CREATE OR REPLACE TABLE DIRECT_RESULTS_BAD_ROWS (
    result_id INT AUTOINCREMENT PRIMARY KEY,
    test_bad_rows_id INT REFERENCES REFINED_TEST_BAD_ROWS(id),
    BoolResult VARCHAR NOT NULL,
    result VARCHAR NOT NULL,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`,`

CREATE OR REPLACE TABLE DIRECT_RESULTS_COUNTS_SUMS(
    result_id INT AUTOINCREMENT PRIMARY KEY,
    test_counts_sums_id INT REFERENCES REFINED_TEST_COUNTS_SUMS(id),
    BoolResult VARCHAR NOT NULL,
    result VARCHAR NOT NULL,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`,`

CREATE OR REPLACE TABLE DIRECT_RESULTS_CUSTOM_RESULTS (
    result_id INT AUTOINCREMENT PRIMARY KEY,
    test_custom_results_id INT REFERENCES REFINED_TEST_CUSTOM_RESULTS(id),
    BoolResult VARCHAR NOT NULL,
    result VARCHAR NOT NULL,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`,`
create or replace view COMPLETE_RESULTS
as 
select 
dbr.test_name as Name,
dbr.test_comment as Comment,
rbr.BOOLRESULT as Test_result,
rbr.execution_time
from QUALITY.INTERNAL.DIRECT_RESULTS_BAD_ROWS rbr
left join QUALITY.INTERNAL.REFINED_TEST_BAD_ROWS dbr
on rbr.test_bad_rows_id=dbr.id

union all

select 
dcs.test_name as Name,
dcs.test_comment as Comment,
rcs.BOOLRESULT as Test_result,
rcs.execution_time
from QUALITY.INTERNAL.DIRECT_RESULTS_COUNTS_SUMS rcs
left join QUALITY.INTERNAL.REFINED_TEST_COUNTS_SUMS dcs
on rcs.test_counts_sums_id=dcs.id

union all

select 
dcr.test_name as Name,
dcr.test_comment as Comment,
rcr.BOOLRESULT as Test_result,
rcr.execution_time
from QUALITY.INTERNAL.DIRECT_RESULTS_CUSTOM_RESULTS rcr
left join QUALITY.INTERNAL.REFINED_TEST_CUSTOM_RESULTS dcr
on rcr.test_custom_results_id=dcr.id;
`
    ];

    for (var i = 0; i < tables.length; i++) {
        var statement = snowflake.createStatement({sqlText: tables[i]});
        statement.execute();
    }

    return "All tables created or replaced successfully!";
$$;




-- Execution procedures

CREATE OR REPLACE PROCEDURE internal.EXECUTE_TEST_BAD_ROWS(id_test VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var id_test = ID_TEST;
    var results = [];
    var statement1 = snowflake.createStatement({
        sqlText: "SELECT id, test_query, max_error_count FROM QUALITY.INTERNAL.REFINED_TEST_BAD_ROWS where id=?",
        binds: [id_test]
    });
    var resultSet1 = statement1.execute();
    
    if (resultSet1.next()) {
        var id = resultSet1.getColumnValue(1);
        var test_query = resultSet1.getColumnValue(2);
        var max_error_count = resultSet1.getColumnValue(3);
        
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

        var sqlText = "SELECT email,message FROM QUALITY.INTERNAL.ALERT_DEFINITION WHERE test_bad_rows_id ="+ id;
        var statement3 = snowflake.createStatement({sqlText: sqlText});
        var resultSet3 = statement3.execute();
        if (resultSet3.next() && is_correct_str == 'false') {
            var email = resultSet3.getColumnValue(1);
            var message = resultSet3.getColumnValue(2);

            var call_alert = "CALL SEND_ALERT_EMAIL('"+ result +"' , 'BAD_ROWS','"+ id +"','"+ email +"','"+ message +"') ";
            var statement4 = snowflake.createStatement({sqlText: call_alert});
            statement4.execute();
            
        }

        var statement5 = snowflake.createStatement({
            sqlText: "INSERT INTO QUALITY.INTERNAL.DIRECT_RESULTS_BAD_ROWS (test_bad_rows_id,BoolResult, result) VALUES (?, ?, ?)",
            binds: [id_test, is_correct_str, result]
        });
        statement5.execute();
        results.push(is_correct_str, result);
    }
    return "Tests executed. Results: " + results.join(", ");
$$;
------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE internal.EXECUTE_TEST_CUSTOM_RESULTS(id_test VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var id_test = ID_TEST;
    var results = [];
    var statement1 = snowflake.createStatement({
        sqlText: "SELECT id, test_query, expected_result FROM QUALITY.INTERNAL.REFINED_TEST_CUSTOM_RESULTS where id=?",
        binds: [id_test]
    });
    var resultSet1 = statement1.execute();
    
    if (resultSet1.next()) {
        var id = resultSet1.getColumnValue(1);
        var test_query = resultSet1.getColumnValue(2);
        var expected_result = resultSet1.getColumnValue(3);
        
        
        var statement2 = snowflake.createStatement({sqlText: test_query});
        var resultSet2 = statement2.execute();
        resultSet2.next();
        var result = resultSet2.getColumnValue(1);
        
        var is_correct;
        // Si el número de filas erróneas es menor o igual al valor esperado, el test pasa
        is_correct = (result == expected_result) ;
        is_correct_str=is_correct.toString();
        
        // Alertas

        var sqlText = "SELECT email,message FROM QUALITY.INTERNAL.ALERT_DEFINITION WHERE test_custom_results_id ="+ id;
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
            sqlText: "INSERT INTO QUALITY.INTERNAL.DIRECT_RESULTS_CUSTOM_RESULTS (test_custom_results_id, BoolResult, result) VALUES (?, ?, ?)",
            binds: [id_test, is_correct_str, result]
        });
        statement5.execute();
        results.push(is_correct_str);
    }
    return call_alert;
    //"Tests executed. Results: " + results.join(", ");
$$;

-------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE internal.EXECUTE_TEST_COUNTS_SUMS(id_test VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var id_test = ID_TEST;
    var results = [];
    var statement1 = snowflake.createStatement({
        sqlText: "SELECT id, test_query, expected_range FROM QUALITY.INTERNAL.REFINED_TEST_COUNTS_SUMS where id=?",
        binds: [id_test]
    });
    var resultSet1 = statement1.execute();
    
    if (resultSet1.next()) {
        var id = resultSet1.getColumnValue(1);
        var test_query = resultSet1.getColumnValue(2);
        var expected_range = resultSet1.getColumnValue(3);
        
        
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

        var sqlText = "SELECT email,message FROM QUALITY.INTERNAL.ALERT_DEFINITION WHERE test_counts_sums_id ="+ id;
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
            sqlText: "INSERT INTO QUALITY.INTERNAL.DIRECT_RESULTS_COUNTS_SUMS (test_counts_sums_id,BoolResult, result) VALUES (?, ?, ?)",
            binds: [id_test, is_correct_str, result]
        });
        statement5.execute();
        results.push(is_correct_str);
    }
    return "Tests executed. Results: " + results.join(", ");
$$;
-------------------------------------------------------------------
-- Triguers creation 

CREATE OR REPLACE PROCEDURE internal.TASK_CREATION(test_type VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$

    // Repetir bloque para cada tipo de test
    
    // Obtenemos los test de nueva creación desde el último lanzamiento de la task
    var test_type = TEST_TYPE;

    var sqlText = `SELECT cron_schedule, id FROM QUALITY.INTERNAL.REFINED_TEST_${test_type} WHERE to_date(creation_timestamp)>dateadd(hour,-1,current_date())`
    var statement1 = snowflake.createStatement({
        sqlText: sqlText});

    var resultSet = statement1.execute();
    
    while (resultSet.next()) {
        var cron_schedule = resultSet.getColumnValue(1);
        var id = resultSet.getColumnValue(2);

        // Crear la tarea
        var create_task_sql = `
            CREATE OR REPLACE TASK DQ_${test_type}_${id}
            SCHEDULE = 'USING CRON ${cron_schedule}'
            AS
            CALL EXECUTE_TEST_${test_type}('${id}');
        `;

        var statement2 = snowflake.createStatement({sqlText: create_task_sql});
        statement2.execute();

        var sqlTexto = "ALTER TASK DQ_"+ test_type +"_" + id + " RESUME";
        var statement3 = snowflake.createStatement({sqlText: sqlTexto});

        statement3.execute();
    }
        return "Task created successfully!";

$$;



--------------------------------------------------------------------------------------
-- Send email

CREATE OR REPLACE PROCEDURE internal.send_alert_email(test_result STRING, test_type VARCHAR, test_id VARCHAR, email VARCHAR, message VARCHAR)
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
    var sqlText = "SELECT test_name, test_query, test_comment ,$5  FROM QUALITY.INTERNAL.REFINED_TEST_"+ test_type +" where id="+ test_id;
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


---------------------------------------------------------------------------------------

---- General insert 


CREATE OR REPLACE PROCEDURE internal.general_test(
    test_type STRING,
    test_name STRING,
    test_query STRING,
    expected STRING,
    test_comment STRING,
    cron_schedule STRING,
    alert_email STRING,
    alert_message STRING 
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    try {
        var beginStatement = snowflake.createStatement({sqlText: "BEGIN"});
        beginStatement.execute();
        
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
        var checkSql = "SELECT COUNT(*) FROM QUALITY.INTERNAL.REFINED_TEST_" + test_types[i] + " WHERE test_name = '"+ test_name +"'";
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
        } else if (test_type === 'COUNTS_SUMS') {
            var expected_column_name = 'expected_range';
        } else if (test_type === 'CUSTOM_RESULTS') {
            var expected_column_name = 'expected_result';
        } else {
            return "Invalid test_type provided.";
        }
        var insert_sql = "INSERT INTO QUALITY.INTERNAL.REFINED_TEST_"+ test_type +"(test_name, test_query, cron_schedule, "+ expected_column_name +", test_comment) VALUES ('"+ test_name +"', \$\$"+ test_query +"\$\$, '"+ cron_schedule +"', '"+ expected +"', '"+ test_comment +"') ";
        
    
        var statement1 = snowflake.createStatement({sqlText: insert_sql});
        var resultSet = statement1.execute();
        resultSet.next();
    
        var id_return = "select id from QUALITY.INTERNAL.REFINED_TEST_"+ test_type +" where test_name = '"+ test_name +"' and cron_schedule = '"+ cron_schedule +"' and "+ expected_column_name +" = '"+  expected+"' and test_comment = '"+ test_comment+"'";
        var statement2 = snowflake.createStatement({sqlText: id_return})
        var resultSet2 = statement2.execute();
        resultSet2.next();
        var id_test = resultSet2.getColumnValue(1);
    
        var test_type_lower = test_type.toLowerCase();
        // Si se proporciona alert_email, insertar en ALERT_DEFINITION
        if (alert_email) {
            var alert_sql = "INSERT INTO QUALITY.INTERNAL.ALERT_DEFINITION(test_"+ test_type_lower +"_id, email, message) VALUES ('"+ id_test +"', '"+ alert_email +"', '"+ alert_message +"')";
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
----------------------------------------------------------------------------------------------------------------------
----- General delete


CREATE OR REPLACE PROCEDURE internal.TEST_DELETION(test_name VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var test_name = TEST_NAME;
    var test_types = ['BAD_ROWS','COUNTS_SUMS','CUSTOM_RESULTS'];
    
    
    for (var i = 0; i < test_types.length; i++) {

        var check = "SELECT ID FROM QUALITY.INTERNAL.REFINED_TEST_"+ test_types[i] +" WHERE TEST_NAME = '"+ test_name +"'";
        var statement2 = snowflake.createStatement({sqlText: check});
        var resultSet2 = statement2.execute();
        if (resultSet2.next()) {

            var id_test = resultSet2.getColumnValue(1);
            
            var check = "DELETE FROM QUALITY.INTERNAL.REFINED_TEST_"+ test_types[i] +" WHERE TEST_NAME = '"+ test_name +"'";
            var statement2 = snowflake.createStatement({sqlText: check});
            var resultSet2 = statement2.execute();
            
            
            var checko = "DELETE FROM QUALITY.INTERNAL.ALERT_DEFINITION WHERE TEST_"+ test_types[i] +"_ID = '"+ id_test +"'";
            var statement2 = snowflake.createStatement({sqlText: checko});
            var resultSet2 = statement2.execute();
            
            return test_types[i] +" type test was deleted."
        }
    }
    
    return " No test called '"+ test_name +"' was found.";
$$;


-----------------------------------------------------------------------------------------------------------------------
----- Drop unnecessary triguers

CREATE OR REPLACE PROCEDURE internal.TRIGUERS_DELETION(dq_database VARCHAR, dq_schema VARCHAR)
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

            var check = "SELECT * FROM QUALITY.INTERNAL.REFINED_TEST_"+ test_types2[i] +" WHERE ID = '"+ id_test +"'";
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
------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------
--- Predefined test


CREATE OR REPLACE PROCEDURE internal.predefined_string_patterns_test(
    table_name STRING,
    column_names  ARRAY,
    test_name STRING,
    cron_schedule STRING,
    test_comment STRING,
    alert_email STRING,
    alert_message STRING,
    mode_get_query STRING
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var column_names = COLUMN_NAMES.join(", ");
    var mode_get_query = MODE_GET_QUERY;
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
            when a.KC = b.TC then TRUE
        else FALSE
        end as is_unique
    from Aux a 
    join base b
    `;

    if (mode_get_query){
        return test_query;
    }
    var call_sql = `
        CALL CALL QUALITY.INTERNAL.general_test(
            'CUSTOM_RESULTS',
            '${test_name}',
            '${test_query}',
            '${cron_schedule}',
            'TRUE',
            '${test_comment}',
            '${alert_email}',
            '${alert_message || ''}'
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
    return 'String_patterns test implemented succesfully!';
$$;


                    
CREATE OR REPLACE PROCEDURE internal.predefined_uniqueness_test(
    table_name STRING,
    column_names  ARRAY,
    test_name STRING,
    test_comment STRING,
    cron_schedule STRING,
    alert_email STRING,
    alert_message STRING,
    mode_get_query STRING
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var column_names = COLUMN_NAMES.join(", ");
    var mode_get_query = MODE_GET_QUERY;
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
            when a.KC = b.TC then TRUE
        else FALSE
        end as is_unique
    from Aux a 
    join base b
    `;

    if (mode_get_query){
        return test_query;
    }
    var call_sql = `
        CALL QUALITY.INTERNAL.general_test(
            'CUSTOM_RESULTS',
            '${test_name}',
            '${test_query}',
            'TRUE',
            '${test_comment}',
            '${cron_schedule}',
            '${alert_email}',
            '${alert_message || ''}'
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

    
CREATE OR REPLACE PROCEDURE internal.predefined_volumetry_test(
    table_name STRING,
    test_name STRING,
    expected_range STRING,
    condition STRING,
    test_comment STRING,
    cron_schedule STRING,
    alert_email STRING,
    alert_message STRING,
    mode_get_query STRING
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var mode_get_query = MODE_GET_QUERY;
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

    if (mode_get_query){
        return test_query;
    }

    var call_sql = `
        CALL QUALITY.INTERNAL.general_test(
            'COUNTS_SUMS',
            '${test_name}',
            \$\$ ${test_query} \$\$,
            '${expected_range}',
            '${test_comment}',
            '${cron_schedule}',
            '${alert_email}',
            '${alert_message || ''}'
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



CREATE OR REPLACE PROCEDURE internal.predefined_null_test(
    table_name STRING,
    column_name STRING,
    test_name STRING,
    max_error_count STRING,
    max_percentage STRING,
    test_comment STRING,
    cron_schedule STRING,
    alert_email STRING,
    alert_message STRING,
    mode_get_query STRING
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$

    var mode_get_query = MODE_GET_QUERY;
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

        if (mode_get_query){
            return test_query;
        }
        var call_sql = `
            CALL QUALITY.INTERNAL.general_test(
                'BAD_ROWS',
                '` + test_name + `',
                '` + test_query + `',
                '` + max_error_count + `',
                '` + test_comment + `',
                '` + cron_schedule + `',
                '` + alert_email + `',
                '` + (alert_message || '') + `'
            );
        `;

        var statement = snowflake.createStatement({sqlText: call_sql});
        statement.execute();
        
    } else if (max_percentage) {
        
        var test_query = "WITH one AS (SELECT count(*) AS TOTAL FROM "+ table_name +"), " + 
                         "due AS (SELECT count(*) AS NULLEO FROM " + table_name + 
                         " WHERE " + column_name + " IS NULL) " +
                         "SELECT case when TOTAL=0 then 0 else concat('-', to_varchar(round(NULLEO/TOTAL, 2))) "+
                         "end as Null_Percentage FROM one JOIN due";
        if (mode_get_query){
            return test_query
        }
        var call_sql_2 = "CALL QUALITY.INTERNAL.general_test('COUNTS_SUMS','" + test_name + "',\$\$" + test_query + "\$\$,'-" + max_percentage + "','" + test_comment + "','" + cron_schedule + "','" + alert_email + "','" + (alert_message || ' ') + "')";
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

--CREATE OR REPLACE TASK internal.TEST_ACTIVATION
--
--  SCHEDULE = '60 minutes'
--AS
--    BEGIN
--      CALL TRIGUERS_DELETION();
--      CALL TASK_CREATION('BAD_ROWS');
--      CALL TASK_CREATION('COUNTS_SUMS');
--      CALL TASK_CREATION('CUSTOM_RESULTS');
--    END;
--    
--
--alter task test_activation resume;


-- Permisos para el esquema CORE y INTERNAL

GRANT USAGE ON SCHEMA internal TO APPLICATION ROLE app_public;

GRANT USAGE ON PROCEDURE internal.TEST_DELETION(VARCHAR) TO APPLICATION ROLE app_public;

GRANT USAGE ON PROCEDURE internal.TRIGUERS_DELETION(VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

GRANT USAGE ON PROCEDURE internal.predefined_string_patterns_test(VARCHAR, ARRAY, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

GRANT USAGE ON PROCEDURE internal.predefined_uniqueness_test(VARCHAR, ARRAY, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

GRANT USAGE ON PROCEDURE internal.predefined_volumetry_test(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;

GRANT USAGE ON PROCEDURE internal.predefined_null_test(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;



-- Permisos para las vistas

--GRANT SELECT ON VIEW internal.COMPLETE_RESULTS TO APPLICATION ROLE app_public;

-- Permisos para las tablas
--GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE aux.REFINED_TEST_BAD_ROWS TO APPLICATION ROLE app_public;
--GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE aux.REFINED_TEST_COUNTS_SUMS TO APPLICATION ROLE app_public;
--GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE aux.REFINED_TEST_CUSTOM_RESULTS TO APPLICATION ROLE app_public;
--GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE aux.ALERT_DEFINITION TO APPLICATION ROLE app_public;
--GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE aux.DIRECT_RESULTS_BAD_ROWS TO APPLICATION ROLE app_public;
--GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE aux.DIRECT_RESULTS_COUNTS_SUMS TO APPLICATION ROLE app_public;
--GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE aux.DIRECT_RESULTS_CUSTOM_RESULTS TO APPLICATION ROLE app_public;

-- Permisos para las procedures
--GRANT USAGE ON PROCEDURE internal.refresh_ddl() TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE internal.EXECUTE_TEST_BAD_ROWS(VARCHAR) TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE internal.EXECUTE_TEST_CUSTOM_RESULTS(VARCHAR) TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE internal.EXECUTE_TEST_COUNTS_SUMS(VARCHAR) TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE internal.TASK_CREATION(VARCHAR) TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE internal.send_alert_email(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE internal.general_test(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE app_public;


CREATE SCHEMA IF NOT EXISTS code_schema;

CREATE STREAMLIT IF NOT EXISTS code_schema.dq_snowflake_streamlit
  FROM  '/Streamlit'
  MAIN_FILE = '/trial.py'
;

GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE app_public;
GRANT USAGE ON STREAMLIT code_schema.dq_snowflake_streamlit TO APPLICATION ROLE app_public;