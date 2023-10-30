# HQuality

Welcome to HQuality, the application that will allow you the implementation of quality test directly in Snowflake at the moment, every test will be executed at the moment that you decide and you will receive a mail if it fails (and you want to receive it). 

Please before begin to use the App and explore it read and execute this simple steps so your setup is completed and ready to function.

### 1. Commands to execute in your account

Now your application is installed and prepared for your account, it only needs some permissions in order to be able to schedule and execute quality tests. First it need permission to execute tasks and serverless tasks, don't worry, the app has a section of costs so you can monitor how much compute are you using and establish some alerts and stops if the consume grow without control. It need also access to the SNOWFLAKE database to get the credits used for the executions.
Finally you will create a procedure in order to add the list of emails that the app can send messages to. Don't worry once all of this is executed you will work only in the app and add emails from there.

    --sql code to execute with ACCOUNTADMIN:
    
    GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO  APPLICATION HQUALITY;
    GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO APPLICATION HQUALITY;

    CREATE DATABASE HQUALITY_AUX;
    CREATE SCHEMA HQUALITY_AUX.SETTINGS;
    CREATE OR REPLACE PROCEDURE HQUALITY_AUX.SETTINGS.addemail(emails VARCHAR)
       RETURNS STRING
       LANGUAGE JAVASCRIPT
       EXECUTE AS OWNER
    AS
    $$
        var emails = EMAILS;
        var emailIntegration = "CREATE OR REPLACE NOTIFICATION INTEGRATION Emails"+
        " TYPE = EMAIL"+
        " ENABLED = TRUE"+ 
        " ALLOWED_RECIPIENTS = ('"+emails+"');";

        var grantsEmailIntegration = "GRANT USAGE ON INTEGRATION EMAILS TO APPLICATION HQUALITY; ";

        try {
            var statement1 = snowflake.createStatement({sqlText: emailIntegration});
            statement1.execute();

            var statement2 = snowflake.createStatement({sqlText: grantsEmailIntegration});
            statement2.execute();

            return "Email list updated";

        } catch (err) {
            return `Error: ${err.message} : ${emailIntegration}`;
        }

    $$;

    GRANT USAGE ON DATABASE HQUALITY_AUX TO APPLICATION HQUALITY;
    GRANT USAGE ON SCHEMA HQUALITY_AUX.SETTINGS TO APPLICATION HQUALITY;
    GRANT USAGE ON PROCEDURE HQUALITY_AUX.SETTINGS.addemail(VARCHAR) TO APPLICATION HQUALITY;

### 2. Press button "First time using app"

By pressing this button in the right column of the app you will activate the app. This means that from now onall the checks that you implement will run whenever it's time. Please notice that the application uses a task that every hour checks if there is any new test implemented in order to activate it. The same for deletion, if the app runs al find that there is an active test that a user marked to delete it will delete it. We can ensure that a test is activated 1 hora after implementing it in the worst case, if the first executions is planned before this time interval is possible that nothing happends for this first execution. 

### 3. Grant permission to the app to see the tables in your account

If you want to implement a test first thing you will have to do is make sure that the app has permission of select upon the table that you want to test. This can be achieved by executing this 3 commands.

        GRANT USAGE ON DATABASE MY_DB TO APPLICATION HELLO_SNOWFLAKE_APP;
    GRANT USAGE ON SCHEMA MY_DB.MY_SCHEMA TO APPLICATION HELLO_SNOWFLAKE_APP;
    GRANT SELECT ON TABLE MY_DB.MY_SCHEMA:MY_TABLE TO APPLICATION HELLO_SNOWFLAKE_APP;
    
We strongly recommend you to give permission to select most of the tables in your Snowflake account because in this way you won't have to give a new permission with any new test and any user will be able to implement a test without further adjustments. Because of this we recommend to execute some bulk grants like:

        -- for every DB you want 
    GRANT SELECT ON ALL TABLES IN DATABASE MY_DB TO APPLICATION HELLO_SNOWFLAKE_APP; 




### 4. You're ready to begin implementing tests!

Navigate to the right tab in this menu and begin to investigate the application, we try to make it as simple as posible so it easier for you to understand and use. If you struggle to implement some type of test you can read the documentation tab that you will find in the app. Please contact us if anything is not working, you don't understand any section or you have and idea to make the app better! It will be a pleasure to receive your feedback. Thanks for using the app and enjoy!