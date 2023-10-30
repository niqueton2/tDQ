# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import numpy as np
import altair as alt
from stramlit_sections import test_parameters,Menu,try_test_button,enter_email,implement
from modules import cron_builder,query_lista,execute_procedure,procedures_dict,TimeZones
from datetime import datetime, timedelta
import time as time_module
from datetime import time


#--------------------------------------------------------------------------------------------------
# -- Fixes
#   -- Observability
#   -- Alerts and quotas
#   -- Arreglar Uniqueness try
#
#
# -- Improvements
#   -- OpenAI
#   -- 
#
#----------------------------------------------------------------------------------------------------


# Get the current credentials
session = get_active_session()
st.set_page_config(layout="wide")

def main_page():
   
   Menu(session)
   with st.container():
        
        with st.container():
            st.markdown(" # Create a new check ✅ ")


            basic,schedule = st.columns([2,1],gap="large")
            with basic:

                st.markdown(" ## General fields 📝")

                test_choice = st.selectbox("Choose test type :", ("Custom query", "Uniqueness", "Nulls","Volumetry"))
            
                parametersArray = test_parameters(session,test_choice)
                #if 'parametersArray' not in st.session_state:
                #    st.session_state['parametersArray'] = parametersArray
                ## Marca : Poner botón en el medio o poner la zona más bonita en general
                #if st.button('Continue to schedule and alert settings',use_container_width = True):
                #   st.session_state['current_page'] = "Configuración de programación y alertas"
                #st.write('Once you complete the implementation you can go to Manage Test 🧰 section to try your test and modify whathever neccesary')
                
            with schedule:
                
                st.markdown('##  Schedule 🕟')
                st.write("")
                st.write("")
                with st.expander("Click to configure"):
                    # Selección de timeZone
                    lista_minutos = ['00','01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59']
                    lista_horas = ['00','01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22','23']
                    timeZone = st.selectbox("Select a Time Zone :",TimeZones,help="Select the time zone you're working in",index = 315)
                    # Selección de días de la semana
                    dias = st.multiselect("Select the days of the week :", 
                                          ["All","Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],help="Your test will be executed this days")
                    # Selección de horas
                    horas = st.multiselect("Select the hours :", lista_horas,help="Your test will be executed at this hours")       
                    # Selección de minutos
                    minutos = st.multiselect("Select the minutes :", lista_minutos,help="Your test will be executed at this minutes")
                    cron = cron_builder(dias, horas, minutos)+" "+str(timeZone)     
                    diasPLUS=""
                    if dias:
                        diasPLUS = "on "+",".join(dias)
                    time_var = ""
                    for i in range(len(horas)):
                        for j in range(len(minutos)):
                            if (i,j) == (0,0):
                                time_var += "at "+horas[i]+":"+minutos[j]
                            else:
                                time_var += ", "+ horas[i]+":"+minutos[j] 
                      
                if dias == ['All']:
                    st.write(f"Your test will be executed everyday {time_var} in {timeZone} time.")
                else:
                    st.write(f"Your test will be executed {diasPLUS} {time_var} in {timeZone} time.")

                with st.container():
                    st.markdown('## Alert settings ☎️')
                    df_emails = session.sql("SELECT distinct email as Emails from hquality_aux.settings.list_emails")
                    df_emails_pd = df_emails.to_pandas()
                    list_emails = df_emails_pd['EMAILS'].tolist()

                    alert_email_array = st.multiselect('Alert Email :', list_emails,help='Read expander before set it for the first time, btw you can insert more than one email separated by comma.')
                    alert_emails_procedure = ",".join(list_emails)
                    alert_email = ','.join(alert_email_array)
                    with st.expander('Requirements email 🔽'):
                        st.markdown( 
                            """
                            You will only have to do this if is the first time you want to implement an alert, from now on you will see your mail in the selectbox and all will be already configurated

                            - You will need to verify your email by clicking the button in the profile tab in snowflake
                                (If you don't see nothing like "verify" your email is already verify and you can continue)

                            - If you don't see your email in the selectbox go to the app settings and refresh the email list
                        """
                        )
   
        with st.container():
            st.write("")
            st.write("")
            emp1,try1,emp2,imp,emp3  = st.columns(5,gap="medium")
            with try1:
                try_test = 0
                if 'try_test' not in st.session_state:
                    st.session_state['try_test'] = try_test 
                label_try_test = """Try test now

▶️"""
                if st.button(label_try_test,use_container_width=True):
           
                    try_test = 1
                    st.session_state['try_test'] = try_test


            
            with imp:
                label_implement = """Implement test

➕"""

                if st.button(label_implement,use_container_width=True):
                    parts = cron.split(' ')
                    if len(parts) < 6:
                        st.warning('Please configure a valid schedule 👀')
                    else:
                        implement(session,parametersArray,cron,alert_email) 




        with st.container():

                if try_test:
                    implement(session,parametersArray,cron,alert_email,mode_try = 'YES') 
                    test_type1 = query_lista(session,f"SELECT test_type FROM core.TEST_NAME_TYPE where test_name = '{parametersArray[2]}'","TEST_TYPE")
                    query_test = f"SELECT * FROM core.REFINED_TEST_{test_type1[0][0]} where test_name = '{parametersArray[2]}'"
                    test_id1 = query_lista(session,query_test,"ID")
                    test_id = test_id1[0][0]
                    
                    procedure_call = f"CALL core.EXECUTE_TEST_{test_type1[0][0]}('{test_id}','YES')"
                    lista_result = execute_procedure(session, procedure_call)

                    BoolYResult = lista_result[0][f"EXECUTE_TEST_{test_type1[0][0]}"]
                    partes = BoolYResult.split(':')
                    Bool = partes[0]
                    Result = partes[1]

                    session.sql(f"DELETE FROM core.REFINED_TEST_{test_type1[0][0]} where ID <0").collect()

                    content,close = st.columns([5,1],gap='small')
                    with content:
                        if Bool == 'false':
                            if test_choice == 'Uniqueness':
                                st.error(f"❌ TEST FAILED | Query result : Is not unique ")
                            elif test_choice == 'Nulls':
                                print(f'parametersArray[5] : {parametersArray[5]}')
                                if parametersArray[5] != "":

                                    st.error(f"❌ TEST FAILED | Result : {Result}% of null values found")
                                else:
                                    st.error(f"❌ TEST FAILED | Result : {Result} null values found")
                            elif test_choice == 'Volumetry':
                                st.error(f"❌ TEST FAILED | Row count : {Result} ")

                            else:
                                st.error(f"❌ TEST FAILED | Query result : {Result} ")

                        elif Bool == 'true':
                            if test_choice == 'Uniqueness':
                                st.success(f"✅ TEST PASSED | Result : Is unique")
                            elif test_choice == 'Nulls':
                                if parametersArray[5]:
                                    st.success(f"✅ TEST PASSED | Result : {Result}% of null values found")
                                else:
                                    st.success(f"✅ TEST PASSED | Result : {Result} null values found")
                            elif test_choice == 'Volumetry':
                                st.success(f"✅ TEST PASSED | Row count : {Result} ")
                            else:
                                st.success(f"✅ TEST PASSED | Query result : {Result}")
                        elif Bool =='Error':
                            st.warning(f"🤨  Error with test query : {Result}")
                    with close:
                        if st.button('🔼',help = 'Close'):
                            try_test = 0
                            st.session_state['try_test'] = try_test






def docs_page():
    Menu(session)
    with st.container():
        st.markdown(' # Documentation 📖')
        
        with st.expander('General app'):
            'Details'
        with st.expander('Custom query tests'):
            'Details'
        with st.expander('Predifined tests catalog'):
            'Details'
        with st.expander('Monitor and cap cost'):
            'Details'





def delete_edit_page():
    edited_test = pd.DataFrame()
    Menu(session)
    with st.container():
        st.markdown(' # Manage tests 🧰')
        test_name_view="No test found"
        lista1 = query_lista(session,f"SELECT DISTINCT TEST_NAME FROM core.TEST_NAME_TYPE","TEST_NAME")

        test_name_view = st.selectbox('Search test name:', lista1[0])
        test_id = None

        if test_name_view != 'No tests found':
            try:
                test_type1 = query_lista(session,f"SELECT test_type FROM core.TEST_NAME_TYPE where test_name = '{test_name_view}'","TEST_TYPE")

                query_test = f"SELECT * FROM core.REFINED_TEST_{test_type1[0][0]} where test_name = '{test_name_view}'"
                df_pd2 = query_lista(session,query_test)
                df_pd = df_pd2[1]
                if isinstance(df_pd2[1], pd.DataFrame):
                    st.dataframe(df_pd)

                test_id1 = query_lista(session,query_test,"ID")
                test_id = test_id1[0][0]
                query_alert = f"SELECT EMAIL FROM core.ALERT_DEFINITION where TEST_{test_type1[0][0]}_ID = '{test_id}'"
                email_actuales1 = query_lista(session,query_alert,"EMAIL")
                emaily = email_actuales1[0][0]
                message = f"If this test fails an alert will be sent by email to {emaily}"
                if message == "If this test fails an alert will be sent by email to No tests found":
                    st.write("No alerts configured for this test yet")
                else:
                    st.write(message)

            except Exception as e:
                st.write(f"An error occurred: {e} ")           

            change_query,change_schedule,changeAlert_Receiver,delete,activate,trytest = st.columns(6,gap="small")


            with delete:    
                label_delete = """Delete test 

🗑️"""
                if st.button(label_delete,use_container_width = True):
                    try:
                        delete_query = f"DELETE FROM core.REFINED_TEST_{test_type1[0][0]} where test_name = '{test_name_view}'"
                        delete_alert = f"DELETE FROM core.ALERT_DEFINITION where TEST_{test_type1[0][0]}_ID = '{test_id}'"
                        session.sql(delete_query).collect()
                        session.sql(delete_alert).collect()
                        st.success('Test deleted') 
                    except Exception as e:
                        st.write(f"An error occurred: {e}")
            with change_query: 
                new_query = 0 
                if 'new_query' not in st.session_state:
                    st.session_state['new_query'] = new_query  
                if st.button('Modify query   \n ✏️',use_container_width = True):
                    new_schedule = 0
                    st.session_state['new_schedule'] = new_schedule
                    new_alert_receiver = 0
                    st.session_state['new_alert_receiver'] = new_alert_receiver

                    new_query = 1
                    st.session_state['new_query'] = new_query 


            
            with change_schedule: 
                new_schedule = 0  
                if 'new_schedule' not in st.session_state:
                    st.session_state['new_schedule'] = new_schedule 
                if st.button('Modify schedule  \n 🕑',use_container_width = True):
                    new_query = 0
                    st.session_state['new_query'] = new_query
                    new_alert_receiver = 0
                    st.session_state['new_alert_receiver'] = new_alert_receiver

                    new_schedule = 1
                    st.session_state['new_schedule'] = new_schedule


            with changeAlert_Receiver:
                new_alert_receiver = 0
                if 'new_alert_receiver' not in st.session_state:
                    st.session_state['new_alert_receiver'] = new_alert_receiver 
                if st.button('Modify alert receivers  \n 🧑‍💼',use_container_width = True):
                    new_query = 0
                    st.session_state['new_query'] = new_query
                    new_schedule = 0
                    st.session_state['new_schedule'] = new_schedule

                    new_alert_receiver = 1
                    st.session_state['new_alert_receiver'] = new_alert_receiver

            with activate: 

                tq_1 = query_lista(session,query_test,"IS_ACTIVE")
                active_flag = tq_1[0][0]


                if active_flag =="NO":
                    label_activate="""Activate

🟢"""
                    if st.button(label_activate,use_container_width = True):
                        # Update
                        try:
                            query_test = f"UPDATE core.REFINED_TEST_{test_type1[0][0]} set IS_ACTIVE = 'YES' where test_name = '{test_name_view}'"
                            session.sql(query_test).collect()
                            st.success("Done, reload to see the update")
                        except Exception as e:
                            st.error(f"An error occurred: {e}")
                else:
                    label_deactivate="""Deactivate

🔴"""
                    if st.button(label_deactivate,use_container_width = True):
                        # Update
                        try:
                            query_test = f"UPDATE core.REFINED_TEST_{test_type1[0][0]} set IS_ACTIVE = 'NO' where test_name = '{test_name_view}'"
                            session.sql(query_test).collect()
                            st.success(f"Done, reload to see the update")
                        except Exception as e:
                            st.error(f"An error occurred: {e}")
            with trytest: 

                tq_1 = query_lista(session,query_test,"TEST_QUERY")
                test_query = tq_1[0][0]


                if test_query =="" or test_query is None:
                    st.write("Test without 'TEST_QUERY' cannot be executed")
                    
                else:
                    try_test = 0
                    if 'try_test' not in st.session_state:
                        st.session_state['try_test'] = try_test 
                    label_try_test = """Try test now

▶️"""
                    if st.button(label_try_test,use_container_width = True):
                        try_test = 1
                        st.session_state['try_test'] = try_test


            with st.container():
                new_query = st.session_state['new_query']
                new_schedule = st.session_state['new_schedule']
                new_alert_receiver = st.session_state['new_alert_receiver']

                if try_test:
                    procedure_call = f"CALL core.EXECUTE_TEST_{test_type1[0][0]}('{test_id}','YES')"
                    lista_result = execute_procedure(session, procedure_call)
                    BoolYResult = lista_result[0][f"EXECUTE_TEST_{test_type1[0][0]}"]
                    partes = BoolYResult.split(':')
                    Bool = partes[0]
                    Result = partes[1]
                    content,close = st.columns([5,1],gap='small')
                    with content:
                        if Bool == 'false':
                            st.error(f"❌ TEST FAILED | Query result : {Result} ")
                        elif Bool == 'true':
                            st.success(f"✅ TEST PASSED | Query result : {Result}")
                        elif Bool =='Error':
                            st.warning(f"🤨  Error with test query : {Result}")
                    with close:
                        if st.button('🔼',help = 'Close'):
                            try_test = 0
                            st.session_state['try_test'] = try_test
                if new_query:
                    tq_1 = query_lista(session,query_test,"TEST_QUERY")
                    test_query = tq_1[0][0]
                    new_q = st.text_area("Enter here the modified query",help = 'Once you apply tha change you can try your new query with the botton "Try test now"',placeholder=test_query)
                    if st.button('Apply changes ✳️'):

                        if new_q == '':
                            st.write("Please introduce an SQL query")
                        else:
                            query_test = f"UPDATE core.REFINED_TEST_{test_type1[0][0]} set TEST_QUERY = $${new_q}$$ where test_name = '{test_name_view}'"
                            session.sql(query_test).collect()
                            time_module.sleep(1)
                            new_query = 0
                            st.session_state['new_query'] = new_query
                if new_alert_receiver:
                    df_emails = session.sql("SELECT distinct email as Emails from core.list_emails")
                    df_emails_pd = df_emails.to_pandas()
                    list_emails = df_emails_pd['EMAILS'].tolist()
                    list_emails.append('None')
                    
                    

                    alert_email_array = st.multiselect('New emails for this alert:', list_emails)
                    alert_email = ','.join(alert_email_array)
                                                
                    if st.button('Apply changes ✳️'):
                        if alert_email == 'None':
                            query_test = f"DELETE FROM core.ALERT_DEFINITION  where TEST_{test_type1[0][0]}_ID = '{test_id}'"
                            session.sql(query_test).collect()
                        else:
                            query_test = f"UPDATE core.ALERT_DEFINITION set EMAIL = '{alert_email}' where TEST_{test_type1[0][0]}_ID = '{test_id}'"
                            updateCount1 = query_lista(session,query_test,"number of rows updated")
                            uCount = str(updateCount1[0][0])
                            if uCount == '0':
                                query_insert = f"INSERT INTO core.ALERT_DEFINITION (ALERT_ID, TEST_{test_type1[0][0]}_ID, EMAIL) VALUES (core.SEQ_AD.nextval,'{test_id}','{alert_email}')"
                                try:
                                    session.sql(query_insert).collect()
                                except Exception as e:
                                    st.write(f"An error occurred: {e} , ")   
                        time_module.sleep(1)
                        new_alert_receiver = 0
                        st.session_state['new_alert_receiver'] = new_alert_receiver
                if new_schedule:
                    # Selección de timeZone
                    lista_minutos = ['00','01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59']
                    lista_horas = ['00','01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22','23']
                    timeZone = st.selectbox("Select a Time Zone :",TimeZones,help="Select the time zone you're working in",index = 315)
                    # Selección de días de la semana
                    dias = st.multiselect("Select the days of the week :", 
                                          ["All","Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],help="Your test will be executed this days")
                    # Selección de horas
                    horas = st.multiselect("Select the hours :", lista_horas,help="Your test will be executed at this hours")
                    # Selección de minutos
                    minutos = st.multiselect("Select the minutes :", lista_minutos,help="Your test will be executed at this minutes")
                    cron = cron_builder(dias, horas, minutos)+" "+str(timeZone)
                    diasPLUS=""
                    if dias:
                        diasPLUS = "on "+",".join(dias)
                    time_var = ""
                    for i in range(len(horas)):
                        for j in range(len(minutos)):
                            if (i,j) == (0,0):
                                time_var += "at "+horas[i]+":"+minutos[j]
                            else:
                                time_var += ", "+ horas[i]+":"+minutos[j]
                    if dias == ['All']:
                        st.write(f"Your test will be executed everyday {time_var} in {timeZone} time.")
                    else:
                        st.write(f"Your test will be executed {diasPLUS} {time_var} in {timeZone} time.")
                    if st.button('Apply changes ✳️'):
                        query_test = f"UPDATE core.REFINED_TEST_{test_type1[0][0]} set CRON_SCHEDULE = '{cron}' where test_name = '{test_name_view}'"
                        session.sql(query_test).collect()
                        time_module.sleep(1)
                        new_schedule = 0
                        st.session_state['new_schedule'] = new_schedule
            with st.container():

                colo1, colo2 = st.columns([1,2],gap="small")
                query_results = f"SELECT * FROM core.DIRECT_RESULTS_{test_type1[0][0]} where test_name = '{test_name_view}'"
                df_results2 = query_lista(session,query_results)
                df_results = df_results2[1]
                cron_schedule = df_pd.loc[0, 'CRON_SCHEDULE']
                time_zone_str = cron_schedule.split()[-1]
                if df_results.empty:
                    st.warning("No executions for this test yet")
                    
                else:
                    df_results['EXECUTION_TIME'] = pd.to_datetime(df_results['EXECUTION_TIME'])
                    df_results['EXECUTION_TIME'] = df_results['EXECUTION_TIME'].dt.tz_localize('UTC').dt.tz_convert(time_zone_str)


                    with colo1:
                        st.markdown(' ## Metrics')
                        # Número de veces ejecutado
                        
                        
                        execution_count = df_results.shape[0]
                        st.metric(label="Number of Times Executed", value=execution_count)
                        # Primera vez que se ejecutó
                        first_execution_time = df_results['EXECUTION_TIME'].min().strftime('%Y-%m-%d %H:%M')
                        st.metric(label="First Time Executed", value=first_execution_time)
                        # Porcentaje de veces que ha fallado
                        fail_percentage = (df_results['BOOLRESULT'] == 'false').mean() * 100
                        st.metric(label="Failure Percentage", value=f"{fail_percentage:.2f}%")

                        # Número de ejecuciones correctas desde el último fallo
                        reverse_df = df_results.iloc[::-1]
                        try:
                            last_failure_index = reverse_df['BOOLRESULT'].tolist().index('false')
                            successful_streak = last_failure_index
                        except ValueError:  # No se encontró ningún 'false' en la lista, lo que indica que no hubo fallos
                            successful_streak = df_results.shape[0]
                        st.metric(label="Successful Execution Streak", value=successful_streak)

                    with colo2: 
                        st.markdown(' ## Previous executions')
                        

                        df_results_filtered = df_results.sort_values(by='EXECUTION_TIME', ascending=False).head(10)[['QUERY_EXECUTED', 'BOOLRESULT', 'EXECUTION_TIME']]
                        for index, row in df_results_filtered.iterrows():
                             execution_time = pd.to_datetime(row['EXECUTION_TIME'])
                             formatted_execution_time = execution_time.strftime('%Y-%m-%d %H:%M')
                             if row['BOOLRESULT'].lower() == 'true':
                                 st.success(f"{formatted_execution_time} Success : {row['QUERY_EXECUTED']} ")
                             else:
                                 st.error(f"{formatted_execution_time} Error : {row['QUERY_EXECUTED']}")

        else:
            st.write('No test found yet, nothing to do.')
        




def costs_page():
    Menu(session)
    with st.container():
        st.markdown(' # Cost control 	💵')

        selector, go_quota = st.columns([2,1],gap="large")

        with selector:
            cost_query = "select NAME,CREDITS_USED::varchar as CREDITS_STR,START_TIME,END_TIME from snowflake.account_usage.metering_history where service_type = 'SERVERLESS_TASK';"
            df_costs2 = query_lista(session,cost_query)
            df_cost = df_costs2[1]

            # Step 1: Permitir que el usuario elija un rango de fechas
            date_range_option = st.selectbox(
                'Select Date Range',
                ('Last 90 Days', 'Last 30 Days', 'Last 7 Days', 'Last 1 Day')
            )

            # Step 2: Calcular el offset de fecha basado en la selección del usuario
            if date_range_option == 'Last 90 Days':
                start_date = pd.Timestamp.today()-pd.DateOffset(days=90)
            elif date_range_option == 'Last 30 Days':
                start_date = pd.Timestamp.today()-pd.DateOffset(days=30)
            elif date_range_option == 'Last 7 Days':
                start_date = pd.Timestamp.today()-pd.DateOffset(days=7)
            elif date_range_option == 'Last 1 Day':
                start_date = pd.Timestamp.today()-pd.DateOffset(days=1)

            # Calcular la fecha de inicio basada en el offset
            current_datetime = pd.Timestamp.today()
            current_date = current_datetime.normalize()

            start_date = start_date.tz_localize('Europe/madrid')

            

            # Asegúrate de que START_TIME es una columna datetime
            if df_cost.empty:
                    st.write("No executions yet")
            else:
                df_cost['START_TIME'] = pd.to_datetime(df_cost['START_TIME'])

                # Filtrar el DataFrame por fecha y nombre de tarea y calcular las métricas
                df_filtered_all = df_cost[(df_cost['NAME'].str.startswith('TEST_ACTIVATION') | df_cost['NAME'].str.startswith('DQ_')) & (df_cost['START_TIME'] >= start_date)]
                df_filtered_test_activation = df_cost[df_cost['NAME'].str.startswith('TEST_ACTIVATION') & (df_cost['START_TIME'] >= start_date)]
                df_filtered_DQ = df_cost[df_cost['NAME'].str.startswith('DQ_') & (df_cost['START_TIME'] >= start_date)]



                # Convierte CREDITS_STR a float
                df_filtered_all['CREDITS_STR'] = df_filtered_all['CREDITS_STR'].astype(float)
                df_filtered_test_activation['CREDITS_STR'] = df_filtered_test_activation['CREDITS_STR'].astype(float)
                df_filtered_DQ['CREDITS_STR'] = df_filtered_DQ['CREDITS_STR'].astype(float)

                # Calcula la suma de CREDITS_USED y la cantidad de ejecuciones para cada conjunto de datos
                sum_all = df_filtered_all['CREDITS_STR'].sum()
                sum_test_activation = df_filtered_test_activation['CREDITS_STR'].sum()
                num_executions_DQ = len(df_filtered_DQ)
        with go_quota:

            # Espacio 
            st.write("")
            st.write("")

            label_quota = """Alerts and quotas 

🎚️"""
            if st.button(label_quota,use_container_width = True):
                st.session_state['current_page'] = "Quotas" 
        with st.container():    
            # Muestra las métricas en Streamlit
            col1, col2, col3 = st.columns(3, gap="small")  # Aquí está la corrección
            with col1:
                if sum_all:
                    st.metric(label="Total Credits Used", value=round(sum_all, 4))
            with col2:    
                st.metric(label="Maintenance Credits Used", value=round(sum_test_activation, 4))
            with col3: 
                st.metric(label="Number of DQ_ Executions", value=num_executions_DQ)

            # Paso 1: Agrega una nueva columna con solo la fecha a cada DataFrame
            df_filtered_DQ['DATE'] = df_filtered_DQ['START_TIME'].dt.date
            df_filtered_test_activation['DATE'] = df_filtered_test_activation['START_TIME'].dt.date
            df_filtered_DQ['HOUR'] = df_filtered_DQ['START_TIME'].dt.floor('H').dt.strftime('%I:%M')
            df_filtered_test_activation['HOUR'] = df_filtered_test_activation['START_TIME'].dt.floor('H').dt.strftime('%I:%M')

            # Paso 2: Agrupa cada DataFrame por fecha y nombre de tarea y calcula la suma de CREDITS_USED
            daily_sums_DQ = df_filtered_DQ.groupby(['DATE'])['CREDITS_STR'].sum().reset_index()
            daily_sums_DQ_check = df_filtered_DQ.groupby(['NAME'])['CREDITS_STR'].sum().reset_index()
            daily_sums_test_activation = df_filtered_test_activation.groupby(['DATE'])['CREDITS_STR'].sum().reset_index()
            hourly_sums_DQ = df_filtered_DQ.groupby(['HOUR'])['CREDITS_STR'].sum().reset_index()
            hourly_sums_test_activation = df_filtered_test_activation.groupby(['HOUR'])['CREDITS_STR'].sum().reset_index()
            # Paso 3: Combina los dos DataFrames agrupados en un solo DataFrame
            daily_sums = pd.merge(daily_sums_DQ, daily_sums_test_activation, on='DATE', how='outer', suffixes=('_DQ', '_test_activation'))
            hourly_sums = pd.merge(hourly_sums_DQ, hourly_sums_test_activation, on='HOUR', how='outer', suffixes=('_DQ', '_test_activation'))
            # Renombra las columnas de créditos usados
            daily_sums.rename(columns={'CREDITS_STR_DQ': 'Execution', 'CREDITS_STR_test_activation': 'Maintenance'}, inplace=True)
            hourly_sums.rename(columns={'CREDITS_STR_DQ': 'Execution', 'CREDITS_STR_test_activation': 'Maintenance'}, inplace=True)
            daily_sums_DQ_check.rename(columns={'CREDITS_STR': 'Credits used', 'NAME': 'Test name'}, inplace=True)

            # Ordenar el DataFrame por 'Credits used' de mayor a menor
            daily_sums_DQ_check.sort_values(by='Credits used', ascending=False, inplace=True)

            # Paso 4: Crea un gráfico de barras con barras separadas por color para cada categoría de tarea
            with st.container():
                # Rellena los valores NaN con 0
                daily_sums.fillna(0, inplace=True)
                hourly_sums.fillna(0, inplace=True)
                # Crear nuevas columnas con la fecha como varchar y con fecha y hora
                daily_sums['Date'] = daily_sums['DATE'].astype(str).str.replace('-', '/')


                daily_sums_M = daily_sums[['Maintenance', 'Date']].rename(columns={'Maintenance': 'Credits', 'Date': 'Date'})
                daily_sums_M['Type'] = 'Maintenance'
                daily_sums_E = daily_sums[['Execution', 'Date']].rename(columns={'Execution': 'Credits', 'Date': 'Date'})
                daily_sums_E['Type'] = 'Execution'

                hourly_sums_M = hourly_sums[['Maintenance', 'HOUR']].rename(columns={'Maintenance': 'Credits', 'HOUR': 'Hour'})
                hourly_sums_M['Type'] = 'Maintenance'
                hourly_sums_E = hourly_sums[['Execution', 'HOUR']].rename(columns={'Execution': 'Credits', 'HOUR': 'Hour'})
                hourly_sums_E['Type'] = 'Execution'

                daily_result = pd.concat([daily_sums_M, daily_sums_E], ignore_index=True)
                hourly_result = pd.concat([hourly_sums_M, hourly_sums_E], ignore_index=True)


                # Crea un gráfico de barras con Streamlit

                if date_range_option == 'Last 1 Day':

                    chart1 = alt.Chart(hourly_result).mark_bar().encode(
                        x='Hour:O',
                        y='Credits:Q',
                        color='Type:N'
                    )


                    st.altair_chart(chart1,use_container_width = True,theme = None)
                else:

                    chart2 = alt.Chart(daily_result).mark_bar().encode(
                        x='Date:O',
                        y='Credits:Q',
                        color='Type:N'
                    )


                    st.altair_chart(chart2,use_container_width = True,theme = None)
        






def observability(session):
    Menu(session)
    with st.container():
        st.markdown("# Observability 👁️")
        query_count = f"select count(*) as Conteo from core.complete_results"  
        total_executions1 = query_lista(session,query_count,"CONTEO")

        total_execution = total_executions1[0][0]
        if total_execution == "0":
            st.warning("No executions yet...")
        else:
            m1,m2,m3,m4 = st.columns(4,gap="small")
            with m1:
                st.metric(label="Number of executions", value=total_execution)
            with m2:
                query_correct = f"select count(*) as Conteo_correct from core.complete_results where test_result = TRUE"
                total_correct1 = query_lista(session,query_correct,"CONTEO_CORRECT")
                st.write("Success rate")
                total_correct = total_correct1[0][0]
                percentage_correct = int(100*round(int(total_correct)/int(total_execution),2))
                st.progress(percentage_correct, text=f"{percentage_correct}% tests passed")

            with m3:
                today = datetime.today().date()
                one_week_ago = today - timedelta(days=7)
                date_range = st.date_input("Date range",[one_week_ago,today])

            with m4:
                tag = st.selectbox("TAG",["All","GA4","BLUECONIC","SELLIGENT"])
                " Añadir TAG a todo "

            with st.container():
                query_obser = """
                    select 
                        name as TEST_NAME,
                        count(*) as TIMES_EXECUTED,
                        max(execution_time) as LAST_EXECUTION_TIME
                    from core.complete_results
                    group by name
                    order by max(execution_time)
                    """
                name1 = query_lista(session,query_obser,"TEST_NAME")
                names = name1[0]

                times_exec1 = query_lista(session,query_obser,"TIMES_EXECUTED")
                times_exec = times_exec1[0]

                last_exec1 = query_lista(session,query_obser,"LAST_EXECUTION_TIME")
                last_exec = last_exec1[0]
                space = '\hspace{20mm}'
                space_small = '\hspace{5mm}'

                label_header = "$\hspace{20mm} \large Name \hspace{20mm}|\hspace{20mm}\large Times~executed \hspace{20mm}|\hspace{20mm}\large  Last~date \hspace{5mm}-\hspace{5mm}\large time$"
                st.write(label_header)
                for i in range(len(names)):
                    datetime_obj = datetime.strptime(last_exec[i], '%Y-%m-%d %H:%M:%S.%f')
                    formatted_date_only = datetime_obj.strftime('%d/%m/%Y')
                    formatted_time_only = datetime_obj.strftime('%H:%M')
                    

                    label_expander = f"${space}\large {names[i].replace(' ','~')}{space}|{space}{space}\large {times_exec[i]} {space}{space}|{space} \large {formatted_date_only}{space_small}-{space_small}\large {formatted_time_only}$"





                    #label_expander = f"  {names[i]}      -|-     {times_exec[i]}     -|-      {last_exec[i]}"
                    with st.expander(label_expander): 

                        
                        with st.container():
                            met,graf = st.columns([1,4],gap="small")

                            with met:
                                query_correct_sub = f"select count(*) as Conteo_correct from core.complete_results where test_result = TRUE and name = '{names[i]}'"
                                query_fail_sub = f"select count(*) as Conteo_fail from core.complete_results where test_result = FALSE and name = '{names[i]}'"
                                total_correct1_sub = query_lista(session,query_correct_sub,"CONTEO_CORRECT")

                                total_fail1_sub = query_lista(session,query_fail_sub,"CONTEO_FAIL")

                                total_correct_sub = total_correct1_sub[0][0]
                                total_fail_sub = total_fail1_sub[0][0]
                                percentage_correct_sub = int(100*round(int(total_correct_sub)/int(times_exec[i]),2))
                                st.progress(percentage_correct_sub, text=f"{percentage_correct_sub}% test passed")

                                st.metric(label="Number of successes", value=total_correct_sub)
                                st.metric(label="Number of failures", value=total_fail_sub)
                                #Arreglar
                                st.metric(label="Number of errors", value=0)
                            with graf:
                                query_type = f"select test_type from core.test_name_type where test_name = '{names[i]}'"
                                type0 = query_lista(session,query_type,"TEST_TYPE")
                                test_type = type0[0][0]
                                query_results = f"select r.RESULT,r.EXECUTION_TIME,d.$4 as EXPECTED from core.DIRECT_RESULTS_{test_type} r join core.REFINED_TEST_{test_type} d where test_name = '{names[i]}'"

                                st.write('Test result vs expected')

                                df_results2 = query_lista(session,query_results)
                                df_results = df_results2[1]
                                st.dataframe(df_results)
                                df_results['DATE_TIME_STR'] = df_results['EXECUTION_TIME'].dt.strftime('%Y-%m-%d %H:%M')

                                # Crear la gráfica con Altair usando DATE_TIME_STR
                                d = alt.Chart(df_results).mark_circle().encode(
                                    x=alt.X("DATE_TIME_STR:O", title="Date and Time"),
                                    y=alt.Y(alt.repeat('layer')).title("Query results"),
                                    color=alt.ColorDatum(alt.repeat('layer'))
                                ).repeat(layer=["RESULT", "RESULT"])

                                st.altair_chart(d, use_container_width=True)

def settings_page(session):
    Menu(session)
    with st.container():
        st.markdown("# Settings ⚙️")
        power,Contactandinfo = st.columns(2,gap="medium")
        with power:

            st.markdown(" ## Power  🟥↔️🟢")
            st.write("Here you can swich on or off the app. Please now that if you switch off the app the maintenance task will stop and all your test will be deactivated and you will have to activate the ones that you want after once you power on the app again.")
            ## Calcular on o of
            result1 = query_lista(session,"select count(*) as SI from core.on_off","SI")

            on_of = None

            if result1[0][0] != '0':
                on_of = True
            emp1,bot,emp2 = st.columns(3,gap="small")
            with bot:
                
                if on_of is None:    

                    
                    if st.button("ON 🟢",use_container_width=True): 
                        try:
                            result_list  = execute_procedure(session,"CALL CORE.ACTIVATE('0/60 9-17 * * 1-5 Europe/Madrid')")
                            mensaje = list(result_list[0].values())[0]
                            result_liste  = execute_procedure(session,"CALL HQUALITY_AUX.SETTINGS.updateemail()")
                            mensajen = list(result_liste[0].values())[0]
                            if mensaje == 'Application initialised, enjoy!' and mensajen == "Email list updated":
                                st.write("Done, reload to verify it here ↙️ ") 
                            else: 
                                st.write('Make sure you executed all the commands detailed in the information tab and retry')
                        except Exception as e:
                            st.write(f"Please read the info and grant the necessary permision for the app, you can go there by clicling the i button top right: {e}")
                    st.error("Current state")

                else:                    

                    
                    if st.button("OFF 🟥",use_container_width=True):
                        try:
                            result_list  = execute_procedure(session,"CALL CORE.SHUTDOWN()")
                            mensaje = list(result_list[0].values())[0]
                            session.sql("UPDATE REFINED_TEST_BAD_ROWS SET IS_ACTIVE = 'NO'").collect()
                            session.sql("UPDATE REFINED_TEST_COUNTS_SUMS SET IS_ACTIVE = 'NO'").collect()
                            session.sql("UPDATE REFINED_TEST_CUSTOM_RESULTS SET IS_ACTIVE = 'NO'").collect()
                            result_listo  = execute_procedure(session,"CALL CORE.TRIGUERS_DELETION('HQUALITY','CORE')")
                            mensajeo = list(result_listo[0].values())[0]
                            if mensaje == 'Application off':
                                st.write("Done , reload to verify it here ↙️")  

                        except Exception as e:
                            st.write(f"Please read the info and grant the necessary permision for the app, you can go there by clicling the i button top right: {e}")
                    st.success("Current state")


            with st.container():
                st.write("")
                st.write("")
                st.markdown("## Email update ✉️")
                st.write("The list of avaible emails is automatically retrived from Snowflake once you install the app, you can press the button below to update the list with the current users if you don't see you email")
                emp1,bot,emp2 = st.columns(3,gap="small")
                with bot:
                    if st.button('🔄',use_container_width=True):
                        try:
                            result_liste  = execute_procedure(session,"CALL HQUALITY_AUX.SETTINGS.updateemail()")
                            mensajen = list(result_liste[0].values())[0]
                            if mensajen == "Email list updated":
                                st.success('Email list updated 👍')
                            else:
                                st.warning(f"{mensajen}")

                        except Exception as e:
                            st.error(f" {e}")
        with Contactandinfo:

            st.markdown("## Maintenance schedule 🕟")

            period1 = query_lista(session,"select cron from hquality.core.maintenance_period where last_update = (select max(last_update) from hquality.core.maintenance_period)  ","CRON")
            period = str(period1[0][0])
            partes = period.split(" ")

            # Extraer el periodo (minutos)
            periodo_aux = partes[0].split("/")
            periodo  = int(periodo_aux[1])
            # Extraer la hora de comienzo y la hora de fin
            hora_comienzo, hora_fin = map(int, partes[1].split("-"))

            # Extraer los días de la semana
            dias_semana = partes[4]

            # Extraer la zona horaria
            timezone123 = partes[5]

            day,timezone = st.columns(2,gap="small")
            with day:
                if dias_semana == "1-5":
                    default = 1
                elif dias_semana == "*":
                    default = 0
                day_option = st.selectbox("Choose which days you want the maintenance to be run :",["All the week", "From Monday to Friday"],index = default)
            with timezone:
                timezoneo = st.selectbox("Select a Time Zone :",TimeZones,help="Select the time zone you're working in",index = 315)
            appointment = st.slider("Select the active hours:",value=(time(int(hora_comienzo), 00), time(int(hora_fin), 00)),step = timedelta(hours=1),help = "This doesn't mean that no test will be executed out of this interval, it only means that the maintenance task that create the trigguers for new test and delete the trigguers associated with deleted test does not run out of the interval.")
            
            hora_inicio = appointment[0].strftime('%H:%M')
            hora_final = appointment[1].strftime('%H:%M')

            # Extraer la hora y los minutos de hora_inicio
            inicio_hora_int = int(hora_inicio.split(":")[0])
            inicio_minuto_int = int(hora_inicio.split(":")[1])

            # Extraer la hora y los minutos de hora_final
            final_hora_int = int(hora_final.split(":")[0])
            final_minuto_int = int(hora_final.split(":")[1])

            st.write(f"Maintenance will be done from {hora_inicio} to {hora_final}")

            periodu = st.slider("Period of maintenance execution in minutes",5,120,periodo,5)
            st.write(f"The maintenance task will be executed every {str(periodu)} minutes")

            if day_option == "All the week":
                week  = "*"
            elif day_option == "From Monday to Friday":
                week = "1-5"
            
            cron  = f"{inicio_minuto_int}/{periodu} {inicio_hora_int}-{final_hora_int} * * {week} {timezoneo}"
            emp2,bot,emp1 = st.columns(3,gap="small")
            with bot:
                if st.button("Apply changes ✳️",use_container_width=True):



                    try:
                        session.sql(f"INSERT INTO HQUALITY.CORE.MAINTENANCE_PERIOD (cron) values ('{cron}')").collect()
                        periodi1 = query_lista(session,"select cron from hquality.core.maintenance_period where last_update = (select max(last_update) from hquality.core.maintenance_period) ","CRON")
                        croni = str(periodi1[0][0])
                        session.sql(f"CALL HQUALITY.CORE.ACTIVATE('{croni}')").collect()
                        st.success(f"Succesfully change the maintenance schedule !")
                    except Exception as e:
                        st.error(f"An error occurred: {e}")  

def quota(session):
    Menu(session)
    with st.container():
        st.markdown("# Costs alerts and quotas 🎚️")
        


# Inicializa la página actual a "Página principal" si no se ha inicializado aún
if 'current_page' not in st.session_state:
    st.session_state['current_page'] = "Página principal"

# Esto determina qué página mostrar basándose en el estado de la sesión
if st.session_state['current_page'] == "Página principal":
    main_page()
elif st.session_state['current_page'] == "Quotas":
    quota(session)
elif st.session_state['current_page'] == "Eliminación y edición de tests":
    delete_edit_page()
elif st.session_state['current_page'] == "Documentación":
    docs_page()
elif st.session_state['current_page'] == "Costs":
    costs_page()
elif st.session_state['current_page'] == "Observability":
    observability(session)
elif st.session_state['current_page'] == "Settings":
    settings_page(session)
