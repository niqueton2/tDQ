import streamlit as st
import pandas as pd
from modules import get_columns,get_databases,get_schema,get_tables,TimeZones,cron_builder,execute_procedure,query_lista,procedures_dict
import time as time_module




def test_parameters(session,test_choice):
    
    ParametersOne,ParametersTwo = st.columns(2,gap="medium")
    parametersArray=[]

    if test_choice == "Custom query":
        with ParametersOne:
            test_name = st.text_input('Test name :',help='Mandatory')
            test_comment = st.text_input('Comment :',help='Optional')
            test_type = st.selectbox('Test type :',['BAD_ROWS','COUNTS_SUMS','CUSTOM_RESULTS'],help="Take a look at the documentation to understand better what this type means")

        with ParametersTwo:
            test_query = st.text_area('Test query :',help='Mandatory')
            if test_type == 'BAD_ROWS':
                expected = st.number_input('Max error rows accepted :',0)
            elif test_type == 'COUNTS_SUMS':
                min = st.number_input('Lower limit acceptable :',0)
                max = ''
                if st.checkbox('Detail upper limit'):
                    max = st.number_input('Upper limit accetable',1)
                expected = str(min)+"-"+str(max)
            else:
                expected = st.text_input('Expected result')
            parametersArray=[test_choice,test_type,test_name,test_query,expected,test_comment]

    else:
        with ParametersOne:
            test_name = st.text_input('Test name :',help='Mandatory')
            test_comment = st.text_input('Comment :',help='Optional')
            #bd = st.selectbox(label='Database :',options = ['1','2','3'])
            bd = st.selectbox(label='Database :',options = get_databases(session))
            
            if test_choice != "Uniqueness":
                #schema = st.selectbox(label='Schema :',options=['1','2','3'])
                schema = st.selectbox(label='Schema :',options=get_schema(session,bd))
               
                if test_choice not in ["Nulls","Volumetry" ]:
                    #tabla = st.selectbox('Table :',['1','2','3'],help='Choose the table in wich you want to implement the test')
                    tabla = st.selectbox('Table :',get_tables(session,bd,schema),help='Choose the table in wich you want to implement the test')
                    table_name = str(bd)+"."+str(schema)+"."+str(tabla)

        with ParametersTwo:
            if test_choice == "Nulls":
                #tabla = st.selectbox('Table :',['1','2','3'],help='Choose the table in wich you want to implement the test')
                tabla = st.selectbox('Table :',get_tables(session,bd,schema),help='Choose the table in wich you want to implement the test')
                table_name = str(bd)+"."+str(schema)+"."+str(tabla)
                #columna = st.selectbox('Column :',['1','2','3'],help='Choose the column to search for null values')
                columna = st.selectbox('Column :',get_columns(session,bd,schema,tabla),help='Choose the column to search for null values')
                if st.radio('Type of expected value :',['Direct row count','Percentage'],help='You can put an upper limit to the rows that can be null without the test failing') == 'Direct row count':
                    max_error_count = st.number_input('Max null count acceptable :',0,value=0)
                    max_percentage = ""
                    test_type = 'BAD_ROWS'
                else:
                    max_percentage = st.number_input('Max percentage acceptable :',0,100,help='%',value=0)
                    max_error_count = ""
                    test_type = 'COUNTS_SUMS'




                # Almacenar la variable test_query del resultado (asumiendo que es la primera columna del resultado)

                parametersArray = [test_choice,table_name,test_name,columna,max_error_count,max_percentage,test_comment,test_type]


            elif test_choice == "Uniqueness":

                #schema = st.selectbox(label='Schema',options=['1','2','3'],help='Choose the schema wich contains the table you want to test')
                schema = st.selectbox(label='Schema',options=get_schema(session,bd),help='Choose the schema wich contains the table you want to test')
                #tabla = st.selectbox('Table',['1','2','3'],help='Choose the table in wich you want to implement the test')
                tabla = st.selectbox('Table',get_tables(session,bd,schema),help='Choose the table in wich you want to implement the test')
                #columnas = st.multiselect('Columns :',['1','2','3'],help='Choose the columns that ypu want to verify as PK, if you choose more than 1 the verification will be tested against the concatenation of them')
                columnas = st.multiselect('Columns :',get_columns(session,bd,schema,tabla),help='Choose the columns that ypu want to verify as PK, if you choose more than 1 the verification will be tested against the concatenation of them')


                table_name = str(bd)+"."+str(schema)+"."+str(tabla)
                test_type = 'CUSTOM_RESULTS'

                
                parametersArray = [test_choice,table_name,test_name,columnas,test_comment,test_type]


            elif test_choice == "Volumetry":

                #tabla = st.selectbox('Table :',['1','2','3'],help='Choose the table in wich you want to implement the test')
                tabla = st.selectbox('Table :',get_tables(session,bd,schema),help='Choose the table in wich you want to implement the test')
                table_name = str(bd)+"."+str(schema)+"."+str(tabla)
                condition = st.text_area('Condition',help='Filter to apply before VolumetryCount, example: columnX < 3 (something that you can puto in a where clause)')
                min = st.number_input('Lower limit acceptable count :',0)
                max = ''
                if st.checkbox('Detail upper limit'):
                    max = st.number_input('Upper limit accetable count',1)
                expected = str(min)+"-"+str(max)
                test_type = 'COUNTS_SUMS'
                
        
                parametersArray=[test_choice,table_name,test_name,expected,condition,test_comment,test_type]

            else:
                'Muerte psicológica amigo, desarrolla esta opción'
                
    # with st.container():

    #     if test_choice == "Custom query":


    #         if st.button("Try test now ▶️"):
    #             df2 = query_lista(session,test_query)

    #             st.dataframe(df2[1])
            

    #     else:

    #         result_list = execute_procedure(session, call_procedure_query)

    #         procedure_key = procedures_dict.get(test_choice)
            
    #         if st.button("Try test now ▶️"):
    #             if procedure_key:
    #                 # Usa la clave para obtener el valor correcto de result_list[0]
    #                 query_sql = result_list[0].get(procedure_key)

    #                 if test_choice=="Nulls" and max_error_count != "":
    #                     query_sql = f"SELECT COUNT(*) as NullCount FROM ({query_sql}) t "

    #                 if query_sql:
    #                     # Pasar la consulta SQL a query_lista
    #                     df2 = query_lista(session, query_sql,is_trytest=True)

    #                     st.dataframe(df2[1])
    #                 else:
    #                     st.write(f"No se encontró la consulta SQL para la clave: {procedure_key}")
    #             else:
    #                 st.write(f"La opción de elección de test '{test_choice}' no esta funcionando")                 


    
    return parametersArray

def Menu(session):
        
    

    with st.container():
        
        c1,c2,c3,c4,c5,c6 = st.columns(6,gap="small")

        pages_dict = { 
        "Página principal": ["New check ","✅","main_page()",c1],
        "Observability": ["Observability ","👁️","obserbavility()",c3],
        "Eliminación y edición de tests": ["Manage tests"," 🧰","delete_edit_page()",c2],
        "Costs": ["Cost control"," 💵","costs_page()",c4],
        "Documentación": ["Documentation" ,"📖","docs_page()",c6],
        "Settings":["Settings ","⚙️","settings_page()",c5]
        }

        for key in pages_dict:
            labele = f"""
{pages_dict[key][0]} 

{pages_dict[key][1]}"""
            with pages_dict[key][3]:
                
                if st.button(labele,use_container_width = True):
                        st.session_state['current_page'] = key
                if st.session_state['current_page'] == str(key):
                    st.divider()

        st.divider()

        ## Calcular on o of
        #result1 = query_lista(session,"select count(*) as SI from core.on_off","SI")
#
        #on_of = None
#
        #if result1[0][0] != '0':
        #    on_of = True
#
        #if on_of is None:        
        #    if st.button("Power On",use_container_width = True): 
        #        try:
        #            result_list  = execute_procedure(session,"CALL CORE.ACTIVATE('60')")
        #            mensaje = list(result_list[0].values())[0]
        #            if mensaje == 'Application initialised, enjoy!':
        #                st.write("Done 🟢 Reload to verify it here ↙️ ") 
#
        #        except Exception as e:
        #            st.write(f"Please read the info and grant the necessary permision for the app, you can go there by clicling the i button top right: {e}")
        #    st.error("OFF")
        #  
        #else:                    
#
        #    if st.button("Power Off",use_container_width = True): 
        #        try:
        #            result_list  = execute_procedure(session,"CALL CORE.SHUTDOWN()")
        #            mensaje = list(result_list[0].values())[0]
        #            if mensaje == 'Application off':
        #                st.write("Done 🟥 Reload to verify it here ↙️")  
#
        #        except Exception as e:
        #            st.write(f"Please read the info and grant the necessary permision for the app, you can go there by clicling the i button top right: {e}")
        #    st.success("ON")



def try_test_button(session,call):

    if st.button('Try test now ▶️'):
        
        try:
            result = execute_procedure(session,call)
            df_1 = query_lista(session,result)
            st.dataframe(data=df_1[1],hide_index=True)
        except Exception as e:

            st.write(f"{e}")
        


def enter_email(session,req='',add=''):

    df_emails = session.sql("SELECT distinct email as Emails from core.list_emails")
    df_emails_pd = df_emails.to_pandas()
    list_emails = df_emails_pd['EMAILS'].tolist()

    alert_email_array = st.multiselect('Alert Email :', list_emails,help='Read expander before set it for the first time, btw you can insert more than one email separated by comma.')
    alert_emails_procedure = ",".join(list_emails)
    alert_email = ','.join(alert_email_array)

    with st.expander('Requirements email 🔽'):
            st.markdown( 
                """
                You will only have to do this if is the first time you want to implement an alert, from now on you will see your mail in the selectbox and all will be already configurated

                - You need to use the email that appears in your snowflake profile, you can take a look at it in Snowsight

                - You will need to verify it by clicking the button in the same tab

                - Finally go to the settings tab of the app and add your email
            """
            )
            email = st.text_input("Then, enter the email here and click the button below")
            if email:
                session.sql(f"INSERT INTO hquality.core.list_emails select '{email}'").collect()
            
            boton1 , boton2 = st.columns(2,gap="small")

            with boton1:
                if st.button("SEND TEST EMAIL"):
                    try:
                        try:
                            session.sql(f"INSERT INTO hquality.core.list_emails select '{email}'").collect()
                        except Exception as e:
                            st.write(f"El insert del error --> {e}")

                        llamada = f"CALL HQUALITY_AUX.SETTINGS.addemail('{alert_emails_procedure}')"
                        log = execute_procedure(session,llamada)
                        session.sql(f"CALL SYSTEM$SEND_EMAIL('EMAILS','{email}','HQuality test email','Come back to the app and mark received in the checkbox 👍');").collect()
                        st.write('✅ All seems good, it can take a minute')
                        session.sql(f"DELETE FROM core.list_emails where email = '{email}'").collect() 
                    except Exception as e:
                        st.write(f"Excepción al final {e}")

            with boton2:
                st.write('Check your mailbox, if you did all well you will have to receive a mail in the next minutes')
                received = ''
                if st.checkbox('I received it'):
                    try:
                        session.sql(f"INSERT INTO core.list_emails select '{email}'").collect()
                        st.success("Email added to the list, reload to see it")

                    except Exception as e:

                        st.write(f"{e}")

                    


                
    return alert_email


def implement(session,parametersArray,cron,alert_email,mode_try = "NO"):
    if parametersArray[0] == 'Custom query':
         call_procedure_query = f"""
         CALL core.general_test(
             '{parametersArray[1]}',
             '{parametersArray[2]}',
             $${parametersArray[3]}$$,
             '{parametersArray[4]}',  
             '{parametersArray[5]}',
             '{cron}',
             '{alert_email}',
             '',
             '{mode_try}'
         )
         """
    elif parametersArray[0] == 'Nulls':
        call_procedure_query = f"""
        CALL core.predefined_null_test(
            '{parametersArray[1]}',
            '{parametersArray[3]}',
            '{parametersArray[2]}',
            '{parametersArray[4]}',  
            '{parametersArray[5]}',
            '{parametersArray[6]}',
            '{cron}',
            '{alert_email}',
            '',
            '{mode_try}'
        )
        """
    elif parametersArray[0] == 'Uniqueness':
        call_procedure_query = f"""
        CALL core.predefined_uniqueness_test(
            '{parametersArray[1]}',
            '{parametersArray[2]}',
            {parametersArray[3]},
            '{parametersArray[4]}',  
            '{cron}',
            '{alert_email}',
            '',
            '{mode_try}'
        )
        """


    elif parametersArray[0] == 'Volumetry':
        call_procedure_query = f"""
        CALL core.predefined_volumetry_test(
            '{parametersArray[1]}',
            '{parametersArray[2]}',
            '{parametersArray[3]}',
            $${parametersArray[4]}$$,  
            '{parametersArray[5]}',
            '{cron}',
            '{alert_email}',
            '',
            '{mode_try}'
        )
        """
        
    else:
        call_procedure_query = f"""
            Select 'Joder amigo'
        """
    # Ejecutar la consult   
    try:
        result_list = execute_procedure(session, call_procedure_query)   
        # Extraer el mensaje del resultado (asumiendo que es el primer elemento de la lista y tiene una clave 'message')
        if result_list and isinstance(result_list, list) and 'message' in result_list[0]:
            message = result_list[0]['message']
        else:
            procedure_key = procedures_dict.get(parametersArray[0])
            message = result_list[0].get(procedure_key)   
        if message == "The test name already exists. Please use another name.":
            if mode_try == 'NO':
                st.error(f'{message}')
            else:
                st.warning(f"{message} (The results that you see below correspond to the already existing test)")
        else:
            if mode_try == 'NO':
                #st.snow() # Fumadon que no se ve la imagen
                # Crear una barra de progreso          
                progress_bar = st.progress(0)
                start_time = time_module.time()
                for i in range(100):
                    while time_module.time() - start_time < i * 0.01:  # Ajusta este valor para cambiar la duración total
                        pass
                    progress_bar.progress(i + 1)
                #st.toast('Test implemented !', icon='✅') Version su puta madre
                    if i == 80:
                        st.success(f'{message}')

    except Exception as e:
        st.write(f"An error occurred: {e}  call_procedure_query = {call_procedure_query}")   