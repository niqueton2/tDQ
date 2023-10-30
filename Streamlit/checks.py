# Importaciones necesarias
import streamlit as st



from test import test_parameters
import snowflake.connector
import pandas as pd
from st_pages import Page, show_pages, add_page_title
#from modules import conectar_a_snowflake,obtener_bases_de_datos,obtener_esquemas,cargar_credenciales,obtener_test_names,search_test_type,obtener_time_zone

import json

# Función para cargar credenciales
def cargar_credenciales(archivo_json):
    with open(archivo_json, "r") as file:
        credenciales = json.load(file)
    return credenciales


# Función para conectar a Snowflake
def conectar_a_snowflake(usuario,contrasenia, cuenta, warehouse, database, esquema):
    conn = snowflake.connector.connect(
        user=usuario,
        password=contrasenia,
        account=cuenta,
        warehouse=warehouse,
        database=database,
        schema=esquema
    )
    return conn

# # Función para obtener las bases de datos disponibles
#def obtener_bases_de_datos(conn):
#    cur = conn.cursor()
#    cur.execute("SHOW DATABASES")
#    return [row[1] for row in cur.fetchall()]
#
# # Función para obtener los esquemas de una base de datos
#def obtener_esquemas(conn, base_de_datos):
#    cur = conn.cursor()
#    cur.execute(f"SHOW SCHEMAS IN {base_de_datos}")
#    return [row[1] for row in cur.fetchall()]
#
## Función para obtener los esquemas de una base de datos
#def obtener_tablas(conn, base_de_datos,esquema):
#    cur = conn.cursor()
#    cur.execute(f"SHOW TABLES IN {base_de_datos}.{esquema}")
#    return [row[1] for row in cur.fetchall()]
#
#def obtener_columnas(conn, base_de_datos,esquema,table):
#    cur = conn.cursor()
#    cur.execute(f"DESC TABLE {base_de_datos}.{esquema}.{table}")
#    return [row[0] for row in cur.fetchall()]
#

# Función para obtener las bases de datos disponibles
def obtener_bases_de_datos(conn):
    cur = conn.cursor()
    cur.execute("SHOW DATABASES")
    resultado = [row[1] for row in cur.fetchall()]
    return resultado if resultado else ['No databases found']

# Función para obtener los esquemas de una base de datos
def obtener_esquemas(conn, base_de_datos):
    cur = conn.cursor()
    cur.execute(f"SHOW SCHEMAS IN {base_de_datos}")
    resultado = [row[1] for row in cur.fetchall()]
    return resultado if resultado else ['No schema found']

# Función para obtener las tablas de un esquema
def obtener_tablas(conn, base_de_datos, esquema):
    cur = conn.cursor()
    cur.execute(f"SHOW TABLES IN {base_de_datos}.{esquema}")
    resultado = [row[1] for row in cur.fetchall()]
    return resultado if resultado else ['No tables found']

# Función para obtener las columnas de una tabla
def obtener_columnas(conn, base_de_datos, esquema, table):
    cur = conn.cursor()
    if table == "No tables found":
        return ['Select table first']
    cur.execute(f"DESC TABLE {base_de_datos}.{esquema}.{table}")
    resultado = [row[0] for row in cur.fetchall()]
    return resultado if resultado else ['No columns found']

def obtener_test_names(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM  QUALITY.INTERNAL.TEST_NAMES")
    resultado = [row[0] for row in cur.fetchall()]
    
    return resultado if resultado else ['No tests founds']

def search_test_type(conn,test_name):
    cur = conn.cursor()
    cur.execute(f"SELECT type FROM  QUALITY.INTERNAL.TEST_NAMES where test_name='{test_name}'")
    resultado = [row[0] for row in cur.fetchall()]
    return resultado if resultado else ['No tests founds']

def obtener_time_zone(conn):
    cur = conn.cursor()
    cur.execute("SELECT distinct Identifier FROM  QUALITY.INTERNAL.TimeZone")
    resultado = [row[0] for row in cur.fetchall()]
    return resultado
## Cargar credenciales
credenciales = cargar_credenciales("Streamlit/credentials.json")
#
## Conexion a Snowflake
conn = conectar_a_snowflake(credenciales["user"],credenciales["password"], credenciales["account"], credenciales["warehouse"], credenciales["database"], credenciales["schema"])
#
#with open('Streamlit/style.css') as f:
#    st.markdown(f'<style>{f.read()}</style>',unsafe_allow_html=True)
#

def test_parameters(conn,test_choice):
    
    ParametersOne,ParametersTwo = st.columns(2,gap="medium")
    parametersArray=[]

    if test_choice == "Custom query":
        with ParametersOne:
            test_name = st.text_input('Test name :',help='Mandatory')
            test_comment = st.text_input('Comment :',help='Optional')
            test_type = st.selectbox('Test type :',['BAD_ROWS','COUNTS_SUMS','CUSTOM_RESULTS'])

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
            st.session_state['parametersArray'] = parametersArray
    else:
        with ParametersOne:
            test_name = st.text_input('Test name :',help='Mandatory')
            test_comment = st.text_input('Comment :',help='Optional')
            bd = st.selectbox(label='Database :',options=obtener_bases_de_datos(conn),help='Choose the database wich contains the table you want to test')
            
            if test_choice != "Uniqueness":
                schema = st.selectbox(label='Schema :',options=obtener_esquemas(conn,bd),help='Choose the schema wich contains the table you want to test')
               
                if test_choice not in ["Nulls","Volumetry" ]:
                    tabla = st.selectbox('Table :',obtener_tablas(conn,bd,schema),help='Choose the table in wich you want to implement the test')
                    table_name = str(bd)+"."+str(schema)+"."+str(tabla)

        with ParametersTwo:
            if test_choice == "Nulls":
                tabla = st.selectbox('Table :',obtener_tablas(conn,bd,schema),help='Choose the table in wich you want to implement the test')
                table_name = str(bd)+"."+str(schema)+"."+str(tabla)
                columna = st.selectbox('Column :',obtener_columnas(conn,bd,schema,tabla),help='Choose the column to search for null values')
                if st.radio('Type of expected value :',['Direct row count','Percentage'],help='You can put an upper limit to the rows that can be null without the test failing') == 'Direct row count':
                    max_error_count = st.number_input('Max null count acceptable :',0)
                    max_percentage = ""
                    test_type = 'BAD_ROWS'
                else:
                    max_percentage = st.number_input('Max percentage acceptable :',0,100,help='%')
                    max_error_count = ""
                    test_type = 'COUNTS_SUMS'
                call_procedure_query = f"""
                    CALL QUALITY.internal.predefined_null_test(
                        '{table_name}',
                        '{columna}',
                        '{test_name}',
                        '',  -- Aquí puedes poner el cron_schedule si lo tienes, si no, déjalo como una cadena vacía
                        '{max_error_count}',
                        '{max_percentage}',
                        '',
                        '',
                        '',
                        'TRUE'
                    )
                    """
                        # Ejecutar la consulta
                cur = conn.cursor()
                cur.execute(call_procedure_query)

                # Obtener el resultado
                resultado = cur.fetchall()

                # Almacenar la variable test_query del resultado (asumiendo que es la primera columna del resultado)
                if resultado:
                    test_query = resultado[0][0]
                else:
                    test_query = "Select 'Culpa de Nico el Mamahuevos q no funcione' as SITUACION"
                parametersArray = [test_choice,table_name,columna,test_name,max_error_count,max_percentage,test_comment]
                st.session_state['parametersArray'] = parametersArray

            elif test_choice == "Uniqueness":

                schema = st.selectbox(label='Schema',options=obtener_esquemas(conn,bd),help='Choose the schema wich contains the table you want to test')
                tabla = st.selectbox('Table',obtener_tablas(conn,bd,schema),help='Choose the table in wich you want to implement the test')
                columnas = st.multiselect('Columns :',obtener_columnas(conn,bd,schema,tabla),help='Choose the columns that ypu want to verify as PK, if you choose more than 1 the verification will be tested against the concatenation of them')

                table_name = str(bd)+"."+str(schema)+"."+str(tabla)
                test_type = 'CUSTOM_RESULTS'
                call_procedure_query = f"""
                    CALL QUALITY.internal.predefined_uniqueness_test(
                        '{table_name}',
                        {columnas},
                        '{test_name}',
                        '',  
                        '',
                        '',
                        '',
                        'TRUE'
                    )
                    """
                # Ejecutar la consulta
                cur = conn.cursor()
                cur.execute(call_procedure_query)

                # Obtener el resultado
                resultado = cur.fetchall()

                # Almacenar la variable test_query del resultado (asumiendo que es la primera columna del resultado)
                if resultado:
                    test_query = resultado[0][0]
                else:
                    test_query = "Select 'Culpa de Nico el Mamahuevos q no funcione' as SITUACION"
                parametersArray = [test_choice,table_name,columnas,test_name,test_comment]
                st.session_state['parametersArray'] = parametersArray

            elif test_choice == "Volumetry":

                tabla = st.selectbox('Table :',obtener_tablas(conn,bd,schema),help='Choose the table in wich you want to implement the test')
                table_name = str(bd)+"."+str(schema)+"."+str(tabla)
                condition = st.text_area('Condition',help='Filter to apply before VolumetryCount, example: columnX < 3 (something that you can puto in a where clause)')
                min = st.number_input('Lower limit acceptable count :',0)
                max = ''
                if st.checkbox('Detail upper limit'):
                    max = st.number_input('Upper limit accetable count',1)
                expected = str(min)+"-"+str(max)
                test_type = 'COUNTS_SUMS'
                call_procedure_query = f"""
                    CALL QUALITY.internal.predefined_volumetry_test(
                        '{table_name}',
                        '{test_name}',
                        '{expected}',
                        $${condition}$$,  
                        '',
                        '',
                        '',
                        '',
                        'TRUE'
                    )
                    """
                # Ejecutar la consulta
                cur = conn.cursor()
                cur.execute(call_procedure_query)

                # Obtener el resultado
                resultado = cur.fetchall()

                # Almacenar la variable test_query del resultado (asumiendo que es la primera columna del resultado)
                if resultado:
                    test_query = resultado[0][0]

                else:
                    test_query = "Select 'Culpa de Nico el Mamahuevos q no funcione' as SITUACION"
                parametersArray=[test_choice,table_name,test_name,expected,condition,test_comment]
                st.session_state['parametersArray'] = parametersArray
            else:
                'Muerte psicológica amigo, desarrolla esta opción'
                
    with st.container():

        if st.button("Try test now", key='Botton'):
            conn = conectar_a_snowflake(credenciales["user"], credenciales["password"], credenciales["account"], credenciales["warehouse"], credenciales["database"], credenciales["schema"])
            cur = conn.cursor()

            if not test_query.strip():
                st.write("Empty query, please write something")
                return

            try:
                if test_type == 'BAD_ROWS':
                    cur.execute(f'SELECT COUNT(*) as Number_of_bad_rows FROM ({test_query}) t')
                else:
                    cur.execute(f'{test_query}')

                resultado = cur.fetchall()
                columnas = [col[0] for col in cur.description]
                st.table(pd.DataFrame(resultado, columns=columnas))

            except Exception as e:
                st.write(f"An error occurred: {e}")
                # Aquí podrías querer cerrar la conexión y el cursor para limpiar
                cur.close()
                conn.close()

    
    return parametersArray
    
def main_page():

    MainCol,chatCol = st.columns([3.3,1],gap="small")

    with MainCol:
        
        with st.container():
            st.markdown(" # Create a new check ")
            test_choice = st.selectbox("Choose test type :", ("Custom query", "Uniqueness", "Nulls","Volumetry"))

        with st.container():

            parametersArray = test_parameters(conn,test_choice)

            if 'parametersArray' not in st.session_state:
                st.session_state['parametersArray'] = parametersArray

            if st.button('Continue to schedule and alert settings'):
                st.experimental_set_query_params(page="Configuración de programación y alertas")
                st.experimental_rerun()
    with chatCol:
        'Anything else?'

        if st.button("Delete a test"):
            st.experimental_set_query_params(page="Eliminación de tests")
            st.experimental_rerun()
        if st.button("Help by chat"):
            st.experimental_set_query_params(page="chatBox")
            st.experimental_rerun()
def chat_page():
    MainCol, chatCol = st.columns([3.3, 1], gap="small")  # Aquí está la corrección
    with MainCol:
        st.markdown(' # Chat Support')
        chat_message = st.text_area('Any question about the APP or Data Quality in general ? :')
        if chat_message:
            'Tampoco lo tengo desarrollado todo, no flipemos'
    with chatCol:
        if st.button("New check"):
            st.experimental_set_query_params(page="Página principal")
            st.experimental_rerun()
        if st.button("Delete check"):
            st.experimental_set_query_params(page="Eliminación de tests")
            st.experimental_rerun()

def deletion_page(conn):


    MainCol, chatCol = st.columns([3.3, 1], gap="small")  # Aquí está la corrección
    with MainCol:
        st.markdown(' # Delete check')
        test_name_deletion = st.selectbox('Search test name:', obtener_test_names(conn))
        conn = conectar_a_snowflake(credenciales["user"], credenciales["password"], credenciales["account"], credenciales["warehouse"], credenciales["database"], credenciales["schema"])
        cur = conn.cursor()
        if test_name_deletion != 'No tests founds':
            try:
                testType = str(search_test_type(conn,test_name_deletion)).replace("['", '').replace("']", '')           
                cur.execute(f"SELECT *  FROM QUALITY.INTERNAL.REFINED_TEST_{testType} where test_name = '{test_name_deletion}'")
                resultado = cur.fetchall()
                columnas = [col[0] for col in cur.description]
                st.table(pd.DataFrame(resultado, columns=columnas))
            except Exception as e:
                st.write(f"An error occurred: {e}")           
                cur.close()
                conn.close()
        if st.button('Delete test'):
            try:
                cur.execute(f"DELETE FROM QUALITY.INTERNAL.REFINED_TEST_{testType} where test_name = '{test_name_deletion}'")
                st.success('Test deleted') 
            except Exception as e:
                st.write(f"An error occurred: {e}")   
  
    with chatCol:
        if st.button("New check"):
            st.experimental_set_query_params(page="Página principal")
            st.experimental_rerun()
        if st.button("Help by chat"):
            st.experimental_set_query_params(page="chatBox")
            st.experimental_rerun()

def schedule_and_alerts_page(conn):
    
    Schedule,AlertSettings = st.columns([0.5,0.5],gap="large")
    if 'parametersArray' in st.session_state:
        parametersArray = st.session_state['parametersArray']

    with Schedule:
        st.markdown('##  Schedule')
        def cron_builder(dias, horas, minutos):
        # Traduce los días de la semana a su representación en CRON
            dias_cron = {
                "All": "*",
                "Monday": "1",
                "Tuesday": "2",
                "Wednesday": "3",
                "Thursday": "4",
                "Friday": "5",
                "Saturday": "6",
                "Sunday": "7"
            }

            dias_seleccionados = [dias_cron[dia] for dia in dias]

            # Construye la expresión CRON
            cron_expresion = f"{','.join(map(str, minutos))} {','.join(map(str, horas))} * * {','.join(dias_seleccionados)}"

            return cron_expresion

        # Selección de timeZone
        lista_minutos = ['00','01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59']
        lista_horas = ['00','01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22','23']
        timeZone = st.selectbox("Select a Time Zone :",obtener_time_zone(conn),index=315)
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
        time = ""
        for i in range(len(horas)):
            for j in range(len(minutos)):
                if (i,j) == (0,0):
                    time += "at "+horas[i]+":"+minutos[j]
                else:
                    time += ", "+ horas[i]+":"+minutos[j]

        if dias == ['All']:
            st.write(f"Your test will be executed everyday {time} in {timeZone} time.")
        else:
            st.write(f"Your test will be executed {diasPLUS} {time} in {timeZone} time.")
        

    with AlertSettings:
        st.markdown('## Alert settings')
        alert_email = st.text_input('Alert Email :', '',help='Read expander before set it for the first time, btw you can insert more than one email separated by comma.')
        with st.expander('Requirements email'):
            st.markdown(
                """
                - You need to use the email that appears in your snowflake profile, you can take a look at it in Snowsight

                - You will need to verify it by clicking the button in the same tab

                - For now you will need to talk to your administrator and he will ad your email to the notification_integration ("Emails" in this case)
            """
            )
        alert_message = st.text_area('Alert Message :', '',placeholder='Default info',help="If not completly neccesary leave it as default, it will send an email with all the information about the failure and test ")

    #    # Regresar a la página principal
    if st.button('Back to basic test settings'):
        st.experimental_set_query_params(page="Página principal")
        st.experimental_rerun()

    elif st.button('Implement Test'):
        try:
            if parametersArray[0] == 'Custom query':
                call_procedure_query = f"""
                CALL QUALITY.internal.general_test(
                    '{parametersArray[1]}',
                    '{parametersArray[2]}',
                    $${parametersArray[3]}$$,
                    '{parametersArray[4]}',  
                    '{parametersArray[5]}',
                    '{cron}',
                    '{alert_email}',
                    '{alert_message}'
                )
                """
            elif parametersArray[0] == 'Nulls':
                call_procedure_query = f"""
                CALL QUALITY.internal.predefined_null_test(
                    '{parametersArray[1]}',
                    '{parametersArray[2]}',
                    '{parametersArray[3]}',
                    '{parametersArray[4]}',  
                    '{parametersArray[5]}',
                    '{parametersArray[6]}',
                    '{cron}',
                    '{alert_email}',
                    '{alert_message}',
                    null
                )
                """
            elif parametersArray[0] == 'Uniqueness':
                call_procedure_query = f"""
                CALL QUALITY.internal.predefined_uniqueness_test(
                    '{parametersArray[1]}',
                    {parametersArray[2]},
                    '{parametersArray[3]}',
                    '{parametersArray[4]}',  
                    '{cron}',
                    '{alert_email}',
                    '{alert_message}',
                    null
                )
                """
            elif parametersArray[0] == 'Volumetry':
                call_procedure_query = f"""
                CALL QUALITY.internal.predefined_volumetry_test(
                    '{parametersArray[1]}',
                    '{parametersArray[2]}',
                    '{parametersArray[3]}',
                    $${parametersArray[4]}$$,  
                    '{parametersArray[5]}',
                    '{cron}',
                    '{alert_email}',
                    '{alert_message}',
                    null
                )
                """
            else:
                call_procedure_query = f"""
                    Select 'Joder amigo'
                """
            # Ejecutar la consulta
            cur = conn.cursor()
            cur.execute(call_procedure_query)
            # Obtener el resultado
            resultado = str(cur.fetchall()).replace("[('",'').replace("',)]",'')
            # Almacenar la variable test_query del resultado (asumiendo que es la primera columna del resultado)
            if resultado:
                if resultado =="The test name already exists. Please use another name.":
                    st.error(f'{resultado}')
                else:
                    st.success(f' {resultado}')
            else:
                st.success('Culpa de Nico')
        except Exception as e:
            st.error(f'Ha ocurrido un error: {e} ')

        


## Si no se ha elegido ninguna página, predeterminamos a "Página principal"
current_page = st.experimental_get_query_params().get("page", ["Página principal"])[0]

# Esto determina qué página mostrar basándose en el parámetro de consulta
if current_page == "Página principal":
    main_page()
elif current_page == "Configuración de programación y alertas":
    schedule_and_alerts_page(conn)
elif current_page == "Eliminación de tests":
    deletion_page(conn)
elif current_page == "chatBox":
    chat_page()












