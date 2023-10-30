import streamlit as st
from modules import obtener_bases_de_datos,obtener_esquemas,conectar_a_snowflake,cargar_credenciales,obtener_tablas,obtener_columnas
import pandas as pd

## Cargar credenciales
credenciales = cargar_credenciales("Streamlit/credentials.json")

## Conexion a Snowflake
conn = conectar_a_snowflake(credenciales["user"],credenciales["password"], credenciales["account"], credenciales["warehouse"], credenciales["database"], credenciales["schema"])

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

    'Execute the test right now'      
    return parametersArray
    

