import snowflake.connector
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