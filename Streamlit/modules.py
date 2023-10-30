import pandas as pd
import streamlit as st

TimeZones = ["Africa/Abidjan", "Africa/Accra", "Africa/Addis_Ababa", "Africa/Asmara", "Africa/Bamako", 
    "Africa/Banjul", "Africa/Bissau", "Africa/Blantyre", "Africa/Brazzaville", "Africa/Bujumbura", 
    "Africa/Cairo", "Africa/Casablanca", "Africa/Ceuta", "Africa/Dakar", "Africa/Djibouti", 
    "Africa/Douala", "Africa/Freetown", "Africa/Gaborone", "Africa/Harare", "Africa/Juba", 
    "Africa/Kampala", "Africa/Kigali", "Africa/Lome", "Africa/Luanda", "Africa/Maputo", 
    "Africa/Monrovia", "Africa/Ndjamena", "Africa/Ouagadougou", "Africa/Sao_Tome", "Africa/Windhoek", 
    "Africa/Johannesburg", "Africa/Niamey", "Africa/Nouakchott", "America/Argentina/San_Luis", 
    "America/Argentina/Tucuman", "America/Argentina/Ushuaia", "America/Aruba", "America/Asuncion", 
    "America/Atikokan", "America/Blanc-Sablon", "America/Boa_Vista", "America/Ciudad_Juarez", 
    "America/Costa_Rica", "America/Danmarkshavn", "America/Detroit", "America/Dominica", 
    "America/Grenada", "America/Halifax", "America/Indiana/Petersburg", "America/Indiana/Tell_City", 
    "America/Jamaica", "America/Kentucky/Louisville", "America/Mazatlan", "America/North_Dakota/Center", 
    "America/Nuuk", "America/Port_of_Spain", "America/Porto_Velho", "America/Santiago", 
    "America/Santo_Domingo", "America/Thule", "America/Tortola", "Asia/Macau", 
    "Asia/Oral", "Atlantic/Reykjavik", "Africa/Bangui", "Africa/Algiers", 
    "Africa/Conakry", "Africa/Dar_es_Salaam", "Africa/El_Aaiun", "Africa/Khartoum", 
    "Africa/Lagos", "Africa/Libreville", "Africa/Lusaka", "Africa/Malabo","Africa/Mbabane", 
    "Africa/Mogadishu", "Africa/Porto-Novo", "Africa/Tripoli", "Africa/Tunis", 
    "America/Anchorage", "America/Anguilla", "America/Araguaina", "America/Argentina/Buenos_Aires", 
    "America/Argentina/Catamarca", "America/Argentina/Cordoba", "America/Argentina/Jujuy", 
    "America/Argentina/La_Rioja", "America/Argentina/Rio_Gallegos", "America/Argentina/Salta", 
    "America/Argentina/San_Juan", "America/Bahia", "America/Barbados", "America/Belem", 
    "America/Bogota", "America/Boise", "America/Campo_Grande", "America/Cancun", "America/Cayman", 
    "America/Chicago", "America/Creston", "America/Cuiaba", "America/Curacao", "America/Dawson", 
    "America/Dawson_Creek", "America/Denver", "America/Edmonton", "America/Eirunepe", "America/Fort_Nelson", 
    "America/Glace_Bay", "America/Goose_Bay", "America/Grand_Turk", "America/Guadeloupe", "America/Guyana", 
    "America/Indiana/Knox", "America/Indiana/Marengo", "America/Indiana/Vincennes", "America/Indiana/Winamac", 
    "America/Inuvik", "America/Iqaluit", "America/Kentucky/Monticello", "America/Kralendijk", 
    "America/La_Paz", "America/Lima", "America/Maceio", "America/Martinique", "America/Merida", 
    "America/Metlakatla", "America/Mexico_City", "America/Moncton", "America/Montevideo", "America/Montserrat", 
    "America/Nassau", "America/Ojinaga", "America/Paramaribo", "America/Phoenix", "America/Rio_Branco", 
    "America/St_Barthelemy", "America/St_Kitts", "America/St_Lucia", "America/Toronto", "America/Vancouver", 
    "Antarctica/Davis", "Antarctica/DumontDUrville", "Antarctica/Macquarie", "Antarctica/Mawson", 
    "Antarctica/McMurdo", "Antarctica/Rothera", "Antarctica/Troll", "Asia/Amman", "Asia/Anadyr", 
    "Asia/Beirut", "Asia/Brunei", "Asia/Dushanbe", "Asia/Famagusta", "Asia/Jakarta",
    "Asia/Kathmandu", "Asia/Manila", "Asia/Pontianak", "Asia/Seoul", "Asia/Taipei", 
    "Asia/Yerevan", "Atlantic/Bermuda", "Atlantic/Canary", "Atlantic/Cape_Verde", "Atlantic/Faroe", 
    "Australia/Brisbane", "Australia/Eucla", "Australia/Lindeman", "Europe/Athens", "Europe/Bucharest", 
    "Europe/Budapest", "Europe/Dublin", "Europe/Gibraltar", "Europe/Kyiv", "Europe/Monaco", 
    "Europe/Riga", "Europe/Ulyanovsk", "Europe/Vienna", "Indian/Kerguelen", "Pacific/Honolulu", 
    "Pacific/Kiritimati", "Africa/Lubumbashi", "Africa/Maseru", "Africa/Nairobi", "America/Adak", 
    "Africa/Kinshasa", "America/Antigua", "America/Argentina/Mendoza", "America/Bahia_Banderas", 
    "America/Belize", "America/Cambridge_Bay", "America/Caracas", "America/Cayenne", "America/Chihuahua", 
    "America/El_Salvador", "America/Fortaleza", "America/Guatemala", "America/Havana", "America/Indiana/Vevay", 
    "America/Juneau", "America/Los_Angeles", "America/Lower_Princes", "America/Managua", "America/Manaus", 
    "America/Marigot", "America/Matamoros", "America/Menominee", "America/Miquelon", "America/Monterrey", 
    "America/New_York", "America/Nome", "America/Noronha", "America/North_Dakota/Beulah", "America/North_Dakota/New_Salem", 
    "America/Panama", "America/Port-au-Prince", "America/Puerto_Rico", "America/Punta_Arenas", "America/Rankin_Inlet", 
    "America/Recife", "America/Regina", "America/Santarem", "America/Scoresbysund", "America/Sitka", 
    "America/St_Johns", "America/St_Thomas", "America/St_Vincent", "America/Tegucigalpa", "America/Tijuana", 
    "America/Whitehorse", "America/Winnipeg", "America/Yakutat", "Antarctica/Casey", "Antarctica/Palmer", 
    "Antarctica/Syowa", "Antarctica/Vostok", "Arctic/Longyearbyen", "Asia/Aqtobe", "Asia/Atyrau",
    "Asia/Bahrain", "Asia/Bangkok", "Asia/Barnaul", "Asia/Bishkek", "Asia/Chita", 
    "Asia/Choibalsan", "Asia/Damascus", "Asia/Dhaka", "Asia/Dili", "Asia/Dubai", 
    "Asia/Hebron", "Asia/Hong_Kong", "Asia/Hovd", "Asia/Jayapura", "Asia/Jerusalem", 
    "Asia/Kabul", "Asia/Kamchatka", "Asia/Karachi", "Asia/Khandyga", "Asia/Kolkata", 
    "Asia/Krasnoyarsk", "Asia/Kuala_Lumpur", "Asia/Kuching", "Asia/Kuwait", "Asia/Magadan", 
    "Asia/Makassar", "Asia/Nicosia", "Asia/Novokuznetsk", "Asia/Novosibirsk", "Asia/Omsk", 
    "Asia/Phnom_Penh", "Asia/Pyongyang", "Asia/Qatar", "Asia/Qostanay", "Asia/Riyadh", 
    "Asia/Sakhalin", "Asia/Samarkand", "Asia/Singapore", "Asia/Tbilisi", "Asia/Tehran", 
    "Asia/Tokyo", "Asia/Ulaanbaatar", "Asia/Urumqi", "Asia/Ust-Nera", "Asia/Vientiane", 
    "Asia/Yangon", "Asia/Yekaterinburg", "Atlantic/Azores", "Atlantic/Madeira", "Atlantic/South_Georgia", 
    "Atlantic/Stanley", "Australia/Broken_Hill", "Australia/Darwin", "Australia/Hobart", "Australia/Melbourne", 
    "Australia/Perth", "Australia/Sydney", "Europe/Andorra", "Europe/Belgrade", "Europe/Berlin", 
    "Europe/Bratislava", "Europe/Brussels", "Europe/Busingen", "Europe/Chisinau", "Europe/Copenhagen", 
    "Europe/Guernsey", "Europe/Helsinki", "Europe/Isle_of_Man", "Europe/Istanbul", "Europe/Jersey", 
    "Europe/Kaliningrad", "Europe/Kirov", "Europe/Lisbon", "Europe/Ljubljana", "Europe/London", 
    "Europe/Luxembourg", "Europe/Madrid", "Europe/Malta", "Europe/Mariehamn", "Europe/Podgorica", 
    "Europe/Prague", "Europe/Rome", "Europe/Samara", "Europe/San_Marino", "Europe/Sarajevo", 
    "Europe/Saratov", "Europe/Simferopol", "Europe/Skopje", "Europe/Stockholm", "Europe/Tallinn", 
    "Europe/Tirane", "Europe/Vatican", "Europe/Vilnius", "Europe/Volgograd", "Europe/Warsaw",
    "Europe/Zagreb", "Indian/Antananarivo", "Indian/Chagos", "Indian/Christmas", "Indian/Cocos", 
    "Indian/Comoro", "Indian/Maldives", "Indian/Mauritius", "Indian/Reunion", "Pacific/Apia", 
    "Pacific/Chatham", "Pacific/Fakaofo", "Pacific/Funafuti", "Pacific/Gambier", "Pacific/Guadalcanal", 
    "Pacific/Guam", "Pacific/Majuro", "Pacific/Midway", "Pacific/Nauru", "Pacific/Noumea", 
    "Pacific/Pago_Pago", "Pacific/Palau", "Pacific/Pitcairn", "Pacific/Pohnpei", "Pacific/Port_Moresby", 
    "Pacific/Saipan", "Pacific/Tarawa", "Pacific/Kosrae", "Pacific/Wallis", "America/Guayaquil", 
    "America/Hermosillo", "America/Indiana/Indianapolis", "America/Resolute", "America/Sao_Paulo", "America/Swift_Current", 
    "Asia/Almaty", "Asia/Aqtau", "Asia/Ashgabat", "Asia/Baghdad", "Asia/Baku", 
    "Asia/Colombo", "Asia/Ho_Chi_Minh", "Asia/Irkutsk", "Asia/Muscat", "Asia/Qyzylorda", 
    "Asia/Shanghai", "Asia/Srednekolymsk", "Asia/Thimphu", "Asia/Tomsk", "Asia/Vladivostok", 
    "Asia/Yakutsk", "Atlantic/St_Helena", "Australia/Adelaide", "Australia/Lord_Howe", "Europe/Amsterdam", 
    "Europe/Astrakhan", "Europe/Minsk", "Europe/Moscow", "Europe/Oslo", "Europe/Paris", 
    "Europe/Sofia", "Europe/Vaduz", "Europe/Zurich", "Indian/Mahe", "Indian/Mayotte", 
    "Pacific/Auckland", "Pacific/Bougainville", "Pacific/Chuuk", "Pacific/Easter", "Pacific/Efate", 
    "Pacific/Fiji", "Pacific/Galapagos", "Pacific/Kanton", "Pacific/Kwajalein", "Pacific/Marquesas", 
    "Pacific/Niue", "Pacific/Norfolk", "Pacific/Rarotonga", "Pacific/Tongatapu", "Pacific/Wake", 
    "Asia/Aden", "Asia/Gaza", "Asia/Tashkent", "Pacific/Tahiti" ]




def query_lista(session, query, nombre_columna='DFRETURN',is_trytest=False):
    try:
        # Paso 1: Obtener la lista de objetos Row
        if query is None or query == "":
            data_frame_pd = pd.DataFrame({'message': [f"Please introduce 'Test query' "]})
            return [["Error"],data_frame_pd]
        
        data_frame = session.sql(query).collect()

        # Paso 2: Convertir la lista de objetos Row a un DataFrame de Pandas
        data_frame_pd = pd.DataFrame([row.asDict() for row in data_frame])

        # Paso 3: Crear una Lista de Opciones
        lista = []
        if nombre_columna != 'DFRETURN':
            if data_frame_pd.empty:
                lista = ['No tests found']
            else:
                lista = [str(x) for x in data_frame_pd[nombre_columna]]

        return [lista, data_frame_pd]
    

    
    except Exception as e:
        if is_trytest:
            error_df = pd.DataFrame({'message': [f"Please fill the mandatory fields and retry"]})
            return ["Error", error_df]
        error_df = pd.DataFrame({'message': [f"Error: {e}"]})
        return ["Error", error_df]



def cron_builder(dias =  "All", horas = "00", minutos = "00"):
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

def get_databases(session):
    try:
        # Paso 1: Obtener la lista de objetos Row
        data_frame = session.sql("SELECT DISTINCT TABLE_CATALOG FROM INFORMATION_SCHEMA.TABLES").collect()  

        # Paso 2: Convertir la lista de objetos Row a un DataFrame de Pandas
        data_frame_pd = pd.DataFrame([row.asDict() for row in data_frame])

        # Paso 3: Crear una Lista de Opciones
        lista = [str(x) for x in data_frame_pd['TABLE_CATALOG']]
        return lista
    
    except Exception as e:

        return f"Error: {e}"
    


def get_schema(session,db):
    try:
        # Paso 1: Obtener la lista de objetos Row
        data_frame = session.sql(f"SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '{db}'").collect()  

        # Paso 2: Convertir la lista de objetos Row a un DataFrame de Pandas
        data_frame_pd = pd.DataFrame([row.asDict() for row in data_frame])

        # Paso 3: Crear una Lista de Opciones
        lista = [str(x) for x in data_frame_pd['TABLE_SCHEMA']]
        return lista      
      
    except Exception as e:

        return f"Error: {e}"
    

def get_tables(session,db,schema):
    try:
        # Paso 1: Obtener la lista de objetos Row
        data_frame = session.sql(f"SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '{db}' AND TABLE_SCHEMA = '{schema}'").collect()  

        # Paso 2: Convertir la lista de objetos Row a un DataFrame de Pandas
        data_frame_pd = pd.DataFrame([row.asDict() for row in data_frame])

        # Paso 3: Crear una Lista de Opciones
        lista = [str(x) for x in data_frame_pd['TABLE_NAME']]
        return lista
    
    except Exception as e:

        return f"Error: {e}"
    
def get_columns(session,db,schema,table):
    try:
        # Paso 1: Obtener la lista de objetos Row
        data_frame = session.sql(f"DESC TABLE {db}.{schema}.{table} ").collect()  

        # Paso 2: Convertir la lista de objetos Row a un DataFrame de Pandas
        data_frame_pd = pd.DataFrame([row.asDict() for row in data_frame])

        # Paso 3: Crear una Lista de Opciones
        lista = [str(x) for x in data_frame_pd["name"]]
        return lista
    
    
    except Exception as e:

        return f"Error: {e}"
    


def execute_procedure(session, procedure_call):
    try:
        # Ejecutar el procedimiento almacenado
        result = session.sql(procedure_call).collect()
        
        # Convertir el resultado (si lo hay) en una lista de Python
        if result:
            result_list = [row.asDict() for row in result]
            return result_list
        else:
            return ["Procedimiento ejecutado con éxito, pero no devolvió resultados."]
    except Exception as e:
        return [f"Error: {e}"]

procedures_dict = {
                "Custom query":"GENERAL_TEST",
                "Nulls":"PREDEFINED_NULL_TEST",
                "Volumetry":"PREDEFINED_VOLUMETRY_TEST",
                "Uniqueness":"PREDEFINED_UNIQUENESS_TEST"
            }