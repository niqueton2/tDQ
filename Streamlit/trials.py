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
from snowflake.snowpark import Session

# Configura la sesión
session = Session.builder.config(
    "snowpark.account", "QGSSFWO-NT11439.snowflakecomputing.com",
    "snowpark.user", "NicApp",
    "snowpark.password", "MeryGP27092003",
    "snowpark.role", "ACCOUNTADMIN",
    "snowpark.warehouse", "COMPUTE_WH",
    "snowpark.database", "HQUALITY",
    "snowpark.schema", "CORE"
).create()

# Ejemplo: ejecuta una consulta
df = session.sql("SELECT CURRENT_VERSION()").toPandas()
print(df)

# Cierra la sesión
session.stop()
