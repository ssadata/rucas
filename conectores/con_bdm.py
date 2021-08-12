#
#########################   PACKAGES / LIBRERIAS   ######################

import os
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

#########################   ENGINE / CONECTOR   ######################

POSTGRES_ADDRESS = 'localhost' ## INSERT YOUR DB ADDRESS IF IT'S NOT ON PANOPLY
POSTGRES_PORT = '5432'         ## CONNECT WITH THE PORT
POSTGRES_USERNAME = 'user' ## CHANGE THIS TO YOUR PANOPLY/POSTGRES USERNAME
POSTGRES_PASSWORD = 'pswrd' ## CHANGE THIS TO YOUR PANOPLY/POSTGRES PASSWORD
POSTGRES_DBNAME = 'rucas' ## CHANGE THIS TO YOUR DATABASE NAME

# Información de validación de Postgres
postgres_str = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_ADDRESS}:{POSTGRES_PORT}/{POSTGRES_DBNAME}"

# Conector que servirá de motor de búsqueda
conn = create_engine(postgres_str)	

#########################   DIR_PATH'S / DIRECTORIOS   ######################

dir_path = "/dir_path/csv/bdm/"
dir_pathj = "/dir_path/json/bdm/"

#########################   DB / BASE DE DATOS   ######################

for file in os.listdir(dir_path):
    print(f"Procesando {file}")

    db = pd.read_csv(dir_path+file, sep=',', thousands=".", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
##   Observacion; todas las tablas deben utilizar sep=';'

    if db.columns.shape[0] < 2:
        db = pd.read_csv(dir_path+file, sep=';', thousands=",", header = 0, error_bad_lines=False, encoding="ISO-8859-1")

#########################   JSON / ETIQUETAS  ######################

    with open(f"{dir_pathj+file.split('.')[0]}.json", "r") as f:
	      cols = json.load(f)
    db = db.rename(columns=cols)
	      
    if db.columns.shape[0] > 1:
	      db.to_sql(file.split(".")[0], conn, index = False)
	
