#

import os
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine


POSTGRES_ADDRESS = 'localhost' ## INSERT YOUR DB ADDRESS IF IT'S NOT ON PANOPLY
POSTGRES_PORT = '5432'         ## CONNECT WITH THE PORT
POSTGRES_USERNAME = 'userRucas' ## CHANGE THIS TO YOUR PANOPLY/POSTGRES USERNAME
POSTGRES_PASSWORD = 'passwordRucas123' ## CHANGE THIS TO YOUR PANOPLY/POSTGRES PASSWORD
POSTGRES_DBNAME = 'base_datos' ## CHANGE THIS TO YOUR DATABASE NAME

# Información de validación de Postgres
postgres_str = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_ADDRESS}:{POSTGRES_PORT}/{POSTGRES_DBNAME}"

# Conector que servirá de motor de búsqueda
conn = create_engine(postgres_str)	

dir_path = "/data/csv/"
#dir_pathj = "/data/json/"

for file in os.listdir(dir_path):
    print(f"Procesando {file}")
    db = pd.read_csv(dir_path+file, sep=',', thousands=".", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
### Observacion; la condicion (IF) genera un bug en la carga de las tablas con sep=';', por el momento todas las tablas utilizan sep=';'
    if db.columns.shape[0] < 2:
        db = pd.read_csv(dir_path+file, sep=';', thousands=",", header = 0, error_bad_lines=False, encoding="ISO-8859-1")


### Lectura y reemplazo de columnas mediante archivos .json

   # with open(f"{dir_pathj+file.split('.')[0]}.json", "r") as f:    
   #     cols = json.load(f)
   # db = db.rename(columns=cols)

    if db.columns.shape[0] > 1: 
        db.to_sql(file.split(".")[0], conn, index = False)
	
#### PARA TODO EFECTO; BRISAS DEL MAR = 1 / MARTA BRUNET = 2


