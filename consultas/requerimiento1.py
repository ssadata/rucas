#########################   PACKAGES / LIBRERIAS   ######################

import os
import json
import pandas as pd
import pyreadstat

########################## DIR_PATH'S / DIRECTORIOS #######################

main_path = "/home/ubuntu/Rucas/data/dir_path/csv"
table_paths =["/bdm/", "/mb/"]

##########################   DB  /  BASE DE DATOS   #######################

bases = {}
for path in table_paths:
    current_path = main_path + path
    for file in os.listdir(current_path):
        print(f"procesando {file.split('.')[0]}")
        db = pd.read_csv(current_path+file, sep=',', thousands=".", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
        if db.columns.shape[0] < 2:
            db = pd.read_csv(current_path+file, sep=';', thousands=",", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
        bases[file.split('.')[0]] = db


##########################   ID  /  IDENTIFICADOR   #######################
              
for base in bases:
    bases[base]["folio_unico"] = bases[base]["folio_villa"].astype('str') + '-' + bases[base]["folio_vivienda"].astype('str')              

#########################    QUERIES  /  CONSULTAS    ######################
#**************************************************************************#
##################################   N °   1   #############################

# 1. Ejemplo de unión de dos bases de datos con distinto número de filas: BBDD Botones y BDM W2.

# a. Generar una base de datos que tenga las variables de BBDD de Botones y de los datos de la encuesta correspondientes a esas viviendas, exportarlas como CSV,
##########################   COLS / COLUMNAS   #############################

cols = ["folio_unico", "folio_villa", "folio_vivienda"]              
              
df1 = bases["w2_bdm_boton"]
df2 = bases["w2_bdm_e_beta"]
result = (df1.join(df2.set_index('folio_unico'), how = "left", on = 'folio_vivienda', lsuffix ="", rsuffix = "_01")
print(result.head())              
#result.to_csv('/home/ubuntu/Rucas/data/dir_path/csv/tab/requerimiento1.csv', sep=',', encoding='utf-8', index = False)
              
              
# b. Montar nuevamente en el sistema,
# c. Ver en Metabase una tabla cruzada de: Media de temperatura (T_Ddia_prom) y humedad (H_Ddia_prom) con número de integrantes de la vivienda (total_integrantes_vivienda).

