#########################   PACKAGES / LIBRERIAS   ######################

import os
import json
import pandas as pd
import pyreadstat
import re

########################## DIR_PATH'S / DIRECTORIOS #######################

main_path = "/home/ubuntu/Rucas/data/dir_path/csv"
table_paths =["/bdm/", "/mb/", "/tab/"]

##########################   DB  /  BASE DE DATOS   #######################

bases = {}
for path in table_paths:
    current_path = main_path + path
    for file in os.listdir(current_path):
        print(f"procesando {file.split('.')[0]}")
        db = pd.read_csv(current_path+file, sep=',', header = 0, error_bad_lines=False, encoding="ISO-8859-1")
        if db.columns.shape[0] < 2:
            db = pd.read_csv(current_path+file, sep=';', thousands=",", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
        bases[file.split('.')[0]] = db


##########################   ID  /  IDENTIFICADOR   #######################
              
for base in bases:
    bases[base]["folio_unico"] = bases[base]["folio_villa"].astype('str') + '-' + bases[base]["folio_vivienda"].astype('str')              

#########################    QUERIES  /  CONSULTAS    ######################
#**************************************************************************#
##################################   N °   2   #############################

# 1. Unir bases de datos "hacia el lado”: Tenemos 3 olas de BDM.
##########################     DATAFRAMES      #############################
cols1 = ["estado_intervencion", "telefonica"]
cols2 = ["cambio_vivienda", "fuera_villa", "hogar_dividido", "cambio_inf_baseline", "cambio_inf_prev", "fuera_villa", "hogar_dividido"]
              
df1 = bases["w1_bdm_bbdd_hh_20_04_24"].loc[:, cols1]
df2 = bases["w2_bdm_bbdd_hh_20_04_24"].loc[:, cols1 + cols2]
df3 = bases["w3_bdm_bbdd_hh_20_04_24"].loc[:, cols1 + cols2]
            
################       ELABORACION DE REQUERIMIENTO      ###################              
result = (df1.join(df2.set_index('folio_unico'), how = "left", on = 'folio_unico', lsuffix ="", rsuffix = "_01"))

################    ELIMINACION DE VALORES DUPLICADOS    ################### 
#f_result = result.drop_duplicates('folio_vivienda')              
print(result.head())   
              
################  ALMACENAMIENTO DE NUEVA TABLA COMO CSV ###################               
f_result.to_csv('/home/ubuntu/Rucas/data/dir_path/csv/tab/requerimiento2.csv', sep=',', float_format='%g', encoding='utf-8', index = False)
           
              
# b. Montar nuevamente en el sistema,
              
#############################    OBSERVACION    ############################
#**************************************************************************#
#**************************************************************************#              
# Previa revisión de haber "eliminado" la tabla identica de Adminer.       #
# Crear y guardar el 'json' respectivo a la tabla creada.                  #
# Ejecutar `con_tab.py` el cual subirá el contenido de la carpeta /tab     #
# donde están alojadas las tablas creadas por consultas_complejas          #
#**************************************************************************#
#**************************************************************************#
              
# c. Ver en Metabase una tabla cruzada de: Media de temperatura (T_Ddia_prom) y humedad (H_Ddia_prom) con número de integrantes de la vivienda (total_integrantes_vivienda).
