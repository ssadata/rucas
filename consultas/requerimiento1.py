#########################   PACKAGES / LIBRERIAS   ######################

import os
import json
import pandas as pd
import pyreadstat
import re

########################## DIR_PATH'S / DIRECTORIOS #######################

main_path = "/home/ubuntu/Rucas/data/dir_path/csv"
table_paths =["/bdm/", "/mb/"]

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
##################################   N °   1   #############################

# 1. Ejemplo de unión de dos bases de datos con distinto número de filas: BBDD Botones y BDM W2.

# a. Generar una base de datos que tenga las variables de BBDD de Botones y de los datos de la encuesta correspondientes a esas viviendas, exportarlas como CSV,
##########################   COLS / COLUMNAS   #############################

df1 = bases["w2_bdm_boton"]
df2 = bases["w2_bdm_e_beta"]

              
#############################    OBSERVACION    ############################
#**************************************************************************#
#**************************************************************************#              
# Los 'float' pasan a 'string' al momendo de ejecutar el savToCsv.py debido#
# al argumento `float_format = '%.12g'` 1) ¿se puede guardar en sav los    #
# valores con puntuación ' . ' ? y no ' , '.                               #
# Ej: '12,1' --> '12.1'                                                    #
# De lo contrario hay que reemplazarlos manualmente utilizando 'reemplazar'#
# en la hoja de cálculo y cambiar ' , ' por ' . '.                         #
# Esto es requerimiento para que las líneas 63 a 66 puedan ejecutarse y así#
# las columnas con decimales puedan volver a convertirse en float.         #
#                                                                          #
# Los requerimientos solicitados fueron cumplidos, esta observación cumple #
# la función de nota de campo para su posterior desarrollo.                #
#**************************************************************************#
#**************************************************************************#
 
################          CONVERSION STRING A FLOAT      ###################
vars = ["T_Ddia_prom", "T_Ddia_sd", "T_Ddia_min", "T_Ddia_max", "H_Ddia_sd", "T_L_prom", "T_L_sd", "T_L_min", "T_L_max", "H_L_sd", "T_O_prom", "T_O_min"]

for var in vars:
    df1[var].astype('float64')
           

################       ELABORACION DE REQUERIMIENTO      ###################              
result = (df1.join(df2.set_index('folio_vivienda'), how = "left", on = 'folio_vivienda', lsuffix ="", rsuffix = "_01"))

################    ELIMINACION DE VALORES DUPLICADOS    ################### 
f_result = result.drop_duplicates('folio_vivienda')              
print(f_result.head())   
              
################  ALMACENAMIENTO DE NUEVA TABLA COMO CSV ###################               
f_result.to_csv('/home/ubuntu/Rucas/data/dir_path/csv/tab/requerimiento1_1.csv', sep=',', float_format='%g', encoding='utf-8', index = False)
#############################    OBSERVACION    ############################
#**************************************************************************#
#**************************************************************************#              
# Acá nuevamente se utiliza el argumento `float_format = '%.12g'` pero la  #
# puntuación será con ' . ' y por ello no requerirá nueva intervención     #
# salvo por replicar el ciclo (con sus respectivas columnas) de las líneas #
# 63 a 66.
#**************************************************************************#
#**************************************************************************#              
              
# b. Montar nuevamente en el sistema,
              
#############################    OBSERVACION    ############################
#**************************************************************************#
#**************************************************************************#              
# Previa revisión de haber "eliminado" la tabla de Adminer.                #
# Crear y guardar el 'json' respectivo a la tabla creada.                  #
# Ejecutar `con_tab.py` el cual subirá el contenido de la carpeta /tab     #
# donde están alojadas las tablas creadas por consultas_complejas          #
#**************************************************************************#
#**************************************************************************#
              
# c. Ver en Metabase una tabla cruzada de: Media de temperatura (T_Ddia_prom) y humedad (H_Ddia_prom) con número de integrantes de la vivienda (total_integrantes_vivienda).

