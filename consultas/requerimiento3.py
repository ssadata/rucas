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
##################################   N °   3   #############################
# 3. Unir dos bases de datos "hacia abajo":
# Utilizar las mismas variables del punto 2, pero pegando hacia abajo la base de datos.
              
##########################     DATAFRAMES      #############################
cols1 = ["folio_vivienda", "folio_unico", "estado_intervencion", "telefonica"]
cols2 = ["cambio_vivienda", "fuera_villa", "hogar_dividido", "cambio_inf_baseline", "cambio_inf_prev", "fuera_villa", "hogar_dividido"]
cols3 = ["inf_clave", "inf_clave1", "participa1", "participa2", "total_integrantes_vivienda", "edad", "H3", "SA1", "SA10", "SA10A", "SA12B", "SA11", "SA12", "SA13a", "SA13b", "SA13c", "SA13d", "CS16", "menores_15", "edad_cat_2", "NEDU", "sfdt", "cise", "cise_ocupados", "hacin_dormitorios", "hacin_dormitoriosrec", "hacin_dormitorioscat", "p12_1cat", "mc1sat", "mc3sat", "mc5sat", "mc1insat", "mc3insat", "mc5insat", "mc2anun", "mc2bnun", "mc2asie", "mc2bsie", "SA1_cat", "PHQ_2", "PHQ_2_cat", "GHQ_dg", "GHQ_dg_cat1", "urg_resp_cat", "hosp_resp_cat", "urg_resp_mes_6", "hosp_resp_mes_12", "fumador_actual", "CS2_c", "CS3_c", "cs11_12", "cs11_12_c", "RecArUse", "u3_aux" ]
cols4 = ["v1sat", "v5sat", "v1insat", "v5insat", "v2cat", "v3cat"]              
              
df1 = bases["w1_bdm_bbdd_hh_20_04_24"].loc[:, cols1]
df2 = bases["w2_bdm_bbdd_hh_20_04_24"].loc[:, cols1 + cols2]
df3 = bases["w3_bdm_bbdd_hh_20_04_24"].loc[:, cols1 + cols2]
              
####### hasta este punto el código funciona, pero arroja error ya que una o mas de las variables entregadas en cols3 no están bien escritas
####### o derechamente no se encuentran presenten en las tablas solicitadas.
####### Se deja manifiesto el proceder pero los nombres de cols3 y cols4 han de ser revisado para que puedan ejecutar el requerimiento.
              
df4 = bases["w1_bdm_e_beta"].loc[:, cols3 + cols4]              
df5 = bases["w2_bdm_e_beta"].loc[:, cols3]              
df6 = bases["w3_bdm_e_beta"].loc[:, cols3 + cols4]
              

################       ELABORACION DE REQUERIMIENTO      ###################     
######### NOTA IMPORTANTE: MERGE PARA EL LADO / CONCAT PARA ABAJO ##########
              
              
result1 = (pd.concat([df4, df5, df6], axis = 1, sort = True))
              
            
             
################  ALMACENAMIENTO DE NUEVA TABLA COMO CSV ###################               
result1.to_csv('/home/ubuntu/Rucas/data/dir_path/csv/tab/requerimiento3.csv', sep=',', float_format='%g', encoding='utf-8', index = False)
         
              
