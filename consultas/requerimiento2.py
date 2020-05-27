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
cols1 = ["folio_vivienda", "folio_unico", "estado_intervencion", "telefonica"]
cols2 = ["cambio_vivienda", "fuera_villa", "hogar_dividido", "cambio_inf_baseline", "cambio_inf_prev", "fuera_villa", "hogar_dividido"]
cols3 = ["inf_clave", "inf_clave1", "participa1", "participa2", "total_integrantes_vivienda", "edad", "H3", "SA1", "SA10", "SA10A", "SA12B", "SA11", "SA12", "SA13a", "SA13b", "SA13c", "SA13d", "CS16", "menores_15", "edad_cat_2", "NEDU", "sfdt", "cise", "cise_ocupados", "hacin_dormitorios", "hacin_dormitoriosrec", "hacin_dormitorioscat", "p12_1cat", "mc1sat", "mc3sat", "mc5sat", "mc1insat", "mc3insat", "mc5insat", "mc2anun", "mc2bnun", "mc2asie", "mc2bsie", "SA1_cat", "PHQ_2", "PHQ_2_cat", "GHQ_dg", "GHQ_dg_cat1", "urg_resp_cat", "hosp_resp_cat", "urg_resp_mes_6", "hosp_resp_mes_12", "fumador_actual", "CS2_c", "CS3_c", "cs11_12", "cs11_12_c", "RecArUse", "u3_aux" ]
cols4 = ["v1sat", "v5sat", "v1insat", "v5insat", "v2cat", "v3cat"]              
              
df1 = bases["w1_bdm_bbdd_hh_20_04_24"].loc[:, cols1]
df2 = bases["w2_bdm_bbdd_hh_20_04_24"].loc[:, cols1 + cols2]
df3 = bases["w3_bdm_bbdd_hh_20_04_24"].loc[:, cols1 + cols2]

df4 = bases["w1_bdm_e_beta"].loc[:, cols3 + cols4]              
df5 = bases["w2_bdm_e_beta"].loc[:, cols3]              
df6 = bases["w3_bdm_e_beta"].loc[:, cols3 + cols4]

             
      
            
################       ELABORACION DE REQUERIMIENTO      ###################              
result1 = (pd.merge(df1, df2, how = 'left', left_index= True, suffixes=('', '_h2'), on = 'folio_unico', sort = False))
result2 = (pd.merge(result1, df3, how = 'left', left_index= True, suffixes=('', '_h3'), on = 'folio_unico', sort = False))
print(result2.head())   
                         
result3 = (pd.merge(result2, df4 how = 'left', left_index= True, suffixes=('', '_h4'), on = 'folio_unico', sort = False))
result4 = (pd.merge(result3, df5 how = 'left', left_index= True, suffixes=('', '_h5'), on = 'folio_unico', sort = False))
result5 = (pd.merge(result4, df6 how = 'left', left_index= True, suffixes=('', '_h6'), on = 'folio_unico', sort = False))
              
              
              
################    ELIMINACION DE VALORES DUPLICADOS    ################### 
#f_result = result.drop_duplicates('folio_vivienda')              
print(result2.head())   
              
################  ALMACENAMIENTO DE NUEVA TABLA COMO CSV ###################               
result2.to_csv('/home/ubuntu/Rucas/data/dir_path/csv/tab/requerimiento2.csv', sep=',', float_format='%g', encoding='utf-8', index = False)
           
              
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
