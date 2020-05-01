#### CONSULTA REALIZADA POR FRAN EL DÍA 30 DE ABRIL A LAS 17.30 HRS

import os
import json
import pandas as pd
import pyreadstat



# Directorio de bases a cargar
dir_path = "/home/ubuntu/Rucas/data/csv/"

### Carga de tablas como base de datos:
# * Se crea el directorio vacío "bases" en el cual se cargarán todas las bases.
# * Se cargarán tantas bases como archivos csv hayan presentes en el directorio seleccionado. 

bases = {} 

for file in os.listdir(dir_path):
    print(f"Procesando {file.split('.')[0]}")
    db = pd.read_csv(dir_path+file, sep=',', thousands=".", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
    if db.columns.shape[0] < 2:
        db = pd.read_csv(dir_path+file, sep=';', thousands=",", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
    bases[file.split('.')[0]] = db


#Pregunta n° 1:
      
for base in bases:
          bases[base]["folio_unico"] = bases[base]["folio_villa"].astype('str') + '-' + bases[base]["folio_vivienda"].astype('str')
          
          
cols = ["folio_unico", "folio_hogar", "num_int_hogar", "edad", "H3", "SA1", "SA11", "SA12", "CS16", "CS18", "MC2a"]
  
          
df1 = bases["w1_bdm_e_beta"].loc[:, ["folio_villa", "folio_vivienda"] + cols]
df2 = bases["w2_bdm_e_beta"].loc[:, cols]
df3 = bases["w3_bdm_e_beta"].loc[:, cols]
df = (df1.append([df2, df3]))
          
#PROPONGO UTILIZAR UN CÓDIGO DE ALMACENAMIENTO DE ARCHIVOS COMO EL PUESTO A CONTINUACIÓN (O CUALQUIERA SIMILAR)
df.to_csv('/home/ubuntu/Rucas/data/consultas/30abr_Cc2.csv', sep=',', float_format = '%.12g', encoding='utf-8', index = False)


print("La consulta fue almacenada con éxito. IMPORTANTE: recuerda editar el nombre del archivo con la fecha y hora actual")


