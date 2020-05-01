#### CONSULTA REALIZADA POR FRAN EL DÍA 30 DE ABRIL A LAS 17.30 HRS

import os
import json
import pandas as pd
import pyreadstat

#Ejemplo Join
#Ejemplo de Merge
#Ejemplo de Concat


# Directorio de bases a cargar
dir_path = "/home/ubuntu/Rucas/data/csv/"

### Carga de tablas como base de datos:
# * Se crea el directorio vacío "bases" en el cual se cargarán todas las bases.
# * Se cargarán tantas bases como archivos csv hayan presentes en el directorio seleccionado. 
# * Inicialmente quedarán con el nombre del archivo y aunque pueden ser editados no es estrictamente necesario


bases = {} # UNA COSA ES LOS ARCHIVOS Y DONDE SE ALOJAN, Y OTRA ES LA CARGA DE VALORES DE LOS ARCHIVOS AL MOMENTO DE EJECUTAR LA CONSULTA.
#LO QUE HACE PYTHON ES LEER LOS ARCHIVOS .CSV EN SU ORIGEN Y CARGAR TODOS SUS VALORES EN ESTA BASE TEMPORAL.
#UNA VEZ CERRADO PYTHON ESTE OBJETO BASES DESAPARECE.

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
#"folio_villa", "folio_vivienda"

          
          # set_index(cols)  
df1 = bases["w1_bdm_e_beta"].loc[:, cols]
df2 = bases["w2_bdm_e_beta"].loc[:, cols]
df3 = bases["w3_bdm_e_beta"].loc[:, cols]
df = (df1.append(df2))


print(df.head())
df.to_csv('/home/ubuntu/Rucas/data/csv/base1.csv', sep=',', float_format = '%.12g', encoding='utf-8', index = False)



