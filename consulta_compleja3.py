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


for base in bases:
          bases[base]["folio_unico"] = bases[base]["folio_villa"].astype('str') + '-' + bases[base]["folio_vivienda"].astype('str')
          
  
#PREGUNTA   
          
cols = ["folio_unico", "folio_villa", "folio_vivienda", "P15_1_1"]
#"P15_1_2", "P15_1_3A", "P15_1_3B", "P15_2_1B", "P15_2_2", "P15_2_3A", "P15_2_3B", "P15_3_1B", "P15_3_2", "P15_3_3A", "P15_3_3B", "P15_1_W1", "P15_2_W1", "P15_3_W1"]
          
df1 = bases["w1_bdm_p"].loc[:, cols]
          
#df2 = bases["w2_bdm_p"].loc[:, cols]
          
#df3 = bases["w3_bdm_p"].loc[:, cols]
          
#df = (df1.append([df2, df3]))
          
#df.to_csv('/home/ubuntu/Rucas/data/consultas/01may_Cc3.csv', sep=',', float_format = '%.12g', encoding='utf-8', index = False)


print("La consulta fue almacenada con éxito. IMPORTANTE: recuerda editar el nombre del archivo con la fecha y hora actual")
