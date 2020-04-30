# El objetivo del siguiente test es poder realizar las consultas solicitadas en un ambiente separado del programa principal con el objetivo de resguardar la información ya generada y mantener la autonomía de los procesos. 
# AAAAA
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


bases = {} # QUÉ ES BASES?? ES UN DIRECTORIO? PARA QUÉ SRIVE SI YA TENEMOS TODAS LAS BASES EN UNA CARPETA?

for file in os.listdir(dir_path):
    print(f"Procesando {file.split('.')[0]}")
    db = pd.read_csv(dir_path+file, sep=',', thousands=".", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
    if db.columns.shape[0] < 2:
        db = pd.read_csv(dir_path+file, sep=';', thousands=",", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
    bases[file.split('.')[0]] = db


# ##### Ejemplo de base cargada: Se menciona el directorio, especifíca la base y llama a la acción "head()" que imprime los 5 primeros casos de la tabla completa.
# No visualiza las 135 columnas por un tema de espacio:

bases["w1_bdm_e_beta"].head() 

#Pregunta n° 1:

##### Base con información de Brisas del Mar, de las 3 olas, que contenga las preguntas H3, H5, H9, H7, CS19, CS20, O19a, O20 y  para cada una de las viviendas. Las olas se agregan como columnas.
##### Corresponde ahora seleccionar aquellas variables que si perduran en las distintas waves (para un mejor ejemplo)
          
for base in bases:
    bases[base]["folio_unico"] = bases[base]["folio_villa"].astype('str') + '-' + bases[base]["folio_vivienda"].astype('str')

cols = ["folio_villa", "folio_vivienda","folio_hogar","num_int_hogar","edad","H3", "SA1","SA11","SA12","CS16", "CS18","MC2a", "folio_unico"]

          ## quiero pegarlas hacia abajo
 
df1 = bases["w1_bdm_e_beta"].set_index(cols)   
          #index = "folio_unico"
#df1 = bases["w1_bdm_e_beta"].loc[:, cols]
#df2 = bases["w2_bdm_e_beta"].loc[:, cols]
#df3 = bases["w3_bdm_e_beta"].loc[:, cols]
#df = (df1.append(df2))


df.head()
df.to_csv('/home/ubuntu/Rucas/data/csv/base1.csv', sep=',', encoding='utf-8', index = False)



