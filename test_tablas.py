# El objetivo del siguiente test es poder realizar las consultas solicitadas en un ambiente separado del programa principal con el objetivo de resguardar la información ya generada y mantener la autonomía de los procesos. 
 
import os
import json
import pandas as pd
import pyreadstat

# Directorio de bases a cargar
dir_path = "/directorio/de/carpeta/donde/alojan/las/bases/"

### Carga de tablas como base de datos:
# * Se crea el directorio vacío "bases" en el cual se cargarán todas las bases.
# * Se cargarán tantas bases como archivos csv hayan presentes en el directorio seleccionado. 
# * Inicialmente quedarán con el nombre del archivo y aunque pueden ser editados no es estrictamente necesario


bases = {}

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

cols = ["H3", "H5", "H9", "H7", "CS19", "CS20", "O19a", "O20", "folio_unico"]

df1 = bases["w1_bdm_e_beta"].loc[:, ["folio_villa", "folio_vivienda"] + cols]
df2 = bases["w2_bdm_e_beta"].loc[:, cols]
df3 = bases["w3_bdm_e_beta"].loc[:, cols]
df = (df1.join(df2.set_index("folio_unico"), how="left", on="folio_unico", lsuffix="_00", rsuffix="_01")
         .join(df3.set_index("folio_unico"), how="left", on="folio_unico", lsuffix="", rsuffix="_02")
     )


df.head()
#df.to_csv('ejemplo1.csv', sep=',', encoding='utf-8', index = False)


# Pregunta n°2:

#### Base con informacion de Brisas del Mar, que para cada sujeto contenga las variables "SB3", "SU2_1", "CS19", "SA15a" (de la línea de base) y las preguntas "P1_1", "P1_2", "P11" (de las 3 olas). Las olas se agregan como columnas.

cols2 = ["folio_unico", "SB3", "SU2_1", "CS19", "SA15a"]
cols3 = ["P1_1", "P1_2", "P11"] 

dfa = bases["w1_bdm_e_beta"].loc[:, ["folio_vivienda"] + cols2]
dfb = bases["w1_bdm_p"].loc[:, cols3]
dfc = bases["w2_bdm_p"].loc[:, cols3]
dfd = bases["w3_bdm_p"].loc[:, cols3]


dfb1 = (dfa.join(dfb.set_index('folio_vivienda'), how="left", on="folio_vivienda", lsuffix="_00a", rsuffix="_01a")
        .join(dfc.set_index('folio_unico'), how="left", on="folio_unico", lsuffix="", rsuffix="_02a")
        .join(dfd.set_index('folio_unico'), how="left", on="folio_unico", lsuffix="", rsuffix="_03a")
       )


dfb1.head()
#dfb1.to_csv('ejemplo2.csv', sep=',', encoding='utf-8', index = False)


# Dentro de cada folio de vivienda integre los otros
# Probar pegar DF con las bases

# ### 2 errores a corregir:

# * Averiguar por qué al crear el dfb1 "cols" deja de tomar los valores o no los identifica

          
# Pregunta n°3:

#### Base con información de Brisas del Mar y Marta Brunet de la pauta de observación, todas las preguntas. Las olas y villas se agregan como filas. 

df7 = bases["w1_bdm_p"].loc[:, ["folio_villa", "folio_vivienda"] + cols]
df8 = bases["w1_mb_p"].loc[:, cols]
#dfc = (df7.join(df8.set_index('folio_unico'), how="left", on="folio_unico", lsuffix="_00", rsuffix="_01")
#      )

result = pd.concat([df7, df8], sort = True) 

result.head()

result.to_csv('ejemplo3.csv', sep=',', encoding='utf-8', index = False)

# ###### P1_2 solo debería estar en bm1 
# Probar el concat con variables específicas y forzar poner variables diferentes (ver error) 
# Ordenar las variables (folios al principio)

def count_col(df, col):
    return df[col].count()


count_H3 = count_col(dfb1, 'SD1')


df["P1_1"].fillna(0).astype('int').head() 

df["P1_1"] = df["P1_1"].astype('int') 




#ASDSADASDSADASDAS
          #ASDASDSADSADAS
