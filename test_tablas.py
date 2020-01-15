# El objetivo del siguiente test es poder realizar las consultas solicitadas en un ambiente separado del programa principal con el objetivo de resguardar la información ya generada y mantener la autonomía de los procesos. 
 
import os
import json
import pandas as pd
import pyreadstat

# Directorio de bases a cargar
dir_path = "/home/haze/Documentos/Programa/AtomProyects/RUCAS/tablas/TEST/test_consultas_SQL/Tablas/"

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

##### Base con información de Brisas del Mar, de las 3 olas, que contenga las preguntas P1, P11 y P15 para cada una de las viviendas. Las olas se agregan como columnas.

for base in bases:
    bases[base]["folio_unico"] = bases[base]["folio_villa"].astype('str') + '-' + bases[base]["folio_vivienda"].astype('str')

cols = ["P1_1", "P1_3", "P1_4", "P1_6", "P1_7" ,"P1_8", "P1_9", "P1_10", "P1_11", "P1_12", "P1_13", "P11A_1_2", 
         "P11A_2_1", "P11A_2_2", "P11A_3_1", "P11A_3_2", "P11A_4_1", "P11A_4_2", "P15_1_1", "P15_1_1A", "P15_1_2", 
         "P15_1_3A", "P15_1_3B", "P15_2_1", "P15_2_1A", "P15_2_2", "P15_2_3A", "P15_2_3B", "P15_3_1", "P15_3_1A", 
         "P15_3_2", "P15_3_3A", "P15_3_3B", "folio_unico"]

df1 = bases["w1_bdm_e_beta"].loc[:, ["folio_villa", "folio_vivienda"] + cols]
df2 = bases["w2_bdm_p"].loc[:, cols]
df3 = bases["w3_bdm_p"].loc[:, cols]
df = (df1.join(df2.set_index('folio_unico'), how="left", on="folio_unico", lsuffix="_00", rsuffix="_01")
         .join(df3.set_index("folio_unico"), how="left", on="folio_unico", lsuffix="", rsuffix="_02")
     )


df.head()
#df.to_csv('ejemplo1.csv', sep=',', encoding='utf-8', index = False)


# Pregunta n°2:

#### Base con informacion de Brisas del Mar, que para cada sujeto contenga las variables H3, SA1, SD1. SD2, MC1 (de la línea de base) y las preguntas P1, P11 y P15 (de las 3 olas). Las olas se agregan como columnas.

cols2 = ["H3", "SA1", "SD1", "SD2", "MC1", "folio_unico"]

dfa = bases["w1_bdm_e_beta"].loc[:, ["folio_vivienda"] + cols2]
dfb = bases["w1_bdm_p"].loc[:, cols]
dfc = bases["w2_bdm_p"].loc[:, cols]
dfd = bases["w3_bdm_p"].loc[:, cols]

#dfb1 = (dfa.join(dfb.set_index('folio_unico'), how="left", on='folio_unico', lsuffix="_00", rsuffix="_01")
#         .join(dfc.set_index('folio_unico'), how="left", on='folio_unico', lsuffix="", rsuffix="_02")
#         .join(dfd.set_index('folio_unico'), how= "left", on='folio_unico', lsuffix = "", rsuffix= "_03")
#      )


dfb1 = (df.join(dfa.set_index('folio_vivienda'), how="left", on='folio_vivienda', lsuffix="", rsuffix="_01a")
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




