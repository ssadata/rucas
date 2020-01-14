#!/usr/bin/env python
# coding: utf-8

# # El objetivo del siguiente test es poder realizar las consultas solicitadas en un ambiente separado del programa principal con el objetivo de resguardar la información ya generada y mantener la autonomía de los procesos. 
# 

# In[12]:


import os
import json
import pandas as pd
import pyreadstat


# In[13]:


# Directorio de bases a cargar
dir_path = "/home/haze/Documentos/Programa/AtomProyects/RUCAS/tablas/TEST/test_consultas_SQL/Tablas/"


# ### Carga de tablas como base de datos:

# * Se crea el directorio vacío "bases" en el cual se cargarán todas las bases.
# * Se cargarán tantas bases como archivos csv hayan presentes en el directorio seleccionado. 
# * Inicialmente quedarán con el nombre del archivo y aunque pueden ser editados no es estrictamente necesario

# In[3]:


bases = {}

for file in os.listdir(dir_path):
    print(f"Procesando {file.split('.')[0]}")
    db = pd.read_csv(dir_path+file, sep=',', thousands=".", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
    if db.columns.shape[0] < 2:
        db = pd.read_csv(dir_path+file, sep=';', thousands=",", header = 0, error_bad_lines=False, encoding="ISO-8859-1")
    bases[file.split('.')[0]] = db


# ##### Ejemplo de base cargada: Se menciona el directorio, especifíca la base y llama a la acción "head()" que imprime los 5 primeros casos de la tabla completa.
# No visualiza las 135 columnas por un tema de espacio
# 

# In[4]:


bases["w1_bdm_e_beta"].head() 


# # Pregunta n° 1:

# #### Base con información de Brisas del Mar, de las 3 olas, que contenga las preguntas P1, P11 y P15 para cada una de las viviendas. Las olas se agregan como columnas.

# In[8]:


for base in bases:
    bases[base]["folio_unico"] = bases[base]["folio_villa"].astype('str') + '-' + bases[base]["folio_vivienda"].astype('str')


# In[9]:


cols = ["P1_1", "P1_3", "P1_4", "P1_6", "P1_7" ,"P1_8", "P1_9", "P1_10", "P1_11", "P1_12", "P1_13", "P11A_1_2", 
         "P11A_2_1", "P11A_2_2", "P11A_3_1", "P11A_3_2", "P11A_4_1", "P11A_4_2", "P15_1_1", "P15_1_1A", "P15_1_2", 
         "P15_1_3A", "P15_1_3B", "P15_2_1", "P15_2_1A", "P15_2_2", "P15_2_3A", "P15_2_3B", "P15_3_1", "P15_3_1A", 
         "P15_3_2", "P15_3_3A", "P15_3_3B", "folio_unico"]


# In[10]:


df1 = bases["w1_bdm_e_beta"].loc[:, ["folio_villa", "folio_vivienda"] + cols]
df2 = bases["w2_bdm_p"].loc[:, cols]
df3 = bases["w3_bdm_p"].loc[:, cols]
df = (df1.join(df2.set_index('folio_unico'), how="left", on="folio_unico", lsuffix="_00", rsuffix="_01")
         .join(df3.set_index("folio_unico"), how="left", on="folio_unico", lsuffix="", rsuffix="_02")
     )


# Corregir uso de conceptos Tabla / base de datos
# Corregir el sufijo: W1, W2 (...) etc
# Corregir el "folio_unico" debe ir como tercera columna

# In[11]:


df.head()
#df.to_csv('ejemplo1.csv', sep=',', encoding='utf-8', index = False)


# # Pregunta n°2:

# #### Base con informacion de Brisas del Mar, que para cada sujeto contenga las variables H3, SA1, SD1. SD2, MC1 (de la línea de base) y las preguntas P1, P11 y P15 (de las 3 olas). Las olas se agregan como columnas.

# In[11]:


cols2 = ["H3", "SA1", "SD1", "SD2", "MC1", "folio_unico"]


# In[12]:


dfa = bases["04_bmw1encu"].loc[:, ["folio_vivienda"] + cols2]
dfb = bases["00_bmw1pauta"].loc[:, cols]
dfc = bases["01_bmw2pauta"].loc[:, cols]
dfd = bases["02_bmw3pauta"].loc[:, cols]

#dfb1 = (dfa.join(dfb.set_index('folio_unico'), how="left", on='folio_unico', lsuffix="_00", rsuffix="_01")
#         .join(dfc.set_index('folio_unico'), how="left", on='folio_unico', lsuffix="", rsuffix="_02")
#         .join(dfd.set_index('folio_unico'), how= "left", on='folio_unico', lsuffix = "", rsuffix= "_03")
#      )
     


# In[13]:


dfb1 = (df.join(dfa.set_index('folio_vivienda'), how="left", on='folio_vivienda', lsuffix="", rsuffix="_01a")
       )


# In[14]:


dfb1.head()
#dfb1.to_csv('ejemplo2.csv', sep=',', encoding='utf-8', index = False)


# Dentro de cada folio de vivienda integre los otros
# Probar pegar DF con las bases

# ### 2 errores a corregir:
# * cambiar los float por int (ej: 1.0 a 1) en los csv (para lectura)
# * Averiguar por qué al crear el dfb1 "cols" deja de tomar los valores o no los identifica
# 

# # Pregunta n°3:

# #### Base con información de Brisas del Mar y Marta Brunet de la pauta de observación, todas las preguntas. Las olas y villas se agregan como filas. 

# In[16]:


df7 = bases["00_bmw1pauta"].loc[:, ["folio_villa", "folio_vivienda"] + cols]
df8 = bases["03_mbw1pauta"].loc[:, cols]
#dfc = (df7.join(df8.set_index('folio_unico'), how="left", on="folio_unico", lsuffix="_00", rsuffix="_01")
#      )


# In[17]:


result = pd.concat([df7, df8], sort = True) #ok


# In[18]:


result.head()


# In[16]:


result.to_csv('ejemplo3.csv', sep=',', encoding='utf-8', index = False)


#     

# ###### P1_2 solo debería estar en bm1 
# Probar el concat con variables específicas y forzar poner variables diferentes (ver error) 
# Ordenar las variables (folios al principio)

# In[24]:


def count_col(df, col):
    return df[col].count()


# In[32]:


count_H3 = count_col(dfb1, 'SD1')


# In[41]:


df["P1_1"].fillna(0).astype('int').head() 

df["P1_1"] = df["P1_1"].astype('int') 


# In[ ]:




