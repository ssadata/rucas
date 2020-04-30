#######################################################################

##### El presente scrip cumple con la función de convertir los archivos de SPSS (`.sav`) en CSV ######


# 1) Para cumplir dicho objetivo leemos desde el directorio de minio el cual tiene alojados los archivos `.sav` 
#[/home/ubuntu/Rucas/data/sav/]

# 2) Luego de eso debemos agregar el nombre del archivo que transformaremos [reemplazar `<filename>`]

# 3) Por último le asignamos nombre al archivo que guardaremos como `.csv` [reemplazar `<name>`]

import pyreadstat as prst
# 1) #################################################### 2)  #########
df0, meta = prst.read_sav('/home/ubuntu/Rucas/data/sav/base.sav')


############# 3)  #####################################################
df0.to_csv('/home/ubuntu/Rucas/data/csv/base.csv', sep=',', encoding='utf-8', index = False)


# En caso de no poder ejecutar este documento: http://ezcsv2sav.com/about/ " 