# Pandas para crear nuevas tablas
![Pandas](https://cdn-images-1.medium.com/max/800/1*bxWjusjpwm3PHP4q6lWnQQ.png)


*Este tutorial tiene como objetivo sintetizar y explicar las tres formas de realizar consultas a bases relacionales utilizando la librería Pandas en Python (>3.6). Merge, Concatenate y Join.
Encontrarán de forma sencilla ejemplos de las tres funciones que utilizaremos pero se sugiere encarecidamente revisar la documentación oficial de la página adjunta al final de esta publicación.*

Pandas nos facilita la vida en muchos aspectos cuando se trata de trabajar con Series y DataFrames en cuanto a las formas de vincular y relacionarles a fin de obtener bases relacionales desde nuestras diferentes fuentes.

## Concatenar tablas : concat() 

La función concat () se encarga de realizar operaciones de concatenación en función de un eje único. Así también podemos aplicar lógica de conjuntos (unión o intersección) de los índices.
Antes de sumergirnos en todos los detalles de concat y lo que puede hacer, aquí hay un ejemplo simple:


~~~~
df1 = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
                    'B': ['B0', 'B1', 'B2', 'B3'],
                    'C': ['C0', 'C1', 'C2', 'C3'],
                    'D': ['D0', 'D1', 'D2', 'D3']},
                    index=[0, 1, 2, 3])

df2 = pd.DataFrame({'A': ['A4', 'A5', 'A6', 'A7'],
                    'B': ['B4', 'B5', 'B6', 'B7'],
                    'C': ['C4', 'C5', 'C6', 'C7'],
                    'D': ['D4', 'D5', 'D6', 'D7']},
                    index=[4, 5, 6, 7])

df3 = pd.DataFrame({'A': ['A8', 'A9', 'A10', 'A11'],
                    'B': ['B8', 'B9', 'B10', 'B11'],
                    'C': ['C8', 'C9', 'C10', 'C11'],
                    'D': ['D8', 'D9', 'D10', 'D11']},
                    index=[8, 9, 10, 11])

frames = [df1, df2, df3]
result = pd.concat(frames)


print(result)

~~~~
El resultado que obtendremos mediante el `print(result)`es la imagen a continuación 
![Output](https://pandas.pydata.org/pandas-docs/version/0.23.0/_images/merging_concat_basic.png)

En el ejercicio presentado lo primero que podemos observar es que los DataFrame (df) son homogeneos y con un único eje reconocible (mediante el cual se relacionan). Pero en para todos aquellos casos en los que no se trate de df's homogeneos tenemos una serie de Argumentos (args) editables con los que podemos especificar preferencias.
La args en la función concat() presenta la siguiente configuración por default:

~~~~
pd.concat(df's, axis=0, join='outer', join_axes=None, ignore_index=False,
          keys=None, levels=None, names=None, verify_integrity=False,
          copy=True)
~~~~
[Se recomienda siempre chequear los valores por default]

* `df's` : serán los objetos mapeados (series, dfs o panel pero en nuestro caso df's). El comportamiento es mas sencillo cuando se pasa un dictado (dict{key:value}) como en el caso de nuestro ejemplo ya que las key's seran utilizadas como el identificador a menos que se establezca otro criterio dentro de los args. Se manifiesta la observación que los objetos None no serán considerados a no ser que todos sean None y habr un ValueError ya que estaríamos concatenando un puñado de nada.   

* `axis` : Será el eje por el que se concatenarán los df's. Como ya sabemos en Python el primer elemento siempre se considera en la posición [0] cuando contamos de izquierda a derecha ([0, 1, 2,...]) y [-1] si contamos de derecha a izquierda ([..., -3, -2, -1]). Si ese ocupa el valor default (0) el segundo DF se unirá abajo del primer DF. Si el valor es diferente (1) el segundo df se unirá a la derecha del primero. 

* `join`: {‘inner’, ‘outer’} Por default 'outer' que implica union de los df's y 'inner' implica intersección. Lo que podrá ser util eventualmente pero por el momento innecesario considerando que únicamente queremos juntar bases.

* `ignore_index`: boolean (variable dicotómica True/False). Por default False, este args es útil en la medida que los df's no tengan un eje de concatenación relevante ya que al pasar el valor a True generará que ignore los valores de indice de los df's y comience a etiquetar [0, 1, 2 ...etc].

* `join_axes`: Lista de (objetos) índices. Estos son índices especficos que se utilizan para los otros ejes n-1 en lugar de utilizar la lógica inner/outer join [es para utilizar otros ejes especficos].

* `keys`: [por defecto None] Crea el índice jerárquico utilizando las 'claves' seleccionadas como el nivel más externo.

* `levels`: [Lista de secuencias, por defecto None] Niveles especificos para construir un multi index, de lo contrario se infiere desde las keys.

* `names`: [Lista por defecto None] Nombres para los niveles en el indice jerarquico resultante.

* `verify_integrity`: [Boolean por defecto False] Verifica si el nuevo eje concatenado posee duplicados.

* `copy`: [Boolean por defecto True] Si es False no copia datos innecesariamente. 

En cuanto al uso de estos Argumentos es muy probable que no sean necesarios inicialmente, a no ser que comiencen a utilizar Python para el trabajo de analisis de datos. De todas formas podemos continuar con el ejemplo anterior. Supongamos que queremos vincular Keys especficas con cada una de las sub-partes del DataFrame, así utilizando las keys como argumentos: 

`result = pd.concat(frames, keys=['x', 'y', 'z'])`

![Output](https://pandas.pydata.org/pandas-docs/version/0.23.0/_images/merging_concat_keys.png)

Tal como se puede observar, el (objeto) Indice resultante es jerárquico. Esto significa que podemos seleccionar en especifico cada uno de las partes del DF:

~~~~
In :
`result.loc['y']` 

Out: 
A   B   C   D
4  A4  B4  C4  D4
5  A5  B5  C5  D5
6  A6  B6  C6  D6
7  A7  B7  C7  D7
~~~~

Otra forma de concatenar es `df1.append(df2)` el cual es menos especfica en sus Argumentos pero cumple con la función basica [concatenando con el eje 0], aunque también puede concatenar múltiples objetos tal cual se muestra en el siguiente ejemplo: 

~~~~

result = df1.append([df2, df3])

~~~~

![Output](https://pandas.pydata.org/pandas-docs/version/0.23.0/_images/merging_append3.png)

Siguiendo con `df.append()` podemos configurar que al momento de concatenar los DF's se ignore los indices de estos y establezca uno estandar para el nuevo subconjunto utilizando el argumento `ignore_index = True` [que tambien sirve con `concat()`] como en el siguiente ejemplo:

~~~~
result = df1.append(df4, ignore_index=True)
~~~~

![Output](https://pandas.pydata.org/pandas-docs/version/0.23.0/_images/merging_concat_ignore_index.png)

### Concatenar Series y DF's
Podemos concatenar Series y DF's como podremos observar en el ejemplo, en donde la Serie pasara a convertirse en parte del DF's resultante [nombrando la columna que corresponde a la Serie]:

~~~~

s1 = pd.Series(['X0', 'X1', 'X2', 'X3'], name='X') 

result = pd.concat([df1, s1], axis=1) 
~~~~

Siguiendo con el uso de los DF's de los ejemplos anteriores ahora podemos mezclar la Serie `s1` con el df1 (o cualquier otro)

![Output](https://pandas.pydata.org/pandas-docs/version/0.23.0/_images/merging_concat_mixed_ndim.png)

Si es que llegasen a pasar Series sin nombre el DF resultante las enumerara consecutivamente. También debemos recordar que si utilizamos `ignore_index = True` descartamos todos los nombres de referencia.

##### Podríamos profundizar en las variantes de `concat()` y `append()` pero para el uso que le daremos ya tenemos cubierta las necesidades

### Agregando fílas a un DataFrame

A veces no necesitamos incluir una tabla completa sino que únicamente uno o dos caso al DataFrame, para ello podemos utilizar Dict o Series (recordemos que son objetos diferentes a un DataFrame o tabla). Esto en particuar es algo que podríamos encontrarnos pero no necesariamente utilizaremos.

~~~~
s2 = pd.Series(['X0', 'X1', 'X2', 'X3'], index=['A', 'B', 'C', 'D'])

result = df1.append(s2, ignore_index=True)

s2 es la serie que será agregada al df1 (dataframe ya creado)
y en donde le señalamos que ignore los index de s2 a fin de que los datos 
sean encasillados en el index respectivo del df1.
~~~~

![Output](https://pandas.pydata.org/pandas-docs/stable/_images/merging_append_series_as_row.png)

### Funciones `merge()`y `join()`para unir tablas

La librería Pandas nos ofrece funciones simples, en particular `merge()` como punto de entrada para gestionar las uniones de dataframes (y/o Series) cuando éstas son normales, es decir cuando son uniones sencillas sin muchos condicionantes.
Para ello podemos utilizar una serie de Argumentos disponibles en la función a fin de especificar el resultado:

~~~~
pd.merge(left, right, how='inner', on=None, left_on=None, right_on=None,
         left_index=False, right_index=False, sort=True,
         suffixes=('_x', '_y'), copy=True, indicator=False,
         validate=None)
~~~~

* `left`: Corresponde al DF del lado izquierdo (o primero)
* `right`: Corresponde al DF del lado derecho (segundo)
* `how`: Típo de union en la que se determina cuales keys seran las utilizadas en el DF resultante. [Default `inner`]
  * `inner`: Usa la intersección de keys de ambos Df's
  * `outer`: Usa la union de keys de ambos Df's
  * `letf`: Usa unicamente las keys del DF izquierdo [un poco obvio]
  * `right`: Usa únicamente las keys del DF derecho
* `on`: Nombre de la columna(s) a unirse la cual debe encontrarse presente en los Df's
* `left_on`: Para utilizar las columnas o index del DF izquierdo como keys en la unión.
* `right_on`: Para utilizar las columnas o index del DF derecho como keys en la unión.
* `left_index`: Si es Verdadero(True), use el índice (etiquetas de fila) del DataFrame o Serie izquierdo como su (s) clave (s) de unión. En el caso de un DataFrame o Series con un MultiIndex (jerárquico), el número de niveles debe coincidir con el número de claves de unión del DataFrame o Series correcto.
* `right_index`: Lo mismo que el anterior pero considerando el Df derecho 
* `sort`: Ordena el DF resultante según las keys en orden lexicográfico. Por default True pero se recomienda declarar False a fin de no alterar el resultado esperado.
* `suffixes`: Sufijo que se acopla al nombre de las columnas. Lo hemos utilizado antes como lsuffix o rsuffix (_0, _1)
* `copy`: 
* `indicator`:
* `validate`:



















