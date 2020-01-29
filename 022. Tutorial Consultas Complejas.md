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

* `axis` : Será el eje por el que se concatenarán los df's. Como ya sabemos en Python el primer elemento siempre se considera en la posición [0] cuando contamos de izquierda a derecha ([0, 1, 2,...]) y [-1] si contamos de derecha a izquierda ([..., -3, -2, -1]).

* `join`: {‘inner’, ‘outer’} Por default 'outer' que implica union de los df's y 'inner' implica intersección. Lo que podrá ser util eventualmente pero por el momento innecesario considerando que únicamente queremos juntar bases.

* `ignore_index`: boolean (variable dicotómica True/False). Por default False, este args es útil en la medida que los df's no tengan un eje de concatenación relevante ya que al pasar el valor a True generará que ignore los valores de indice de los df's y comience a etiquetar [0, 1, 2 ...etc].

* `join_axes`:   

