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
