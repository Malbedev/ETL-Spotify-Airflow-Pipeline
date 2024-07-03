
# <h1 align="center">![Descripción](https://img.shields.io/badge/Spotify_'_s_"New_Releases"_:_ETL--airflow--pipeline--Project-20B2AA?style=for-the-badge)</h1> # :rocket: :rocket:


# ![Descripción](https://img.shields.io/badge/DESCRIPCION-7B9AE6?style=plastic) 

Este proyecto consiste en un Pipeline de un ETL utilizando lenguaje Python, Airflow , la API de Spotify y AWS Redshift para recopilar datos de los álbums, que se actualiza periódicamente según los últimos 50 lanzamientos subidos en dicha plataforma en todo el mundo.

Los componentes principales de este proyecto son las dos clases contructoras DataManager y Dataconn situadas en el Módulo __ETL_manager.py__, cada una de las cuales ejecuta a través de funciones los pasos de extracción, transformación, conexión con base de datos y carga de datos en la misma, respectivamente. 
Todo esto orquestado con funciones y tareas creadas desde un DAG de airflow en el archivo __Pipeline-ETL_Spotify.py__.


## ![La función Extract ](https://img.shields.io/badge/LA_FUNCION_EXTRACT-8A2BE2)*![ok ](https://img.shields.io/badge/data_exctract()-orange) #

La forma en que interactuamos con la API de Spotify es mediante el uso de la biblioteca __SPOTIPY__ y ayuda principalmente con la autenticación de la API.Una vez que se han extraído los datos, se almacenan en un diccionario de python.
Se puede conseguir las credenciales de la API de Spotyfy en el siguiente link
Generate your Spotify API access keys here: https://developer.spotify.com

## ![La función Transform ](https://img.shields.io/badge/LA_FUNCION_TRANSFORM-8A2BE2)*![ok ](https://img.shields.io/badge/data__trasnform()-orange) #

La función instancia y estructura en un Dataframe de __PANDAS__ los datos obtenidos en el proceso de Extracción; separa la totalidad de los datos de Spotify en conjuntos de datos lógicos para los datos de Album y Artista y se realiza un limpieza y trasformación de los mismos, según el caso.

## ![ok ](https://img.shields.io/badge/LA_FUNCION_LOAD-8A2BE2)*![ok ](https://img.shields.io/badge/data__upload()-orange) #

Después de que los datos del álbum, artista se hayan transformado y estructurados en nuestro Dataframe, a travez de la libreria __psycopg2__ primero realizamos la conexión con el motor de base de datos con la función __connect_Db()__, se crea la tabla correspondiente con la función __create_Table()__; se carga la información en dicha tabla en el Datawerehouse de __AWS-Redshift__, con __upload_data()__.


# ![Dependencias y librerías ](https://img.shields.io/badge/DEPENDENCIAS_Y_LIBRERIAS-7B9AE6?style=plastic) #

El proyecto esta montado en un contenedor de docker específico configurado para correr __Airflow__ , donde monatermos todos los 
requerimientos necesarios para que pueda funcionar el código en cualquier entorno.
La configuración esta descripta en el __DOCKERFILE__ y la plantilla __docker-compose.yaml__
Todas las librerías y dependencias necesiarias que utilizamos en el proyecto estan en el archivo __requirements.txt__
Simplemente situandonos en la carpeta del proyecto y escribiendo el comando *docker compose up --build*, daremos inicio a la construcción del contenedor con todas las configuraciones, dependencias y librerias necesarias para correr Nuestro DAG de tareas en airflow, expuesta su interfaz gráfica en el puerto *localhost:8080*.

