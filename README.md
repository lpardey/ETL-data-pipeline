# Celes Challenge

Este repositorio contiene la solución a la prueba técnica para el equipo de Data Engineers en Celes.  
La prueba técnica se compone de cinco partes cuyo objetivo es demostrar las habilidades específicas en el manejo y procesamiento de datos a gran escala utilizando Python y Google Cloud Platform.

## Contenido

1. [Parte 1: Preparación de Datos Sintéticos](#parte-1-preparación-de-datos-sintéticos)
2. [Parte 2: Carga y Transformación de Datos](#parte-2-carga-y-transformación-de-datos)
3. [Parte 3: Almacenamiento en Google Cloud Storage](#parte-3-almacenamiento-en-google-cloud-storage)
4. [Parte 4: Lectura y Agregación de Datos](#parte-4-lectura-y-agregación-de-datos)
5. [Parte 5: Documentación y Arquitectura](#parte-5-documentación-y-arquitectura)
6. [Conclusiones](#conclusiones)


## Parte 1: Preparación de Datos Sintéticos

En esta etapa, se generó un dataset sintético de 10 millones de registros. Los datos incluyen los siguientes campos: id_cliente, fecha_de_transacción, cantidad_de_venta, categoria_de_producto y region_de_venta.

Se utilizan las librerías `numpy` y `pandas` para facilitar la generación y manejo de los datos, además de `pydantic` para estructurar parte de la información con validación de tipado. Dada la cantidad de registros a producir, se optó separar su generación en lotes de 1 millón de registros para optimizar el uso de memoria y mejorar la eficiencia. Cada lote se almacena en un DataFrame de pandas. Una vez generados todos los lotes, los DataFrames se concatenan en un único DataFrame final, que luego se guarda en un archivo CSV. La configuración del programa se maneja a través de una clase Config que permite definir parámetros esenciales como el número de registros, el tamaño de los lotes, y las categorías de datos. 

## Parte 2: Carga y Transformación de Datos

Se desarrolló un script para leer los datos generados desde archivos planos (CSV) y transformarlos al formato Parquet. Para este proceso, se utilizó `pyarrow` para el tratamiento general del archivo y `argparse` para definir y parsear argumentos de línea de comandos. El script permite especificar varios parámetros a través de argumentos de línea de comandos, tales como el archivo de entrada, el destino de salida, las columnas de partición, y si se debe forzar la sobreescritura de archivos existentes.

Ejemplo de uso:
```
python -m transform_load_to_parquet.main.py mi_dataset.csv -o mi_parquet_output -p region_de_venta -f
```

## Parte 3: Almacenamiento en Google Cloud Storage

Se implementó un script que permite la carga de archivos en formato Parquet a un bucket en Google Cloud Storage (GCS). Para esto, se configuró una cuenta en Google Cloud Platform y se utilizó la librería `google-cloud-storage` para subir los archivos al bucket. El script permite la creación automática del bucket si este no existe, y, de acuerdo con el contenido del directorio de entrada, gestiona la carga de un archivo único o múltiples archivos de manera concurrente.

Ejemplo de uso:
```
python -m upload_to_google_cloud.main mi_parquet_output -n mi_bucket -w 8
```

## Parte 4: Lectura y Agregación de Datos

En esta sección se implementaron tres scripts para leer los archivos Parquet almacenados en GCP y realizar operaciones de ordenamiento y agregación de datos, como sumas totales por categoría de producto y promedios de ventas por región. `Pandas`, `Dask` y `Pyspark` fueron la librerías escogidas para cada implementación.

## Parte 5: Documentación y Arquitectura

Se diseñó una mini arquitectura y un diagrama explicativo para ilustrar cómo los servicios de GCP se integran para completar la tarea.

### Diagrama de Arquitectura

![Diagrama de Arquitectura](/resources/Mini%20arquitectura%20GCP.png)

Este diseño de arquitectura utiliza componentes disponibles en GCP para gestionar grandes volúmenes de datos. En las secciones anteriores de la prueba, la información se genera como un dataset que se transforma a formato parquet, para  luego, a través de scripts con diferentes librerías (pandas, pyarrow, pyspark y dask), gestionar la transformación y consulta, y su almacenamiento en GCS. En un caso de uso más realista la fuente de datos sería un stream de eventos proveniente del backend de la aplicación (datos de ventas en tiempo real), aunque también se podrían cargar datos en batch (en este caso se asume desde GCS). 

Para procesar estos orígenes de datos se utilizaría Dataflow con un pipeline para los datos en streaming y otro pipeline para los datos en batch. El primero, consumiría el stream de datos en tiempo real agregando múltiples eventos en un mismo batch, con el fin de reducir el número de llamadas a BigQuery. El segundo comprobaría periodicamente si hay archivos nuevos en el bucket de GCS, procesando los últimos archivos añadidos.

En ambos casos, se escriben los datos validados y procesados a una tabla en BigQuery, que actuaría como datawarehouse, permitiendo la lectura y agregación de datos de forma eficiente.

Finalmente, los resultados se consultan desde alguna herramienta que permita su análisis y visualización, en este caso Looker Studio.

### Seguridad

Para asegurar la arquitectura propuesta, se consideraron las siguientes políticas de seguridad:

- **IAM Roles**: Asignación de roles de acceso mínimo necesario para los servicios.
- **Buckets Privados**: Restricción de acceso público a los buckets de almacenamiento.
- **Cifrado de Datos**: Uso de claves de cifrado gestionadas por el cliente para proteger los datos almacenados. Idealmente, los datos, tanto en tránsito como en reposo, deberían estar encriptados.
