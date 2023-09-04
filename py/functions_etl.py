# Databricks notebook source
# MAGIC %md
# MAGIC # Importar Funciones

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

import pandas as pd
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

print("Pandas v" + pd.__version__)
print("Spark v" + spark.sparkContext.version)
print (sys.version)

# COMMAND ----------

# MAGIC %md
# MAGIC Función para leer un archivo parquet del datalake de acuerdo con la ubicación indicada por parámetro.

# COMMAND ----------

def read_df_from_container(secret_scope, param_key_datalake, blob_account_name, blob_container_name, folder_path, table_name, parquet_file_extension):
    try:
        key_datalake = dbutils.secrets.get(scope=secret_scope, key=param_key_datalake)

        #blob_account_name = "dlsdataplatformclsdev"
        #blob_container_name = "silver"
        blob_relative_path = folder_path + table_name
        # blob_relative_path = "DATOS_GENERALES_2.csv"
        blob_sas_token = key_datalake

        wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
        spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
        print('Remote blob path: ' + wasbs_path)

        # Create an empty RDD
        emp_RDD = spark.sparkContext.emptyRDD()

        # Create empty schema
        columns = StructType([])

        # Create an empty RDD with empty schema
        df_parquet = spark.createDataFrame(data = emp_RDD,
                                     schema = columns)
        try:
            df_parquet = spark.read.option("header","true").parquet(wasbs_path + parquet_file_extension)
        except AnalysisException as err:
            raise Exception('No existe el dataframe {} en el container {}. Msg error {}'.format(table_name, blob_container_name, err))
            # exit('job failed!')
            # dbutils.notebook.exit('job failed!')
            
        return df_parquet
    except Exception as ex:
        raise Exception("Fallo el método 'read_df_from_container'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para leer una tabla pasada por parámetro de la base de datos de dominios.

# COMMAND ----------

def read_control_database_table(secret_scope, param_key_az_sql, jdbcHostname, jdbcPort, jdbcDatabase, user, schema, table):
    try:
        password = dbutils.secrets.get(scope=secret_scope, key=param_key_az_sql)

        driver ="com.microsoft.sqlserver.jdbc.SQLServerDriver"

        properties = {
            "user":user,
            "password":password,
            "driver" :driver
        }

        url = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"

        # Query
        pushdown_query = f"(select * from {schema}.{table})  t"
        #Read from SQL database
        df = spark.read.jdbc(url=url, table=pushdown_query, properties=properties)

        return df
    except Exception as ex:
        raise Exception("Fallo el método 'read_control_database_table'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC # Funciones Generales

# COMMAND ----------

# MAGIC %md
# MAGIC Función para generar un mapeo clave valor con las columnas especificadas por parámetro de un dataframe.

# COMMAND ----------

def dict_columns(df, key, value):
    try:
        # Declare an empty Dictionary
        map_table = {}

        # Convert PySpark DataFrame to Pandas 
        # DataFrame

        if value == "":
            df_list_ent_pd = df.select(key).toPandas()
            for index, row in df_list_ent_pd.iterrows():

                map_table[row[key]] = value
        else:
            df_list_ent_pd = df.select(key, value).toPandas()

            for index, row in df_list_ent_pd.iterrows():

                map_table[row[key]] = row[value]

        return map_table
    except Exception as ex:
        raise Exception("Fallo el método 'dict_columns'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para generar un mapeo clave valor con las columnas especificadas por parámetro de un dataframe.

# COMMAND ----------

def map_columns_into_dict(df_param, key, value):
    try:
        # Declare an empty Dictionary
        map_columns = {}

        # Convert PySpark DataFrame to Pandas 
        # DataFrame
        df_map_columns_pd = df_param.select(key, value).drop_duplicates().toPandas()

        for index, row in df_map_columns_pd.iterrows():
            map_columns[row[key]] = row[value]
        return map_columns
    except Exception as ex:
        raise Exception("Fallo el método 'map_columns_into_dict'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función equivalente en Pandas para generar un mapeo clave valor con las columnas especificadas por parámetro de un dataframe.

# COMMAND ----------

def map_columns_into_dict_pd(df_param, key, value):
    try:
        # Declare an empty Dictionary
        map_columns = {}

        # Convert PySpark DataFrame to Pandas 
        # DataFrame
        #df_map_columns_pd = df_param.select(key, value).drop_duplicates().toPandas()
        df_map_columns_pd = df_param[[key, value]].drop_duplicates()

        for index, row in df_map_columns_pd.iterrows():
            map_columns[row[key]] = row[value]
        return map_columns
    except Exception as ex:
        raise Exception("Fallo el método 'map_columns_into_dict_pd'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para generar listas de valores únicos recibiendo por parámetro el dataframe y la columna a aplicar.

# COMMAND ----------

def generate_list(df, field_name, sort_col=None):
    try:
        # Declare an empty List
        list_values = []

        if not df.isEmpty():
            if sort_col is not None:
                df_parametric_pd = df.filter(col(field_name).isNotNull()).select(field_name, sort_col).dropDuplicates() \
                    .toPandas().sort_values(sort_col)
            else:
                df_parametric_pd = df.filter(col(field_name).isNotNull()).select(field_name).dropDuplicates() \
                    .toPandas()

            # Con este bucle creamos una lista con los campos
            for index, row in df_parametric_pd.iterrows():
                list_values.append(df_parametric_pd[field_name][index])
        return list_values
    except Exception as ex:
        raise Exception("Fallo el método 'generate_list'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función equivalente en Pandas para generar listas de valores únicos recibiendo por parámetro el dataframe y la columna a aplicar.

# COMMAND ----------

def generate_list_pd(df, field_name):
    try:
        return df[field_name].values.tolist()
    except Exception as ex:
        raise Exception("Fallo el método 'generate_list'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para crear estructura de un dataframe vacío recibiendo como parámetro el dataframe

# COMMAND ----------

def create_empty_dataframe(dict_col):
    try:
        columns = StructType()
        for key in dict_col:
            data_type = StringType()
            if dict_col[key] == 'Text':
                data_type = StringType()
            elif dict_col[key] == 'Number':
                data_type = IntegerType()
            elif dict_col[key] == 'DateTime':
                data_type = DateType()

            columns = columns.add(key, data_type)

        emptyRDD = spark.sparkContext.emptyRDD()

        df_json_mds = spark.createDataFrame(data = emptyRDD, schema = columns)

        return df_json_mds
    except Exception as ex:
        raise Exception("Fallo el método 'create_empty_dataframe'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para escribir un dataframe de spark en el formato especificado por parámetro. Adicional, recibe como parametros los datos de conexión del datalake y la ruta de escritura.

# COMMAND ----------

def write_file(df, format_file, param_key_datalake, secret_scope, storage_account_name, container, folder_path, file_name, extent):
    try:
        # Guardar nuevo dataset estandarizado en container Gold

        key_datalake = dbutils.secrets.get(scope=secret_scope, key=param_key_datalake)

        # Establecer secreto del keyvault con el password del datalake
        spark.conf.set(
          "fs.azure.account.key.{}.dfs.core.windows.net".format(storage_account_name),
          key_datalake
        )

        # Crear ruta para escribir nuevo parquet
        dir_write = "abfss://{}@{}.dfs.core.windows.net/{}".format(container, storage_account_name, folder_path)
        name_table_file = file_name + extent

        path_file_part = "{}/{}".format(dir_write, file_name)

        try:
            # Se guarda archivo con una sola partición
            if format_file == "csv":
                df.write.option("header", "true").csv(path_file_part)
            else:
                df.repartition(1).write.format(format_file).mode("overwrite").save(path_file_part)
        except Exception as err:
            raise Exception("No se pudo escribir el archivo. Msg error {}".format(err))

        # Obtener lista de archivos que se crearon en la carpeta donde queda guardado el dataset en formato parquet
        list_files = dbutils.fs.ls(path_file_part)
        print(list_files)

        # Copiar archivo con nombre en formato part-xxxxx.extension a su nombre original en la ubicación gold/ParquetFiles/1-CONSULTORES_LTDA/PROVEEDORES
        path_file = "{}/{}".format(dir_write, name_table_file)
        for variable in list_files:
            if variable[1].endswith(extent):
                dbutils.fs.cp(variable[0], path_file, True)

        # Remover archivo temporal que contiene la carpeta con las particiones del archivo
        dbutils.fs.rm(path_file_part, True)
    except Exception as ex:
        raise Exception("Fallo el método 'write_file'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para escribir un string con una estructura JSON en la ubicación especificada por parámetro dentro del datalake.

# COMMAND ----------

def write_string_json(secret_scope, param_key_datalake, container_gold, storage_account_name, str_json, path, name_table_file):
    try:
        # Guardar nuevo dataset estandarizado en container Gold
        key_datalake = dbutils.secrets.get(scope=secret_scope, key=param_key_datalake)

        # Establecer secreto del keyvault con el password del datalake
        spark.conf.set(
          "fs.azure.account.key.{}.dfs.core.windows.net".format(storage_account_name),
          key_datalake
        )

        # Crear ruta para escribir nuevo parquet
        dir_write = "abfss://{}@{}.dfs.core.windows.net/{}/".format(container_gold, storage_account_name, path)

        dbutils.fs.put(dir_write + name_table_file, str_json, True)
    except Exception as ex:
        raise Exception("Fallo el método 'write_string_json'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Esta función recibe por parámetro una lista con diccionarios (equivalentes a un JSON) para escribirla en diferentes archivos .json o particiones en la ubicación especificada dentro del datalake.

# COMMAND ----------

def write_partitioned_file(json_path, partition_size, list_rows, file_name, secret_scope, param_key_datalake, container_gold, storage_account_name):
    try:
        # Crear lista para particionar por filas la tabla con los golden record generados:
        list_data = []
        print(list_data)
        for i in range(0, partition_size):
            list_data.append([])
        print(list_data)

        for i in range(0, len(list_rows)):
            # print("i % partition_size: {} i: {}".format(i % partition_size, i))
            # Adjuntar a cada sublista los datos correspondientes a la partición
            list_data[i % partition_size].append(list_rows[i])

        for i in range(0, len(list_data)):
            if len(list_data[i]) != 0:
                # Convertir listas y diccionarios de Python en un string
                str_item = json.dumps(list_data[i])

                # Comprobamos que el tipo efectivamente sea string
                #print(type(str_item))

                name_table_file = "{}_part_{}.json".format(file_name, i)

                write_string_json(secret_scope, param_key_datalake, container_gold, storage_account_name, str_item, json_path, name_table_file)
            
    except Exception as ex:
        raise Exception("Fallo el método 'write_partitioned_file'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC # Proceso para leer JSON

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construimos funciones para leer los JSON descargados por el API de MDS debido a que vienen particionados y toca darle formato a la estructura

# COMMAND ----------

# MAGIC %md
# MAGIC Función de lectura de un archivo .json dada una ruta de lectura, nombre del archivo y parámetros de conexión al datalake.

# COMMAND ----------

def read_json_mds(secret_scope, param_key_datalake, blob_account_name, blob_container_name, json_path, json_file_name, extent_json):
    try:
        # Leemos el secreto de la conexión al datalake
        key_datalake = dbutils.secrets.get(scope=secret_scope, key=param_key_datalake)

        # blob_account_name = "dlsdataplatformclsdev"
        # blob_container_name = "silver"
        # blob_relative_path = "JSONFiles/D_usuario/detalle_organizacion/D_usuario-detalle_organizacion__page-1.json"
        blob_relative_path = json_path + json_file_name + extent_json
        blob_sas_token = key_datalake

        wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
        spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
        print('Remote blob path: ' + wasbs_path)

        df_json = spark.read.format("json") \
                                 .option("inferSchema", "true") \
                                 .option("multiLine", "true") \
                                 .load(wasbs_path)
        return df_json
    except Exception as ex:
        raise Exception("Fallo el método 'read_json_mds'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Listar archivos JSON de una ubicación específica en el datalake dentro del container Silver.

# COMMAND ----------

def generate_list_part_json_file(secret_scope, param_key_datalake, blob_account_name, blob_container_name, json_path):
    try:
        key_datalake = dbutils.secrets.get(scope=secret_scope, key=param_key_datalake)

        # Establecer secreto del keyvault con el password del datalake
        spark.conf.set(
          "fs.azure.account.key.{}.dfs.core.windows.net".format(blob_account_name),
          key_datalake
        )

        # Obtener lista de archivos que se crearon en la carpeta donde queda guardado el dataset en formato parquet
        path = "abfss://{}@{}.dfs.core.windows.net/{}".format(blob_container_name, blob_account_name, json_path)

        list_files = []
        try:
            list_files = dbutils.fs.ls(path)
        except Exception as ex:
            print("Advertencia no existe el archivo: ", ex)

        return list_files
    except Exception as ex:
        raise Exception("Fallo el método 'generate_list_part_json_file'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para estructurar en un dataframe en una tabla legible para spark (esta función de momento solo sirve con la estructura con las que el método "members" del API genera los JSON con las entidades).

# COMMAND ----------

def struct_json(df_json, list_columns, entity_name_mds):
    try:
        df_json_struct = df_json.withColumn("datos", explode(df_json.members))

        for item in list_columns:
            try:
                df_json_struct = df_json_struct.withColumn(item, col("datos").getItem(item))
            except AnalysisException as er:
                df_json_struct = df_json_struct.withColumn(item, lit(None))
                # Esta excepción puede saltar debido a que es posible que una columna de la tabla paramétrica no exista en el archivo, en este caso se deja como NULL, pero se muestra advertencia
                print("Advertencia: No coincide la estructura de la tabla paramétrica con el archivo JSON")

        df_json_struct = df_json_struct.select(*list_columns)

        member_count = df_json.select("member_count").dropDuplicates().collect()[0][0]
        print("Total registros dicha por archivo (página): " + str(member_count))
        print("Total conteo registros de página: " + str(df_json_struct.count()))
        if member_count != df_json_struct.count():
            raise Exception("Excepción, archivo corrupto no coincide la cantidad de registros leída con la que se debería recibir. Entidad: {}".format(entity_name_mds))

        return df_json_struct
    except Exception as ex:
        raise Exception("Fallo el método 'struct_json'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función que lee una serie de archivos particionados en formato JSON para crear un dataframe de Pyspark equivalente. Recibe por parámetros la ruta de lectura, el nombre de la entidad y los parámetros de conexión al datalake.

# COMMAND ----------

def generate_df_mds(df_param_mds, secret_scope, param_key_datalake, blob_account_name, blob_container_name, json_path, entity_name):
    try:
        folder_path_json = json_path + entity_name + "/"
        list_files = generate_list_part_json_file(secret_scope, param_key_datalake, blob_account_name, blob_container_name, folder_path_json)

        dict_col = dict_columns(df_param_mds, "nombre_atributo_mds", "tipo_dato_mds")
        df_all_json = create_empty_dataframe(dict_col)


        tot_files = len(list_files)
        print("Total archivos en container: " + str(tot_files))

        if tot_files == 0:
            raise Exception("No existe información en el container '{}' en la ruta '{}' para la entidad de MDS '{}'".format(blob_container_name, folder_path_json, entity_name))

        for variable in list_files:
            arr_json_file = variable[1].split('.')
            json_file = arr_json_file[0]
            extent_json = arr_json_file[1]
            df_json = read_json_mds(secret_scope, param_key_datalake, blob_account_name, blob_container_name, folder_path_json, json_file, '.' + extent_json)

            # Comprobar si el total de páginas que se encuentra registrado en un archivo JSON coincide con el
            #  total de archivos existentes en la carpeta
            total_pages = df_json.select("total_pages").dropDuplicates().collect()[0][0]
            print("Total paginas dicha por documento: " + str(total_pages))
            if total_pages != tot_files:
                msg = "Excepción, la cantidad de archivos JSON existentes no coincide con la cantidad esperada. Entidad: '{}'. Esperados: '{}'. Existen en el dl: '{}'".format(entity_name, tot_files, total_pages)
                raise Exception(msg)
                # dbutils.notebook.exit(msg)


            df_json = struct_json(df_json, df_all_json.columns, entity_name)

            df_all_json = df_all_json.union(df_json)

        # Añadir columna adicional para identificar la fuente

        # Fuente 4: SQL Server
        df_all_json = df_all_json.withColumn('id_fuente', lit(4))
        return df_all_json
    except Exception as ex:
        raise Exception("Fallo el método 'generate_df_mds'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para enumerar registros de un dataframe.

# COMMAND ----------

def row_number_df(df, sort_criteria):
    try:
        window = Window.orderBy(col(sort_criteria).desc())
        df_with_consecutive_increasing_id = df.withColumn('id_registro', row_number().over(window))
        return df_with_consecutive_increasing_id
    except Exception as ex:
        raise Exception("Fallo el método 'row_number_df'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para enumerar registros por partición y un criterio de ordenación, por defecto el criterio de orden es descendente.

# COMMAND ----------

def row_number_by_attribute(df, col_partition, sort_criteria):
    try:
        window = Window.partitionBy(col(col_partition)).orderBy(col(sort_criteria).desc())
        df_with_consecutive_increasing_id = df.withColumn('id_registro', row_number().over(window))
        return df_with_consecutive_increasing_id
    except Exception as ex:
        raise Exception("Fallo el método 'row_number_by_natural_key'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para enumerar registros particionando por una lista de campos.

# COMMAND ----------

def row_number_by_list_partition(df, lists_columns, sort_criteria, type_order='DESC'):
    try:
        if type_order == 'DESC':
            window = Window.partitionBy(lists_columns).orderBy(col(sort_criteria).desc())
        elif type_order == 'ASC':
            window = Window.partitionBy(lists_columns).orderBy(col(sort_criteria).asc())
        else:
            window = Window.partitionBy(lists_columns).orderBy(col(sort_criteria).asc())
            
        df_with_consecutive_increasing_id = df.withColumn('rn', row_number().over(window))
        return df_with_consecutive_increasing_id
    except Exception as ex:
        raise Exception("Fallo el método 'row_number_by_list_partition'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para quitar duplicados en un dataframe de Pandas solo por una columna específica manteniendo el último registro.

# COMMAND ----------

def drop_duplicates_by_column_pd(df_f_usuario, name_col):
    df_uniques = df_f_usuario.drop_duplicates(subset=name_col, keep="last")
    return df_uniques

# COMMAND ----------

# MAGIC %md
# MAGIC Función para quitar duplicados en un dataframe de Pyspark solo por una columna específica.

# COMMAND ----------

def drop_duplicates_by_column(df_f_usuario, name_col):
    df_uniques = df_f_usuario.dropDuplicates([name_col])
    return df_uniques

# COMMAND ----------

# MAGIC %md
# MAGIC Función para filtar la primera fila de un dataframe de Spark.

# COMMAND ----------

def first_row(df):
    df_f_first_row = row_number_by_attribute(df, df.columns[0], df.columns[0]) \
        .filter(col("id_registro") == 1)
    return df_f_first_row

# COMMAND ----------

# MAGIC %md
# MAGIC Función para filtar la primera fila de un dataframe de Pandas.

# COMMAND ----------

def first_row_pd(df):
    return df.head(1)
