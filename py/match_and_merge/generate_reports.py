# Databricks notebook source
# MAGIC %md
# MAGIC # Importar Funciones Personalizadas

# COMMAND ----------

# MAGIC %run /Repos/transform_data_mdm/DatosMaestrosMDSData_Transform/functions_etl

# COMMAND ----------

# MAGIC %md
# MAGIC # Importar funciones de Python

# COMMAND ----------

# MAGIC %md
# MAGIC Instalamos las librerías que necesitamos

# COMMAND ----------

!pip install fuzzywuzzy
!pip install python-Levenshtein

# COMMAND ----------

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from pyspark.sql.window import *
from pyspark.sql.types import *

from datetime import datetime

# COMMAND ----------

# Leer parámetros
dbutils.widgets.text("filialName", "","")
filial_name = dbutils.widgets.get("filialName")
dbutils.widgets.text("subdomain", "","")
subdomain = dbutils.widgets.get("subdomain")
dbutils.widgets.text("modelNameMDS", "","")
model_name_mds = dbutils.widgets.get("modelNameMDS")
dbutils.widgets.text("entityNameMDS", "","")
entity_name_mds = dbutils.widgets.get("entityNameMDS")
dbutils.widgets.text("finalPath", "","")
final_path = dbutils.widgets.get("finalPath")
dbutils.widgets.text("readingPath", "","")
reading_path = dbutils.widgets.get("readingPath")
dbutils.widgets.text("lenAccuracy", "","")
len_accuracy = int(dbutils.widgets.get("lenAccuracy"))
dbutils.widgets.text("reportExtension", "","")
report_extension = dbutils.widgets.get("reportExtension")
dbutils.widgets.text("parquetFileExtension", "","")
parquet_file_extension = dbutils.widgets.get("parquetFileExtension")
dbutils.widgets.text("paramKeyDatalake", "","")
param_key_datalake = dbutils.widgets.get("paramKeyDatalake")
dbutils.widgets.text("paramKeyAzSql", "","")
param_key_az_sql = dbutils.widgets.get("paramKeyAzSql")
dbutils.widgets.text("hostName", "","")
host_name = dbutils.widgets.get("hostName")
dbutils.widgets.text("port", "","")
port = int(dbutils.widgets.get("port"))
dbutils.widgets.text("dbName", "","")
db_name = dbutils.widgets.get("dbName")
dbutils.widgets.text("usrName", "","")
usr_name = dbutils.widgets.get("usrName")
dbutils.widgets.text("schema", "","")
schema = dbutils.widgets.get("schema")
dbutils.widgets.text("secretScopeDatabricks", "","")
secret_scope = dbutils.widgets.get("secretScopeDatabricks")
dbutils.widgets.text("storageAccountName", "","")
storage_account_name = dbutils.widgets.get("storageAccountName")
dbutils.widgets.text("container", "","")
container = dbutils.widgets.get("container")


print(filial_name)
print(subdomain)
print(model_name_mds)
print(entity_name_mds)
print(final_path)
print(reading_path)
print(len_accuracy)
print(report_extension)
print(parquet_file_extension)
print(param_key_datalake)
print(param_key_az_sql)
print(host_name)
print(port)
print(db_name)
print(usr_name)
print(schema)
print(secret_scope)
print(storage_account_name)
print(container)

# COMMAND ----------

df_parametric = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_matriz_dato")
#display(df_parametric)

# COMMAND ----------

df_f_parametric = df_parametric.filter((col("nombre_filial") == filial_name) & (col("nombre_modelo") == model_name_mds) \
                                         & (col("golden_record") == "Y") & (col("subdominio") == subdomain))
#display(df_f_parametric)

# COMMAND ----------

# MAGIC %md
# MAGIC Filtramos para obtener solo los registros de la entidad (de MDS) que recibimos por parámetro

# COMMAND ----------

df_parametric_ent = df_f_parametric.filter((col("nombre_entidad_mds") == entity_name_mds))
#display(df_parametric_ent)

# COMMAND ----------

# MAGIC %md
# MAGIC Leeemos los datos de la tabla tbl_mds_dominios (es la información que devuelve el método getDomain del API de MDS).

# COMMAND ----------

df_domains = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_mds_dominios")
#display(df_domains)

# COMMAND ----------

df_f_domains = df_domains.filter(col("nombre_dominio") == model_name_mds)
#display(df_f_domains)

# COMMAND ----------

if df_f_domains.count() == 0:
    raise Exception("No existe el modelo de MDS con nombre: {}".format(model_name_mds))

# COMMAND ----------

# MAGIC %md
# MAGIC # Controles de excepciones

# COMMAND ----------

# MAGIC %md
# MAGIC Comprobamos si la entidad recibida por parámetro se encuentra en la tabla de dominios.

# COMMAND ----------

df_test_rel = df_f_domains.filter(col("nombre_entidad") == entity_name_mds)

#display(df_test_rel)

# COMMAND ----------

if df_test_rel.count() == 0:
    list_entities_mds = generate_list(df_f_domains, 'nombre_entidad')
    raise Exception("Error: La entidad '{}' no se encuentra dentro del modelo de MDS. Por favor parametrizar alguna de las siguientes entidades: {}".format(entity_name_mds, list_entities_mds))

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura de entidad recibida por parámetro:

# COMMAND ----------

# Primero formateamos el nombre de la entidad para que el formato esté en como se encuentran las copias de las vistas de MDS
name_view = "TBL_SLV_MDS_{}".format(entity_name_mds)
name_view

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura de los datos que se encuentran actualmente en MDS por medio de unas vistas que se encuentra en la tabla paramétrica.

# COMMAND ----------

df_entity_mds = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name, schema, name_view)
#display(df_entity_mds)

# COMMAND ----------

# MAGIC %md
# MAGIC # Extracción de id natural de la tabla matriz dato.

# COMMAND ----------

try:
    natural_id = df_parametric_ent.filter(col("es_id_natural") == "Y").select("nombre_atributo_mds") \
        .dropDuplicates().collect()[0][0]
    print(natural_id)
except IndexError as ae:
    raise Exception("No existe id natural para la entidad {}. Msg error: ".format(entity_name_mds, ae))

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtro de columna a aplicar el reporte

# COMMAND ----------

# MAGIC %md
# MAGIC Se filtra la columna que corresponda al id que es a la que se le va a aplicar el reporte.

# COMMAND ----------

df_data_by_col = df_entity_mds.select(natural_id).dropDuplicates()
#display(df_data_by_col)

# COMMAND ----------

if type(df_data_by_col.schema[natural_id].dataType) is not StringType:
    print("Advertencia, la columna '{}' de la entidad '{}' no es de tipo string, se procede a realizar su conversión a tipo texto.".format(natural_id, entity_name_mds))
    df_data_by_col = df_data_by_col.withColumn(natural_id, col(natural_id).cast(StringType()))
    #display(df_data_by_col)

# COMMAND ----------

# MAGIC %md
# MAGIC Se genera un id único por dato de la columna a la que se le está aplicando el reporte.

# COMMAND ----------

df_data_by_col = row_number_df(df_data_by_col, natural_id)
#display(df_data_by_col)

# COMMAND ----------

# MAGIC %md
# MAGIC Reparticionamos el dataframe para mejorar el performance durante la aplicación de la función de coincidencias

# COMMAND ----------

# Repartición para mejorar el performance
df_data_by_col = df_data_by_col.repartition(80)

# COMMAND ----------

# MAGIC %md
# MAGIC # Generación del reporte

# COMMAND ----------

# MAGIC %md
# MAGIC Función para crear un dataframe vacío con la estructura del reporte

# COMMAND ----------

def create_empty_report_structure():
    try:
        columns = StructType([StructField('tabla',
                                        StringType(), True),
                              StructField('columna',
                                        StringType(), True),
                              StructField('atributo_1',
                                        StringType(), True),
                              StructField('id_atributo_1',
                                        IntegerType(), True),
                            StructField('atributo_2',
                                        StringType(), True),
                             StructField('id_atributo_2',
                                        IntegerType(), True),
                             StructField('precision',
                                        IntegerType(), True)])
        emptyRDD = spark.sparkContext.emptyRDD()

        df_report = spark.createDataFrame(data = emptyRDD, schema = columns)

        return df_report
    except Exception as ex:
        raise Exception("Fallo el método 'standard_dates'. Msg {}".format(ex))

# COMMAND ----------

# Función que recibe por parámetro dos strings y aplica la función "ratio" de la librería fuzz
def match_strings(str1, str2):
    return fuzz.ratio(str1, str2)

# COMMAND ----------

# Convertir a función UDF de Spark la función de coincidencias
match_strings_udf = udf(match_strings, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC Función auxiliar para generar el reporte de coincidencias haciendo el cruce entre los valores de una misma columna recibida por parámetro.

# COMMAND ----------

def generate_report_by_attribute1(df, name_attr, len_accuracy):
    try:
        df_2 = df.select(col(name_attr).alias(name_attr + "_2"), col("id_registro").alias("id_registro_2"))
        df_cross_join = df.crossJoin(df_2)
        df_cross_join = df_cross_join.filter((col(name_attr) != col(name_attr + "_2")) & (col("id_registro") < col("id_registro_2")))

        # Repartición para mejorar el performance
        df_cross_join = df_cross_join.repartition(80)

        df_report = df_cross_join.withColumn('precision', match_strings_udf(col(name_attr), col(name_attr + "_2")))
        df_report = df_report.filter(col('precision') > len_accuracy)
        df_report = df_report.select(col(name_attr).alias("atributo_1"), col("id_registro").alias("id_atributo_1"),
                                     col(name_attr + "_2").alias("atributo_2"), col("id_registro_2").alias("id_atributo_2"), "precision")


        return df_report
    except Exception as ex:
        raise Exception("Fallo el método 'generate_report_by_attribute1'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función auxiliar para generar el reporte por atributo de una tabla recibida por parámetro.

# COMMAND ----------

def generate_report_by_attribute(df, name_attr, len_accuracy, table_name):
    try:
        df_report = generate_report_by_attribute1(df, name_attr, len_accuracy)
        df_report = df_report.withColumn('tabla', lit(table_name)) \
                            .withColumn('columna', lit(name_attr))

        df_report = df_report.select("tabla", "columna", "atributo_1", "id_atributo_1", "atributo_2", "id_atributo_2", "precision")
        return df_report
    except Exception as ex:
        raise Exception("Fallo el método 'generate_report_by_attribute'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función principal para generar el reporte de coincidencias.

# COMMAND ----------

def generate_report(df, df_unique_attr, table_name, attr_name):
    try:
        df_report = create_empty_report_structure()
        # TODO: Mejora en próxima versión, generar un reporte de coincidencias con más de dos atributos
        #for item in list_columns:
        df_iter = generate_report_by_attribute(df_unique_attr, attr_name, len_accuracy, table_name)
        df_report = df_report.union(df_iter)
        return df_report
    except Exception as ex:
        raise Exception("Fallo el método 'generate_report'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Generar y visualizar reporte

# COMMAND ----------

df_report = generate_report(df_parametric_ent, df_data_by_col, entity_name_mds, natural_id)
#display(df_report)

# COMMAND ----------

now = datetime.now() # Objeto para capturar la fecha actual

# COMMAND ----------

# MAGIC %md
# MAGIC Para mantener la traza de la columna a la que se le aplicó el reporte (manteniendo la relación de ids únicos), se guarda en un archivo parquet

# COMMAND ----------

name_df_col_with_ids = '{}_mds_columna_{}_{}'.format(entity_name_mds, natural_id, now.date())
name_df_col_with_ids

# COMMAND ----------

write_file(df_data_by_col, "csv", param_key_datalake, secret_scope, storage_account_name, container, final_path, name_df_col_with_ids, report_extension)

# COMMAND ----------

# MAGIC %md
# MAGIC Escribir reporte principal

# COMMAND ----------

# Dar formato al mombre del reporte con el que se va a guardar en el container
report_name = 'report_{}_mds_{}'.format(entity_name_mds, now.date())
report_name

# COMMAND ----------

write_file(df_report, "csv", param_key_datalake, secret_scope, storage_account_name, container, final_path, report_name, report_extension)

# COMMAND ----------

# Limpiar memoria caché del driver de spark
spark.catalog.clearCache()
