# Databricks notebook source
# MAGIC %md
# MAGIC # Importar Funciones Personalizadas

# COMMAND ----------

# MAGIC %run /Repos/transform_data_mdm/DatosMaestrosMDSData_Transform/functions_etl

# COMMAND ----------

# MAGIC %md
# MAGIC # Importar funciones de Python

# COMMAND ----------

from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

# Leer parámetros
dbutils.widgets.text("filialName", "","")
filial_name = dbutils.widgets.get("filialName")
dbutils.widgets.text("subdomain", "","")
subdomain = dbutils.widgets.get("subdomain")
dbutils.widgets.text("modelNameMDS", "","")
model_name_mds = dbutils.widgets.get("modelNameMDS")
dbutils.widgets.text("tblGoldenRecordName", "","")
tbl_golden_record_name = dbutils.widgets.get("tblGoldenRecordName")
dbutils.widgets.text("mdsPath", "","")
mds_path = dbutils.widgets.get("mdsPath")
dbutils.widgets.text("finalPath", "","")
final_path = dbutils.widgets.get("finalPath")
dbutils.widgets.text("folderInsertMDS", "","")
folder_insert_mds = dbutils.widgets.get("folderInsertMDS")
dbutils.widgets.text("folderUpdateMDS", "","")
folder_update_mds = dbutils.widgets.get("folderUpdateMDS")
dbutils.widgets.text("parquetExtensionFile", "","")
parquet_extension_file = dbutils.widgets.get("parquetExtensionFile")
dbutils.widgets.text("paramKeyDatalake", "","")
param_key_datalake = dbutils.widgets.get("paramKeyDatalake")
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
dbutils.widgets.text("paramKeyAzSql", "","")
param_key_az_sql = dbutils.widgets.get("paramKeyAzSql")
dbutils.widgets.text("secretScopeDatabricks", "","")
secret_scope = dbutils.widgets.get("secretScopeDatabricks")
dbutils.widgets.text("storageAccountName", "","")
storage_account_name = dbutils.widgets.get("storageAccountName")
dbutils.widgets.text("containerGold", "","")
container_gold = dbutils.widgets.get("containerGold")
dbutils.widgets.text("containerSilver", "","")
container_silver = dbutils.widgets.get("containerSilver")

print(filial_name)
print(subdomain)
print(model_name_mds)
print(tbl_golden_record_name)
print(mds_path)
print(final_path)
print(folder_insert_mds)
print(folder_update_mds)
print(parquet_extension_file)
print(param_key_datalake)
print(host_name)
print(port)
print(db_name)
print(usr_name)
print(schema)
print(param_key_az_sql)
print(secret_scope)
print(storage_account_name)
print(container_gold)
print(container_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura de tabla paramétrica

# COMMAND ----------

df_parametric = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_matriz_dato")
#display(df_parametric)

# COMMAND ----------

df_f_parametric = df_parametric.filter((col("nombre_filial") == filial_name) & (col("nombre_modelo") == model_name_mds) \
                                         & (col("golden_record") == "Y"))
#display(df_f_parametric)

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
# MAGIC Lectura tabla con relaciones de primary keys para las entidades de MDS.

# COMMAND ----------

df_map_pk = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "vw_identificadores_naturales_x_entidad")

df_map_pk = df_map_pk.filter((col("nombre_filial") == filial_name) & (col("subdominio") == subdomain))
df_map_pk = df_map_pk.filter(col("es_id_upsert") == "Y")

#display(df_map_pk)

# COMMAND ----------

if df_map_pk.count() == 0:
    raise Exception("La tabla de identificadores_naturales_x_entidad está vacia por lo que no se encuentran llaves para hacer el proceso del upsert.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Extracción de Campo Llave en Variable

# COMMAND ----------

collect_reg = df_map_pk.collect()
collect_reg

# COMMAND ----------

# Primero extraemos el id natural para la tabla de golden record

# En esta versión de la aplicación se hace el cruce del upsert por una única llave
try:
    campo_nat_fuente = collect_reg[0]["campo_clave_entidad_2"]
    print(campo_nat_fuente)
except IndexError as ae:
    raise Exception("No existe campo llave parametrizado para la tabla de golden record de la fuente en la tabla de relaciones para hacer el proceso del upsert. Msg error: ".format(ae))

# COMMAND ----------

# Luego extraemos el id natural equivalente en la tabla de MDS
try:
    id_nat_mds = collect_reg[0]["nombre_atributo_mds"]
    print(id_nat_mds)
except IndexError as ae:
    raise Exception("No existe campo llave parametrizado para la tabla de MDS en la tabla de relaciones para hacer el proceso del upsert. Msg error: ".format(ae))

# COMMAND ----------

# Extraemos el nombre de la entidad de MDS que mapea al campo de id natural
entity_name_mds = collect_reg[0]["nombre_entidad_mds"]
entity_name_mds

# COMMAND ----------

df_golden_record_m_m = read_df_from_container(secret_scope, param_key_datalake, storage_account_name, container_gold, final_path, tbl_golden_record_name, parquet_extension_file)
#display(df_golden_record_m_m)

# COMMAND ----------

# MAGIC %md
# MAGIC Leemos el mapeo de la entidad en la tabla de dominios de la entidad de MDS que contiene el campo de cruce.

# COMMAND ----------

df_cols_mds = df_f_domains.filter(col("nombre_entidad") == entity_name_mds) \
    .select(col("nombre_entidad").alias("nombre_entidad_mds"), col("nombre_atributo").alias("nombre_atributo_mds"), col("tipo_atributo").alias("tipo_dato_mds"))
#display(df_cols_mds)

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura vista con entidad de MDS (de la base de datos paramétrica).

# COMMAND ----------

name_view = "TBL_SLV_MDS_{}".format(entity_name_mds) # En la base de datos paramétrica, se guarda una copia de la vista de MDS siguiendo la nomenclatura TBL_SLV_MDS_<<nombre_vista>>
try:
    df_currently_ent_mds = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                        schema, name_view)
    #display(df_currently_ent_mds)
except Exception as ex:
    name_view_mds = "vw{}_{}".format(model_name_mds, entity_name_mds)
    raise Exception("Falló la lectura de la entidad {}, es posible que no exista la vista de esta entidad en MDS con la nomenclatura: {}; o que no exista en la tabla de dominios. Msg error: {}".format(entity_name_mds, name_view_mds, ex))

# COMMAND ----------

# MAGIC %md
# MAGIC # Registros Para Insertar

# COMMAND ----------

# MAGIC %md
# MAGIC Para evitar ambigüedades en las columnas renombramos una de las dos y solo seleccionamos la llave natural por la que queremos unir

# COMMAND ----------

# Filtro final para registros a insertar
df_insert = df_golden_record_m_m.join(df_currently_ent_mds, col(campo_nat_fuente) == col(id_nat_mds), 'left')
df_insert = df_insert.filter(col(id_nat_mds).isNull())

# Seleccionar las columnas originales de la tabla de golden record ya que se agregaron las de las entidades de MDS al momento de hacer el join
df_insert = df_insert.select(*df_golden_record_m_m.columns)
    
#display(df_insert)

# COMMAND ----------

# Prueba para saber si se seleccionaron solo los registros a insertar
print(df_golden_record_m_m.count())
print(df_insert.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Registros Para Actualizar

# COMMAND ----------

df_update = df_golden_record_m_m.join(df_currently_ent_mds, col(campo_nat_fuente) == col(id_nat_mds), 'inner')

# Seleccionar las columnas originales de la tabla de golden record ya que se agregaron las de las entidades de MDS al momento de hacer el join
df_update = df_update.select(*df_golden_record_m_m.columns)
    
#display(df_update)

# COMMAND ----------

# Prueba para saber si se seleccionaron solo los registros a actualizar
print(df_golden_record_m_m.count())
print(df_update.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Proceso de Escritura

# COMMAND ----------

# MAGIC %md
# MAGIC Registros para actualizar en MDS

# COMMAND ----------

path_update = final_path + folder_update_mds
path_update

# COMMAND ----------

update_name = 'update_' + tbl_golden_record_name
update_name

# COMMAND ----------

write_file(df_update, "parquet", param_key_datalake, secret_scope, storage_account_name, container_gold, path_update, update_name, parquet_extension_file)

# COMMAND ----------

# MAGIC %md
# MAGIC Registros para insertar en MDS

# COMMAND ----------

insert_name = 'insert_' + tbl_golden_record_name
insert_name

# COMMAND ----------

path_insert = final_path + folder_insert_mds
path_insert

# COMMAND ----------

write_file(df_insert, "parquet", param_key_datalake, secret_scope, storage_account_name, container_gold, path_insert, insert_name, parquet_extension_file)

# COMMAND ----------

# Limpiar memoria caché del driver de spark
spark.catalog.clearCache()
