# Databricks notebook source
# MAGIC %md
# MAGIC # Importar Funciones Personalizadas

# COMMAND ----------

# MAGIC %run /Repos/transform_data_mdm/DatosMaestrosMDSData_Transform/functions_etl

# COMMAND ----------

# MAGIC %md
# MAGIC # Importar Funciones de Upsert

# COMMAND ----------

# MAGIC %run /Repos/transform_data_mdm/DatosMaestrosMDSData_Transform/files_upsert/functions_upsert

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
dbutils.widgets.text("modelNameMDS", "","")
model_name_mds = dbutils.widgets.get("modelNameMDS")
dbutils.widgets.text("subdomain", "","")
subdomain = dbutils.widgets.get("subdomain")
dbutils.widgets.text("entityNameMDS", "","")
entity_name_mds = dbutils.widgets.get("entityNameMDS")
dbutils.widgets.text("tblGoldenRecordName", "","")
name_tbl_golden_record = dbutils.widgets.get("tblGoldenRecordName")
dbutils.widgets.text("goldenRecordUpdPath", "","")
golden_record_upd_path = dbutils.widgets.get("goldenRecordUpdPath")
dbutils.widgets.text("mdsPath", "","")
mds_path = dbutils.widgets.get("mdsPath")
dbutils.widgets.text("finalPath", "","")
final_path = dbutils.widgets.get("finalPath")
dbutils.widgets.text("parquetFileExtension", "","")
parquet_file_extension = dbutils.widgets.get("parquetFileExtension")
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
print(model_name_mds)
print(subdomain)
print(entity_name_mds)
print(golden_record_upd_path)
print(name_tbl_golden_record)
print(mds_path)
print(final_path)
print(parquet_file_extension)
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
# MAGIC Lectura de tabla paramétrica.

# COMMAND ----------

df_parametric = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_matriz_dato")
#display(df_parametric)

# COMMAND ----------

df_f_parametric = df_parametric.filter((col("nombre_filial") == filial_name) & (col("nombre_modelo") == model_name_mds) \
                                         & (col("golden_record") == "Y"))
#display(df_f_parametric)

# COMMAND ----------

df_rel_modelo = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_rel_subdominio_x_modelo")

df_rel_modelo = df_rel_modelo.filter((col("nombre_subdominio") == subdomain) & (col("nombre_modelo") == model_name_mds) & (col("nombre_filial") == filial_name))
#display(df_rel_modelo)

# COMMAND ----------

# MAGIC %md
# MAGIC Extraer en variables el nombre de la entidad principal del modelo de MDS, el nombre de la entidad principal que está relacionada con el subdominio y la entidad de la fuente de ingesta que mapea a la entidad principal del modelo.

# COMMAND ----------

collect_rel_main = df_rel_modelo.collect()[0]
ent_rel_main_model = collect_rel_main['entidad_principal_modelo']
print(ent_rel_main_model)

ent_rel_main_subdomain = collect_rel_main['entidad_vs_subdominio']
print(ent_rel_main_subdomain)

ent_main_font = collect_rel_main['entidad_principal_fuente']
print(ent_main_font)

# COMMAND ----------

# MAGIC %md
# MAGIC Filtramos para obtener solo los registros de la entidad (de MDS) que recibimos por parámetro

# COMMAND ----------

if entity_name_mds == ent_rel_main_subdomain:
    df_parametric_ent = spark.createDataFrame([(ent_rel_main_subdomain, "estado", "Domain", ent_main_font, "estadoregistro")], schema = ["nombre_entidad_mds", "nombre_atributo_mds", "tipo_dato_mds", "nombre_entidad", "nombre_funcional"])
else:
    df_parametric_ent = df_f_parametric.filter((col("nombre_entidad_mds") == entity_name_mds))

#display(df_parametric_ent)

# COMMAND ----------

# MAGIC %md
# MAGIC Leemos los datos de la tabla tbl_mds_dominios (es la información que devuelve el método getDomain del API de MDS).

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
# MAGIC Leer tabla o vista con llaves relacionadas de la fuente con su respectiva entidad de MDS

# COMMAND ----------

df_entities_vs_mds = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "vw_info_llaves_x_entidad_mds")

df_entities_vs_mds = df_entities_vs_mds.filter((col("nombre_filial") == filial_name) & (col("subdominio") == subdomain))
#display(df_entities_vs_mds)

# COMMAND ----------

# MAGIC %md
# MAGIC Si la entidad es la que relaciona la entidad principal de MDS con el subdominio, se añade un mapeo extra debido a que esta no viene mapeada por el usuario.

# COMMAND ----------

if entity_name_mds == ent_rel_main_subdomain:
    df_entities_vs_mds = update_tbl_entities_vs_mds([[ent_rel_main_model, 'subdominio']], df_entities_vs_mds, entity_name_mds)

#display(df_entities_vs_mds)

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura de vista que relaciona los ids naturales entre la entidad principal del modelo de MDS y la entidad de la fuente que mapea dicha entidad.

# COMMAND ----------

df_main_keys = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "vw_main_entity_font_vs_mds")

df_main_keys = df_main_keys.filter(col("nombre_subdominio") == subdomain)

#display(df_main_keys)

# COMMAND ----------

# MAGIC %md
# MAGIC # Controles de excepciones

# COMMAND ----------

# MAGIC %md
# MAGIC Comprobamos si la entidad recibida por parámetro se encuentra en la tabla de dominios.

# COMMAND ----------

df_f_cols_ent = df_f_domains.filter(col("nombre_entidad") == entity_name_mds)

#display(df_f_cols_ent)

# COMMAND ----------

if df_f_cols_ent.count() == 0:
    list_entities_mds = generate_list(df_f_cols_ent, 'nombre_entidad')
    raise Exception("Error: La entidad '{}' no se encuentra dentro del modelo de MDS. Por favor parametrizar alguna de las siguientes entidades: {}".format(entity_name_mds, list_entities_mds))

# COMMAND ----------

data_keys = df_main_keys.collect()
data_keys

# COMMAND ----------

col_user_font = data_keys[0]["campo_clave_entidad_2"]
print(col_user_font)

ent_user_font = data_keys[0]["entidad_principal_fuente"]
print(ent_user_font)

# COMMAND ----------

# MAGIC %md
# MAGIC # Generar Mapeo

# COMMAND ----------

df_map_user = df_parametric_ent.select("nombre_entidad_mds", "nombre_atributo_mds", "tipo_dato_mds", "nombre_entidad", "nombre_funcional")
if entity_name_mds != ent_rel_main_subdomain:
    df_map_user = df_map_user.withColumn("nombre_funcional_rename", concat(col("nombre_entidad"), lit("_"), col("nombre_funcional")))
else:
    df_map_user = df_map_user.withColumn("nombre_funcional_rename", col("nombre_funcional"))
    
df_map_user = df_map_user.select("nombre_entidad_mds", "nombre_atributo_mds", "tipo_dato_mds", "nombre_funcional_rename")
#display(df_map_user)

# COMMAND ----------

name_ent_ref1, name_pk_ref1 = get_key_font_golden_record(df_entities_vs_mds, entity_name_mds)

print(name_ent_ref1)
print(name_pk_ref1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lectura de Tabla de Golden Record y Entidad de MDS

# COMMAND ----------

# MAGIC %md
# MAGIC Leer la tabla de golden record del datalake

# COMMAND ----------

df_tbl_golden_record = read_df_from_container(secret_scope, param_key_datalake, storage_account_name, container_gold, golden_record_upd_path, name_tbl_golden_record, parquet_file_extension)
#display(df_tbl_golden_record)

# COMMAND ----------

# MAGIC %md
# MAGIC Si no hay registros en la tabla de golden records es debido a que hay que revisar que los ids naturales crucen

# COMMAND ----------

if df_tbl_golden_record.count() == 0:
    dbutils.notebook.exit("No se generaron golden records. Esto puede ser debido a que se está haciendo una ingesta inicial.")

# COMMAND ----------

# MAGIC %md
# MAGIC Comprobar si la entidad de MDS que relaciona la entidad con el subdominio tiene la columna "estadoregistro".

# COMMAND ----------

if entity_name_mds == ent_rel_main_subdomain and "estadoregistro" not in df_tbl_golden_record.columns:
    dbutils.notebook.exit("La entidad {} no cuenta con la columna 'estadoregistro' para hacer la actualización".format(entity_name_mds))

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura de Entidad de MDS desde la base de datos paramétrica que contiene una copia de las vistas.

# COMMAND ----------

name_view = "TBL_SLV_MDS_{}".format(entity_name_mds) # En la base de datos paramétrica, se guarda una copia de la vista de MDS siguiendo la nomenclatura TBL_SLV_MDS_<<nombre_vista>>
try:
    df_currently_ent = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, name_view)

    df_currently_ent = rename_domains_columns_view(df_currently_ent)

    #display(df_currently_ent)
except Exception as ex:
    name_view_mds = "vw{}_{}".format(model_name_mds, entity_name_mds)
    raise Exception("Falló la lectura de la entidad {}, es posible que no exista la vista de esta entidad en MDS con la nomenclatura: {}; o que no exista en la tabla de dominios. Msg error: {}".format(entity_name_mds, name_view_mds, ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para renombrar las columnas de la tabla de Golden Record a su equivalente en MDS.

# COMMAND ----------

def map_golden_record(df_map_user, df_entity_to_map, list_cols_mds):
    try:
        # 1. Recolectar en un diccionario el mapeo de las columnas de las entidades mapeadas en la tabla paramétrica
        dict_map_rename = map_columns_into_dict(df_map_user, "nombre_atributo_mds", "nombre_funcional_rename")

        # 2. Recorrer diccionario con mapeo previo para que el golden record quede en "terminos de MDS", es decir con las columnas de MDS
        for key in dict_map_rename:
            df_entity_to_map = df_entity_to_map.withColumn(key, col(dict_map_rename[key]))
        
        # 3. Seleccionar las columnas de toda la entidad incluidas las que no se mapearon por el usuario
        df_entity_to_map = df_entity_to_map.select(*list_cols_mds)
        
        return df_entity_to_map
    except Exception as ex:
        raise Exception("Fallo el método 'map_golden_record'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para quitar duplicados por la llave de la entidad recibida por parámetro y filtrar las columnas equivalentes en MDS.

# COMMAND ----------

def map_ent_by_golden_record(df_map_mds, name_pk, entity_name, df_entity_to_map, list_cols_mds):
    try:
        df_ent_mapeada = drop_duplicates_by_column(df_entity_to_map, name_pk) # Quitar duplicados por columna llave especificada para la entidad que se está iterando
        df_ent_mapeada = map_golden_record(df_map_mds, df_ent_mapeada, list_cols_mds)

        return df_ent_mapeada
    except Exception as ex:
        raise Exception("Fallo el método 'map_ent_by_golden_record'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Generar lista de atributos de la entidad

# COMMAND ----------

list_cols_mds = generate_list(df_f_cols_ent, "nombre_atributo")
list_cols_mds

# COMMAND ----------

# MAGIC %md
# MAGIC # Registros Para Actualizar

# COMMAND ----------

df_entity_to_map = df_tbl_golden_record.join(df_currently_ent, [col(name_pk_ref1) == df_currently_ent.ids_en_fuentes, col(col_user_font) == df_currently_ent.pk_ent_principal], 'inner')


df_update = map_ent_by_golden_record(df_map_user, name_pk_ref1, entity_name_mds, df_entity_to_map, list_cols_mds)

#display(df_update)

# COMMAND ----------

# Conteo para revisar consistencia de números entre el golden record, la entidad con los datos actuales de MDS y la entidad generada
print(df_tbl_golden_record.count())
print(df_currently_ent.count())
print(df_update.count())

# COMMAND ----------

if df_update.count() == 0:
    dbutils.notebook.exit("No hay datos para actualizar en la entidad {}. Finaliza el proceso.".format(entity_name_mds))

# COMMAND ----------

# MAGIC %md
# MAGIC # Proceso de Escritura

# COMMAND ----------

# MAGIC %md
# MAGIC Registros para actualizar en MDS

# COMMAND ----------

write_file(df_update, "parquet", param_key_datalake, secret_scope, storage_account_name, container_gold, final_path, entity_name_mds, parquet_file_extension)
