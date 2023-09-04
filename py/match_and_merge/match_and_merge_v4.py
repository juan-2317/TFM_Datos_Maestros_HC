# Databricks notebook source
# MAGIC %md
# MAGIC # Ejecutar generación de la tabla de golden record

# COMMAND ----------

# Leer parámetros
dbutils.widgets.text("filialName", "","")
filial_name = dbutils.widgets.get("filialName")
dbutils.widgets.text("subdomain", "","")
subdomain = dbutils.widgets.get("subdomain")
dbutils.widgets.text("domainMDSName", "","")
domain_mds_name = dbutils.widgets.get("domainMDSName")
dbutils.widgets.text("tblGoldenRecordName", "","")
tbl_golden_record_name = dbutils.widgets.get("tblGoldenRecordName")
dbutils.widgets.text("sourcePath", "","")
source_path = dbutils.widgets.get("sourcePath")
dbutils.widgets.text("finalPath", "","")
final_path = dbutils.widgets.get("finalPath")
dbutils.widgets.text("parquetFileExtension", "","")
parquet_file_extension = dbutils.widgets.get("parquetFileExtension")
dbutils.widgets.text("paramKeyDatalake", "","")
param_key_datalake = dbutils.widgets.get("paramKeyDatalake")
dbutils.widgets.text("hostName", "","")
host_name = dbutils.widgets.get("hostName")
dbutils.widgets.text("port", "","")
port = dbutils.widgets.get("port")
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

dbutils.widgets.text("entitiesSilverPath", "","")
entities_silver_path = dbutils.widgets.get("entitiesSilverPath")
dbutils.widgets.text("destGoldPath", "","")
dest_gold_path = dbutils.widgets.get("destGoldPath")
dbutils.widgets.text("tblGoldenRecordName", "","")
tbl_golden_record_name = dbutils.widgets.get("tblGoldenRecordName")
dbutils.widgets.text("containerSilver", "","")
container_silver = dbutils.widgets.get("containerSilver")
dbutils.widgets.text("checkWriteGD_DL", "","")
check_write_gd_dl = dbutils.widgets.get("checkWriteGD_DL")


print(filial_name)
print(subdomain)
print(domain_mds_name)
print(tbl_golden_record_name)
print(source_path)
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

print(entities_silver_path)
print(dest_gold_path)
print(tbl_golden_record_name)
print(container_silver)
print(check_write_gd_dl)

# COMMAND ----------

# MAGIC %run /Repos/transform_data_mdm/DatosMaestrosMDSData_Transform/match_and_merge/generate_golden_record_sink_v2

# COMMAND ----------

# MAGIC %md
# MAGIC Leeemos los datos de la tabla tbl_mds_dominios (es la información que devuelve el método getDomain del API de MDS).

# COMMAND ----------

df_domains = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_mds_dominios")
#display(df_domains)

# COMMAND ----------

df_f_domains = df_domains.filter(col("nombre_dominio") == domain_mds_name)
#display(df_f_domains)

# COMMAND ----------

if df_f_domains.count() == 0:
    raise Exception("No existe el modelo de MDS con nombre: {}".format(domain_mds_name))

# COMMAND ----------

# MAGIC %md
# MAGIC # Controles de Excepciones

# COMMAND ----------

# MAGIC %md
# MAGIC Control de excepciones en caso de que los nombres de las entidades de la tabla paramétrica no crucen en la tabla de dominios.

# COMMAND ----------

list_parametric_ents_mds = generate_list(df_f_parametric, "nombre_entidad_mds")
list_parametric_ents_mds

# COMMAND ----------

df_check_relations = df_f_domains.filter(col("nombre_entidad").isin(list_parametric_ents_mds))
#display(df_check_relations)

# COMMAND ----------

if df_check_relations.count() == 0:
    raise Exception("Los nombres de las entidades de la tabla paramétrica deben existir en la tabla de dominios")

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura tabla con relaciones de primary keys para las entidades de Silver.

# COMMAND ----------

df_main_map = df_keys_silver

#display(df_main_map)

# COMMAND ----------

# MAGIC %md
# MAGIC # Proceso para quitar duplicados

# COMMAND ----------

# Filtramos de la tabla paramétrica las entidades con fuente prioritaria.

# Nota: Si dos entidades están marcadas como fuente prioritaria o no están marcadas, se van a quitar duplicados al azar.

# TODO: Los subdominios de proveedores y servicios de abastecimiento no contienen tablas equivalentes entre diferentes fuentes por lo que no es necesario hacer un "union all" entre fuentes para quitar duplicados y se puede omitir el proceso para quitar duplicados por fuente prioritaria.
# df_fuente = df_f_parametric.filter(col("es_tabla_prioridad") == 'Y').select("id_fuente").dropDuplicates()
# display(df_fuente)

#if df_fuente.count() == 1:
#    fuente_prioritaria = df_fuente.collect()[0][0]
#else:
#    fuente_prioritaria = df_parametric_ent.select("id_fuente").collect()[0][0]
    
#fuente_prioritaria

#df_to_mds = df_to_mds.withColumn('sort_order', when(col('id_fuente') == fuente_prioritaria, 1).otherwise(0))
#display(df_to_mds)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quitar duplicados en la tabla de Golden Record previo a insertar/actualizar a MDS

# COMMAND ----------

# Generar lista con ids naturales para quitar duplicados (puede ser un campo clave o más para formar una llave compuesta)
lists_columns_nat = generate_list(df_main_map, "campo_id_natural_2")
lists_columns_nat

# COMMAND ----------

# MAGIC %md
# MAGIC Función principal para quitar duplicados en la tabla de golden record teniendo en cuenta la parametrización de los ids naturales y criterio para quitar duplicados.

# COMMAND ----------

def main_m_and_m(df_main_map, df_tbl_golden_record):
    df_duplicate_criteria = df_main_map.select("criterio_duplicados") \
        .filter(col("criterio_duplicados").isNotNull()) # Esta consulta debería devolver solo 1 registro, ya que de momento la implementación soporta solo mantener un registro de acuerdo a un criterio definido
    #display(df_duplicate_criteria)
    
    if df_duplicate_criteria.count() == 1:
        collect_dupl = df_duplicate_criteria.collect()
        duplicate_criteria = collect_dupl[0]["criterio_duplicados"]
        print(duplicate_criteria)

        df_tbl_golden_record.createOrReplaceTempView('tbl_golden_record_m_and_m')

        df_tbl_golden_record_m_and_m = sqlContext.sql("""
            SELECT *, CASE WHEN {} THEN 1 ELSE 2 END AS estadoregistro
            FROM tbl_golden_record_m_and_m
        """.format(duplicate_criteria))

        df_tbl_golden_record_m_and_m = row_number_by_list_partition(df_tbl_golden_record_m_and_m, lists_columns_nat, "estadoregistro", "ASC")
    else: # Si no hay criterios para mantener registros al quitar duplicados o se marca mas de un campo para esto se mantiene el primer registro que se encuentre
        sort_order = df_tbl_golden_record.columns[0]
        print(sort_order)

        df_tbl_golden_record_m_and_m = row_number_by_list_partition(df_tbl_golden_record, lists_columns_nat, sort_order)

    return df_tbl_golden_record_m_and_m

# COMMAND ----------

df_tbl_golden_record_m_and_m = main_m_and_m(df_main_map, df_tbl_golden_record)
#display(df_tbl_golden_record_m_and_m)

# COMMAND ----------

df_tbl_golden_record_m_and_m = df_tbl_golden_record_m_and_m.filter(col('rn') == 1)
#display(df_tbl_golden_record_m_and_m)

# COMMAND ----------

# MAGIC %md
# MAGIC Se eliminan columnas temporales que no son necesarias después del proceso de M&M

# COMMAND ----------

df_tbl_golden_record_m_and_m = df_tbl_golden_record_m_and_m.drop("rn")
#display(df_tbl_golden_record_m_and_m)

# COMMAND ----------

# MAGIC %md
# MAGIC # Escribir Resultados Finales

# COMMAND ----------

# MAGIC %md
# MAGIC Escribir tabla del golden record en el dl (sin el M&M)

# COMMAND ----------

# Esta variable contiene el valor del parámetro "checkWriteGD_DL" del pipeline "PL_Match_And_Merge" y recordar que sirve para indicar si se desea escribir la tabla física del golden record en el datalake(previa al M&M)
if check_write_gd_dl == "Y":
    write_file(df_tbl_golden_record, "parquet", param_key_datalake, secret_scope, storage_account_name, container_gold, dest_gold_path, tbl_golden_record_name, parquet_file_extension)

# COMMAND ----------

# MAGIC %md
# MAGIC Escribir tabla del golden record post M&M

# COMMAND ----------

new_tbl_golden_record_name = tbl_golden_record_name + '_m_and_m'
new_tbl_golden_record_name

# COMMAND ----------

write_file(df_tbl_golden_record_m_and_m, "parquet", param_key_datalake, secret_scope, storage_account_name, container_gold, final_path, new_tbl_golden_record_name, parquet_file_extension)

# COMMAND ----------

conteo_union = df_tbl_golden_record.count()
conteo_post_dupl = df_tbl_golden_record_m_and_m.count()

dbutils.notebook.exit("Originalmente existian {} registros, después de quitar duplicados quedaron {} registros, eliminándose {} registros".format(conteo_union, conteo_post_dupl, conteo_union - conteo_post_dupl)) # Escribir en la salida estándar de databricks este mensaje.
