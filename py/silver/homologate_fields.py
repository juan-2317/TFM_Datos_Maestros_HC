# Databricks notebook source
# MAGIC %md
# MAGIC # Importar Funciones Personalizadas

# COMMAND ----------

# MAGIC %run /Repos/transform_data_mdm/DatosMaestrosMDSData_Transform/functions_etl

# COMMAND ----------

# MAGIC %md
# MAGIC Leemos los parámetros que usaremos más adelante

# COMMAND ----------

# Creating widgets for leveraging parameters, and printing the parameters
dbutils.widgets.text("filialName", "","")
filial_name = dbutils.widgets.get("filialName")
dbutils.widgets.text("modelNameMDS", "","")
model_name_mds = dbutils.widgets.get("modelNameMDS")
dbutils.widgets.text("subdomain", "","")
subdomain = dbutils.widgets.get("subdomain")
dbutils.widgets.text("tableName", "","")
table_name = dbutils.widgets.get("tableName")
dbutils.widgets.text("folderPath", "","")
folder_path = dbutils.widgets.get("folderPath")
dbutils.widgets.text("mdsPath", "","")
mds_path = dbutils.widgets.get("mdsPath")
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
dbutils.widgets.text("container", "","")
container_name = dbutils.widgets.get("container")


print(filial_name)
print(model_name_mds)
print(subdomain)
print(table_name)
print(folder_path)
print(mds_path)
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
print(container_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lectura de dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC Leer entidad de silver a la que se le va a aplicar el proceso de homologación

# COMMAND ----------

df_parquet = read_df_from_container(secret_scope, param_key_datalake, storage_account_name, container_name,
                                    folder_path, table_name, parquet_file_extension)
#display(df_parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC Leer tabla de dominios

# COMMAND ----------

df_domains = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_mds_dominios")
#display(df_domains)

# COMMAND ----------

df_f_domains = df_domains.filter(col("nombre_dominio") == model_name_mds)
#display(df_f_domains)

# COMMAND ----------

if df_f_domains.count() == 0:
    dbutils.notebook.exit("No existe el modelo de MDS con nombre: {}. No se procede con la homologación para la entidad: {}".format(model_name_mds, table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC Filtramos de la tabla de dominos los datos correspondientes a las columnas de la tabla de homologación

# COMMAND ----------

# Esta variable guarda el nombre de la tabla de homologación que se encuentra en MDS
name_tbl_homologation = "homologacion"
name_tbl_homologation

# COMMAND ----------

df_cols_homologation = df_f_domains.filter(col("nombre_entidad") == name_tbl_homologation) \
    .select(col("nombre_atributo").alias("nombre_atributo_mds"), col("tipo_atributo").alias("tipo_dato_mds"))
#display(df_cols_homologation)

# COMMAND ----------

name_view = "TBL_SLV_MDS_{}".format(name_tbl_homologation) # En la base de datos paramétrica, se guarda una copia de la vista de MDS siguiendo la nomenclatura TBL_SLV_MDS_<<nombre_vista>>
try:
    df_homologate = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, name_view)

    #display(df_homologate)
except Exception as ex:
    name_view_mds = "vw{}_{}".format(model_name_mds, name_tbl_homologation)
    raise Exception("Falló la lectura de la entidad {}, es posible que no exista la vista de esta entidad en MDS con la nomenclatura: {}; o que no exista en la tabla de dominios. Msg error: {}".format(name_tbl_homologation, name_view_mds, ex))

# COMMAND ----------

if df_homologate.count() == 0:
    raise Exception("No se encuentran datos en la tabla de homologación. Por favor validar")

# COMMAND ----------

# MAGIC %md
# MAGIC Leer datos de la tabla parametrica

# COMMAND ----------

df_parametric = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_matriz_dato")
#display(df_parametric)

# COMMAND ----------

# MAGIC %md
# MAGIC Filtrar por nombre de entidad de acuerdo a lo que llegue por parámetro

# COMMAND ----------

df_parametric_ent = df_parametric.filter((col('nombre_entidad') == table_name) & (col('golden_record') == lit('Y')) \
                                        & (col("subdominio") == subdomain) & (col("nombre_filial") == filial_name))
#display(df_parametric_ent)

# COMMAND ----------

# MAGIC %md
# MAGIC Registrar como vistas la tabla paramétrica y la de dominios

# COMMAND ----------

df_parametric_ent.createOrReplaceTempView("tbl_matriz_dato")
df_f_domains.createOrReplaceTempView("tbl_mds_dominios")

# COMMAND ----------

# MAGIC %md
# MAGIC # Proceso Homologación

# COMMAND ----------

# MAGIC %md
# MAGIC Filtramos de la tabla paramétrica los campos a los que se les vaya a aplicar el proceso de homologación

# COMMAND ----------

df_param_homologate = sqlContext.sql("""
    SELECT a.nombre_funcional, a.nombre_entidad, a.es_id_natural, a.tipo_dato_mds, a.nombre_entidad_referenciada,
        a.nombre_atributo_mds, a.nombre_entidad_mds
    FROM tbl_matriz_dato a
    WHERE a.nombre_entidad_mds IS NOT NULL
    AND EXISTS(
        SELECT b.nombre_entidad
        FROM tbl_mds_dominios b
        WHERE a.nombre_entidad_referenciada = b.nombre_entidad
        AND b.esListaReferencia = 'true'
    )
""")

#display(df_param_homologate)

# COMMAND ----------

# MAGIC %md
# MAGIC Revisión de que todos los códigos de homologación de la fuente se encuentren en la tabla de homologación.

# COMMAND ----------

def check_codes_into_homologation_table(df_parquet_h, col_name_ref, subdomain, table_name, reference_list):
    df_verif = df_parquet_h.filter((col(col_name_ref) != "") & (col("id_en_fuente").isNull()))

    msg = ""
    if df_verif.count() > 0:
        list_codigos = generate_list(df_verif, col_name_ref)
        list_codigos_esp = generate_list(df_parquet_h.filter(col("id_en_fuente").isNotNull()), "id_en_fuente")

        msg = "Advertencia, los códigos de la columna {}; para la entidad de silver: {}; del subdomino: {} para la lista de referencia: {} no se encuentran en la tabla homologación.\nCodigos de la entidad de silver: {}\nPor favor revisar que dichos códigos coincidan con los siguientes códigos de la tabla de homologación de MDS: {}\n".format(col_name_ref, table_name, subdomain, reference_list, list_codigos, list_codigos_esp)
    
    return msg

# COMMAND ----------

msg_compl = "" # En esta variable se guardarán mensajes de advertencia en caso de que no todos los campos se puedan homologar

# COMMAND ----------

# MAGIC %md
# MAGIC Función para homologar un campo de un dataframe específico.

# COMMAND ----------

def homologate_field(df_homologate, df_parquet, reference_list, funcional_name, subdomain, table_name):
    try:
        global msg_compl
        # Filtrar los datos correspondientes a la lista de referencia que se esté iterando
        df_f_homologate = df_homologate.filter(col("nombre_lista_referencia") == reference_list)
        if df_f_homologate.count() == 0:
            raise Exception("La tabla de homologación no contiene datos para la lista de referencia: {}".format(reference_list))

        # Filtramos solo las columnas que necesita la tabla de homologación
        df_f_homologate = df_f_homologate.select("id_mds", "id_en_fuente")

        # Homologar códigos en la tabla destino
        df_parquet_h = df_parquet.join(df_f_homologate, col(funcional_name) == df_f_homologate.id_en_fuente, 'left')

        # Revisión de que todos los códigos de homologación de la fuente se encuentren en la tabla de homologación
        msg = check_codes_into_homologation_table(df_parquet_h, funcional_name, subdomain, table_name, reference_list)

        msg_compl = msg_compl + msg
        # Reemplazar los valores homologados en la columna original
        df_parquet_h = df_parquet_h.withColumn(funcional_name, col('id_mds'))

        # Seleccionar columnas del dataframe original
        df_parquet_h = df_parquet_h.select(*df_parquet.columns)

        return df_parquet_h
    except Exception as ex:
        raise Exception("Fallo el método 'homologate_field'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Recorrer de manera dinámica los campos que se detectaron como listas de referencia:

# COMMAND ----------

def loop_homologate_fields(df_param_homologate, df_parquet, subdomain, table_name):
    try:
        data_collect = df_param_homologate.collect()
        df_parquet_h = df_parquet

        for row in data_collect:
            # while looping through each
            df_parquet_h = homologate_field(df_homologate, df_parquet_h, row["nombre_entidad_referenciada"], row["nombre_funcional"], subdomain, table_name)

        return df_parquet_h
    except Exception as ex:
        raise Exception("Fallo el método 'loop_homologate_fields'. Msg {}".format(ex))

# COMMAND ----------

df_parquet_h = loop_homologate_fields(df_param_homologate, df_parquet, subdomain, table_name)
#display(df_parquet_h)

# COMMAND ----------

# MAGIC %md
# MAGIC # Guardar nuevo dataset homologado en container Silver

# COMMAND ----------

write_file(df_parquet_h, "parquet", param_key_datalake, secret_scope, storage_account_name, container_name, folder_path, table_name, parquet_file_extension)

# COMMAND ----------

# Con esta condición evaluamos si se generó un mensaje de advertencia al no cruzar códigos en algunos campos para mostrarlo en la salida y logs de datafactory
if msg_compl != "":
    dbutils.notebook.exit(msg_compl)

# COMMAND ----------

# Limpiar memoria caché del driver de spark
spark.catalog.clearCache()
