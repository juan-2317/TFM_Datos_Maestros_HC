# Databricks notebook source
# MAGIC %md
# MAGIC # Importar funciones de Python

# COMMAND ----------

from pyspark.sql.window import *
from pyspark.sql.types import *
from pyspark.sql.functions import sha2, concat_ws

# COMMAND ----------

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

# Leer parámetros
dbutils.widgets.text("filialName", "","")
filial_name = dbutils.widgets.get("filialName")
dbutils.widgets.text("modelNameMDS", "","")
model_name_mds = dbutils.widgets.get("modelNameMDS")
dbutils.widgets.text("subdomain", "","")
subdomain = dbutils.widgets.get("subdomain")
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
# MAGIC # Lectura de Datos

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
# MAGIC Leer tabla o vista con llaves relacionadas de la fuente con su respectiva entidad de MDS

# COMMAND ----------

df_entities_vs_mds = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "vw_info_llaves_x_entidad_mds")

df_entities_vs_mds = df_entities_vs_mds.filter((col("nombre_filial") == filial_name) & (col("subdominio") == subdomain))
#display(df_entities_vs_mds)

# COMMAND ----------

# MAGIC %md
# MAGIC Leer vista con las primary key principales tanto de la fuente como del modelo de MDS

# COMMAND ----------

df_main_keys = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "vw_main_entity_font_vs_mds")

df_main_keys = df_main_keys.filter(col("nombre_subdominio") == subdomain)
#display(df_main_keys)

# COMMAND ----------

# MAGIC %md
# MAGIC Leer vista que nos sirve para obtener el mapeo realizado por el usuario en la tabla paramétrica junto con los atributos de tipo domain (excluyendo listas de referencia que no hayan sido mapeadas por el usuario).

# COMMAND ----------

df_map_user_with_keys = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "vw_mapeo_usuario_con_llaves_mds")
df_map_user_with_keys = df_map_user_with_keys.filter((col("nombre_filial") == filial_name) & (col("nombre_modelo") == model_name_mds) & (col("subdominio") == subdomain))
#display(df_map_user_with_keys)

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
# MAGIC Filtramos de la tabla de primary keys de las entidades principales, el campo que nos sirve para identificar los datos del usuario dentro del golden record

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
# MAGIC Leer tabla de orden de inserción

# COMMAND ----------

df_orden_ins = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_mds_orden_entidades") \
                    .orderBy(col("orden"))
#display(df_orden_ins)

# COMMAND ----------

if df_orden_ins.count() == 0:
    raise Exception("Por favor asegurarse de tener datos en la tabla de orden de inserción de entidades para poder ejecutar el proceso de update-insert")

# COMMAND ----------

# MAGIC %md
# MAGIC Leer tabla o vista con llaves relacionadas de la fuente con su respectiva entidad de MDS

# COMMAND ----------

df_entities_vs_mds = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "vw_info_llaves_x_entidad_mds")
#display(df_entities_vs_mds)

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura de entidad principal del modelo de MDS, id del subdominio, id de filial y la entidad que relaciona el subdominio con la entidad principal ya que el usuario no las mapea desde la tabla paramétrica, pero se deben incluir.

# COMMAND ----------

ent_main_model, id_subdomain, id_filial, ent_rel_subdomain = read_map_subdomain_x_filial(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name, schema, model_name_mds, subdomain)

print(ent_main_model)
print(id_subdomain)
print(id_filial)
print(ent_rel_subdomain)

# COMMAND ----------

list_entities_mds = list_entities_map_user(df_orden_ins, df_f_parametric)
list_entities_mds

# COMMAND ----------

list_special_ents_to_map = list_special_entities(df_orden_ins, list_entities_mds, ent_main_model)
list_special_ents_to_map

# COMMAND ----------

# Concatenado de listas mapeadas por el usuario y especiales que se cruzará con la tabla de orden
list_consolidated_map = list_special_ents_to_map + list_entities_mds
list_consolidated_map

# COMMAND ----------

list_consolidated_map_sort = generate_list(df_orden_ins.filter(col("entidad").isin(list_consolidated_map)), "entidad", "orden")
list_consolidated_map_sort

# COMMAND ----------

# En la tabla paramétrica concatenamos los nombres de entidades funcionales con sus atributos,
df_f_sel_parametric = df_f_parametric.withColumn("nombre_funcional_rename", concat(col("nombre_entidad"), lit("_"), col("nombre_funcional"))) \
    .withColumn("nombre_atributo_mds_rename", concat(col("nombre_entidad_mds"), lit("_"), col("nombre_atributo_mds")))

df_f_sel_parametric = df_f_sel_parametric.select("nombre_entidad", "nombre_funcional_rename", "nombre_entidad_mds", "nombre_atributo_mds")

#display(df_f_sel_parametric)

# COMMAND ----------

# MAGIC %md
# MAGIC # Proceso Principal

# COMMAND ----------

#inicializar diccionario de inserts (SOLO TIENE LOS INSERTS NUEVOS (TIENE LA MISMA ESTRUCTURA DEL DICCIONARIO GENERAL))
dict_df_insert_ent = {}
dict_df_new_pks = {} # Diccionario con filtro del golden record solo con los inserts nuevos de esa entidad (en formato original sap)

# COMMAND ----------

# MAGIC %md
# MAGIC Función para generar diccionario relacionando los nombres de las entidades de MDS con su nombre en la base de datos de lectura.

# COMMAND ----------

def generate_prefix_MDS_lists(list_consolidated_map_sort):
    try:
        dict_pref_consolidated = {}
        for item in list_consolidated_map_sort:
            # El nombre de la tabla de silver se le concatena el prefijo como se guardo en la base de datos

            tbl_name_gold_mds = "TBL_SLV_MDS_{}".format(item)
            dict_pref_consolidated[item] = tbl_name_gold_mds

        return dict_pref_consolidated
    except Exception as ex:
        raise Exception("Fallo el método 'generate_prefix_MDS_lists'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para seleccionar las columnas asociadas a una entidad de MDS de acuerdo con la tabla de dominios.

# COMMAND ----------

def select_cols_ent(df_map_user_with_keys, df_currently_ent, name_ent):
    try:
        df_f_cols_ent = df_map_user_with_keys.filter(col("nombre_entidad_mds") == name_ent)

        list_cols_ent = generate_list(df_f_cols_ent, "nombre_atributo_mds")
        df_currently_ent = df_currently_ent.select(*list_cols_ent)
        return df_currently_ent
    except Exception as ex:
        raise Exception("Falló método 'select_cols_ent' para entidad: {}. Msg error: {}".format(name_ent, ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para cargar en el diccionario los datos de MDS dentro de un diccionario.

# COMMAND ----------

def load_currently_data_for_mds(df_map_user_with_keys, list_consolidated_map_sort):
    try:
        dicc_cons_ent_map = {}
        dict_pref_consolidated = generate_prefix_MDS_lists(list_consolidated_map_sort)

        for item in dict_pref_consolidated: # Recorrer diccionario con nombres de entidades de MDS y su nombre equivalente en BBDD
            df_currently_ent = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                                    schema, dict_pref_consolidated[item])

            df_currently_ent = rename_domains_columns_view(df_currently_ent)

            df_currently_ent = select_cols_ent(df_map_user_with_keys, df_currently_ent, item)

            dicc_cons_ent_map[item] = df_currently_ent # Guardar entidad en diccionario

        return dicc_cons_ent_map
    except Exception as ex:
        raise Exception("Falló método 'load_currently_data_for_mds'. Msg error: {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para cargar en el diccionario los datos de MDS dentro de un diccionario tomando en cuenta los JSON descargados del API dentro del datalake.

# COMMAND ----------

dicc_cons_ent_map = load_currently_data_for_mds(df_map_user_with_keys, list_consolidated_map_sort)

#display(dicc_cons_ent_map)

# COMMAND ----------

# MAGIC %md
# MAGIC Función principal del proceso que ejecuta los 2 primeros casos del algoritmo (ya que el 4 caso es para insertar datos en la entidad principal y relacionarlos con el subdominio, esta información no se va a actualizar). Por otro lado, esta función recibe como parámetros el nombre de la entidad con su mapeo, la tabla de Golden Record y la lista de entidades a mapear.

# COMMAND ----------

def main_references(entity_name, df_f_sel_parametric, df_entities_vs_mds, df_tbl_golden_record, ent_user_font, col_user_font, id_subdomain, list_entities_mds, list_special_ents_to_map):
    global dicc_cons_ent_map
    
    try:
        if entity_name in list_entities_mds:
            # 0. Seleccionar campos de la tabla paramétrica para la entidad recibida por parámetro
            df_map_mds = df_f_sel_parametric.filter(col("nombre_entidad_mds") == entity_name)

            df_map_mds = df_map_mds.select("nombre_entidad", "nombre_funcional_rename", "nombre_atributo_mds")

            # Leer tabla de llaves
            name_ent, name_pk = get_key_font_golden_record(df_entities_vs_mds, entity_name) # Extraer nombre de entidad de la fuente y su pk (que se va a mapear al campo "ids_en_fuentes") para quitar duplicados en la entidad que se está iterando

            # Añadir nueva fila con ids_en_fuentes para MDS
            new_row1 = spark.createDataFrame([(name_ent, name_pk, "ids_en_fuentes")], schema = ["nombre_entidad", "nombre_funcional_rename", "nombre_atributo_mds"]) # Mapeo de ids_en_fuentes para futuro proceso de update

            # Añadir nueva fila con pk de la columna que representa el id del usuario de la entidad principal del modelo
            new_row2= spark.createDataFrame([(ent_user_font, col_user_font, "pk_ent_principal")], schema = ["nombre_entidad", "nombre_funcional_rename", "nombre_atributo_mds"])

            df_map_mds = df_map_mds.union(new_row1).union(new_row2)

            # 1. Mapeo de entidad de MDS recibida por parámetro
            df_ent_mds = map_ent_by_golden_record(df_map_mds, name_pk, entity_name, df_tbl_golden_record, ent_user_font, col_user_font)

        # if está mapeada: caso 1
        # elif no está mapeada, tiene solo dos referencias y una es a subdominio: caso 4
        # elif no está mapeada, tiene solo dos referencias y ninguna es a subdominio: caso 2
        # else: reporte de caso no considerado.

        if entity_name in list_entities_mds:

            if len(var_lista_refs_internas) != 0:
                df_new_ent_mds = df_ent_mds

                for item in var_lista_refs_internas:
                    ent_ref_name = item[0] # Recuperar el nombre de la entidad referenciada
                    col_ref_name = item[1] # Recuperar el nombre del atributo referenciada

                    df_ent_ref = dicc_cons_ent_map[ent_ref_name] # Rescatar del diccionario la entidad referenciada

                    name_ent_ref, name_pk_ref = get_key_font_golden_record(df_entities_vs_mds, ent_ref_name) # De acuerdo al nombre de la entidad de MDS recibida por parámetro, se extrae de silver el nombre de entidad de la fuente y su pk (que se va a mapear al campo "ids_en_fuentes") para quitar duplicados

                    df_new_ent_mds = case1(df_new_ent_mds, df_ent_ref, ent_ref_name, col_ref_name, name_pk_ref, name_pk, entity_name, df_tbl_golden_record, ent_user_font, col_user_font)

                if entity_name in dicc_cons_ent_map: # Comprobamos si la entidad ya está guardada en el diccionario para hacer el append
                    df_ent_ant = dicc_cons_ent_map[entity_name]
                    
                    if df_ent_mds.count() > 0:
                        list_cols = df_new_ent_mds.columns
                        df_ent_ant = df_ent_ant.select(*list_cols) # Para hacer el unión entre dos entidades, hacemos un select teniendo el mismo orden en las dos entidades a unir
                        dicc_cons_ent_map[entity_name] = df_ent_ant.union(df_new_ent_mds)
                else: # Sino se agrega clave con la entidad nueva
                    dicc_cons_ent_map[entity_name] = df_new_ent_mds

                dict_df_insert_ent[entity_name] = df_new_ent_mds
            else: # Si la entidad está mapeada por el usuario, pero no entra en caso1, se omite su ejecución y se guarda en el diccionario
                if entity_name in dicc_cons_ent_map: # Comprobamos si la entidad ya está guardada en el diccionario para hacer el append
                    df_ent_ant = dicc_cons_ent_map[entity_name]
                    
                    if df_ent_mds.count() > 0:
                        list_cols = df_ent_mds.columns
                        df_ent_ant = df_ent_ant.select(*list_cols)
                        dicc_cons_ent_map[entity_name] = df_ent_ant.union(df_ent_mds) # Para hacer el unión entre dos entidades, hacemos un select teniendo el mismo orden en las dos entidades a unir
                        
                else: # Sino se agrega clave con la entidad nueva
                    dicc_cons_ent_map[entity_name] = df_ent_mds # Agregar al diccionario entidad generada

                dict_df_insert_ent[entity_name] = df_ent_mds

        elif len(var_lista_refs_internas) == 2:
            df_ent_mds = case2(var_lista_refs_internas, df_tbl_golden_record, df_entities_vs_mds, col_user_font, entity_name)
            
            if entity_name in dicc_cons_ent_map: # Comprobamos si la entidad ya está guardada en el diccionario para hacer el append
                
                df_ent_ant = dicc_cons_ent_map[entity_name]
                
                if df_ent_mds.count() > 0:
                    list_cols = df_ent_mds.columns # Extraemos los nombres de las columnas donde se incluye el pk_ent_principal, ids_en_fuentes y campos de fks
                    
                    df_ent_ant = df_ent_ant.select(*list_cols)
                
                    dicc_cons_ent_map[entity_name] = df_ent_ant.union(df_ent_mds)
            else: # Sino se agrega clave con la entidad nueva
                dicc_cons_ent_map[entity_name] = df_ent_mds
            
            dict_df_insert_ent[entity_name] = df_ent_mds

        else:
            raise Exception("Metodo main_references, la entidad {} no entra en ninguno de los casos".format(entity_name))
    except Exception as ex:
        raise Exception("Fallo el método 'main_references'. Entidad: '{}' Msg {}".format(entity_name, ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Desencadenar proceso principal recorriendo las entidades en orden.

# COMMAND ----------

for entity_name_mds in list_consolidated_map_sort: # LISTA TIENE IR EN ORDEN DE CREACION
    
    var_lista_refs_internas = generate_list_refs(df_f_domains, entity_name_mds, list_entities_mds, list_special_ents_to_map)
    
    if entity_name_mds in list_entities_mds:
        # ELSE CASO 1:
        df_currently_ent = dicc_cons_ent_map[entity_name_mds]

        df_f_gd = df_tbl_golden_record.join(df_currently_ent, col(col_user_font) == df_currently_ent.pk_ent_principal, 'left') \
           .filter(df_currently_ent.pk_ent_principal.isNull()) \
            .select(*df_tbl_golden_record.columns)

        dict_df_new_pks[entity_name_mds] = df_f_gd
    
        main_references(entity_name_mds, df_f_sel_parametric, df_entities_vs_mds, df_f_gd, ent_user_font, col_user_font, id_subdomain, list_entities_mds, list_special_ents_to_map)
    elif len(var_lista_refs_internas) == 2 and ('subdominio' in var_lista_refs_internas[0] or 'subdominio' in var_lista_refs_internas[1]):
        df_entities_vs_mds = update_tbl_entities_vs_mds(var_lista_refs_internas, df_entities_vs_mds, entity_name_mds) # Actualizar el mapeo para que la entidad puente incluya el nombre de la llave de la entidad principal del modelo
    elif len(var_lista_refs_internas) == 2: # Identificar si la entidad es caso2 (tabla puente no mapeada) 
        
        name_ent_ref1 = var_lista_refs_internas[0][0]
        name_ent_ref2 = var_lista_refs_internas[1][0]
        
        if name_ent_ref1 in dict_df_new_pks and name_ent_ref2 in dict_df_new_pks:
            df_ent_ref1 = dict_df_new_pks[name_ent_ref1]
            df_ent_ref2 = dict_df_new_pks[name_ent_ref2]
            df_f_gd = df_ent_ref1.union(df_ent_ref2).dropDuplicates() # (DISTINCT DE LAS FILAS ENTERAS)
        elif name_ent_ref1 in dict_df_new_pks:
            df_f_gd = dict_df_new_pks[name_ent_ref1]
        elif name_ent_ref2 in dict_df_new_pks:
            df_f_gd = dict_df_new_pks[name_ent_ref2]
        else: # No debería llegar al else
            raise Exception("No hay datos del golden record para insert de ninguna de las dos entidades referenciadas")
        
        main_references(entity_name_mds, df_f_sel_parametric, df_entities_vs_mds, df_f_gd, ent_user_font, col_user_font, id_subdomain, list_entities_mds, list_special_ents_to_map)
    else:
        raise Exception("La entidad '{}' No pasó por ningún caso".format(entity_name_mds))

# COMMAND ----------

# MAGIC %md
# MAGIC # Escribir en MDS

# COMMAND ----------

write_mds(dict_df_insert_ent, param_key_datalake, secret_scope, storage_account_name, container_gold, final_path, parquet_file_extension)
