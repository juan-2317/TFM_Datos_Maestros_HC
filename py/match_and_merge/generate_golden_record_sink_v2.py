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

# COMMAND ----------

# Leer parámetros

# TODO: Descomentar estos parámetros en caso de que se desee probar el notebook manualmente, en caso contrario no es necesario debido a que dichos parámetros se están llamando en el notebook del M&M llamado "match_and_merge_v4"
# dbutils.widgets.text("filialName", "","")
# filial_name = dbutils.widgets.get("filialName")
# dbutils.widgets.text("subdomain", "","")
# subdomain = dbutils.widgets.get("subdomain")
# dbutils.widgets.text("domainMDSName", "","")
# domain_mds_name = dbutils.widgets.get("domainMDSName")
# dbutils.widgets.text("tblGoldenRecordName", "","")
# tbl_golden_record_name = dbutils.widgets.get("tblGoldenRecordName")
# dbutils.widgets.text("sourcePath", "","")
# source_path = dbutils.widgets.get("sourcePath")
# dbutils.widgets.text("finalPath", "","")
# final_path = dbutils.widgets.get("finalPath")
# dbutils.widgets.text("parquetFileExtension", "","")
# parquet_file_extension = dbutils.widgets.get("parquetFileExtension")
# dbutils.widgets.text("paramKeyDatalake", "","")
# param_key_datalake = dbutils.widgets.get("paramKeyDatalake")
# dbutils.widgets.text("hostName", "","")
# host_name = dbutils.widgets.get("hostName")
# dbutils.widgets.text("port", "","")
# port = int(dbutils.widgets.get("port"))
# dbutils.widgets.text("dbName", "","")
# db_name = dbutils.widgets.get("dbName")
# dbutils.widgets.text("usrName", "","")
# usr_name = dbutils.widgets.get("usrName")
# dbutils.widgets.text("schema", "","")
# schema = dbutils.widgets.get("schema")
# dbutils.widgets.text("paramKeyAzSql", "","")
# param_key_az_sql = dbutils.widgets.get("paramKeyAzSql")
# dbutils.widgets.text("secretScopeDatabricks", "","")
# secret_scope = dbutils.widgets.get("secretScopeDatabricks")
# dbutils.widgets.text("storageAccountName", "","")
# storage_account_name = dbutils.widgets.get("storageAccountName")
# dbutils.widgets.text("containerGold", "","")
# container_gold = dbutils.widgets.get("containerGold")

# dbutils.widgets.text("entitiesSilverPath", "","")
# entities_silver_path = dbutils.widgets.get("entitiesSilverPath")
# dbutils.widgets.text("destGoldPath", "","")
# dest_gold_path = dbutils.widgets.get("destGoldPath")
# dbutils.widgets.text("containerSilver", "","")
# container_silver = dbutils.widgets.get("containerSilver")
# dbutils.widgets.text("checkWriteGD_DL", "","")
# check_write_gd_dl = dbutils.widgets.get("checkWriteGD_DL")


# print(filial_name)
# print(subdomain)
# print(domain_mds_name)
# print(tbl_golden_record_name)
# print(source_path)
# print(final_path)
# print(parquet_file_extension)
# print(param_key_datalake)
# print(host_name)
# print(port)
# print(db_name)
# print(usr_name)
# print(schema)
# print(param_key_az_sql)
# print(secret_scope)
# print(storage_account_name)
# print(container_gold)

# print(entities_silver_path)
# print(dest_gold_path)
# print(container_silver)
# print(check_write_gd_dl)

# COMMAND ----------

# MAGIC %md
# MAGIC Leeemos los datos de la tabla paramétrica

# COMMAND ----------

df_parametric = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_matriz_dato")
#display(df_parametric)

# COMMAND ----------

df_f_parametric = df_parametric.filter((col("golden_record") == "Y") & (col("nombre_filial") == filial_name) \
                                         & (col("subdominio") == subdomain))
#display(df_f_parametric)

# COMMAND ----------

# MAGIC %md
# MAGIC Leer vista con entidades de silver que forman el golden record

# COMMAND ----------

df_silver_gd = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "vw_entidades_silver_golden_record")

df_silver_gd = df_silver_gd.filter((col("nombre_filial") == filial_name) & (col("subdominio") == subdomain))
#display(df_silver_gd)

# COMMAND ----------

# MAGIC %md
# MAGIC Leer vista que contiene el mapeo de llaves entre entidades y tablas de bronze y silver, así como los ids naturales y criterio para quitar duplicados en el M&M.

# COMMAND ----------

df_keys_silver = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "vw_info_llaves_tabla_entidad")

df_keys_silver = df_keys_silver.filter((col("nombre_filial") == filial_name)
                                         & (col("subdominio") == subdomain))
#display(df_keys_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC # Leemos y registramos las entidades de Silver (mapeadas al Golden Record) como vistas para poder hacer consultas SQL sobre Pyspark

# COMMAND ----------

list_entities_silver = generate_list(df_silver_gd, 'nombre_entidad')
list_entities_silver

# COMMAND ----------

for item in list_entities_silver:
    try:
        # Leer entidad de silver en un dataframe
        df_entity = read_df_from_container(secret_scope, param_key_datalake, storage_account_name, container_silver, entities_silver_path, item, parquet_file_extension)
        df_entity.createOrReplaceTempView(item)
    except Exception as ex:
        raise Exception("Falló lectura de la entidad {}. Es posible que falte parametrizar la entidad tanto en la tabla paramétrica como en la tabla de relaciones. Msg error {}".format(item, ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Leemos la tabla que contiene las relaciones para la tabla de golden record

# COMMAND ----------

df_main_rel_silver = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_relacion_entidades_silver")
#display(df_main_rel_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Filtramos los registros correspondientes a la filial y subdominio a procesar

# COMMAND ----------

df_rel_silver = df_main_rel_silver.filter((col("nombre_filial") == filial_name) \
                                         & (col("subdominio") == subdomain))

#display(df_rel_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Control de excepciones para verificar que los tipos de relación se encuentren en los tres tipos permitidos (LEFT, RIGHT, INNER)

# COMMAND ----------

if df_rel_silver.filter(~col("tipo_relacion").isin("LEFT", "RIGHT", "INNER", "FULL OUTER")).count() > 0:
    raise Exception("Por favor revisar la tabla de relaciones debido a que hay un tipo de relación que no se encuentra entre los tipos admitidos [LEFT, RIGHT, INNER].")

# COMMAND ----------

# MAGIC %md
# MAGIC Debido a que nos importa el orden para armar las relaciones del modelo, están las columnas de orden_relacion_entidad y orden_en_grupo y procedemos a ordenarlas por estas

# COMMAND ----------

df_rel_silver = df_rel_silver.orderBy("orden_relacion_entidad", "orden_en_grupo")
#display(df_rel_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Concatenamos (es decir hacer un union) de los campos de la tabla paramétrica con los campos de las llaves de silver y con la tabla de los campos ingestados de silver quitando finalmente duplicados. Esto se hace con el objetivo de seleccionar para el golden record todas las columnas incluidas las llaves

# COMMAND ----------

# Crear alias de los ampos para poder hacer el unión correctamente
df_f_rel_silver = df_rel_silver.select(col("nombre_entidad_padre").alias("nombre_entidad"), col("campo_clave").alias("nombre_funcional"))
df_f_keys_silver = df_keys_silver.select("nombre_entidad", col("campo_clave_entidad").alias("nombre_funcional"))

# El unión también se hace con las entidades referenciadas de la tabla de relaciones de silver
df_f2_rel_silver = df_rel_silver.select(col("entidad_relacion").alias("nombre_entidad"), col("campo_relacion").alias("nombre_funcional"))

df_tbl_cols = df_f_parametric.select("nombre_entidad", "nombre_funcional") \
        .union(df_f_rel_silver) \
        .union(df_f2_rel_silver) \
        .union(df_f_keys_silver)

df_tbl_cols = df_tbl_cols.dropDuplicates()
#display(df_tbl_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC Esta función nos sirve para generar un nombre de campo único para cada entidad de silver de la tabla paramétrica concatenando el nombre de la entidad con el nombre funcional

# COMMAND ----------

def concat_table_and_col_name(df_tbl_cols):
    try:

        df_rename_cols = df_tbl_cols.withColumn('nombre_funcional_rename',
                                                      concat(col('nombre_entidad'), lit('_'), col('nombre_funcional')))

        df_rename_cols = df_rename_cols.select("nombre_entidad", "nombre_funcional", "nombre_funcional_rename")

        return df_rename_cols
    except Exception as ex:
        raise Exception("Fallo el método 'concat_table_and_col_name'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Debido a que los nombres de los atributos se pueden repetir entre varias tablas y para formar el golden record necesitamos nombres de atributos únicos, se concatena el nombre de la tabla con el nombre de la columna

# COMMAND ----------

df_rename_cols = concat_table_and_col_name(df_tbl_cols)
#display(df_rename_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC Esta función nos sirve para generar y retornar un string con la parte select de una consulta SQL con base a una tabla recibida por parámetro con el nombre de la entidad y el nombre funcional

# COMMAND ----------

def create_str_select_golden_record(df_rename_cols):
    try:
        sql = "SELECT "

        df_collect_names = df_rename_cols.collect()

        i = 0
        len_names = len(df_collect_names)
        for row in df_collect_names:

            sql = sql + row["nombre_entidad"] + "." + row["nombre_funcional"] + " AS " + row["nombre_funcional_rename"]

            if i < (len_names - 1):
                sql = sql + ", "

            i = i + 1
        return sql
    except Exception as ex:
        raise Exception("Fallo el método 'create_str_select_golden_record'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC # Generación de dataframes con grupos de consultas para formar la tabla de golden records

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Esta función nos sirve para iterar los grupos principales que no son un subgrupo de la tabla y armar dataframes de los mismos

# COMMAND ----------

def generate_tbl_main_groups(df_rel_silver, df_rename_cols):
    try:
        df_collect = df_rel_silver.filter(col("nombre_grupo").isNotNull() & (col("orden_en_grupo").isNull())).collect()
        list_df_groups = []

        for row in df_collect:
            sql = ""

            # Para formar la tabla de golden records hacemos un select * de las tablas del grupo
            if df_rel_silver.filter((col("entidad_relacion").like("GRUPO%")) & (col("ID") == row["ID"]) & (col("nombre_grupo").like("GRUPO%"))).count() == 1:
                # Para la iteración filtramos solo las columnas de las entidades de las dos entidades del registro de la tabla de relaciones
                df_f_rename_cols = df_rename_cols.filter(col("nombre_entidad").isin(row["nombre_entidad_padre"]))

                sql_select  = create_str_select_golden_record(df_f_rename_cols)

                # Hacer select * de frente de todos los campos ya formados
                sql_select = sql_select + " , {}.*".format(row["entidad_relacion"])
            else:
                # En caso contrario hacemos el select dinámico sobre las columnas de la entidad padre y la entidad referenciada:

                # Para la iteración filtramos solo las columnas de las entidades de las dos entidades del registro de la tabla de relaciones
                df_f_rename_cols = df_rename_cols.filter(col("nombre_entidad").isin(row["nombre_entidad_padre"], row["entidad_relacion"]))

                sql_select  = create_str_select_golden_record(df_f_rename_cols)


            sql_part_join = sql_select + " FROM {} {} JOIN {} ON {}.{} = {}.{}".format(row["nombre_entidad_padre"], row["tipo_relacion"],
                                                                             row["entidad_relacion"],
                                                                             row["nombre_entidad_padre"],
                                                                             row["campo_clave"],
                                                                             row["entidad_relacion"],
                                                                             row["campo_relacion"])
            sql = sql + sql_part_join

            try:
                df_grupo = sqlContext.sql(sql)
            except AnalysisException as ae:
                raise Exception("Ha ocurrido excepción en el método 'generate_tbl_main_groups' en el grupo '{}' con la consulta sql\n{}.\n\nMsg error: {}".format(row["nombre_grupo"], sql, ae))
            
            df_grupo.createOrReplaceTempView(row["nombre_grupo"])

            list_df_groups.append((row["nombre_grupo"], df_grupo))
        
        return list_df_groups
    except Exception as ex:
        raise Exception("Fallo el método 'generate_tbl_main_groups'. Msg {}".format(ex))

# COMMAND ----------

list_main_groups = generate_tbl_main_groups(df_rel_silver, df_rename_cols)
#list_main_groups

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Esta función nos sirve para iterar los subgrupos y armar dataframes de los mismos, estos subgrupos hacen un join de una o mas entidades secundarias a la entidad principal del subgrupo (campo: nombre_entidad_padre)

# COMMAND ----------

def generate_tbl_subgroups(df_rel_silver, df_rename_cols):
    try:
        # Filtramos de la tabla de relaciones de silver los nombres de cada subgrupo para iterarlos
        df_f_group = df_rel_silver.filter(col("nombre_grupo").isNotNull() & (col("orden_en_grupo").isNotNull())).select("nombre_grupo").dropDuplicates()

        list_subgroups = generate_list(df_f_group, "nombre_grupo")
        
        list_df_subgroups = []

        # Con este bucle iteramamos cada subgrupo
        for item in list_subgroups:
            # Filtramos de la tabla de relaciones el subgrupo que se está iterando
            df_filtro_subgrupo = df_rel_silver.filter(col("nombre_grupo") == item)

            # Con este unión queremos recolectar los nombres de las entidades que se están relacionando (campos entidad padre y entidad relación), para filtrar de la tabla con los nombres funcionales y nombres de entidades, solo las entidades del subgrupo
            df_entities = df_filtro_subgrupo.select(col("nombre_entidad_padre").alias("nombre_entidad")) \
                .union(df_filtro_subgrupo.select(col("entidad_relacion").alias("nombre_entidades")))
            # Generar lista de Python con entidades del paso anterior
            list_ents_silver = generate_list(df_entities, "nombre_entidad")

            # Filtrando los nombres de las columnas y nombres de las entidades del subgrupo formamos el select dinámico
            df_f_rename_cols = df_rename_cols.filter(col("nombre_entidad").isin(list_ents_silver))
            sql_select  = create_str_select_golden_record(df_f_rename_cols)

            # Ya que la entidad principal del subgrupo nos sirve para formar el join con las entidades referenciadas, la extraemos y guardamos en una variable
            nom_entidad_padre = df_filtro_subgrupo.select("nombre_entidad_padre").dropDuplicates().collect()[0][0]
            df_collect = df_filtro_subgrupo.collect()

            sql_from = "" # Variable para formar la parte del FROM y JOIN de una consulta SQL
            cont = 0
            for row in df_collect:
                if cont == 0:
                    sql_from = sql_from + " FROM {} {} JOIN {} ON {}.{} = {}.{}".format(nom_entidad_padre, row["tipo_relacion"], row["entidad_relacion"], nom_entidad_padre, row["campo_clave"], row["entidad_relacion"], row["campo_relacion"])
                else:
                    sql_from = sql_from + " {} JOIN {} ON {}.{} = {}.{}".format(row["tipo_relacion"], row["entidad_relacion"], nom_entidad_padre, row["campo_clave"], row["entidad_relacion"], row["campo_relacion"])
                cont = cont + 1

            sql = sql_select + sql_from # Concatenar la parte SQL SELECT con la parte del join

            try:
                df_grupo = sqlContext.sql(sql)
            except AnalysisException as ae:
                raise Exception("Ha ocurrido excepción en el método 'generate_tbl_subgroups' en el grupo '{}' con la consulta sql\n{}.\n\nMsg error: {}".format(item, sql, ae))
            
            df_grupo.createOrReplaceTempView(item) # Registramos como una vista temporal de SQL cada subgrupo

            list_df_subgroups.append((item, df_grupo))
            
        return list_df_subgroups
    except Exception as ex:
        raise Exception("Fallo el método 'generate_tbl_subgroups'. Msg {}".format(ex))

# COMMAND ----------

list_subgroups = generate_tbl_subgroups(df_rel_silver, df_rename_cols)
#list_subgroups

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Esta función nos sirve para la entidad principal del modelo hacerle el join con cada uno de los subgrupos formados por los pasos anteriores

# COMMAND ----------

def generate_gd_with_main_groups(df_rel_silver, df_rename_cols, tbl_golden_record_name):
    try:
        # Primero se filtra el nombre de la entidad principal de todo el modelo

        df_f_group = df_rel_silver.filter(col("nombre_grupo").isNull() & (col("orden_en_grupo").isNull())).select("nombre_entidad_padre").dropDuplicates()

        # Generar y guardar en una variable el nombre de la entidad principal
        list_groups = generate_list(df_f_group, "nombre_entidad_padre")
        ent_principal = list_groups[0]

        # Filtramos de la tabla de relaciones el nombre de la entidad principal del modelo
        df_rel_silver_ent_principal = df_rel_silver.filter(col("nombre_entidad_padre") == ent_principal)
        # Repetimos el mismo filtro de la entidad principal del modelo para la tabla que nos indica el nombre de las columnas de las entidades de silver
        df_f_rename_cols = df_rename_cols.filter(col("nombre_entidad") == ent_principal)
        # Crear la parte del select para la entidad principal del modelo
        sql_select  = create_str_select_golden_record(df_f_rename_cols)
        # Vamos a iterar cada entidad relacionada (es decir cada subgrupo) que esté asociada a la entidad principal del modelo, para complementar el select dinámico con todos los campos
        list_rel_groups = generate_list(df_rel_silver_ent_principal, "entidad_relacion")
        for item in list_rel_groups:
            sql_select = sql_select + " , {}.*".format(item)

        sql_from = "" # Variable para armar parte del from del relacionamiento principal
        cont = 0

        df_collect = df_rel_silver_ent_principal.collect()
        for row in df_collect:
            if cont == 0:
                sql_from = sql_from + " FROM {} {} JOIN {} ON {}.{} = {}.{}".format(ent_principal, row["tipo_relacion"], row["entidad_relacion"], ent_principal, row["campo_clave"], row["entidad_relacion"], row["campo_relacion"])
            else:
                sql_from = sql_from + " {} JOIN {} ON {}.{} = {}.{}".format(row["tipo_relacion"], row["entidad_relacion"], ent_principal, row["campo_clave"], row["entidad_relacion"], row["campo_relacion"])
            cont = cont + 1

        sql = sql_select + sql_from

        try:
            df_tbl_golden_record = sqlContext.sql(sql)
        except AnalysisException as ae:
            raise Exception("Ha ocurrido excepción en el método 'generate_gd_with_main_groups' con la consulta sql\n{}.\n\nMsg error: {}".format(sql, ae))

        df_tbl_golden_record.createOrReplaceTempView(tbl_golden_record_name) # Registramos como una vista temporal de SQL la tabla completa de golden record
        
        return df_tbl_golden_record
    except Exception as ex:
        raise Exception("Fallo el método 'generate_gd_with_main_groups'. Msg {}".format(ex))

# COMMAND ----------

if df_rel_silver.count() == 1: # Si y solo si hay dos entidades parametrizadas en un único registro, la tabla de golden record final es el primer grupo formado (caso 2)
    df_tbl_golden_record = list_main_groups[0][1]
elif len(list_main_groups) == 0 and len(list_subgroups) != 0 and df_rel_silver.filter(col("orden_en_grupo").isNull()).count() == 0 and df_rel_silver.select("nombre_grupo").dropDuplicates().count() == 1: # Todas las entidades pasaron por el caso 1 de formación de grupos y solo hay un grupo total
    df_tbl_golden_record = list_subgroups[0][1]
else:
    df_tbl_golden_record = generate_gd_with_main_groups(df_rel_silver, df_rename_cols, tbl_golden_record_name)

#display(df_tbl_golden_record)
