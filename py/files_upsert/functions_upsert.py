# Databricks notebook source
# MAGIC %md
# MAGIC Completar relacionamiento para filial y subdominio

# COMMAND ----------

def read_map_subdomain_x_filial(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name, schema, model_name_mds, subdomain):
    try:
        # Leemos la tabla que contiene la info de la entidad principal y la tabla intermedia para relacionar el subdominio
        df_rel_sub = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                                    schema, "tbl_rel_subdominio_x_modelo")

        # Filtramos los datos correspondientes al modelo de MDS, subdominio y filial recibidos por parámetro
        df_f_rel_sub = df_rel_sub.filter((col("nombre_modelo") == model_name_mds) & (col("nombre_filial") == filial_name) & (col("nombre_subdominio") == subdomain))

        # En esta variable guardamos los datos de la entidad principal, id de subdominio, id de filial y entidad vs subdominio que se mandan como constantes en las tablas puente
        list_data = df_f_rel_sub.collect()

        ent_main_model = list_data[0]["entidad_principal_modelo"]

        id_subdomain = list_data[0]["subdominio_id"]

        id_filial = list_data[0]["filial_id"]

        ent_rel_subdomain = list_data[0]["entidad_vs_subdominio"]
        
        return (ent_main_model, id_subdomain, id_filial, ent_rel_subdomain)
    except Exception as ex:
        raise Exception("Fallo el método 'read_map_subdomain_x_filial'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para listar las entidades mapeadas por el usuario dentro de la tabla paramétrica y devolver una lista con el orden en que se debe hacer la inserción a MDS.

# COMMAND ----------

def list_entities_map_user(df_orden_ins, df_f_parametric):
    # 1. Filtrar entidades de MDS que hayan sido mapeadas por el usuario
    df_f_orden_ins = df_orden_ins.join(df_f_parametric, df_orden_ins.entidad == df_f_parametric.nombre_entidad_mds, 'inner') \
            .select("orden", df_orden_ins.entidad).dropDuplicates()
    
    list_entities_mds = generate_list(df_f_orden_ins, "entidad", "orden")
    return list_entities_mds

# COMMAND ----------

# MAGIC %md
# MAGIC Función para listar entidades especiales no mapeadas por el usuario, pero donde las entidades referenciadas se encuentran mapeadas por el usuario.

# COMMAND ----------

def list_special_entities(df_orden_ins, list_entities_mds, ent_main_model):
    try:
        # Listar entidades especiales
        df_f_special_entities = df_orden_ins.filter((col("entidad").like("%\_m\_%")))

        df_ent_ind_m = df_f_special_entities.filter((col("entidad").like("%\_m\_%")) & (~col("entidad").like("%\_mm\_%")))

        list_ents = generate_list(df_ent_ind_m, "entidad")

        list_tot_map_mds_p1 = list_entities_mds.copy()

        list_return_mds = []

        for item in list_ents:

            ent1, ent2 = item.split("_m_")

            if (ent1 in list_entities_mds and ent2 in list_entities_mds) or (ent1 == ent_main_model and ent2 == "subdominio"):
                list_tot_map_mds_p1.append(item)
                list_return_mds.append(item)

        df_ent_ind_m_m = df_f_special_entities.filter((col("entidad").like("%\_mm\_%")))

        list_ents_m_m = generate_list(df_ent_ind_m_m, "entidad")

        for item in list_ents_m_m:
            ent1, ent2 = item.split("_mm_")

            if ent1 in list_tot_map_mds_p1 and ent2 in list_tot_map_mds_p1:
                list_return_mds.append(item)

        return list_return_mds
    except Exception as ex:
        raise Exception("Fallo el método 'list_special_entities'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función que con base a la entidad de MDS recibida por parámetro, nos devuelve una lista que contiene los nombres de los atributos de tipo Domain (es decir los que son fk que hacen referencia a otra entidad).

# COMMAND ----------

def generate_list_refs(df_f_domains, entity_name, list_entities_mds, list_special_ents_to_map):
    try:
        df_temp_iter_refs = df_f_domains.filter((col("nombre_entidad") == entity_name) & (col("tipo_atributo") == "Domain")) # Filtro de solo referencias domain para la entidad recibida por parámetro

        var_lista_refs_internas = []

        df_domain_ref_collect = df_temp_iter_refs.collect()

        # iteración sobre las referencias
        for referencia in df_domain_ref_collect:
            #si la referencia está en lista por mapear o es igual a subdominio
            if (referencia['entidad_dominio'] in list_entities_mds) or (referencia['entidad_dominio'] == 'subdominio') or (referencia['entidad_dominio'] in list_special_ents_to_map):
                var_lista_refs_internas.append([referencia['entidad_dominio'],referencia['nombre_atributo']])

        return var_lista_refs_internas
  
    except Exception as ex:
        raise Exception("Fallo el método 'generate_list_refs'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función que sirve para incluir en una lista las entidades de MDS (mapeadas por el usuario), junto a las entidades especiales que no son mapeadas por el usuario.

# COMMAND ----------

def list_map_mds_complete(df_orden_ins, list_entities_mds, ent_main_model):
    try:
        # Listar entidades especiales
        df_f_special_entities = df_orden_ins.filter((col("entidad").like("%\_m\_%")))

        df_ent_ind_m = df_f_special_entities.filter((col("entidad").like("%\_m\_%")) & (~col("entidad").like("%\_mm\_%")))

        list_ents = generate_list(df_ent_ind_m, "entidad")

        list_tot_map_mds_p1 = list_entities_mds.copy()

        for item in list_ents:
            ent1, ent2 = item.split("_m_")

            if (ent1 in list_entities_mds and ent2 in list_entities_mds) or (ent1 == ent_main_model and ent2 == "subdominio"):
                list_tot_map_mds_p1.append(item)

        list_tot_map_mds = list_tot_map_mds_p1

        df_ent_ind_m_m = df_f_special_entities.filter((col("entidad").like("%\_mm\_%")))

        list_ents_m_m = generate_list(df_ent_ind_m_m, "entidad")

        for item in list_ents_m_m:

            ent1, ent2 = item.split("_mm_")

            if ent1 in list_tot_map_mds_p1 and ent2 in list_tot_map_mds_p1:
                list_tot_map_mds.append(item)
                
        return list_tot_map_mds
    except Exception as ex:
        raise Exception("Fallo el método 'list_map_mds_complete'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para caso 1: ENTIDADES MAPEADAS POR USUARIO.

# COMMAND ----------

def case1(df_ent_mds, df_ent_ref, ent_ref_name, col_ref_name, name_pk_ref, name_pk, entity_name, df_tbl_golden_record, ent_user_font, col_user_font):
    try:
        # 2. Hacer un select distinct de las llaves del golden record que representan el código del usuario y el id en la fuente de la entidad que se está mapeando
        df_dist_gd = df_tbl_golden_record.select(name_pk, col_user_font, name_pk_ref).dropDuplicates()
        
        # 3. Cruzar entidad referenciada con el df de los códigos de las entidades del golden record utilizando el pk principal del modelo
        alias_code_ent_ref = ent_ref_name + "_Code"

        df_temp_cruce1 = df_ent_ref.withColumn(alias_code_ent_ref, col("Code")) \
                .join(df_dist_gd, [df_ent_ref.ids_en_fuentes == col(name_pk_ref), df_ent_ref.pk_ent_principal == col(col_user_font)], 'inner')

        # 4. Cruzar dataframe anterior con la entidad principal recibida por parametro
        alias_code_ent = entity_name + "_Code"
        df_fk_pk_ents = df_ent_mds.withColumn(alias_code_ent, col("Code")) \
                .join(df_temp_cruce1, [df_ent_mds.ids_en_fuentes == col(name_pk), df_ent_mds.pk_ent_principal == col(col_user_font)], 'inner') \
               .select(alias_code_ent, alias_code_ent_ref).dropDuplicates()

        # 5. Finalmente añadir el campo referenciado a la entidad iterada
        df_new_ent_mds = df_ent_mds.join(df_fk_pk_ents, df_ent_mds.Code == col(alias_code_ent), 'left') # Se hace left join porque es posible que un registro no tenga referencia con la entidad en cuestión
        
        df_new_ent_mds = df_new_ent_mds.withColumnRenamed(alias_code_ent_ref, col_ref_name) # Asignar alias de entidad referenciada
        
        df_new_ent_mds = df_new_ent_mds.drop(alias_code_ent)
        
        
        list_sel_cols = df_ent_mds.columns + [col_ref_name]
        df_new_ent_mds = df_new_ent_mds.withColumn("Code", sha2(concat_ws("|", *list_sel_cols), 256))

        return df_new_ent_mds
    except Exception as ex:
        raise Exception("Fallo el método 'case1'. Entidad {}. Pk Principal {}. Entidad referenciada {}. PK Referenciada. Msg {}".format(entity_name, name_pk_ref, ent_ref_name, name_pk, ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para caso2: ENTIDADES NO MAPEADAS POR USUARIO (maximo dos referencias) (no para tablas puentes referenciando tablas puentes).

# COMMAND ----------

def case2(var_lista_refs_internas, df_tbl_golden_record, df_entities_vs_mds, col_user_font, entity_name):
    try:
        #se toma la primera referencia
        ref_uno = var_lista_refs_internas[0] # Lista con referencia de entidad 1
        ref_dos = var_lista_refs_internas[1] # Lista con referencia de entidad 2

        # 1. Lectura entidades referenciada
        name_ent_ref1 = ref_uno[0]
        name_id_ent_ref1 = ref_uno[1]
        name_ent_ref2 = ref_dos[0]
        name_id_ent_ref2 = ref_dos[1]

        df_ent_ref_mds1 = dicc_cons_ent_map[name_ent_ref1]

        df_ent_ref_mds2 = dicc_cons_ent_map[name_ent_ref2]


        # Leemos id en la fuente de entidad referenciada 1
        name_ent_ref1, name_pk_ref1 = get_key_font_golden_record(df_entities_vs_mds, name_ent_ref1)

        # Leemos id en la fuente de entidad referenciada 2
        name_ent_ref2, name_pk_ref2 = get_key_font_golden_record(df_entities_vs_mds, name_ent_ref2)


        # 3. Select distinct de la tabla de golden record de los ids en la fuente de las dos entidades referenciadas

        df_cods_gld = df_tbl_golden_record.select(col(col_user_font).alias("PK_USUARIO_MODELO"), col(name_pk_ref1).alias("PK_GD_ENT1"), col(name_pk_ref2).alias("PK_GD_ENT2")) \
            .dropDuplicates() # Se colocan alias a los campos por si coinciden que los de las entidades referenciadas son los mismos o coinciden con el de la entidad principal del modelo

        # 4.1. Join de la tabla de golden record con la entidad referenciada 1
        df_temp1 = df_cods_gld.join(df_ent_ref_mds1, [df_cods_gld.PK_USUARIO_MODELO == df_ent_ref_mds1.pk_ent_principal, df_cods_gld.PK_GD_ENT1 == df_ent_ref_mds1.ids_en_fuentes], 'inner')
        df_temp1 = df_temp1.withColumnRenamed("Code", name_id_ent_ref1) # Renombrar Code que corresponde al PK de la entidad referenciada uno a su nombre equivalente de FK

        # 4.1. Join de la tabla de golden record con la entidad referenciada 2 (ya incluye la unión con la entidad referenciada1)
        df_temp2 = df_temp1.join(df_ent_ref_mds2, [df_temp1.PK_USUARIO_MODELO == df_ent_ref_mds2.pk_ent_principal, df_temp1.PK_GD_ENT2 == df_ent_ref_mds2.ids_en_fuentes], 'inner')
        df_temp2 = df_temp2.withColumnRenamed("Code", name_id_ent_ref2) # Renombrar Code que corresponde al PK de la entidad referenciada dos a su nombre equivalente de FK

        # 5. Generar entidad intermedia
        
        # Nota: El id en la fuente de la entidad intermedia va a ser el mismo pk_ent_principal
        df_new_ent_mds = df_temp2.select(col("PK_USUARIO_MODELO").alias("ids_en_fuentes"), col("PK_USUARIO_MODELO").alias("pk_ent_principal"), name_id_ent_ref1, name_id_ent_ref2) \
            .dropDuplicates()

        df_new_ent_mds = df_new_ent_mds.filter(col("ids_en_fuentes").isNotNull()) # Se filtra para asegurarse que todos los registros tengan Code
        
        #df_new_ent_mds = row_number_df(df_new_ent_mds, "ids_en_fuentes")
        
        list_sel_cols = ["ids_en_fuentes", "pk_ent_principal", name_id_ent_ref1, name_id_ent_ref2]
        df_new_ent_mds = df_new_ent_mds.withColumn("Code", sha2(concat_ws("|", *list_sel_cols), 256)) # Añadir Code a entidad intermedia
        
        df_new_ent_mds = df_new_ent_mds.select("Code", *list_sel_cols)
        return df_new_ent_mds
    except Exception as ex:
        raise Exception("Fallo el método 'case2'. Entidad {}. Msg {}".format(entity_name, ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para caso 4: (Tabla puente entre entidad mapeada y entidad subdominio) (solo se crea una fila/registro de relacion).

# COMMAND ----------

def case4(var_lista_refs_internas, df_tbl_golden_record, id_subdomain, entity_name, col_user_font):
    try:
        for ref in var_lista_refs_internas: # Iterar lista de referencias
            if ref[0] != 'subdominio':
                # se saca el df de la entidad principal que es referenciada (solo debe tener un registro)
                atr_ref_principal = ref # Guardar sublista con datos del nombre de la entidad principal de MDS y su referencia

                # 1. Recuperar del diccionario dataframe con entidad principal
                var_ref_principal = dicc_cons_ent_map[ref[0]]
            else:
                atr_subd = ref # Guardar sublista con datos del nombre de la entidad de subdominio de MDS y su referencia

        # 2. Select distinct de codigos de entidad principal dentro de la tabla de golden record
        if "estadoregistro" in df_tbl_golden_record.columns:
            df_dist_gd = df_tbl_golden_record.select(col_user_font, "estadoregistro").dropDuplicates()
            list_sel_cols = [atr_ref_principal[1], atr_subd[1], "estado", "ids_en_fuentes", "pk_ent_principal"]
        else:
            df_dist_gd = df_tbl_golden_record.select(col_user_font).dropDuplicates()
            list_sel_cols = [atr_ref_principal[1], atr_subd[1], "ids_en_fuentes", "pk_ent_principal"]
        
        # 3. Select de los campos ids_en_fuentes y Code sobre la entidad principal 
        df_select_ent_main = var_ref_principal.select("Code", "ids_en_fuentes")

        # 4. Join final
        df_new_ent_mds = df_dist_gd.join(df_select_ent_main, col(col_user_font) == df_select_ent_main.ids_en_fuentes, 'inner')
        df_new_ent_mds = df_new_ent_mds.withColumnRenamed("Code", atr_ref_principal[1]) # Renombrar código de entidad principal como fk
        df_new_ent_mds = df_new_ent_mds.withColumn(atr_subd[1], lit(id_subdomain)) # Añadir nueva columna con el id del subdominio que es una constante
        df_new_ent_mds = df_new_ent_mds.withColumnRenamed("estadoregistro", "estado") # Renombrar columna de "estadoregistro" para que coincida con la que se encuentra en MDS
        df_new_ent_mds = df_new_ent_mds.withColumn("pk_ent_principal", col("ids_en_fuentes")) # Para el caso4 el pk_ent_principal corresponde al ids_en_fuentes, que es el PK de la entidad principal del modelo de MDS
        
        df_new_ent_mds = df_new_ent_mds.filter(col("ids_en_fuentes").isNotNull()) # Se filtra para asegurarse que todos los registros tengan Code
        
        #df_new_ent_mds = row_number_df(df_new_ent_mds, "ids_en_fuentes")
        
        df_new_ent_mds = df_new_ent_mds.withColumn("Code", sha2(concat_ws("|", *list_sel_cols), 256)) # Añadir Code a entidad intermedia
        
        # Select de campos de la entidad
        df_new_ent_mds = df_new_ent_mds.select("Code", *list_sel_cols)

        return df_new_ent_mds
    except Exception as ex:
        raise Exception("Fallo el método 'case4'. Entidad {}. Msg {}".format(entity_name, ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para renombrar las columnas del golden record a su equivalente en MDS.

# COMMAND ----------

def map_golden_record(df_map_mds, df_f_golden_record):
    try:
        # 1. Recolectar en un diccionario el mapeo de las columnas de las entidades mapeadas en la tabla paramétrica
        dict_map_rename = map_columns_into_dict(df_map_mds, "nombre_atributo_mds", "nombre_funcional_rename")

        # 2. Recorrer diccionario con mapeo previo para que el golden record quede en "terminos de MDS", es decir con las columnas de MDS
        for key in dict_map_rename:
            df_f_golden_record = df_f_golden_record.withColumn(key, col(dict_map_rename[key]))
        
        # 3. Seleccionar únicamente columnas asociadas a MDS
        list_cols_mds = generate_list(df_map_mds, "nombre_atributo_mds")
        
        # 3.1 Añadir columna Code al select del mapeo
        #list_cols_mds.append("Code")
        df_f_golden_record = df_f_golden_record.select(*list_cols_mds)
        
        return df_f_golden_record
    except Exception as ex:
        raise Exception("Fallo el método 'map_golden_record'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Mapeo o renombramiento de los campos de MDS en la tabla de Golden record de la fuente de ingesta.

# COMMAND ----------

def map_ent_by_golden_record(df_map_mds, name_pk, entity_name, df_f_golden_record, ent_main_modelo, atrib_main_modelo):
    try:
        df_ent_mapeada = drop_duplicates_by_column(df_f_golden_record, name_pk) # Quitar duplicados por columna llave especificada para la entidad que se está iterando
        df_ent_mapeada = df_ent_mapeada.filter(col(name_pk).isNotNull()) # Se filtra para asegurarse que todos los registros tengan Code
        
        df_ent_mapeada = map_golden_record(df_map_mds, df_ent_mapeada)
        
        df_ent_mapeada = df_ent_mapeada.withColumn("Code", sha2(concat_ws("|", *df_ent_mapeada.columns), 256))

        return df_ent_mapeada
    except Exception as ex:
        raise Exception("Fallo el método 'map_ent_by_golden_record'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Esta función nos sirve para actualizar la tabla de mapeo de entidades de silver contra MDS incluyendo la entidad intermedia que no está mapeada por el usuario.

# COMMAND ----------

def update_tbl_entities_vs_mds(var_lista_refs_internas, df_entities_vs_mds, entity_name):
    try:
        for ref in var_lista_refs_internas: # Iterar lista de referencias
            if ref[0] != 'subdominio':
                # se saca el df de la entidad principal que es referenciada (solo debe tener un registro)
                atr_ref_principal = ref # Guardar sublista con datos del nombre de la entidad principal de MDS y su referencia

                #AGREGAR A df_entities_vs_mds RELACION DE ENTIDAD MAPEADA CON LOS DATOS DE LA REF_PRINCIPAL.
                fila_map_ent_princ = df_entities_vs_mds.filter(col("nombre_entidad_mds") == atr_ref_principal[0])

                fila_map_ent_princ = fila_map_ent_princ.withColumn("nombre_entidad_mds", lit(entity_name))

                df_entities_vs_mds = df_entities_vs_mds.union(fila_map_ent_princ)
        return df_entities_vs_mds
    except Exception as ex:
        raise Exception("Fallo el método 'update_tbl_entities_vs_mds'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para obtener el nombre de la entidad en silver y su correspondiente pk apartir del nombre de una entidad de MDS.

# COMMAND ----------

def get_key_font_golden_record(df_entities_vs_mds, entity_name):
    try:
        df_key_font_mds = df_entities_vs_mds.filter(col("nombre_entidad_mds") == entity_name) # Saber que llave de la fuente (SAP) está relacionada para colocarla como PK temporal
        df_key_font_mds = df_key_font_mds.select("nombre_entidad", "campo_clave_entidad_2").drop_duplicates()

        # Extraer en variables nombre de llave de fuente y nombre de tabla
        data_map = df_key_font_mds.collect()
        name_ent = data_map[0]["nombre_entidad"]

        name_pk = data_map[0]["campo_clave_entidad_2"]

        return (name_ent, name_pk)
    except Exception as ex:
        raise Exception("Fallo el método 'get_key_font_golden_record'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para escritura final en el datalake iterando un diccionario con los dataframes de las entidades resultantes.

# COMMAND ----------

def write_mds(dicc_ent_map, param_key_datalake, secret_scope, storage_account_name, container_gold, dest_path_entities, parquet_file_extension):
    try:
        # 1. Recorrer clave del diccionario con el mapeo final
        for key in dicc_ent_map:
            df_entity_mds = dicc_ent_map[key]

            # 2. Escribir entidad en el datalake
            write_file(df_entity_mds, "parquet", param_key_datalake, secret_scope, storage_account_name, container_gold, dest_path_entities, key, parquet_file_extension)
    except Exception as ex:
        raise Exception("Fallo el método 'write_mds'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para renombrar las columnas de las vistas que son fk o de tipo Domain para que coincidan con la tabla del método get domains (tbl_mds_dominios).

# COMMAND ----------

def rename_domains_columns_view(df_currently_ent):
    try:
        for item in df_currently_ent.columns:
            if item.endswith('_Code'):
                df_currently_ent = df_currently_ent.withColumn(item[0:len(item) - 5], col(item))

        return df_currently_ent
    except Exception as ex:
        raise Exception("Fallo el método 'rename_domains_columns_view'. Msg {}".format(ex))
