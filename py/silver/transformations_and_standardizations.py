# Databricks notebook source
# MAGIC %md
# MAGIC # Importar Funciones Personalizadas

# COMMAND ----------

# MAGIC %run /Repos/transform_data_mdm/DatosMaestrosMDSData_Transform/functions_etl

# COMMAND ----------

# MAGIC %md
# MAGIC # Importar funciones de Python

# COMMAND ----------

import re
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC Leemos los parámetros que usaremos más adelante

# COMMAND ----------

# Creating widgets for leveraging parameters, and printing the parameters
dbutils.widgets.text("filialName", "","")
filial_name = dbutils.widgets.get("filialName")
dbutils.widgets.text("subdomain", "","")
subdomain = dbutils.widgets.get("subdomain")
dbutils.widgets.text("tableName", "","")
table_name = dbutils.widgets.get("tableName")
dbutils.widgets.text("folderPath", "","")
folder_path = dbutils.widgets.get("folderPath")
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
dbutils.widgets.text("standardDateFormat", "","")
standard_date_format = dbutils.widgets.get("standardDateFormat")

print(filial_name)
print(subdomain)
print(table_name)
print(folder_path)
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
print(standard_date_format)

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura de entidad recibida por parámetro

# COMMAND ----------

df_parquet = read_df_from_container(secret_scope, param_key_datalake, storage_account_name, container_name, folder_path, table_name, parquet_file_extension)
#display(df_parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC Lectura de tabla paramétrica

# COMMAND ----------

df_parametric = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_matriz_dato")
#display(df_parametric)

# COMMAND ----------

df_f_parametric = df_parametric.filter((col('golden_record') == 'Y') \
                                        & (col("subdominio") == subdomain) & (col("nombre_filial") == filial_name))
#display(df_f_parametric)

# COMMAND ----------

# MAGIC %md
# MAGIC Filtrar por nombre de entidad de acuerdo a lo que llegue por parámetro

# COMMAND ----------

df_parametric_ent = df_parametric.filter((col('nombre_entidad') == table_name) & (col('golden_record') == 'Y') \
                                        & (col("subdominio") == subdomain) & (col("nombre_filial") == filial_name))
#display(df_parametric_ent)

# COMMAND ----------

df_rel_fuentes = read_control_database_table(secret_scope, param_key_az_sql, host_name, port, db_name, usr_name,
                                            schema, "tbl_relacion_tablas_fuentes")
#display(df_rel_fuentes)

# COMMAND ----------

# MAGIC %md
# MAGIC # Controles de Excepciones

# COMMAND ----------

# MAGIC %md
# MAGIC Comprobamos que no se repitan los nombres de los campos en bronze excluyendo los campos llave

# COMMAND ----------

df_comp = df_parametric_ent.groupBy("tabla", "columna").count().filter(col("count") > 1)
if df_comp.count() > 0:
    list_rep_entities = generate_list(df_comp, 'columna')
    raise Exception("Se repiten los nombres de los campos en bronze {} para la entidad {}. Por favor revisar el mapeo".format(list_rep_entities, table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC Comprobamos que no se repitan los nombres de los campos de las entidades en silver

# COMMAND ----------

df_comp = df_parametric_ent.groupBy("nombre_funcional").count().filter(col("count") > 1)
if df_comp.count() > 0:
    list_rep_entities = generate_list(df_comp, 'nombre_funcional')
    raise Exception("Se repiten los nombres de los campos en silver {} para la entidad {}. Por favor revisar el mapeo".format(list_rep_entities, table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos un diccionario con clave el nombre de los campos y valor los tipos de dato

# COMMAND ----------

fields_with_data_types = map_columns_into_dict(df_parametric_ent, 'columna', 'tipo_dato')
fields_with_data_types

# COMMAND ----------

# MAGIC %md
# MAGIC # Funciones para estandarizar fechas

# COMMAND ----------

# MAGIC %md
# MAGIC Función para convertir una cadena de texto a formato de fecha de Python para devolver finalmente la fecha en formato string. Si el parámetro recibido no coincide con una fecha, se devuelve el valor sin modificar.

# COMMAND ----------

def convert_str_to_date(str_date):

    dict_pattern = {
        "^[0-9]{1,2}[0-9]{1,2}[0-9]{4}$": '%d%m%Y',
        "^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$": '%Y-%m-%d',
        "^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$": '%m-%d-%Y',
        "^[0-9]{4}[0-9]{1,2}[0-9]{1,2}$": '%Y%m%d',
        "^[1-9]{1}[0-9]{1}\\/[1-9]{1}[0-9]{1}\\/[0-9]{4}$": '%m/%d/%Y',
        "^[1-9]{1}\\/[1-9]{1}\\/[0-9]{4}$": '%m/%d/%Y',
        "^[1-9]{1}\\/[1-9]{1}[0-9]{1}\\/[0-9]{4}$": '%m/%d/%Y',
        "^[0-9]{1,2}\\/[0-9]{1,2}\\/[0-9]{4}$": '%d/%m/%Y'
    }

    # Por cada clave en el diccionario
    for clave in dict_pattern:
        # Hacer algo con esa clave

        date_pattern = clave
        m = re.match(date_pattern, str_date)
        #print(m) # Returns Match object

        if m:

            continuar = False
            try:
                str_date = datetime.strptime(str_date, dict_pattern[clave]).date()
                str_date = datetime.strftime(str_date, standard_date_format) # Formateamos la fecha en un estándar que es recibido como global parameter
            except ValueError:
                print("No se pudo convertir fecha, se mantiene igual")
                continuar = True
            except Exception as ex:
                raise Exception("Falló el método 'convert_str_to_date'. Msg {}".format(ex))
            
            if not continuar:
                break
        # Sino se encuentra el formato, se mantiene la fecha igual
    return str(str_date)

# COMMAND ----------

# Convertir a función UDF de Spark la función de coincidencias
convert_str_to_date_udf = udf(convert_str_to_date, StringType())

# COMMAND ----------

def standard_dates(df, names_columns_dates):
    try:
        for name_column in names_columns_dates:
            try:
                df = df.withColumn(name_column, convert_str_to_date_udf(col(name_column)))
            except AnalysisException as err:
                tbl_bronze = df_parametric_ent.filter(col("columna") == name_column).select("tabla").collect()[0][0]
                raise Exception('No existe la columna {} en la tabla {} de la fuente original (bronce) para la entidad de silver {} durante el proceso de estandarización de fechas. Msg error: "{}"'.format(name_column, tbl_bronze, table_name, err))

        return df

    except Exception as ex:
            raise Exception("Falló el método 'standard_dates'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Filtramos los campos con tipo de dato "fecha" con el objetivo de guardarlos en un array para pasárlos a la función de estandarización de fechas

# COMMAND ----------

df_filter_fec = df_parametric_ent.filter(col('tipo_dato') == 'FECHA').select('columna')
#display(df_filter_fec)

# COMMAND ----------

date_field_names = generate_list(df_filter_fec, 'columna')
date_field_names

# COMMAND ----------

df_standard = standard_dates(df_parquet, date_field_names)
#display(df_standard)

# COMMAND ----------

# MAGIC %md
# MAGIC # Funciones para estandarizar strings

# COMMAND ----------

# MAGIC %md
# MAGIC Función para quitar espacios al inicio y al final para todos los datos de tipo string de un dataframe.

# COMMAND ----------

def standard_spaces(df, columns_data_type):
    try:
        for key_name_column in columns_data_type:
            try:
                if columns_data_type[key_name_column] == 'ALFABETICO' or columns_data_type[key_name_column] == 'ALFANUMERICO':
                        df = df.withColumn(key_name_column, trim(col(key_name_column)))
            except AnalysisException as err:
                tbl_bronze = df_parametric_ent.filter(col("columna") == key_name_column).select("tabla").collect()[0][0]
                raise Exception('No existe la columna {} en la tabla {} de la fuente original (bronce) durante el proceso de estandarización de espacios. Msg error: "{}"'.format(key_name_column, table_name, err))

        return df

    except Exception as ex:
            raise Exception("Fallo el método 'standard_spaces'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para convertir a mayúsculas los datos de tipo string de un dataframe.

# COMMAND ----------

def standard_case_letter(df, columns_data_type):
    try:
        for key_name_column in columns_data_type:
            try:
                if columns_data_type[key_name_column] == 'ALFABETICO' or columns_data_type[key_name_column] == 'ALFANUMERICO':
                        df = df.withColumn(key_name_column, upper(lower(col(key_name_column))))
            except AnalysisException as err:
                tbl_bronze = df_parametric_ent.filter(col("columna") == key_name_column).select("tabla").collect()[0][0]
                raise Exception('No existe la columna {} en la tabla {} de la fuente original (bronce) para la entidad de silver {} durante el proceso de estandarización de mayúsculas. Msg error: "{}"'.format(key_name_column, tbl_bronze, table_name, err))

        return df

    except Exception as ex:
            raise Exception("Fallo el método 'standard_case_letter'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para remover espacios extra en blanco que puedan existir entre palabras, retorna la cadena sin alterar en caso de que no sea un string.

# COMMAND ----------

def remove_extra_spaces_str(str_text):
    if str_text is not None:
        if isinstance(str_text, str):
            str_text = " ".join(str_text.split())

    return str_text

# COMMAND ----------

remove_extra_spaces = udf(lambda str_txt: remove_extra_spaces_str(str_txt), StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC Función para mantener solo caracteres del alfabeto latino que se encuentran en el teclado.

# COMMAND ----------

def standard_special_chars(string):
    if string is not None:
        if isinstance(string, str):
            string = re.sub(r"[^a-zA-Z0-9 \@\?\"!#\$\&\/\(\)\¿¡\.\*\+\-\[\]ñÑ;,\{\}ÁÉÍÓÚáéíóúüÜ\&\|°\=\'<>_%:]","", string)
    return string

# COMMAND ----------

standard_special_characters = udf(lambda str_txt: standard_special_chars(str_txt), StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC Función para aplicar estandarización de caracteres a todo un dataframe.

# COMMAND ----------

def df_standard_special_characters(df, columns_data_type):
    try:
        for key_name_column in columns_data_type:
            try:
                if columns_data_type[key_name_column] == 'ALFABETICO' or columns_data_type[key_name_column] == 'ALFANUMERICO':
                    df = df.withColumn(key_name_column, standard_special_characters(col(key_name_column)))
            except AnalysisException as err:
                tbl_bronze = df_parametric_ent.filter(col("columna") == key_name_column).select("tabla").collect()[0][0]
                raise Exception('No existe la columna {} en la tabla {} de la fuente original (bronce) para la entidad de silver {}. Msg error: "{}"'.format(key_name_column, tbl_bronze, table_name, err))

        return df

    except Exception as ex:
            raise Exception("Fallo el método 'df_standard_special_characters'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para quitar espacios intermedios entre palabras final para todos los datos de tipo string de un dataframe.

# COMMAND ----------

def remove_extra_spaces_between_words(df, columns_data_type):
    try:
        for key_name_column in columns_data_type:
            try:
                if columns_data_type[key_name_column] == 'ALFABETICO' or columns_data_type[key_name_column] == 'ALFANUMERICO':
                    df = df.withColumn(key_name_column, remove_extra_spaces(col(key_name_column)))
            except AnalysisException as err:
                tbl_bronze = df_parametric_ent.filter(col("columna") == key_name_column).select("tabla").collect()[0][0]
                raise Exception('No existe la columna {} en la tabla {} de la fuente original (bronce) para la entidad de silver {} durante el proceso de remover espacios en blanco extra. Msg error: "{}"'.format(key_name_column, tbl_bronze, table_name, err))
        return df

    except Exception as ex:
            raise Exception("Fallo el método 'remove_extra_spaces_between_words'. Msg {}".format(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Función que orquesta el llamado de todas las funciones de estandarización de caracteres como quitar espacios extra entre palabras, convertir a mayúsculas y quitar espacios al inicio y final de un texto.

# COMMAND ----------

def standard_strings(df, columns_data_type):
    df = remove_extra_spaces_between_words(df, columns_data_type)
    df = standard_spaces(df, columns_data_type)
    df = standard_case_letter(df, columns_data_type)
    df = df_standard_special_characters(df, columns_data_type)
    return df

# COMMAND ----------

df_standard = standard_strings(df_standard, fields_with_data_types)
#display(df_standard)

# COMMAND ----------

# MAGIC %md
# MAGIC Quitar columnas temporales de unión

# COMMAND ----------

df_standard = df_standard.drop('campo_union1', 'campo_union2')
#display(df_standard)

# COMMAND ----------

# MAGIC %md
# MAGIC # Proceso para renombrar columnas

# COMMAND ----------

df_tbl_bronce_pk = df_parametric_ent.select("tabla", "nombre_entidad").dropDuplicates()
#display(df_tbl_bronce_pk)

# COMMAND ----------

df_col_joins_p1 = df_tbl_bronce_pk.join(df_rel_fuentes, [df_tbl_bronce_pk.tabla == df_rel_fuentes.tabla_padre]) \
        .select("campo_clave", col("alias_campo").alias("nombre_funcional"))
df_col_joins_p1 = df_col_joins_p1.dropDuplicates()
#display(df_col_joins_p1)

# COMMAND ----------

if df_col_joins_p1.count() == 0:
    raise Exception("Por favor parametrizar al menos una llave para la entidad {} en el componente relacion_tablas_fuentes".format(table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC Función para añadir una nueva columna con las llaves compuestas que están agrupadas bajo un mismo alias.

# COMMAND ----------

def create_compound_key(df_col_joins_p1, df_parquet):
    df_col_joins_c = df_col_joins_p1.groupBy("nombre_funcional").count()
    
    list_alias_keys = generate_list(df_col_joins_c.filter(col("count") > 1), 'nombre_funcional')
    
    for item in list_alias_keys:
        # Generamos la lista de campos de bronce que conforman llaves individuales
        list_cols_comp_pk = generate_list(df_col_joins_p1.filter(col("nombre_funcional") == item), 'campo_clave')

        # Generar columna dinámica con las llaves compuestas
        df_parquet = df_parquet.withColumn(item, concat_ws('|', *list_cols_comp_pk))
    
    return df_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC Concatenamos las llaves compuestas

# COMMAND ----------

df_ent_keys = create_compound_key(df_col_joins_p1, df_standard)
#display(df_ent_keys)

# COMMAND ----------

# MAGIC %md
# MAGIC Función para duplicar columnas con base a un diccionario que se le pase por parámetro.

# COMMAND ----------

def copy_columns(df_parquet, map_table_entity):
    for key_column in map_table_entity:
        try:
            df_parquet = df_parquet.withColumn(key_column, col(map_table_entity[key_column]))
        except Exception as e:
            raise Exception("Fallo el método 'copy_columns'. Msg error {}".format(e))

    return df_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC Función para añadir al dataframe una columna adicional a las mapeadas por el usuario en la tabla paramétrica con la primary key de la fuente.

# COMMAND ----------

def generate_simple_pk(df_col_joins_p1, df_parquet_rename):
    df_col_joins_u = df_col_joins_p1.groupBy("nombre_funcional").count().filter(col("count") == 1) \
        .select(col("nombre_funcional").alias("nombre_funcional_j"))
    
    df_simple_key = df_col_joins_p1.join(df_col_joins_u, df_col_joins_p1.nombre_funcional == df_col_joins_u.nombre_funcional_j, 'inner')
    
    map_table_entity = map_columns_into_dict(df_simple_key, 'nombre_funcional', 'campo_clave')
    
    df_parquet_rename = copy_columns(df_parquet_rename, map_table_entity)
    
    return df_parquet_rename

# COMMAND ----------

df_ent_keys = generate_simple_pk(df_col_joins_p1, df_ent_keys)
#display(df_ent_keys)

# COMMAND ----------

df_col_joins_p2 = df_parametric_ent.select("columna", col("nombre_funcional"))
#display(df_col_joins_p2)

# COMMAND ----------

map_table_entity = map_columns_into_dict(df_col_joins_p2, 'nombre_funcional', 'columna')
map_table_entity

# COMMAND ----------

# MAGIC %md
# MAGIC Función para renombrar columnas de un archivo parquet.

# COMMAND ----------

def rename_columns(df_parquet, map_table_entity):
    for key_column in map_table_entity:
        try:
            df_parquet = df_parquet.withColumnRenamed(map_table_entity[key_column], key_column)
        except Exception as e:
            raise Exception("Fallo el método 'rename_columns'. Msg error {}".format(e))

    return df_parquet

# COMMAND ----------

df_parquet_rename = rename_columns(df_ent_keys, map_table_entity)
#display(df_parquet_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC Esta función sirve para seleccionar dinámicamente las columnas que vengan desde la tabla paramétrica.

# COMMAND ----------

def select_columns_tbl_parametric(df_col_joins, df_parquet_rename):
    # Seleccionar solo las columnas de que se encuentren mapeadas a la tabla paramétrica
    # Seleccionar de la tabla paramétrica las columna de la entidad
    list_cols_param = generate_list(df_col_joins, "nombre_funcional")
    
    # Seleccionar del parquet unicamente las columnas que estén mapeadas en la tabla paramétrica
    if len(list_cols_param) != 0:
        try:
            df_parquet_rename = df_parquet_rename.select(*list_cols_param)
        except AnalysisException as ae:
            raise Exception("Falló método 'select_columns_tbl_parametric'. Las columnas parametrizadas para la entidad de silver: {}, no coinciden con las columnas del archivo parquet. Msg error {}".format(table_name, ae))
    return df_parquet_rename

# COMMAND ----------

df_col_joins = df_col_joins_p1.union(df_col_joins_p2)
#display(df_col_joins)

# COMMAND ----------

df_parquet_rename = select_columns_tbl_parametric(df_col_joins, df_parquet_rename)
#display(df_parquet_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC # Guardar nuevo dataset estandarizado en container Silver

# COMMAND ----------

write_file(df_parquet_rename, "parquet", param_key_datalake, secret_scope, storage_account_name, container_name, folder_path, table_name, parquet_file_extension)

# COMMAND ----------

# Limpiar memoria caché del driver de spark
spark.catalog.clearCache()
