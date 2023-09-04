SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Vista que permite visualizar los identificadores naturales para cada una de las entidades funcionales.
++ Databricks: filter_gd_to_upsert
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER    VIEW [dbo].[vw_identificadores_naturales_x_entidad] AS
SELECT DISTINCT
    rs.nombre_filial
    ,rs.subdominio
    ,rs.nombre_entidad_padre AS nombre_entidad
    --,md.nombre_funcional AS campo_entidad
    ,rs.id_natural AS campo_id_natural
    ,rs.nombre_entidad_padre + '_' + rs.id_natural AS campo_clave_entidad_2
    ,rs.es_id_upsert
    ,md.nombre_modelo AS modelo_mds
    ,md.id_version AS version_mds
    ,md.tipo_dato_mds
    ,md.nombre_entidad_referenciada AS entidad_referencia_mds
    ,md.nombre_atributo_mds
    ,md.nombre_entidad_mds
FROM tbl_relacion_entidades_silver rs
    ,tbl_matriz_dato md
WHERE 1=1
AND rs.nombre_entidad_padre = md.nombre_entidad
AND rs.nombre_filial = md.nombre_filial
AND rs.subdominio = md.subdominio
AND rs.id_natural = md.nombre_funcional
AND md.golden_record = 'Y'
AND md.nombre_entidad_mds IS NOT NULL
GO
