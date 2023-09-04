SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Vista creada para consulta de los campos clave a nivel de las entidades funcionales y su correspondencia en las entidades de MDS.
++ Databricks: inserts_entities_mds_to_parquets, update_entities_mds_to_parquets y upsert_entities_mds_to_parquets
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER     VIEW [dbo].[vw_info_llaves_x_entidad_mds] AS
    WITH sel_mat_datos AS (
        SELECT  DISTINCT
            xx.nombre_filial
            ,xx.subdominio
            ,xx.nombre_entidad
            ,xx.nombre_entidad + '_' + zz.alias_campo AS campo_clave_entidad_2
            --,xx.golden_record
            , xx.nombre_entidad_mds
        FROM tbl_matriz_dato xx
            ,tbl_relacion_tablas_fuentes zz
        WHERE 1=1
        AND xx.tabla = zz.tabla_padre
        AND xx.golden_record = 'Y'
        AND zz.es_primary_key = 'Y'
        AND xx.nombre_entidad_mds IS NOT NULL
    ), num_sel_mat AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY nombre_entidad_mds ORDER BY nombre_entidad_mds) AS rn
        FROM sel_mat_datos
    )
    SELECT nombre_filial
        , subdominio
        , nombre_entidad
        , campo_clave_entidad_2
        , nombre_entidad_mds
    FROM num_sel_mat
    WHERE rn = 1
GO
