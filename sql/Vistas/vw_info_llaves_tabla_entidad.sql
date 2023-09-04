SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Vista creada para identificar los campos llave en las fuente junto con el campo llave en la entidades funcionales.
++ Databricks: generate_golden_record_sink_v2
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER     VIEW [dbo].[vw_info_llaves_tabla_entidad] AS 
SELECT  DISTINCT
  xx.nombre_filial
  ,xx.subdominio
  ,xx.nombre_entidad
  ,xx.tabla
  ,zz.campo_clave AS campo_clave_tabla
  ,zz.alias_campo AS campo_clave_entidad
  ,rs.nombre_entidad_padre + '_' + zz.alias_campo AS campo_clave_entidad_2
  ,rs.id_natural AS campo_id_natural
  ,rs.nombre_entidad_padre + '_' + rs.id_natural AS campo_id_natural_2
  , rs.criterio_duplicados
  --,xx.golden_record
FROM tbl_matriz_dato xx
    ,tbl_relacion_tablas_fuentes zz
    ,(
        SELECT a.nombre_filial, a.subdominio, a.nombre_entidad AS  nombre_entidad_padre, b.id_natural, b.criterio_duplicados
        FROM vw_entidades_silver_golden_record a
        LEFT JOIN (
            SELECT *
            FROM tbl_relacion_entidades_silver
            WHERE id_natural IS NOT NULL
        ) b
        ON a.nombre_filial = b.nombre_filial AND a.subdominio = b.subdominio
        AND a.nombre_entidad = b.nombre_entidad_padre
    ) rs   
WHERE 1=1
AND xx.tabla = zz.tabla_padre
AND xx.nombre_entidad = rs.nombre_entidad_padre
AND xx.nombre_filial = rs.nombre_filial
AND xx.subdominio = rs.subdominio
AND xx.golden_record = 'Y'
GO
