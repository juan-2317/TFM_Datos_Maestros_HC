SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Vista creada para consulta desde el proceso que genera los golden records desde las fuentes hacia MDS.
++  Selecciona una lista de las entidades tanto padres, como relacionadas; formando el total de entidades que conforman el golden record.
++ Databricks: generate_golden_record_sink_v2 
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER     VIEW [dbo].[vw_entidades_silver_golden_record] AS
  SELECT 
      [nombre_entidad_padre] AS nombre_entidad, nombre_filial, subdominio
  FROM [dbo].[tbl_relacion_entidades_silver]
  UNION
  SELECT 
      [entidad_relacion] AS nombre_entidad, nombre_filial, subdominio
  FROM [dbo].[tbl_relacion_entidades_silver]
  WHERE entidad_relacion NOT IN (
      SELECT 
      [nombre_grupo]
    FROM [dbo].[tbl_relacion_entidades_silver]
    WHERE nombre_grupo IS NOT NULL
  )
GO
