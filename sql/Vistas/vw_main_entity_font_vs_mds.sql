SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Vista creada para consulta la informacion del subdominio vs los campos definidos como ID Natural para las entidades.
++ Databricks: inserts_entities_mds_to_parquets, update_entities_mds_to_parquets y upsert_entities_mds_to_parquets.
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER   VIEW [dbo].[vw_main_entity_font_vs_mds] AS
    SELECT a.entidad_principal_modelo, a.entidad_principal_fuente, a.nombre_filial, a.nombre_subdominio, b.campo_clave_entidad_2,
        b.campo_id_natural, b.campo_id_natural_2
    FROM tbl_rel_subdominio_x_modelo a
    INNER JOIN vw_info_llaves_tabla_entidad b
    ON a.entidad_principal_fuente = b.nombre_entidad
    WHERE campo_id_natural_2 IS NOT NULL
GO
