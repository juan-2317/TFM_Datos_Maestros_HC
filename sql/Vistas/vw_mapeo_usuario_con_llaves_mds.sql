SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Vista creada para concatenar las entidades y atributos mapeados por el usuario, junto a sus campos domain (foreign keys).
++   Además de concatenar los datos de las entidades especiales.
++ Databricks: upsert_entities_mds_to_parquets
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER     VIEW [dbo].[vw_mapeo_usuario_con_llaves_mds] AS
    SELECT b.nombre_filial, b.subdominio, b.nombre_modelo, b.nombre_entidad_mds, b.nombre_atributo_mds, b.tipo_dato_mds
    FROM tbl_matriz_dato b
    WHERE b.golden_record = 'Y'
    AND b.nombre_entidad IS NOT NULL
    AND b.nombre_entidad_mds IS NOT NULL
    AND EXISTS ( -- Verificar que los datos de la tabla paramétrica se encuentren en la tabla de dominios
        SELECT a.nombre_dominio, a.nombre_entidad
        FROM tbl_mds_dominios a 
        WHERE b.nombre_entidad_mds = a.nombre_entidad
        AND b.nombre_modelo = a.nombre_dominio
    )
    UNION
    -- Este bloque del union es para seleccionar los atributos de tipo domain de la tabla paramétrica y que cruzen con la tabla de dominios
    SELECT e.nombre_filial, e.subdominio, c.nombre_dominio AS nombre_modelo, c.nombre_entidad AS nombre_entidad_mds, c.nombre_atributo AS nombre_atributo_mds,
        c.tipo_atributo AS tipo_dato_mds
    FROM tbl_mds_dominios c
    INNER JOIN (
        SELECT DISTINCT nombre_filial, subdominio, nombre_modelo, nombre_entidad_mds
        FROM tbl_matriz_dato
        WHERE golden_record = 'Y'
        AND nombre_entidad_mds IS NOT NULL
    ) e
    ON e.nombre_modelo = c.nombre_dominio AND e.nombre_entidad_mds = c.nombre_entidad
    WHERE c.tipo_atributo = 'Domain'
    AND NOT EXISTS (
        SELECT d.nombre_entidad
        FROM tbl_mds_dominios d
        WHERE d.esListaReferencia = 1
        AND c.nombre_dominio = d.nombre_dominio
        AND c.entidad_dominio = d.nombre_entidad
    )
    AND NOT (c.nombre_entidad = 'usuario' AND c.nombre_atributo = 'id_empresa')
    UNION
    -- Este bloque del union permite seleccionar los campos de Code, pk_ent_principal y ids_en_fuentes que se deben incluir en todas las entidades apesar de no haber sido mapeados por
    --  el usuario
    SELECT g.nombre_filial, g.subdominio, f.nombre_dominio AS nombre_modelo, f.nombre_entidad AS nombre_entidad_mds, f.nombre_atributo AS nombre_atributo_mds,
        f.tipo_atributo AS tipo_dato_mds
    FROM tbl_mds_dominios f
    INNER JOIN (
        SELECT DISTINCT nombre_filial, subdominio, nombre_modelo, nombre_entidad_mds
        FROM tbl_matriz_dato
        WHERE golden_record = 'Y'
        AND nombre_entidad_mds IS NOT NULL
    ) g
    ON g.nombre_modelo = f.nombre_dominio AND g.nombre_entidad_mds = f.nombre_entidad
    WHERE f.nombre_atributo IN ('pk_ent_principal', 'ids_en_fuentes', 'Code')
    UNION
    SELECT nombre_filial, 
        subdominio,
        nombre_modelo,
        nombre_entidad_mds,
        nombre_atributo_mds,
        tipo_dato_mds
    FROM vw_select_special_ents -- Esta vista nos permite seleccionar las entidades especiales o puente que están relacionadas con las entidades mapeadas en la tabla
    -- paramétrica
GO
