SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Vista creada para Esta vista nos permite seleccionar las entidades especiales o puente que están relacionadas con las entidades mapeadas en la tabla
++  paramétrica
++ Utilizada por vista: vw_mapeo_usuario_con_llaves_mds 
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER     VIEW [dbo].[vw_select_special_ents] AS
    WITH group_special_ents AS ( -- Buscar que entidades referenciadas se encuentran el nombre de la entidad puente y agrupar por entidad para separar las entidades referenciadas por coma
        SELECT nombre_dominio, nombre_entidad AS nombre_entidad_mds, STRING_AGG(entidad_dominio, ',') AS Result
        FROM tbl_mds_dominios
        WHERE 1=1
        AND nombre_entidad LIKE '%[_]m[_]%'
        AND nombre_entidad LIKE CONCAT('%', entidad_dominio, '%')
        AND entidad_dominio IS NOT NULL
        GROUP BY nombre_dominio, nombre_entidad
    ), split_ents AS ( -- Dividir nombres de entidades referenciadas en dos columnas
        SELECT *, LEFT(Result, Charindex(',', Result) - 1) AS entidad_1,
            RIGHT(Result, Charindex(',', Reverse(Result)) - 1) AS entidad_2
        FROM group_special_ents
    ), spli_ents_niv2 AS ( -- En caso de que una entidad puente referencie a otra puente, se dividen las entidades referenciadas en otra columna por comas
        SELECT
            a.nombre_dominio
            , nombre_entidad_MDS,
            Result,
            entidad_1,
            entidad_2
            , STRING_AGG(entidad_dominio, ',') AS result2
        FROM split_ents a
        LEFT JOIN tbl_mds_dominios b
        ON b.nombre_entidad = a.entidad_1
        AND b.entidad_dominio IS NOT NULL
        AND b.nombre_entidad LIKE CONCAT('%', b.entidad_dominio, '%')
        GROUP BY a.nombre_dominio
        , nombre_entidad_MDS, Result, entidad_1, entidad_2
    ), res_final AS (
        SELECT nombre_dominio
            , nombre_entidad_MDS, Result, Result2,
            entidad_1, entidad_2,
        RIGHT(result2, Charindex(',', Reverse(result2)) - 1) AS entidad_3,
        LEFT(result2, Charindex(',', result2) - 1) AS entidad_4
        FROM spli_ents_niv2
    ), list_special_ents AS ( -- Recuperar entidades especiales donde sus entidades referenciadas crucen en la tabla paramétrica
        SELECT DISTINCT nombre_dominio, nombre_entidad_mds, entidad_1, entidad_2
        FROM res_final h
        WHERE EXISTS ( -- Primero se comprueba que las subentidades referenciadas sean la entidad principal o la tabla "subdominio"
            SELECT i.nombre_filial, i.subdominio, i.nombre_modelo, i.nombre_entidad_mds
            FROM tbl_matriz_dato i
            WHERE golden_record = 'Y'
            AND i.nombre_entidad_mds IS NOT NULL
            AND (
                (h.entidad_1 = 'usuario' AND h.entidad_2 = 'subdominio')
                OR
                (h.entidad_1 = 'subdominio' AND h.entidad_2 = 'usuario')
            
            )
            AND i.nombre_modelo = h.nombre_dominio
        )
        OR
        EXISTS -- Comprobar que una entidad referenciada se encuentre en la tabla paramétria
        (
            SELECT i.nombre_filial, i.subdominio, i.nombre_modelo, i.nombre_entidad_mds
            FROM tbl_matriz_dato i
            WHERE golden_record = 'Y'
            AND i.nombre_entidad_mds IS NOT NULL
            AND h.entidad_2 = i.nombre_entidad_mds
            AND h.entidad_3 IS NOT NULL
            AND h.entidad_4 IS NOT NULL
            AND i.nombre_modelo = h.nombre_dominio
        )
        OR
        (
            EXISTS -- Comprobar que una entidad referenciada se encuentre en la tabla paramétria
            (
                SELECT i.nombre_filial, i.subdominio, i.nombre_modelo, i.nombre_entidad_mds
                FROM tbl_matriz_dato i
                WHERE golden_record = 'Y'
                AND i.nombre_entidad_mds IS NOT NULL
                AND h.entidad_1 = i.nombre_entidad_mds
                AND i.nombre_modelo = h.nombre_dominio
            )
            AND
            EXISTS -- Comprobar que una entidad referenciada se encuentre en la tabla paramétria
            (
                SELECT i.nombre_filial, i.subdominio, i.nombre_modelo, i.nombre_entidad_mds
                FROM tbl_matriz_dato i
                WHERE golden_record = 'Y'
                AND i.nombre_entidad_mds IS NOT NULL
                AND h.entidad_2 = i.nombre_entidad_mds
                AND i.nombre_modelo = h.nombre_dominio
            )
        )
    ), res_final1 AS (
        SELECT a.nombre_dominio, a.nombre_entidad, a.nombre_atributo, a.tipo_atributo
        FROM tbl_mds_dominios a
        WHERE EXISTS (
            SELECT nombre_dominio, nombre_entidad_mds
            FROM list_special_ents b
            WHERE b.nombre_dominio = a.nombre_dominio AND b.nombre_entidad_mds = a.nombre_entidad
        )
        AND a.entidad_dominio IS NOT NULL
        AND NOT EXISTS ( -- Excluir las listas de referencia
            SELECT d.nombre_entidad
            FROM tbl_mds_dominios d
            WHERE d.esListaReferencia = 1
            AND a.nombre_dominio = d.nombre_dominio
            AND a.entidad_dominio = d.nombre_entidad
        )
    ), res_final2 AS (
        SELECT a.nombre_dominio, a.nombre_entidad, a.nombre_atributo, a.tipo_atributo
        FROM tbl_mds_dominios a
        WHERE EXISTS (
            SELECT nombre_dominio, nombre_entidad_mds
            FROM list_special_ents b
            WHERE b.nombre_dominio = a.nombre_dominio AND b.nombre_entidad_mds = a.nombre_entidad
        )
        AND a.nombre_atributo IN ('pk_ent_principal', 'ids_en_fuentes', 'Code') -- Añadir al mapeo los campos que son obligatorios entre todas las entidades
        UNION
        SELECT nombre_dominio, nombre_entidad, nombre_atributo, tipo_atributo
        FROM res_final1
        UNION
        SELECT nombre_modelo AS nombre_dominio, entidad_vs_subdominio AS nombre_entidad, 'estado' AS nombre_atributo,
            'Domain' AS tipo_atributo -- Recordar que la entidad que relaciona la entidad principal del modelo
        --  con el subdominio siempre va a tener el campo "estado"
        FROM tbl_rel_subdominio_x_modelo a
        WHERE EXISTS (
            SELECT nombre_dominio, nombre_entidad_mds
            FROM list_special_ents b
            WHERE b.nombre_dominio = a.nombre_modelo AND b.nombre_entidad_mds = a.entidad_vs_subdominio
        )
    )

    SELECT DISTINCT g.nombre_filial
        , g.subdominio
        , f.nombre_dominio AS nombre_modelo
        , f.nombre_entidad AS nombre_entidad_mds
        , f.nombre_atributo AS nombre_atributo_mds
        , f.tipo_atributo AS tipo_dato_mds
    FROM res_final2 f
    INNER JOIN (
        SELECT DISTINCT nombre_filial, subdominio, nombre_modelo, nombre_entidad_mds
        FROM tbl_matriz_dato
        WHERE golden_record = 'Y'
        AND nombre_entidad_mds IS NOT NULL
    ) g
    ON g.nombre_modelo = f.nombre_dominio;
GO
