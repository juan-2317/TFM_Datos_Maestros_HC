SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento encargado de crear dinamicamente el mapeo de campos basado en la informacion de dominios descargada de MDS.
++ Data Factory: PL_GetMembersMDS
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER     PROCEDURE [dbo].[sp_mapping_entities_fields] (
     @p_dominiomds  NVARCHAR(MAX)
    ,@p_entidadmds  NVARCHAR(MAX)
    ,@p_returnval   VARCHAR(max) OUTPUT 
) AS
BEGIN
    DECLARE @chk_cursor  VARCHAR(MAX);
    DECLARE @v_mapping   VARCHAR(MAX);
    DECLARE @v_check_int INT = 0;

    SET @v_mapping = '{"type": "TabularTranslator","mappings": [ ' 
    SET @v_mapping = @v_mapping + '{"source": {"path": "[''Id'']"},"sink": {"name": "Id","type": "String"}},'
    SET @v_mapping = @v_mapping + '{"source": {"path": "[''AuditInfo''][''UpdatedDateTime'']"},"sink": {"name": "UpdatedDateTime","type": "String"}},'
    
    DECLARE crs_mapping CURSOR LOCAL FOR
    SELECT nombre_atributo
    FROM tbl_mds_dominios
    WHERE 1=1
    AND nombre_dominio = @p_dominiomds
    AND nombre_entidad = @p_entidadmds;
    OPEN crs_mapping;
    FETCH NEXT FROM crs_mapping INTO @chk_cursor
    WHILE  @@FETCH_STATUS = 0
    BEGIN
        SET @v_mapping = @v_mapping + '{"source": {"path": "[''' + @chk_cursor + ''']"},"sink": {"name":"'+ @chk_cursor + '","type": "String"}},'
        FETCH NEXT FROM crs_mapping INTO @chk_cursor
        SET @v_check_int = @v_check_int + 1
    END 
    CLOSE crs_mapping;
    DEALLOCATE crs_mapping;

    IF (@v_check_int > 0)
    BEGIN
        IF (RIGHT(@v_mapping,1) = ',')
        BEGIN
            SET @v_mapping = LEFT(@v_mapping, LEN(@v_mapping) -1)          
        END

        SET @v_mapping = @v_mapping + '],"collectionReference": "$[''members'']","mapComplexValuesToString": false }'
    END    
    PRINT (@v_mapping)
    SELECT @p_returnval = @v_mapping
    RETURN
END
GO
