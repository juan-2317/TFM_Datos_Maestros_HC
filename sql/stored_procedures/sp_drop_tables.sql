SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento encargado de eliminar dinamicamente las tablas creadas para facilitar la interpretacion de datos en etapas de BRN, SLV y GLD.
++ Data Factory: PL_DropTables_SQL
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER    PROCEDURE [dbo].[sp_drop_tables](
     @p_subdomain   NVARCHAR(MAX)
    ,@p_filialname  NVARCHAR(MAX)
    ,@p_preftable   NVARCHAR(MAX)
    ,@p_returnvalue NVARCHAR(MAX) OUTPUT
)AS
BEGIN TRY
    DECLARE @v_cursor    VARCHAR(MAX);
    DECLARE @v_pref      VARCHAR(MAX);
    DECLARE @v_script    NVARCHAR(MAX);
    DECLARE @v_msjerror  NVARCHAR(MAX) = '';
    DECLARE @v_listtable NVARCHAR(MAX) = '';
    DECLARE @v_cont      INT = 0;

    SET @v_pref = @p_preftable 

    DECLARE crs_list_tables CURSOR LOCAL FOR
    SELECT obj.name AS table_name
    FROM sys.objects obj
    WHERE 1=1
    AND SCHEMA_NAME([schema_id]) = 'dbo'
    AND obj.type = 'U' --> Tablas
    --AND name LIKE '%' + @v_pref + '%';
    AND EXISTS ( SELECT 1
                 FROM tbl_matriz_dato md 
                 WHERE 1=1
                 AND md.golden_record = 'Y'
                 AND md.nombre_filial = @p_filialname
                 AND md.subdominio = @p_subdomain
                 AND obj.name = ( CASE 
                                    WHEN @v_pref = 'TBL_SLV_'       THEN @v_pref + md.nombre_funcional --> Para proceso Silver que crea las entidades funcionales
                                    WHEN @v_pref = 'TBL_BRN_'       THEN @v_pref + md.tabla --> Para proceso Bronce que crea las tablas transaccionales de fuente datos
                                    WHEN @v_pref = 'TBL_SLV_MDS_'   THEN @v_pref + md.nombre_entidad_mds --> Para proceso Silver que crea las entidades de MDS-
                                    WHEN @v_pref = 'TBL_GLD_GR_'    THEN @v_pref + md.subdominio --> Para proceso golden Record sin Match&Merge
                                    WHEN @v_pref = 'TBL_GLD_GR_MM_' THEN @v_pref + md.subdominio --> Para proceso golden Record con Match&Merge 
                                    WHEN @v_pref = 'TBL_GLD_INS_'   THEN @v_pref + md.subdominio --> Para proceso registros candidatos a crear en MDS
                                    WHEN @v_pref = 'TBL_GLD_UPD_'   THEN @v_pref + md.subdominio --> Para proceso registros candidatos a modificar en MDS
                                 END ) )
    OPEN crs_list_tables;
    FETCH NEXT FROM crs_list_tables INTO @v_cursor
    WHILE  @@FETCH_STATUS = 0
    BEGIN  
        SET @v_script = 'DROP TABLE dbo.' + @v_cursor + ';'
        FETCH NEXT FROM crs_list_tables INTO @v_cursor;
        PRINT (@v_script);
        EXEC sp_executesql @v_script, N'@v_msjerror int OUTPUT', @v_msjerror = @v_msjerror OUTPUT;
        SET @v_listtable = @v_cursor + '.' + @v_listtable
        SET @v_cont = @v_cont + 1
    END 
    CLOSE crs_list_tables;
    DEALLOCATE crs_list_tables;

    SET @v_msjerror = 'OK.  Tablas Eliminadas: ' + @v_listtable + ' Total: ' + CONVERT(VARCHAR(MAX),@v_cont)
    SELECT  @p_returnvalue = @v_msjerror;
    PRINT (@p_returnvalue)
    RETURN
END TRY
BEGIN CATCH
    SET @v_msjerror = 'Fallo ejecutando script: ' + @v_script + 'CodError: ' + CONVERT(VARCHAR(MAX),ERROR_NUMBER());
    --SET @v_haserror = 'Y';
    SELECT @p_returnvalue = ' Error ' + @v_msjerror + ' - ' + ERROR_MESSAGE() + '.';
    PRINT @p_returnvalue;
    CLOSE crs_list_tables;
    DEALLOCATE crs_list_tables;
    RETURN
END CATCH

GO
