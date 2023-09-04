SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento creado para las validaciones previo a la ejecucion del proceso de silver.
++ Data Factory: PL_Silver
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER    PROCEDURE [dbo].[sp_validaciones_silver] (
     @p_nom_filial NVARCHAR(MAX)
    ,@p_subdominio NVARCHAR(MAX)
    ,@p_msjerror   NVARCHAR(MAX) OUTPUT 
) AS
BEGIN TRY
    DECLARE @v_check_int INT = 0;
    DECLARE @v_haserror  NVARCHAR(1) = 'N';
    DECLARE @chk_cursor  NVARCHAR(MAX);
    DECLARE @v_msjerror  NVARCHAR(MAX) = '';

    -- Validar que subdominio/filial existen
    SELECT @v_check_int = COUNT(1)
    FROM tbl_matriz_dato t1
    WHERE 1=1
    AND t1.nombre_filial = @p_nom_filial
    AND t1.subdominio = @p_subdominio
    AND t1.golden_record = 'Y'

    IF ( @v_check_int = 0 )
    BEGIN
        SET @v_msjerror = 'Error: No existe configuración para Filial/Subdomnio: ' + @p_nom_filial + '/' + @p_subdominio
        SET @v_haserror = 'Y'
    END

    PRINT 'Salida validacion Filial/Subdominio. ' + @v_haserror + ' - ' + @v_msjerror
    
    IF (@v_haserror = 'N')
    BEGIN
        PRINT 'Entrada cursor cr_check_llaves_fuente  - ' + @v_haserror
        SET @v_check_int = 0;
        -- Revisar que configuracion de llaves entre tablas fuentes han sido definida 
        DECLARE cr_check_llaves_fuente CURSOR LOCAL LOCAL FOR
        SELECT t1.codigo_atributo
        FROM tbl_matriz_dato t1
        WHERE 1=1
        AND t1.nombre_filial = @p_nom_filial
        AND t1.subdominio = @p_subdominio
        AND t1.golden_record = 'Y'
        AND NOT EXISTS ( SELECT 1
                        FROM tbl_relacion_tablas_fuentes t2
                        WHERE 1=1
                        AND t1.tabla = t2.tabla_padre)
        --GROUP BY t1.codigo_atributo
        --HAVING COUNT(1) > 0;
        OPEN cr_check_llaves_fuente;
        FETCH NEXT FROM cr_check_llaves_fuente INTO @chk_cursor
        WHILE  @@FETCH_STATUS = 0
        BEGIN  
            SET @v_msjerror = @v_msjerror + @chk_cursor + ','
            FETCH NEXT FROM cr_check_llaves_fuente INTO @chk_cursor
            SET @v_check_int = @v_check_int + 1
        END 
        CLOSE cr_check_llaves_fuente;
        DEALLOCATE cr_check_llaves_fuente;

        PRINT 'Salida cursor cr_check_llaves_fuente ' + CONVERT(NVARCHAR(MAX),@v_check_int) + ' - ' + @v_haserror + ' - ' + @v_msjerror
        
        IF (@v_check_int > 0)
        BEGIN
            IF (RIGHT(@v_msjerror,1) = ',')
            BEGIN
                SET @v_msjerror = LEFT(@v_msjerror, LEN(@v_msjerror) -1)
            END

            SET @v_msjerror = 'Error: No existe configuración llaves en atributos: ' + @v_msjerror
            SET @v_haserror = 'Y'

        END
    END

    -- Validar que modelo, version y MDS no sean nulos para cuando exista configuracion de MDS 
    IF (@v_haserror = 'N')
    BEGIN
        SET @v_check_int = 0;
        DECLARE cr_check_null_mds CURSOR LOCAL FOR
        SELECT t1.codigo_atributo
        FROM tbl_matriz_dato t1
        WHERE 1=1
        AND t1.nombre_filial = @p_nom_filial
        AND t1.subdominio = @p_subdominio
        AND t1.golden_record = 'Y'
        AND ( t1.nombre_atributo_mds IS NOT NULL OR t1.nombre_entidad_mds IS NOT NULL )
        AND ( t1.nombre_modelo IS NULL OR t1.id_version IS NULL OR t1.tipo_dato_mds IS NULL );
        OPEN cr_check_null_mds;
        FETCH NEXT FROM cr_check_null_mds INTO @chk_cursor
        WHILE  @@FETCH_STATUS = 0
        BEGIN  
            SET @v_msjerror = @v_msjerror + @chk_cursor + ','
            FETCH NEXT FROM cr_check_null_mds INTO @chk_cursor
            SET @v_check_int = @v_check_int + 1
        END 
        CLOSE cr_check_null_mds;
        DEALLOCATE cr_check_null_mds;

        PRINT 'Salida cursor cr_check_null_mds ' + CONVERT(NVARCHAR(MAX),@v_check_int) + ' - ' + @v_haserror + ' - ' + @v_msjerror

        IF (@v_check_int > 0)
        BEGIN
            IF (RIGHT(@v_msjerror,1) = ',')
            BEGIN
                SET @v_msjerror = LEFT(@v_msjerror, LEN(@v_msjerror) -1)
            END

            SET @v_msjerror = 'Error: Dominio/Version/Tipo Dato/Entidad/Atributo MDS NO deben ser nulos para cuando se configura un atributo/entidad de MDS. Atributos con error: ' + @v_msjerror
            SET @v_haserror = 'Y'
        END

    END

    -- Revisar que entidades de MDS han sido definida
    /*
    IF (@v_haserror = 'N')
    BEGIN
        PRINT 'Entrada cursor cr_lista_entidad_mds  - ' + @v_haserror
        SET @v_check_int = 0;
        DECLARE cr_lista_entidad_mds CURSOR LOCAL FOR
        SELECT t1.codigo_atributo
        FROM tbl_matriz_dato t1
        WHERE 1=1
        AND t1.nombre_filial = @p_nom_filial
        AND t1.subdominio = @p_subdominio
        AND t1.golden_record = 'Y'
        AND NOT EXISTS ( SELECT 1
                         FROM tbl_mds_dominios t2
                         WHERE 1=1
                         AND t1.nombre_entidad_mds = t2.nombre_entidad);
        --GROUP BY t1.codigo_atributo
        --HAVING COUNT(1) > 0;
        OPEN cr_lista_entidad_mds;
        FETCH NEXT FROM cr_lista_entidad_mds INTO @chk_cursor
        WHILE  @@FETCH_STATUS = 0
        BEGIN  
            SET @v_msjerror = @v_msjerror + @chk_cursor + ','
            FETCH NEXT FROM cr_lista_entidad_mds INTO @chk_cursor
            SET @v_check_int = @v_check_int + 1
        END 
        CLOSE cr_lista_entidad_mds;
        DEALLOCATE cr_lista_entidad_mds;

        PRINT 'Salida cursor cr_lista_entidad_mds ' + convert(NVARCHAR(MAX),@v_check_int) + ' - ' + @v_haserror + ' - ' + @v_msjerror

        IF (@v_check_int > 0)
        BEGIN
            IF (RIGHT(@v_msjerror,1) = ',')
            BEGIN
                SET @v_msjerror = LEFT(@v_msjerror, LEN(@v_msjerror) -1)
            END

            SET @v_msjerror = 'Error: Entidad/Dominio MDS, NO valida. Atributos con error: ' + @v_msjerror
            SET @v_haserror = 'Y'
        END
    END
    */

    IF (@v_haserror = 'N')
    BEGIN
        -- Pipeline valida como exitoso, mensaje de OK. No cambiar.
        SET @v_msjerror = 'OK. Validaciones exitosas.'
    END 

    SELECT  @p_msjerror = @v_msjerror;
    PRINT ('Salida procedimiento: ' + @p_msjerror)
    RETURN

END TRY
BEGIN CATCH
    SET @v_msjerror = CONVERT(VARCHAR(MAX),ERROR_NUMBER());
    SET @v_haserror = 'Y';
    SELECT @p_msjerror = @v_haserror + ' Error ' + @v_msjerror + ' - ' + ERROR_MESSAGE() + '.';
    PRINT @p_msjerror;
    RETURN
END CATCH;
GO
