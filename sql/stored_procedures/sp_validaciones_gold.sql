SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento creado para las validaciones previo a la ejecucion del proceso de gold.
++ Data Factory: PL_Match_And_Merge
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER     PROCEDURE [dbo].[sp_validaciones_gold]  (
     @p_nom_filial NVARCHAR(MAX)
    ,@p_subdominio NVARCHAR(MAX)
    ,@p_msjerror   NVARCHAR(MAX) OUTPUT 
) AS
BEGIN TRY
    DECLARE @v_check_int INT = 0;
    DECLARE @v_haserror  NVARCHAR(1) = 'N';
    DECLARE @chk_cursor  NVARCHAR(MAX);
    DECLARE @v_msjerror  NVARCHAR(MAX) = '';

    --- Validar que exista un id natural
    SELECT @v_check_int = COUNT(1) 
    FROM tbl_relacion_entidades_silver t2
    WHERE 1=1
    AND t2.nombre_filial = @p_nom_filial
    AND t2.subdominio = @p_subdominio
    AND t2.id_natural IS NOT NULL
    AND EXISTS ( SELECT 1 
                 FROM tbl_matriz_dato t1
                 WHERE 1=1
                 AND t1.nombre_filial = nombre_filial
                 AND t2.subdominio = t2.subdominio )

    IF ( @v_check_int = 0 )
    BEGIN
        SET @v_msjerror = 'Error: Debe existir al menos un identificador natural para Filial/Subdomnio en tabla relacion entidades silver: ' + @p_nom_filial + '/' + @p_subdominio
        SET @v_haserror = 'Y'
    END

    -- Revisar que entidades de MDS han sido definida
    IF (@v_haserror = 'N')
    BEGIN
        PRINT 'Entrada cursor cr_entidad_mds_valida  - ' + @v_haserror
        SET @v_check_int = 0;
        DECLARE cr_entidad_mds_valida CURSOR LOCAL FOR
        SELECT t1.codigo_atributo
        FROM tbl_matriz_dato t1
        WHERE 1=1
        AND t1.nombre_filial = @p_nom_filial
        AND t1.subdominio = @p_subdominio
        AND t1.golden_record = 'Y'
        AND (t1.nombre_entidad_mds IS NOT NULL OR t1.nombre_atributo_mds IS NOT NULL ) 
        AND NOT EXISTS ( SELECT 1
                         FROM tbl_mds_dominios t2
                         WHERE 1=1
                         AND ( t1.nombre_entidad_mds = t2.nombre_entidad OR t1.nombre_atributo_mds = t2.nombre_atributo ) );
        OPEN cr_entidad_mds_valida;
        FETCH NEXT FROM cr_entidad_mds_valida INTO @chk_cursor
        WHILE  @@FETCH_STATUS = 0
        BEGIN  
            SET @v_msjerror = @v_msjerror + @chk_cursor + ','
            FETCH NEXT FROM cr_entidad_mds_valida INTO @chk_cursor
            SET @v_check_int = @v_check_int + 1
        END 
        CLOSE cr_entidad_mds_valida;
        DEALLOCATE cr_entidad_mds_valida;

        PRINT 'Salida cursor cr_entidad_mds_valida ' + CONVERT(NVARCHAR(MAX),@v_check_int) + ' - ' + @v_haserror + ' - ' + @v_msjerror

        IF (@v_check_int > 0)
        BEGIN
            IF (RIGHT(@v_msjerror,1) = ',')
            BEGIN
                SET @v_msjerror = LEFT(@v_msjerror, LEN(@v_msjerror) -1)
            END

            SET @v_msjerror = 'Error: Verifique que el mapeo realizado corresponda a una Entidad/Atributo existente en MDS.  Atributos con error: ' + @v_msjerror
            SET @v_haserror = 'Y'
        END
    END

    -- Revisar que entidades de MDS han sido definida
    IF (@v_haserror = 'N')
    BEGIN
        PRINT 'Entrada cursor cr_cont_atr_mds  - ' + @v_haserror
        SET @v_check_int = 0;
        DECLARE cr_cont_atr_mds CURSOR LOCAL FOR
        SELECT --t1.codigo_atributo
            --,t1.id_fuente
            t1.nombre_atributo_mds
            --,t1.nombre_ENTIDAD_mds
            --,COUNT(1)
        FROM tbl_matriz_dato t1
        WHERE 1=1
        AND t1.nombre_filial = @p_nom_filial
        AND t1.subdominio = @p_subdominio
        AND t1.golden_record = 'Y'
        AND t1.nombre_atributo_mds IS NOT NULL
        GROUP BY t1.id_fuente, t1.nombre_atributo_mds, t1.nombre_entidad_mds
        HAVING COUNT(1) > 1 
        OPEN cr_cont_atr_mds;
        FETCH NEXT FROM cr_cont_atr_mds INTO @chk_cursor
        WHILE  @@FETCH_STATUS = 0
        BEGIN  
            SET @v_msjerror = @v_msjerror + @chk_cursor + ','
            FETCH NEXT FROM cr_cont_atr_mds INTO @chk_cursor
            SET @v_check_int = @v_check_int + 1
        END 
        CLOSE cr_cont_atr_mds;
        DEALLOCATE cr_cont_atr_mds;

        PRINT 'Salida cursor cr_cont_atr_mds ' + CONVERT(NVARCHAR(MAX),@v_check_int) + ' - ' + @v_haserror + ' - ' + @v_msjerror

        IF (@v_check_int > 0)
        BEGIN
            IF (RIGHT(@v_msjerror,1) = ',')
            BEGIN
                SET @v_msjerror = LEFT(@v_msjerror, LEN(@v_msjerror) -1)
            END

            SET @v_msjerror = 'Error: Solo debe configurarse/mapearse un atributo MDS para una misma entidad MDS y una misma fuente. Atributos con error: ' + @v_msjerror
            SET @v_haserror = 'Y'
        END
    END 

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
END CATCH
GO
