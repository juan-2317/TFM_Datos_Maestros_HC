SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento creado para las validaciones del archivo maestro de datos.
++ Data Factory: PL_Load_Matriz_Datos
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/

CREATE OR ALTER    PROCEDURE [dbo].[sp_merge_matriz_dato] (
    --@error    NVARCHAR(1) OUTPUT,
    @msjerror NVARCHAR(MAX) OUTPUT
) AS

BEGIN TRY
	DECLARE @haserror VARCHAR(10) = 'N';
    DECLARE @checkdata VARCHAR(MAX) = 'OK';
    DECLARE @checcursor VARCHAR(MAX);
    DECLARE @checkcont INT = 0;
    DECLARE @messageerror VARCHAR(MAX);
    DECLARE @errornumber  VARCHAR(MAX);
    
    -- Validar que fuente este configurada en tabla fuentes
    SELECT TOP 1 @checkdata = fuente_de_datos
	--SELECT @checkdata = COUNT(1)
    FROM tmp_matriz_dato tmp
    WHERE NOT EXISTS ( SELECT id FROM tbl_fuentes tbl WHERE tbl.nombre_fuente = tmp.fuente_de_datos );

	--PRINT ' VALOR @checkfuente: ' + @checkfuente;

    IF ( @checkdata <> 'OK' )
	BEGIN
        SET @haserror = 'Y'
		SET @messageerror = 'La fuente ' + @checkdata + ' NO existe.'
        SET @errornumber = '5001'
        SELECT @msjerror = @haserror + ' Error:  ' + @errornumber + ' - ' + @messageerror;
        PRINT @msjerror;
        RAISERROR (@errornumber, -1, -1, @messageerror);
	END;

    IF @haserror <> 'Y'
    BEGIN
        SET @checkcont = 0;
        SET @checkdata = '';
        DECLARE crs_duplicado_codatributo CURSOR FOR
        SELECT codigo_atributo --,COUNT(1)
        FROM tmp_matriz_dato
        GROUP BY codigo_atributo
        HAVING COUNT(1) > 1;
        OPEN crs_duplicado_codatributo
        FETCH NEXT FROM crs_duplicado_codatributo INTO @checcursor
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @checkdata = @checkdata + @checcursor + ','
            FETCH NEXT FROM crs_duplicado_codatributo INTO @checcursor
            SET @checkcont = @checkcont + 1
        END
        CLOSE crs_duplicado_codatributo;  
        DEALLOCATE crs_duplicado_codatributo;
        
        IF ( @checkcont > 0 )
        BEGIN
            SET @haserror = 'Y'
            SET @messageerror = 'Existe mas de un codigo atributo repetido. Registros encontrados('+ CONVERT(VARCHAR(MAX),@checkcont) + '): '  + @checkdata;
            SET @errornumber = '5002'
            SELECT @msjerror = @haserror + ' Error:  ' + @errornumber + ' - ' + @messageerror;
            PRINT @msjerror;
            RAISERROR (@errornumber, -1, -1, @messageerror);
        END; 

    END 

    IF @haserror <> 'Y'
    BEGIN
        SET @checkcont = 0;
        SET @checkdata = '';
        -- Validar que campos no vengan con valor nulo para campos golden record
        DECLARE crs_check_nulls CURSOR  FOR
        --SELECT @checkdata = COUNT(1)
        SELECT codigo_atributo
        FROM tmp_matriz_dato
        WHERE 1=1
        AND golden_record = 'Y'
        AND ( tabla_fuente_datos = ''
            OR campo_fuente_datos = ''
            OR nombre_funcional = ''
            OR nombre_entidad = '' );

        OPEN crs_check_nulls
        FETCH NEXT FROM crs_check_nulls INTO @checcursor
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @checkdata = @checkdata + @checcursor + ','
            FETCH NEXT FROM crs_check_nulls INTO @checcursor
            SET @checkcont = @checkcont + 1
        END
        CLOSE crs_check_nulls  
        DEALLOCATE crs_check_nulls 
    
        IF ( @checkcont > 0 )
        BEGIN
            SET @haserror = 'Y'
            --SET @messageerror = 'Msj: Registros candidatos a Golden Record NO deben tener valores nulos. Registros encontrados: ' + CONVERT(VARCHAR(MAX),@checkcont) + '.'
            SET @messageerror = 'Registros candidatos a Golden Record NO deben tener valores nulos. Registros encontrados('+ CONVERT(VARCHAR(MAX),@checkcont) + '): '  + @checkdata;
            SET @errornumber = '5003'
            SELECT @msjerror = @haserror + ' Error:  ' + @errornumber + ' - ' + @messageerror;
            PRINT @msjerror;
            RAISERROR (@errornumber, -1, -1, @messageerror);
        END;
    END;

    IF @haserror <> 'Y'
    BEGIN
        SET @checkcont = 0
        SET @checkdata = '';
         DECLARE crs_check_char CURSOR  FOR
        --SELECT @checkcont = COUNT(1)
        SELECT codigo_atributo
        FROM tmp_matriz_dato
        WHERE 1=1
        AND ([DBO].fn_has_specialchar(codigo_atributo) > 0
            OR [DBO].fn_has_specialchar(subdominio) > 0
            OR [DBO].fn_has_specialchar(nombre_funcional) > 0
            --OR [DBO].fn_has_specialchar(tabla_fuente_datos) > 0 --> Para SAP se debe permitir valores con caracteres especiales. De momento, se omite para todos
            OR [DBO].fn_has_specialchar(campo_fuente_datos) > 0 
            OR [DBO].fn_has_specialchar(nombre_entidad) > 0
            );

        OPEN crs_check_char
        FETCH NEXT FROM crs_check_char INTO @checcursor
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @checkdata = @checkdata + @checcursor + ','
            FETCH NEXT FROM crs_check_char INTO @checcursor
            SET @checkcont = @checkcont + 1
        END
        CLOSE crs_check_char  
        DEALLOCATE crs_check_char 
        
        IF ( @checkcont > 0 )
        BEGIN
            SET @haserror = 'Y'
            --SET @messageerror = 'Msj: Existe informacion con caracteres no validos. Registros encontrados: ' + CONVERT(VARCHAR(MAX),@checkcont) + '.'
            SET @messageerror = 'Existe informacion con caracteres no validos. Registros encontrados('+ CONVERT(VARCHAR(MAX),@checkcont) + '): '  + @checkdata;
            SET @errornumber = '5004'
            SELECT @msjerror = @haserror + ' Error:  ' + @errornumber + ' - ' + @messageerror;
            PRINT @msjerror;
            RAISERROR (@errornumber, -1, -1, @messageerror);
        END;
    END;

	IF ( @haserror = 'N' )
	BEGIN	
		EXEC dbo.sp_remove_tildes_tmpmatriz
		MERGE tbl_matriz_dato AS target
			USING tmp_matriz_dato AS source
			ON ( target.codigo_atributo  = source.codigo_atributo )
			WHEN MATCHED THEN UPDATE SET
					target.codigo_atributo 	    = UPPER(TRIM(source.codigo_atributo))
					,target.subdominio	        = UPPER(TRIM(source.subdominio))
					,target.nombre_funcional	= UPPER(TRIM(source.nombre_funcional))
					--,target.fuente			= UPPER(source.fuente)
                    --,target.id_fuente		    = ( SELECT tbf.id FROM tmp_matriz_dato tmp, tbl_fuentes tbf WHERE tmp.fuente = tbf.nombre_fuente AND target.codigo_atributo  = tmp.codigo_atributo ) 
					,target.id_fuente		    = dbo.fn_get_idfuente(TRIM(source.fuente_de_datos))
                    ,target.tabla			    = UPPER(TRIM(source.tabla_fuente_datos))
					,target.columna			    = UPPER(TRIM(source.campo_fuente_datos))
					,target.golden_record	    = ISNULL(TRIM(UPPER(source.golden_record)),'N')
                    ,target.tipo_dato           = UPPER(TRIM(source.tipo_dato))
                    ,target.es_nulo             = ISNULL(TRIM(UPPER(source.es_nulo)),'N')
                    ,target.es_campo_llave      = ISNULL(TRIM(UPPER(source.es_campo_llave)),'N')
					,target.es_id_natural       = ISNULL(TRIM(UPPER(source.es_id_natural)),'N')
                    ,target.es_tabla_prioridad  = ISNULL(TRIM(UPPER(source.es_tabla_prioridad)),'N')        
                    ,target.nombre_entidad      = UPPER(TRIM(source.nombre_entidad))
                    ,target.nombre_filial       = UPPER(TRIM(source.nombre_filial))
			WHEN NOT MATCHED BY target THEN
				INSERT (codigo_atributo
					,subdominio
					,nombre_funcional
					,id_fuente
                    ,tabla
					,columna
					,golden_record
                    ,tipo_dato
                    ,es_nulo
                    ,es_campo_llave
                    ,es_id_natural
                    ,es_tabla_prioridad
					,nombre_entidad
                    ,nombre_filial)
				VALUES (UPPER(TRIM(source.codigo_atributo))
					,UPPER(TRIM(source.subdominio))
					,UPPER(TRIM(source.nombre_funcional))
					--,UPPER(source.fuente)
                    --,( SELECT tbf.id FROM tmp_matriz_dato tmp, tbl_fuentes tbf WHERE tmp.fuente = tbf.nombre_fuente AND source.codigo_atributo  = tmp.codigo_atributo ) 
					,dbo.fn_get_idfuente(TRIM(source.fuente_de_datos))
                    ,UPPER(TRIM(source.tabla_fuente_datos))
					,UPPER(TRIM(source.campo_fuente_datos))
					,ISNULL(UPPER(TRIM(source.golden_record)),'N')
                    ,UPPER(TRIM(source.tipo_dato))
                    ,ISNULL(UPPER(TRIM(source.es_nulo)),'N')
                    ,ISNULL(UPPER(TRIM(source.es_campo_llave)),'N')
                    ,ISNULL(UPPER(TRIM(source.es_id_natural)),'N') 
                    ,ISNULL(UPPER(TRIM(source.es_tabla_prioridad)),'N')             
					,UPPER(TRIM(source.nombre_entidad)) 
                    ,UPPER(TRIM(source.nombre_filial)) );
			--WHEN NOT MATCHED BY source THEN
			--    DELETE;
	    SELECT @msjerror = @haserror + ' Registros afectados: ' + CONVERT(VARCHAR(MAX),@@ROWCOUNT) + '.';  
    END;

    --SELECT @error = @haserror;

END TRY

BEGIN CATCH
    SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER());
    SET @haserror = 'Y';
    --SELECT @error = @haserror;
    SELECT @msjerror = @haserror + ' Error ' + @errornumber + ' - ' + ERROR_MESSAGE() + '.';
    PRINT @msjerror;
END CATCH;
GO
