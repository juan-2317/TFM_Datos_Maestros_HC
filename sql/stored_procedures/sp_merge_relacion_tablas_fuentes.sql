SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento creado para la carga de datos por medio de un archivo plano
++  en la tabla tbl_relacion_tablas_fuentes.
++ Data Factory: PL_Load_Relacion_tablas_fuente
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/

CREATE OR ALTER      PROCEDURE [dbo].[sp_merge_relacion_tablas_fuentes] (
    --@error    NVARCHAR(1) OUTPUT,
    @msjerror NVARCHAR(MAX) OUTPUT
) AS

BEGIN TRY
	DECLARE @haserror VARCHAR(10) = 'N';
    DECLARE @errornumber  VARCHAR(MAX);

    MERGE tbl_relacion_tablas_fuentes AS target
        USING tmp_rel_fuentes AS source
        ON ( target.id  = source.id )
        WHEN MATCHED THEN UPDATE SET
                target.id = source.id,
                target.tabla_padre = source.tabla_padre,
                target.campo_clave = source.campo_clave,
                target.alias_campo = source.alias_campo,
                target.es_primary_key = source.es_primary_key
              
        WHEN NOT MATCHED BY target THEN
            INSERT (id,
                    tabla_padre,
                    campo_clave,
                    alias_campo,
                    es_primary_key)
            VALUES (source.id,
                    UPPER(TRIM(source.tabla_padre)),
                    source.campo_clave,
                    UPPER(TRIM(source.alias_campo)),
                     source.es_primary_key );
    SELECT @msjerror = @haserror + ' Registros afectados: ' + CONVERT(VARCHAR(MAX),@@ROWCOUNT) + '.';

END TRY

BEGIN CATCH
    SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER());
    SET @haserror = 'Y';
    --SELECT @error = @haserror;
    SELECT @msjerror = @haserror + ' Error ' + @errornumber + ' - ' + ERROR_MESSAGE() + '.';
    PRINT @msjerror;
END CATCH;
GO
