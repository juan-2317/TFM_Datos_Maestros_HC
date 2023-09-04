SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento creado para la carga de datos por medio de un archivo plano
++  en la tabla tbl_datos_conexion_fuentes.
++ Data Factory: PL_Load_Datos_Conexion_Fuentes
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/

CREATE OR ALTER        PROCEDURE [dbo].[sp_merge_datos_conexion_fuentes] (
    --@error    NVARCHAR(1) OUTPUT,
    @msjerror NVARCHAR(MAX) OUTPUT
) AS

BEGIN TRY
	DECLARE @haserror VARCHAR(10) = 'N';
    DECLARE @errornumber  VARCHAR(MAX);

    MERGE tbl_datos_conexion_fuentes AS target
        USING tmp_datos_conexion_fuentes AS source
        ON ( target.ID  = source.ID )
        WHEN MATCHED THEN UPDATE SET
                target.ID = source.ID,
                target.nombre_servidor = source.nombre_servidor,
                target.direccion_ip = source.direccion_ip,
                target.nombre_base_datos = source.nombre_base_datos,
                target.esquema = source.esquema,
                target.puerto = source.puerto,
                target.usuario = source.usuario,
                target.key_value = source.key_value,
                target.id_fuente = source.id_fuente,
                target.activo = source.activo
        WHEN NOT MATCHED BY target THEN
            INSERT (ID,
                    nombre_servidor,
                    direccion_ip,
                    nombre_base_datos,
                    esquema,
                    puerto,
                    usuario,
                    key_value,
                    id_fuente,
                    activo)
            VALUES (source.ID,
                    TRIM(source.nombre_servidor),
                    TRIM(source.direccion_ip),
                    TRIM(source.nombre_base_datos),
                    TRIM(source.esquema),
                    TRIM(source.puerto),
                    TRIM(source.usuario),
                    TRIM(source.key_value),
                    source.id_fuente,
                    TRIM(source.activo) );
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
