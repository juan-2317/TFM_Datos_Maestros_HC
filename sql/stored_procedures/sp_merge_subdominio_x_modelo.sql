SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento creado para la carga de datos por medio de un archivo plano
++  en la tabla tbl_rel_subdominio_x_modelo.
++ Data Factory: PL_Load_Subdominio_x_Modelo
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/

CREATE OR ALTER        PROCEDURE [dbo].[sp_merge_subdominio_x_modelo] (
    --@error    NVARCHAR(1) OUTPUT,
    @msjerror NVARCHAR(MAX) OUTPUT
) AS

BEGIN TRY
	DECLARE @haserror VARCHAR(10) = 'N';
    DECLARE @errornumber  VARCHAR(MAX);

    MERGE tbl_rel_subdominio_x_modelo AS target
        USING tmp_rel_subdominio_x_modelo AS source
        ON ( target.id  = source.id )
        WHEN MATCHED THEN UPDATE SET
                target.id = source.id,
                target.nombre_subdominio = source.nombre_subdominio,
                target.subdominio_id = source.subdominio_id,
                target.nombre_modelo = source.nombre_modelo,
                target.modelo_id = source.modelo_id,
                target.nombre_filial = source.nombre_filial,
                target.filial_id = source.filial_id,
                target.entidad_principal_modelo = source.entidad_principal_modelo,
                target.entidad_vs_subdominio = source.entidad_vs_subdominio,
                target.entidad_principal_fuente = source.entidad_principal_fuente
        WHEN NOT MATCHED BY target THEN
            INSERT (id,
                    nombre_subdominio,
                    subdominio_id,
                    nombre_modelo,
                    modelo_id,
                    nombre_filial,
                    filial_id,
                    entidad_principal_modelo,
                    entidad_vs_subdominio,
                    entidad_principal_fuente)
            VALUES (source.id,
                    UPPER(TRIM(source.nombre_subdominio)),
                    source.subdominio_id,
                    TRIM(source.nombre_modelo),
                    source.modelo_id,
                    UPPER(TRIM(source.nombre_filial)),
                    source.filial_id,
                    TRIM(source.entidad_principal_modelo),
                    TRIM(source.entidad_vs_subdominio),
                    UPPER(TRIM(source.entidad_principal_fuente)) );
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
