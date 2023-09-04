SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento creado para la carga de datos por medio de un archivo plano
++  en la tabla tbl_relacion_entidades_silver.
++ Data Factory: PL_Load_Relacion_entidades_silver
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/

CREATE        PROCEDURE [dbo].[sp_merge_rel_entidades_silver] (
    --@error    NVARCHAR(1) OUTPUT,
    @msjerror NVARCHAR(MAX) OUTPUT
) AS

BEGIN TRY
	DECLARE @haserror VARCHAR(10) = 'N';
    DECLARE @errornumber  VARCHAR(MAX);

    MERGE tbl_relacion_entidades_silver AS target
        USING tmp_rel_entidades_silver AS source
        ON ( target.ID  = source.ID )
        WHEN MATCHED THEN UPDATE SET
                target.ID = source.ID,
                target.nombre_filial = source.nombre_filial,
                target.subdominio = source.subdominio,
                target.nombre_entidad_padre = source.nombre_entidad_padre,
                target.campo_clave = source.campo_clave,
                target.entidad_relacion = source.entidad_relacion,
                target.campo_relacion = source.campo_relacion,
                target.tipo_relacion = source.tipo_relacion,
                target.nombre_grupo = source.nombre_grupo,
                target.orden_relacion_entidad = source.orden_relacion_entidad,
                target.orden_en_grupo = source.orden_en_grupo,
                target.id_natural = source.id_natural,
                target.es_id_upsert = source.es_id_upsert,
                target.criterio_duplicados = source.criterio_duplicados

        WHEN NOT MATCHED BY target THEN
            INSERT (ID,
                    nombre_filial,
                    subdominio,
                    nombre_entidad_padre,
                    campo_clave,
                    entidad_relacion,
                    campo_relacion,
                    tipo_relacion,
                    nombre_grupo,
                    orden_relacion_entidad,
                    orden_en_grupo,
                    id_natural,
                    es_id_upsert,
                    criterio_duplicados)
            VALUES (source.ID,
                    UPPER(TRIM(source.nombre_filial)),
                    source.subdominio,
                    TRIM(source.nombre_entidad_padre),
                    source.campo_clave,
                    UPPER(TRIM(source.entidad_relacion)),
                    source.campo_relacion,
                    TRIM(source.tipo_relacion),
                    TRIM(source.nombre_grupo),
                    source.orden_relacion_entidad,
                    source.orden_en_grupo,
                    TRIM(source.id_natural),
                    source.es_id_upsert,
                    UPPER(TRIM(source.criterio_duplicados)) );
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
