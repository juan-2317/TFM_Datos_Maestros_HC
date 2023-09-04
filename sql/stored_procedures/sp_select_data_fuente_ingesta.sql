SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento usado por proceso ingesta a contenedor bronce para obtener datos de conexion
++ DataFactory: PL_Load_All_Data
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER   PROCEDURE [dbo].[sp_select_data_fuente_ingesta](@nom_dominio nvarchar(150), @nom_filial nvarchar(150))
AS


BEGIN TRY

    DECLARE @messageerror VARCHAR(MAX);
    DECLARE @errornumber  VARCHAR(MAX);

    SELECT DISTINCT CONVERT(NVARCHAR(2),tf.id_fuente) AS fuente
        ,ISNULL(tf.nombre_servidor,tf.direccion_ip) AS nombre_servidor
        ,tf.nombre_base_datos
        ,tf.esquema
        ,tf.puerto
        ,tm.tabla
        --,tm.columna
        ,tf.usuario nombre_usuario
        ,tf.key_value
    FROM dbo.tbl_datos_conexion_fuentes tf
    JOIN dbo.tbl_matriz_dato tm
    ON tf.id_fuente = tm.id_fuente
    WHERE 1=1
    AND tm.golden_record = 'Y'
    AND tf.activo = 'Y'
    --AND tm.subdominio = ISNULL(REPLACE(@nom_dominio,'',NULL),tm.subdominio)
    --AND tm.nombre_filial = ISNULL(REPLACE(@nom_filial,'',NULL),tm.nombre_filial)
	AND tm.subdominio = @nom_dominio
	AND tm.nombre_filial = @nom_filial
    ORDER BY 1 DESC;

END TRY

BEGIN CATCH
    SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errornumber + ' - ' + ERROR_MESSAGE()

END CATCH;
GO
