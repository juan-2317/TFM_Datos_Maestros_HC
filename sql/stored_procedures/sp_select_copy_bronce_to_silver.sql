SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento usado por proceso ingesta a silver para identificar columnas
++ DataFactory: PL_CopyData_BronzeSilver
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER   PROCEDURE [dbo].[sp_select_copy_bronce_to_silver] (@nom_filial nvarchar(150), @nom_dominio nvarchar(150))
AS

BEGIN TRY
   
	DECLARE @errornumber  VARCHAR(MAX);
	
    SELECT DISTINCT CONVERT(varchar(11),tf.id) AS fuente, tm.tabla, tm.nombre_filial, tm.subdominio
    FROM [dbo].[tbl_fuentes] tf
    JOIN dbo.tbl_matriz_dato tm
    ON tf.id = tm.id_fuente
    WHERE tm.golden_record = 'Y'
    AND tm.subdominio = @nom_dominio
	AND tm.nombre_filial = @nom_filial
    ORDER BY 1 DESC;
END TRY

BEGIN CATCH
	SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errornumber + ' - ' + ERROR_MESSAGE()
END CATCH;
GO
