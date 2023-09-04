SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento usado por proceso ingesta contenedor silver. Obtiene informacion de entidad/dominio funcional
++ DataFactory: PL_Standardize_Entities
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER   PROCEDURE [dbo].[sp_select_entities_silver] (@nom_filial nvarchar(150), @nom_dominio nvarchar(150))
AS

BEGIN TRY

	DECLARE @errornumber  VARCHAR(MAX);
	
	SELECT DISTINCT nombre_filial, subdominio, nombre_entidad
	FROM tbl_matriz_dato
	WHERE golden_record = 'Y'
    AND nombre_filial = @nom_filial
	AND subdominio = @nom_dominio;

END TRY

BEGIN CATCH
	SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errornumber + ' - ' + ERROR_MESSAGE()
END CATCH;
GO
