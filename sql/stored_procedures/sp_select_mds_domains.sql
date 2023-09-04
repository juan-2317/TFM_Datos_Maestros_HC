SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento para obtener el modelo MDS basado en la filial
++ DataFactory: PL_GetPathToUpsertMDS 
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER    PROCEDURE  [dbo].[sp_select_mds_domains] (
	 @nom_filial nvarchar(150)
	,@action nvarchar(150))
AS

BEGIN TRY
	DECLARE @errornumber  VARCHAR(MAX);
	
	SELECT DISTINCT (CASE 
			WHEN @action = 'INS' THEN nombre_modelo
			WHEN @action = 'UPD' THEN nombre_modelo + '/>>/' + nombre_entidad_mds -->  " />>/ " indicador para remplazar en el pipeline
			ELSE 'N/A'
			END ) AS nombre_modelo
	FROM tbl_matriz_dato
	WHERE golden_record = 'Y'
	AND nombre_filial = @nom_filial;

END TRY

BEGIN CATCH
	SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errornumber + ' - ' + ERROR_MESSAGE()
END CATCH;

GO
