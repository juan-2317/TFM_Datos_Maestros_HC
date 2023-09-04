SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento usado por proceso ingesta contenedor silver para obtener nombre de la entidad funcional.
++ DataFactory: PL_Standardize_Entities
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER   PROCEDURE [dbo].[sp_select_entities_by_domain_to_transform] (@nom_dominio nvarchar(150), @nom_filial nvarchar(150))
AS

BEGIN TRY
	DECLARE @errornumber  VARCHAR(MAX);
	
	SELECT DISTINCT tbm.nombre_entidad, tbm.tabla, CONVERT(varchar(11), tbf.id) AS fuente, tbm.subdominio, tbm.nombre_filial
	FROM tbl_matriz_dato tbm
    INNER JOIN tbl_fuentes tbf
    ON tbm.id_fuente = tbf.ID
	WHERE tbm.golden_record = 'Y'
    AND tbm.subdominio = @nom_dominio
	AND tbm.nombre_filial = @nom_filial;

END TRY

BEGIN CATCH
	SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errornumber + ' - ' + ERROR_MESSAGE()
END CATCH;
GO
