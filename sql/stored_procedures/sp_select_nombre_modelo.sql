SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento para obtener el nombre del modelo definido en la tabla maestra de datos.
++ DataFactory: PL_GetDomainsMDS
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER   PROCEDURE  [dbo].[sp_select_nombre_modelo] ( @nom_dominio nvarchar(150), @nom_filial nvarchar(150))
AS

BEGIN TRY
    -- 
    DECLARE @errornumber  VARCHAR(MAX);


    SELECT DISTINCT tbmdt.nombre_modelo
    FROM  tbl_matriz_dato tbmdt
    WHERE 1=1
    AND tbmdt.subdominio = @nom_dominio
    AND tbmdt.nombre_filial = @nom_filial
    AND tbmdt.golden_record = 'Y';
END TRY

BEGIN CATCH
	SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errornumber + ' - ' + ERROR_MESSAGE()
END CATCH;
GO
