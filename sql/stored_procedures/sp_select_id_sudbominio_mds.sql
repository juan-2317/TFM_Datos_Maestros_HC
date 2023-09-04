SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento usado para obtener ID de Subdominios creados en MDS.
++ DataFactory: PL_GetIdSubdominioMDS
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER     PROCEDURE [dbo].[sp_select_id_sudbominio_mds] ( @nom_dominio nvarchar(150), @nom_modelo nvarchar(150))
AS

BEGIN TRY

	DECLARE @errornumber  VARCHAR(MAX);

    -- ID Subdominio puede varia entre modelos. Por esta razón se debe incluir el filtro del modelo.
    -- Aplicación no deja crear deja crear dos subdomnios con el mismo nombre en la misma filial. Tabla es alimentada desde la aplicación.
    SELECT subdominio_id, modelo_id
    FROM tbl_rel_subdominio_x_modelo
    WHERE 1=1
    AND UPPER(nombre_subdominio) = UPPER(@nom_dominio)
    AND UPPER(nombre_modelo) = UPPER(@nom_modelo)


END TRY

BEGIN CATCH
	SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errornumber + ' - ' + ERROR_MESSAGE()
END CATCH;
GO
