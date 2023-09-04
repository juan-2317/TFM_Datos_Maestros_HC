SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento usado por proceso de reporte de coincidencias. Obtiene informacion de entidad/dominio en MDS
++ DataFactory: PL_GenerateReports
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER      PROCEDURE [dbo].[sp_select_entities_reports] ( @nom_filial nvarchar(150), @nom_dominio_mds nvarchar(150), @subdominio nvarchar(150) )
AS

BEGIN TRY
    -- 
	DECLARE @errormsj  VARCHAR(MAX);
  
    SELECT DISTINCT nombre_filial
        ,nombre_modelo AS dominio_mds
        ,nombre_entidad_mds
        ,subdominio
    FROM  tbl_matriz_dato
    WHERE 1=1					   
    AND golden_record = 'Y'
    AND nombre_filial = @nom_filial
    AND subdominio = @subdominio
    AND nombre_modelo = @nom_dominio_mds
    AND nombre_entidad_mds IS NOT NULL
    AND nombre_entidad_mds IN (
        SELECT nombre_entidad_mds
        FROM  tbl_matriz_dato
        WHERE 1=1
        AND golden_record = 'Y'
        AND es_id_natural = 'Y'
        AND nombre_filial = @nom_filial
        AND subdominio = @subdominio
        AND nombre_modelo = @nom_dominio_mds
    )
    


END TRY

BEGIN CATCH
	SET @errormsj = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errormsj + ' - ' + ERROR_MESSAGE()
END CATCH;
GO
