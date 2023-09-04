SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento usado por proceso ingesta contenedor gold. Obtiene informacion de entidad/dominio en MDS
++ DataFactory: PL_FilesUpsertMDS_v2
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER        PROCEDURE [dbo].[sp_select_entities_mds_m_and_m] ( @nom_filial nvarchar(150), @nom_dominio_mds nvarchar(150), @subdominio nvarchar(150) )
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
    UNION
    SELECT [nombre_filial]
      ,[nombre_modelo] AS dominio_mds
      ,[entidad_vs_subdominio] AS nombre_entidad_mds
      ,[nombre_subdominio] AS subdominio
    FROM [dbo].[tbl_rel_subdominio_x_modelo]
    WHERE nombre_filial = @nom_filial
    AND nombre_subdominio = @subdominio
    AND nombre_modelo = @nom_dominio_mds


END TRY

BEGIN CATCH
	SET @errormsj = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errormsj + ' - ' + ERROR_MESSAGE()
END CATCH;
GO
