SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento usado para obtener informacion de las entidades en MDS
++ DataFactory: PL_GetListMemberMDS
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER      PROCEDURE [dbo].[sp_select_entities_mds] ( @subdominio nvarchar(150), @nom_filial nvarchar(150))
AS

BEGIN TRY

	DECLARE @errornumber  VARCHAR(MAX);

    -- Se realiza JOIN con tabla dominios MDS para extraer nombre de las entidades/dominios que existen en MDS
    SELECT DISTINCT tbmdt.nombre_filial
        ,tbmdt.subdominio
        ,tbmdt.nombre_modelo AS dominio_mds
        --,dom.entidad_dominio AS nombre_entidad_mds
        ,dom.nombre_entidad AS nombre_entidad_mds
    FROM  tbl_matriz_dato tbmdt
    ,tbl_mds_dominios dom
    WHERE dom.nombre_dominio = tbmdt.nombre_modelo
    AND tbmdt.subdominio = @subdominio
    AND tbmdt.nombre_filial = @nom_filial
    AND dom.nombre_entidad IS NOT NULL
	ORDER BY dom.nombre_entidad


END TRY

BEGIN CATCH
	SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errornumber + ' - ' + ERROR_MESSAGE()
END CATCH;
GO
