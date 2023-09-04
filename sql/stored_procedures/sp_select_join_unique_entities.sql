SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento usado por proceso ingesta contenedor silver. Obtiene informacion del campo llave
++ DataFactory: PL_Transform_Direct_Tables_Entities
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER   PROCEDURE [dbo].[sp_select_join_unique_entities] (
     @nom_dominio nvarchar(150)
    ,@nom_filial nvarchar(150) )
AS

BEGIN TRY
    -- 
	DECLARE @errornumber  VARCHAR(MAX);
	
    SELECT DISTINCT CONVERT(varchar(11),tf.id) AS fuente
		,tm.tabla
		,tm.nombre_entidad
		,tm.subdominio
		,tm.nombre_filial
		,tm.es_campo_llave AS campo_clave
    FROM [dbo].[tbl_fuentes] tf
    JOIN dbo.tbl_matriz_dato tm
    ON tf.id = tm.id_fuente
    WHERE tm.golden_record = 'Y'
    AND tm.es_campo_llave = 'N'
    -- Asegurar ningun campo de entidad exista campo llave, solo se traen los campos que son llave ya que se puede definir mas de un campo llave.
    AND tm.nombre_entidad NOT IN ( SELECT nombre_entidad
                                     FROM tbl_matriz_dato
                                     WHERE es_campo_llave = 'Y'
    )
    AND tm.subdominio = @nom_dominio
    AND tm.nombre_filial = @nom_filial
    ORDER BY 1 DESC;

END TRY

BEGIN CATCH
	SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errornumber + ' - ' + ERROR_MESSAGE()
END CATCH;
GO
