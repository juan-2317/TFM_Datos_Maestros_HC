SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Procedimiento usado para obtener el orden de insercion en las entidades de MDS.
++ DataFactory: PL_Insert_EntitiesMDS y PL_CallProcedureMDS
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER    PROCEDURE [dbo].[sp_select_orden_entidades_mds](
     @p_modelo  NVARCHAR(MAX)
    ,@p_entidad NVARCHAR(MAX))
AS
BEGIN TRY
    DECLARE @messageerror VARCHAR(MAX);
    DECLARE @errornumber  VARCHAR(MAX);

    SELECT DISTINCT oe.entidad, oe.orden
      --,REPLACE(md.tablaStg,'stg.','') AS name_procedure
      ,md.tablaStg AS name_procedure
      ,REPLACE(md.tablaStg,'stg.udp_','') AS table_mds
    FROM tbl_mds_orden_entidades oe
        ,tbl_mds_dominios md
    WHERE 1=1
    AND oe.nombre_modelo = md.nombre_dominio
    AND oe.entidad = md.nombre_entidad
    AND oe.nombre_modelo = @p_modelo 
    AND oe.entidad = ISNULL(@p_entidad,oe.entidad)
    ORDER BY oe.orden ASC; 
END TRY
BEGIN CATCH
    SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errornumber + ' - ' + ERROR_MESSAGE()
END CATCH;
GO
