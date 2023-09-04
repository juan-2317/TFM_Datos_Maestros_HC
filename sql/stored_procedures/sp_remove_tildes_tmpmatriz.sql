SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Actualiza valores en tabla temporal matriz datos, eliminando las tildes
++ Dependencias: fn_remove_tilde
++ Usado por: sp_merge_matriz_dato
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER      PROCEDURE [dbo].[sp_remove_tildes_tmpmatriz]
AS
BEGIN TRY
    DECLARE @messageerror NVARCHAR(MAX)
    DECLARE @errornumber  NVARCHAR(MAX)

    UPDATE tmp_matriz_dato
    SET codigo_atributo      = [dbo].fn_remove_tilde(codigo_atributo)
        ,subdominio          = [dbo].fn_remove_tilde(subdominio)
        ,nombre_funcional    = [dbo].fn_remove_tilde(nombre_funcional)
        ,tabla_fuente_datos  = [dbo].fn_remove_tilde(tabla_fuente_datos)
        ,campo_fuente_datos  = [dbo].fn_remove_tilde(campo_fuente_datos)
        ,nombre_entidad      = [dbo].fn_remove_tilde(nombre_entidad)
        ,tipo_dato           = [dbo].fn_remove_tilde(tipo_dato)
END TRY
BEGIN CATCH
    SET @errornumber = CONVERT(VARCHAR(MAX),ERROR_NUMBER())
    PRINT 'Error ' + @errornumber + ' - ' + ERROR_MESSAGE()
END CATCH
GO
