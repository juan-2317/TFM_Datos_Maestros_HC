SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Funcion para eliminar caracteres especiales.
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER           FUNCTION [dbo].[fn_remove_tilde] ( @teststring NVARCHAR(MAX) )
RETURNS NVARCHAR(MAX) AS
BEGIN
    DECLARE @check NVARCHAR(MAX);
    SELECT @check = TRANSLATE(@teststring, 
      'ñáéíóúàèìòùãõâêîôôäëïöüçÑÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇ ', 
      'naeiouaeiouaoaeiooaeioucNAEIOUAEIOUAOAEIOOAEIOUC_');
    RETURN  @check 
END
GO
