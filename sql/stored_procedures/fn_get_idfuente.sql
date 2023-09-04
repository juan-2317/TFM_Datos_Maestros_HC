SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Funcion para obtener id de la fuente.
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER     FUNCTION [dbo].[fn_get_idfuente]( @nom_fuente NVARCHAR(150))
RETURNS INT AS
BEGIN
    DECLARE @idfuente     INT

    SELECT @idfuente = id
    FROM tbl_fuentes
    WHERE 1=1
    AND nombre_fuente = @nom_fuente;

    RETURN @idfuente;

END 
GO
