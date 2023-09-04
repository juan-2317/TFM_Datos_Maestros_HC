SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++ Autor: Consultores_ltda
++ Descripcion: Funcion para eliminar caracteres especiales.
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
CREATE OR ALTER        FUNCTION [dbo].[fn_has_specialchar] ( @teststring NVARCHAR(MAX) )
RETURNS INT AS 
BEGIN
    -- Caracter NO incluido en la validacion "_", " "
    DECLARE @check INT;
    SET @check = 1;
    SELECT @check = 0
    WHERE @teststring NOT LIKE '%[-!#%&+,./:;<=>@`{|}~"()*\\\\^\?\[\]\'']%' {ESCAPE '\'}
    -- Retorna 1 si validacion no es exitosa
    RETURN @check
END
GO
