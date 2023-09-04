SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tmp_datos_conexion_fuentes](
	[ID] [int] NOT NULL,
	[nombre_servidor] [nvarchar](150) NULL,
	[direccion_ip] [nvarchar](15) NULL,
	[nombre_base_datos] [nvarchar](150) NOT NULL,
	[esquema] [nvarchar](15) NULL,
	[puerto] [nvarchar](6) NULL,
	[usuario] [nvarchar](150) NOT NULL,
	[key_value] [nvarchar](150) NOT NULL,
	[id_fuente] [int] NOT NULL,
	[activo] [nvarchar](1) NULL
) ON [PRIMARY]
GO
