SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tbl_mds_dominios](
	[id_dominio] [nvarchar](max) NULL,
	[nombre_dominio] [nvarchar](max) NULL,
	[id_entidad] [nvarchar](max) NULL,
	[nombre_entidad] [nvarchar](max) NULL,
	[esListaReferencia] [bit] NULL,
	[tablaStg] [nvarchar](max) NULL,
	[id_atributo] [nvarchar](max) NULL,
	[nombre_atributo] [nvarchar](max) NULL,
	[tipo_atributo] [nvarchar](max) NULL,
	[entidad_dominio] [nvarchar](max) NULL,
	[fecha_actualizacion] [date] NULL,
	[id_row] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
