SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tmp_rel_subdominio_x_modelo](
	[id] [int] NOT NULL,
	[nombre_subdominio] [varchar](255) NOT NULL,
	[subdominio_id] [int] NOT NULL,
	[nombre_modelo] [varchar](255) NOT NULL,
	[modelo_id] [int] NOT NULL,
	[nombre_filial] [varchar](255) NOT NULL,
	[filial_id] [int] NOT NULL,
	[entidad_principal_modelo] [varchar](255) NOT NULL,
	[entidad_vs_subdominio] [varchar](255) NOT NULL,
	[entidad_principal_fuente] [varchar](155) NOT NULL
) ON [PRIMARY]
GO
