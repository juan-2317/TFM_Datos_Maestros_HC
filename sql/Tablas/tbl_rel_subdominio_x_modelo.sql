SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tbl_rel_subdominio_x_modelo](
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
ALTER TABLE [dbo].[tbl_rel_subdominio_x_modelo] ADD  CONSTRAINT [PK_tbl_rel_subdominio_x_modelo] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
ALTER TABLE [dbo].[tbl_rel_subdominio_x_modelo] ADD  CONSTRAINT [uc_subdominio__modelo] UNIQUE NONCLUSTERED 
(
	[nombre_subdominio] ASC,
	[nombre_modelo] ASC,
	[nombre_filial] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
