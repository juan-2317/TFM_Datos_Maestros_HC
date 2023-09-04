SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tbl_relacion_entidades_silver](
	[ID] [int] NOT NULL,
	[nombre_filial] [nvarchar](150) NOT NULL,
	[subdominio] [nvarchar](150) NOT NULL,
	[nombre_entidad_padre] [nvarchar](150) NOT NULL,
	[campo_clave] [nvarchar](150) NOT NULL,
	[entidad_relacion] [nvarchar](150) NOT NULL,
	[campo_relacion] [nvarchar](150) NOT NULL,
	[tipo_relacion] [nvarchar](50) NOT NULL,
	[nombre_grupo] [nvarchar](150) NULL,
	[orden_relacion_entidad] [int] NULL,
	[orden_en_grupo] [int] NULL,
	[id_natural] [nvarchar](150) NULL,
	[es_id_upsert] [varchar](155) NOT NULL,
	[criterio_duplicados] [varchar](150) NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[tbl_relacion_entidades_silver] ADD  CONSTRAINT [pk_tbl_relacion_entidades_silver_id] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [dbo].[tbl_relacion_entidades_silver] ADD  DEFAULT ('N') FOR [es_id_upsert]
GO
ALTER TABLE [dbo].[tbl_relacion_entidades_silver]  WITH CHECK ADD  CONSTRAINT [chk_relacion_ent_silver] CHECK  (([tipo_relacion]='FULL OUTER' OR [tipo_relacion]='RIGHT' OR [tipo_relacion]='INNER' OR [tipo_relacion]='LEFT'))
GO
ALTER TABLE [dbo].[tbl_relacion_entidades_silver] CHECK CONSTRAINT [chk_relacion_ent_silver]
GO
