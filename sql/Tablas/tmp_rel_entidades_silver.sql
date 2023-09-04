SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tmp_rel_entidades_silver](
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
