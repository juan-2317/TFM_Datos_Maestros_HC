SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tbl_relacion_tablas_fuentes](
	[id] [int] NOT NULL,
	[tabla_padre] [nvarchar](150) NOT NULL,
	[campo_clave] [nvarchar](150) NOT NULL,
	[alias_campo] [nvarchar](150) NOT NULL,
	[es_primary_key] [varchar](155) NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[tbl_relacion_tablas_fuentes] ADD  CONSTRAINT [pk_tbl_relacion_tablas_fuentes_id] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [dbo].[tbl_relacion_tablas_fuentes] ADD  DEFAULT ('N') FOR [es_primary_key]
GO
