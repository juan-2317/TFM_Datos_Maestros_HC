SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tmp_matriz_dato](
	[codigo_atributo] [varchar](255) NULL,
	[subdominio] [varchar](255) NULL,
	[nombre_funcional] [varchar](255) NULL,
	[fuente_de_datos] [varchar](255) NULL,
	[tabla_fuente_datos] [varchar](255) NULL,
	[campo_fuente_datos] [varchar](255) NULL,
	[golden_record] [varchar](255) NULL,
	[tipo_dato] [varchar](255) NULL,
	[es_nulo] [varchar](255) NULL,
	[es_campo_llave] [varchar](255) NULL,
	[es_id_natural] [varchar](255) NULL,
	[es_tabla_prioridad] [varchar](255) NULL,
	[nombre_entidad] [varchar](255) NULL,
	[nombre_filial] [varchar](255) NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[tmp_matriz_dato] ADD  CONSTRAINT [DF__tmp_matri__1]  DEFAULT ('N') FOR [es_nulo]
GO
ALTER TABLE [dbo].[tmp_matriz_dato] ADD  CONSTRAINT [DF__tmp_matri__2]  DEFAULT ('N') FOR [es_campo_llave]
GO
ALTER TABLE [dbo].[tmp_matriz_dato] ADD  CONSTRAINT [DF__tmp_matri__3]  DEFAULT ('N') FOR [es_id_natural]
GO
ALTER TABLE [dbo].[tmp_matriz_dato] ADD  CONSTRAINT [DF__tmp_matri__4]  DEFAULT ('N') FOR [es_tabla_prioridad]
GO
