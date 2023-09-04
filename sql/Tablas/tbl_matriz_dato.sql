SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tbl_matriz_dato](
	[codigo_atributo] [varchar](255) NOT NULL,
	[subdominio] [varchar](255) NOT NULL,
	[nombre_funcional] [varchar](255) NULL,
	[id_fuente] [varchar](255) NOT NULL,
	[tabla] [varchar](255) NOT NULL,
	[columna] [varchar](255) NOT NULL,
	[golden_record] [varchar](255) NOT NULL,
	[tipo_dato] [varchar](255) NOT NULL,
	[es_nulo] [varchar](255) NOT NULL,
	[es_campo_llave] [varchar](255) NOT NULL,
	[es_id_natural] [varchar](255) NOT NULL,
	[es_tabla_prioridad] [varchar](255) NOT NULL,
	[nombre_entidad] [varchar](255) NULL,
	[nombre_filial] [varchar](255) NULL,
	[nombre_modelo] [varchar](100) NULL,
	[id_version] [varchar](10) NULL,
	[tipo_dato_mds] [varchar](100) NULL,
	[nombre_entidad_referenciada] [varchar](100) NULL,
	[nombre_atributo_mds] [varchar](100) NULL,
	[nombre_entidad_mds] [varchar](100) NULL
) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
ALTER TABLE [dbo].[tbl_matriz_dato] ADD  CONSTRAINT [pk_tbl_matriz_dato] PRIMARY KEY CLUSTERED 
(
	[codigo_atributo] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [dbo].[tbl_matriz_dato] ADD  CONSTRAINT [DF__tbl_matri__1]  DEFAULT ('N') FOR [golden_record]
GO
ALTER TABLE [dbo].[tbl_matriz_dato] ADD  CONSTRAINT [DF__tbl_matri__2]  DEFAULT ('Y') FOR [es_nulo]
GO
ALTER TABLE [dbo].[tbl_matriz_dato] ADD  CONSTRAINT [DF__tbl_matri__3]  DEFAULT ('N') FOR [es_campo_llave]
GO
ALTER TABLE [dbo].[tbl_matriz_dato] ADD  CONSTRAINT [DF__tbl_matri__4]  DEFAULT ('N') FOR [es_id_natural]
GO
ALTER TABLE [dbo].[tbl_matriz_dato] ADD  CONSTRAINT [DF__tbl_matri__5]  DEFAULT ('N') FOR [es_tabla_prioridad]
GO
ALTER TABLE [dbo].[tbl_matriz_dato] ADD  CONSTRAINT [chk_default_tipo_dato_mds]  DEFAULT (N'Text') FOR [tipo_dato_mds]
GO
ALTER TABLE [dbo].[tbl_matriz_dato]  WITH CHECK ADD  CONSTRAINT [chk_golden_record] CHECK  (([golden_record]='N' OR [golden_record]='Y'))
GO
ALTER TABLE [dbo].[tbl_matriz_dato] CHECK CONSTRAINT [chk_golden_record]
GO
ALTER TABLE [dbo].[tbl_matriz_dato]  WITH CHECK ADD  CONSTRAINT [chk_id_natural] CHECK  (([es_id_natural]='N' OR [es_id_natural]='Y'))
GO
ALTER TABLE [dbo].[tbl_matriz_dato] CHECK CONSTRAINT [chk_id_natural]
GO
ALTER TABLE [dbo].[tbl_matriz_dato]  WITH CHECK ADD  CONSTRAINT [chk_nulo] CHECK  (([es_nulo]='N' OR [es_nulo]='Y'))
GO
ALTER TABLE [dbo].[tbl_matriz_dato] CHECK CONSTRAINT [chk_nulo]
GO
ALTER TABLE [dbo].[tbl_matriz_dato]  WITH CHECK ADD  CONSTRAINT [chk_tabla_prioridad] CHECK  (([es_tabla_prioridad]='N' OR [es_tabla_prioridad]='Y'))
GO
ALTER TABLE [dbo].[tbl_matriz_dato] CHECK CONSTRAINT [chk_tabla_prioridad]
GO
ALTER TABLE [dbo].[tbl_matriz_dato]  WITH CHECK ADD  CONSTRAINT [chk_tipo_dato] CHECK  (([tipo_dato]='ALFABETICO' OR [tipo_dato]='ALFANUMERICO' OR [tipo_dato]='BOOLEANO' OR [tipo_dato]='FECHA' OR [tipo_dato]='NUMERICO'))
GO
ALTER TABLE [dbo].[tbl_matriz_dato] CHECK CONSTRAINT [chk_tipo_dato]
GO
