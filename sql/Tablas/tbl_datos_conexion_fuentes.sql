SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tbl_datos_conexion_fuentes](
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
ALTER TABLE [dbo].[tbl_datos_conexion_fuentes] ADD  CONSTRAINT [pk_tbl_datos_conexion_id] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [dbo].[tbl_datos_conexion_fuentes] ADD  CONSTRAINT [DF__tbl_datos__activ__1]  DEFAULT ('N') FOR [activo]
GO
ALTER TABLE [dbo].[tbl_datos_conexion_fuentes]  WITH CHECK ADD  CONSTRAINT [fk_id_fuentes] FOREIGN KEY([id_fuente])
REFERENCES [dbo].[tbl_fuentes] ([ID])
GO
ALTER TABLE [dbo].[tbl_datos_conexion_fuentes] CHECK CONSTRAINT [fk_id_fuentes]
GO
