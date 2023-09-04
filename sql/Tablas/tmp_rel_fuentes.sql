SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tmp_rel_fuentes](
	[id] [int] NOT NULL,
	[tabla_padre] [nvarchar](150) NOT NULL,
	[campo_clave] [nvarchar](150) NOT NULL,
	[alias_campo] [nvarchar](150) NOT NULL,
	[es_primary_key] [varchar](155) NOT NULL
) ON [PRIMARY]
GO
