SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tbl_mds_orden_entidades](
	[entidad] [nvarchar](max) NOT NULL,
	[orden] [bigint] NOT NULL,
	[nombre_modelo] [nvarchar](max) NOT NULL,
	[fecha_copia] [nvarchar](max) NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
