USE [master]
GO
/****** Object:  Database [ETL]    Script Date: 29-08-2024 19:59:00 ******/
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'ETL')
BEGIN
CREATE DATABASE [ETL]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'sp_Data', FILENAME = N'D:\ETL.mdf' , SIZE = 23552KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'sp_Log', FILENAME = N'D:\ETL.ldf' , SIZE = 17664KB , MAXSIZE = 2048GB , FILEGROWTH = 10%)
 WITH CATALOG_COLLATION = DATABASE_DEFAULT, LEDGER = OFF
END
GO
ALTER DATABASE [ETL] SET COMPATIBILITY_LEVEL = 130
GO
USE [ETL]
GO

/****** Object:  Schema [dw]    Script Date: 29-08-2024 19:59:00 ******/
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'dw')
EXEC sys.sp_executesql N'CREATE SCHEMA [dw]'
GO
/****** Object:  Schema [ship]    Script Date: 29-08-2024 19:59:00 ******/
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'ship')
EXEC sys.sp_executesql N'CREATE SCHEMA [ship]'
GO
/****** Object:  Schema [stg]    Script Date: 29-08-2024 19:59:00 ******/
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'stg')
EXEC sys.sp_executesql N'CREATE SCHEMA [stg]'
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pipelinelog]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[pipelinelog](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[Batchid] [int] NULL,
	[PipelineName] [varchar](50) NULL,
	[ExecutionStatus] [varchar](50) NULL,
	[CreatedOn] [datetime2](7) NULL,
	[UpdatedOn] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_Address]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_Address]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_Address](
	[AddressID] [int] IDENTITY(1,1) NOT NULL,
	[AddressLine1] [varchar](255) NULL,
	[AddressLine2] [varchar](255) NULL,
	[City] [varchar](50) NULL,
	[State] [varchar](50) NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_Address] PRIMARY KEY CLUSTERED 
(
	[AddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_Bank]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_Bank]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_Bank](
	[BankID] [int] IDENTITY(1,1) NOT NULL,
	[BankName] [varchar](255) NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_Bank] PRIMARY KEY CLUSTERED 
(
	[BankID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_BillerType]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_BillerType]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_BillerType](
	[BillerTypeID] [int] IDENTITY(1,1) NOT NULL,
	[BillerType] [varchar](255) NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_BillerType] PRIMARY KEY CLUSTERED 
(
	[BillerTypeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_Carriers]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_Carriers]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_Carriers](
	[CarrierID] [int] IDENTITY(1,1) NOT NULL,
	[CarrierName] [varchar](255) NULL,
	[PhoneNumber] [varchar](20) NULL,
	[AddressID] [int] NULL,
	[VehicleNo] [varchar](50) NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_Carriers] PRIMARY KEY CLUSTERED 
(
	[CarrierID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_City]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_City]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_City](
	[CityID] [int] IDENTITY(1,1) NOT NULL,
	[CityName] [varchar](50) NULL,
	[StateName] [varchar](50) NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_City] PRIMARY KEY CLUSTERED 
(
	[CityID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_Customer]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_Customer]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_Customer](
	[CustomerID] [int] IDENTITY(1,1) NOT NULL,
	[CustomerName] [varchar](255) NULL,
	[PhoneNumber] [varchar](20) NULL,
	[AddressID] [int] NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_Customer] PRIMARY KEY CLUSTERED 
(
	[CustomerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_date]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_date]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_date](
	[DateKey] [int] NOT NULL,
	[Date] [datetime] NULL,
	[Year] [int] NULL,
	[Month] [int] NULL,
	[Day] [int] NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_Date] PRIMARY KEY CLUSTERED 
(
	[DateKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_DimBank]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_DimBank]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_DimBank](
	[BankID] [int] IDENTITY(1,1) NOT NULL,
	[BankName] [varchar](255) NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_DimBank] PRIMARY KEY CLUSTERED 
(
	[BankID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_Entity]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_Entity]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_Entity](
	[EntityID] [int] IDENTITY(1,1) NOT NULL,
	[EntityName] [varchar](255) NULL,
	[PhoneNumber] [varchar](20) NULL,
	[AddressID] [int] NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_Entity] PRIMARY KEY CLUSTERED 
(
	[EntityID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_PaymentTypes]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_PaymentTypes]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_PaymentTypes](
	[PaymentTypeID] [int] IDENTITY(1,1) NOT NULL,
	[PaymentType] [varchar](255) NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_PaymentTypes] PRIMARY KEY CLUSTERED 
(
	[PaymentTypeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_Person]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_Person]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_Person](
	[PersonID] [int] IDENTITY(1,1) NOT NULL,
	[PersonName] [varchar](255) NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_Dim_Person] PRIMARY KEY CLUSTERED 
(
	[PersonID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_Product]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_Product]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_Product](
	[ProductID] [int] IDENTITY(1,1) NOT NULL,
	[ProductName] [varchar](255) NULL,
	[ProductCategory] [varchar](255) NULL,
	[ProductSize] [varchar](50) NULL,
	[ProductValue] [numeric](18, 2) NULL,
	[ProductWeight] [numeric](18, 2) NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_Product] PRIMARY KEY CLUSTERED 
(
	[ProductID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[dim_Status]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[dim_Status]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[dim_Status](
	[StatusID] [int] IDENTITY(1,1) NOT NULL,
	[StatusName] [varchar](255) NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_dim_Status] PRIMARY KEY CLUSTERED 
(
	[StatusID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[fact_Invoices]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[fact_Invoices]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[fact_Invoices](
	[InvoiceID] [int] IDENTITY(1,1) NOT NULL,
	[InvoiceNo] [varchar](50) NOT NULL,
	[InvoiceDateKey] [int] NULL,
	[OrderNo] [varchar](50) NULL,
	[InvoiceAmount] [numeric](18, 2) NULL,
	[TaxAmount] [numeric](18, 2) NULL,
	[TaxRate] [numeric](18, 2) NULL,
	[IsPaid] [bit] NULL,
	[CustomerID] [int] NULL,
	[CreatedByID] [int] NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_fact_Invoices] PRIMARY KEY CLUSTERED 
(
	[InvoiceID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[fact_Orders]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[fact_Orders]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[fact_Orders](
	[OrderID] [int] IDENTITY(1,1) NOT NULL,
	[OrderNo] [varchar](50) NOT NULL,
	[OrderDateKey] [int] NULL,
	[OrderWeight] [numeric](18, 2) NULL,
	[OrderValue] [numeric](18, 2) NULL,
	[ProductID] [int] NULL,
	[SenderID] [int] NULL,
	[SenderCityID] [int] NULL,
	[ReceiverCityID] [int] NULL,
	[ReceiverID] [int] NULL,
	[BillerTypeID] [int] NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_fact_Orders] PRIMARY KEY CLUSTERED 
(
	[OrderID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[fact_Shipments]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[fact_Shipments]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[fact_Shipments](
	[ShipmentDetailID] [int] IDENTITY(1,1) NOT NULL,
	[ShipmentNo] [varchar](50) NOT NULL,
	[OrderNo] [varchar](50) NULL,
	[ShipmentDateKey] [int] NULL,
	[CarrierID] [int] NULL,
	[CarrierCityID] [int] NULL,
	[VehicleNo] [varchar](50) NULL,
	[StatusID] [int] NULL,
	[IsSettled] [bit] NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_fact_Shipments] PRIMARY KEY CLUSTERED 
(
	[ShipmentDetailID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[fact_TransactionLineItems]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[fact_TransactionLineItems]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[fact_TransactionLineItems](
	[TransactionLineItemID] [int] NOT NULL,
	[TransactionNo] [varchar](50) NULL,
	[TransactionDateKey] [int] NULL,
	[PaymentTypeID] [int] NULL,
	[BankID] [int] NULL,
	[CustomerID] [int] NULL,
	[TransactionAmount] [numeric](18, 2) NULL,
	[CreatedByID] [int] NULL,
	[Ingestion_Date] [datetime2](7) NULL,
 CONSTRAINT [PK_fact_TransactionLineItems] PRIMARY KEY CLUSTERED 
(
	[TransactionLineItemID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [dw].[watermark]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[watermark]') AND type in (N'U'))
BEGIN
CREATE TABLE [dw].[watermark](
	[Watermarkid] [int] IDENTITY(1,1) NOT NULL,
	[TableName] [varchar](50) NULL,
	[WatermarkValue] [datetime2](7) NULL,
PRIMARY KEY CLUSTERED 
(
	[Watermarkid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
/****** Object:  Table [ship].[Invoices]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ship].[Invoices]') AND type in (N'U'))
BEGIN
CREATE TABLE [ship].[Invoices](
	[InvNo] [varchar](50) NULL,
	[InvDate] [datetime2](7) NULL,
	[Ord] [varchar](50) NULL,
	[Amount] [numeric](18, 2) NULL,
	[TaxRate] [numeric](18, 2) NULL,
	[TaxAmount] [numeric](18, 2) NULL,
	[PaymentDays] [int] NULL,
	[GeneratedBy] [varchar](50) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [ship].[Orders]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ship].[Orders]') AND type in (N'U'))
BEGIN
CREATE TABLE [ship].[Orders](
	[ord] [varchar](50) NULL,
	[Odate] [datetime2](7) NULL,
	[wt] [numeric](18, 2) NULL,
	[OrderValue] [numeric](18, 2) NULL,
	[ProductCategory] [varchar](255) NULL,
	[ProductName] [varchar](255) NULL,
	[ProductSize] [int] NULL,
	[ProductValue] [numeric](18, 2) NULL,
	[ProductWeight] [numeric](18, 2) NULL,
	[NoOfItems] [int] NULL,
	[SenderName] [varchar](255) NULL,
	[SenderAddress] [varchar](255) NULL,
	[SenderCity] [varchar](255) NULL,
	[SenderState] [varchar](255) NULL,
	[SenderPhoneNo] [varchar](100) NULL,
	[ReceiverName] [varchar](255) NULL,
	[ReceiverAddress] [varchar](255) NULL,
	[ReceiverCity] [varchar](255) NULL,
	[ReceiverState] [varchar](255) NULL,
	[ReceiverPhoneNo] [varchar](100) NULL,
	[EstimatedCost] [numeric](18, 2) NULL,
	[BillToSender] [varchar](5) NULL,
	[BillToReceiver] [varchar](5) NULL,
	[BillToOther] [varchar](5) NULL,
	[BillerName] [varchar](255) NULL,
	[BillerAddress] [varchar](255) NULL,
	[BillerCity] [varchar](255) NULL,
	[BillerState] [varchar](255) NULL,
	[BillerPhoneNo] [varchar](100) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [ship].[ShipmentDetails]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ship].[ShipmentDetails]') AND type in (N'U'))
BEGIN
CREATE TABLE [ship].[ShipmentDetails](
	[ShipmentNo] [varchar](50) NULL,
	[ord] [varchar](50) NULL,
	[ShipmentDate] [datetime2](7) NULL,
	[ShipmentCost] [numeric](18, 2) NULL,
	[VehicleNo] [varchar](50) NULL,
	[CarrierName] [varchar](255) NULL,
	[CarrierPhoneNo] [varchar](50) NULL,
	[CarrierCity] [varchar](50) NULL,
	[CarrierState] [varchar](100) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [ship].[ShipmentLineItems]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ship].[ShipmentLineItems]') AND type in (N'U'))
BEGIN
CREATE TABLE [ship].[ShipmentLineItems](
	[ShipmentLineItemID] [int] IDENTITY(1,1) NOT NULL,
	[ShipmentNo] [varchar](50) NULL,
	[ShipmentStatus] [varchar](50) NULL,
	[CreatedOn] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [ship].[TransactionLineItems]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ship].[TransactionLineItems]') AND type in (N'U'))
BEGIN
CREATE TABLE [ship].[TransactionLineItems](
	[TransactionLineItemID] [int] IDENTITY(1,1) NOT NULL,
	[TrNo] [varchar](50) NULL,
	[TrDate] [datetime2](7) NULL,
	[TrAmount] [numeric](18, 2) NULL,
	[Cheque] [varchar](50) NULL,
	[RefNo] [varchar](50) NULL,
	[PaymentType] [varchar](50) NULL,
	[Party] [varchar](100) NULL,
	[BankName] [varchar](50) NULL,
	[Details] [varchar](255) NULL,
	[GeneratedBy] [varchar](50) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[Bank]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[Bank]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[Bank](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[BankName] [varchar](255) NOT NULL,
	[DataRealisationDate] [datetime2](7) NULL,
	[Ingestion_Date] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[Carriers]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[Carriers]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[Carriers](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[CarrierName] [varchar](255) NOT NULL,
	[PhoneNumber] [varchar](50) NULL,
	[City] [varchar](100) NULL,
	[State] [varchar](100) NULL,
	[VehicleNo] [varchar](50) NULL,
	[DataRealisationDate] [datetime2](7) NULL,
	[Ingestion_Date] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[Customer]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[Customer]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[Customer](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[CustomerName] [varchar](255) NOT NULL,
	[PhoneNumber] [varchar](50) NULL,
	[AddressLine1] [varchar](255) NULL,
	[AddressLine2] [varchar](255) NULL,
	[City] [varchar](100) NULL,
	[State] [varchar](100) NULL,
	[DataRealisationDate] [datetime2](7) NULL,
	[Ingestion_Date] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[Entities]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[Entities]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[Entities](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[EntityName] [varchar](255) NOT NULL,
	[PhoneNumber] [varchar](50) NULL,
	[AddressLine1] [varchar](255) NULL,
	[AddressLine2] [varchar](255) NULL,
	[City] [varchar](100) NULL,
	[State] [varchar](100) NULL,
	[DataRealisationDate] [datetime2](7) NULL,
	[Ingestion_Date] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[Invoices]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[Invoices]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[Invoices](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[InvoiceNo] [varchar](50) NULL,
	[InvoiceDate] [datetime2](7) NULL,
	[OrderNo] [varchar](50) NULL,
	[InvoiceAmount] [numeric](18, 2) NULL,
	[TaxRate] [numeric](18, 2) NULL,
	[TaxAmount] [numeric](18, 2) NULL,
	[PaymentDays] [int] NULL,
	[GeneratedBy] [varchar](50) NULL,
	[Ingestion_Date] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[Orders]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[Orders]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[Orders](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[OrderNo] [varchar](50) NULL,
	[OrderDate] [datetime2](7) NULL,
	[OrderWeight] [numeric](18, 2) NULL,
	[OrderValue] [numeric](18, 2) NULL,
	[ProductCategory] [varchar](255) NULL,
	[ProductName] [varchar](255) NULL,
	[ProductSize] [int] NULL,
	[ProductValue] [numeric](18, 2) NULL,
	[ProductWeight] [numeric](18, 2) NULL,
	[NoOfItems] [int] NULL,
	[SenderName] [varchar](255) NULL,
	[SenderAddress] [varchar](255) NULL,
	[SenderCity] [varchar](255) NULL,
	[SenderState] [varchar](255) NULL,
	[SenderPhoneNo] [varchar](100) NULL,
	[ReceiverName] [varchar](255) NULL,
	[ReceiverAddress] [varchar](255) NULL,
	[ReceiverCity] [varchar](255) NULL,
	[ReceiverState] [varchar](255) NULL,
	[ReceiverPhoneNo] [varchar](100) NULL,
	[EstimatedCost] [numeric](18, 2) NULL,
	[BillToSender] [varchar](5) NULL,
	[BillToReceiver] [varchar](5) NULL,
	[BillToOther] [varchar](5) NULL,
	[BillerName] [varchar](255) NULL,
	[BillerAddress] [varchar](255) NULL,
	[BillerCity] [varchar](255) NULL,
	[BillerState] [varchar](255) NULL,
	[BillerPhoneNo] [varchar](100) NULL,
	[Ingestion_Date] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[Person]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[Person]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[Person](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[PersonName] [varchar](255) NOT NULL,
	[DataRealisationDate] [datetime2](7) NULL,
	[Ingestion_Date] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[Products]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[Products]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[Products](
	[ProductID] [bigint] NULL,
	[Name] [nvarchar](500) NOT NULL,
	[ProductNumber] [nvarchar](500) NOT NULL,
	[ListPrice] [float] NOT NULL,
	[Size] [nvarchar](50) NOT NULL,
	[Weight] [nvarchar](50) NOT NULL,
	[ProductCategory] [varchar](100) NULL,
	[DataRealisationDate] [datetime2](7) NULL,
	[Ingestion_Date] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[ShipmentDetails]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[ShipmentDetails]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[ShipmentDetails](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[ShipmentNo] [varchar](50) NULL,
	[OrderNo] [varchar](50) NULL,
	[ShipmentDate] [datetime2](7) NULL,
	[ShipmentCost] [numeric](18, 2) NULL,
	[VehicleNo] [varchar](50) NULL,
	[CarrierName] [varchar](255) NULL,
	[CarrierPhoneNo] [varchar](50) NULL,
	[CarrierCity] [varchar](50) NULL,
	[CarrierState] [varchar](100) NULL,
	[Ingestion_Date] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[ShipmentLineItems]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[ShipmentLineItems]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[ShipmentLineItems](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[ShipmentLineItemID] [int] NULL,
	[ShipmentNo] [varchar](255) NOT NULL,
	[ShipmentStatus] [varchar](50) NULL,
	[CreatedOn] [datetime2](7) NULL,
	[Ingestion_Date] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[TransactionLineItems]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[TransactionLineItems]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[TransactionLineItems](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[TransactionLineItemID] [int] NOT NULL,
	[TransactionNo] [varchar](50) NULL,
	[TransactionDate] [datetime2](7) NULL,
	[TransactionAmount] [numeric](18, 2) NULL,
	[ChequeNo] [varchar](50) NULL,
	[ReferenceNo] [varchar](50) NULL,
	[PaymentType] [varchar](50) NULL,
	[Party] [varchar](100) NULL,
	[BankName] [varchar](50) NULL,
	[Details] [varchar](255) NULL,
	[GeneratedBy] [varchar](50) NULL,
	[Ingestion_Date] [datetime2](7) NULL
) ON [PRIMARY]
END
GO
/****** Object:  Table [stg].[watermark]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[stg].[watermark]') AND type in (N'U'))
BEGIN
CREATE TABLE [stg].[watermark](
	[Watermarkid] [int] IDENTITY(1,1) NOT NULL,
	[TableName] [varchar](50) NULL,
	[WatermarkValue] [datetime2](7) NULL,
PRIMARY KEY CLUSTERED 
(
	[Watermarkid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_dim_Address_IngestionDate]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_Address] ADD  CONSTRAINT [DF_dim_Address_IngestionDate]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_dim_DimBank_IngestionDate]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_Bank] ADD  CONSTRAINT [DF_dim_DimBank_IngestionDate]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_DimBillerType_IngestionDate]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_BillerType] ADD  CONSTRAINT [DF_DimBillerType_IngestionDate]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_dim_Carriers_IngestionDate]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_Carriers] ADD  CONSTRAINT [DF_dim_Carriers_IngestionDate]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_dim_City_IngestionDate]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_City] ADD  CONSTRAINT [DF_dim_City_IngestionDate]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_dim_Customer_IngestionDate]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_Customer] ADD  CONSTRAINT [DF_dim_Customer_IngestionDate]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_dim_date_Ingestion_Date]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_date] ADD  CONSTRAINT [DF_dim_date_Ingestion_Date]  DEFAULT (sysdatetime()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_dim_DimBank_Ingestion_Date]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_DimBank] ADD  CONSTRAINT [DF_dim_DimBank_Ingestion_Date]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_dim_Entity_IngestionDate]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_Entity] ADD  CONSTRAINT [DF_dim_Entity_IngestionDate]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_dim_PaymentTypes_IngestionDate]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_PaymentTypes] ADD  CONSTRAINT [DF_dim_PaymentTypes_IngestionDate]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_DimPerson_IngestionDate]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_Person] ADD  CONSTRAINT [DF_DimPerson_IngestionDate]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_dim_Product_IngestionDate]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_Product] ADD  CONSTRAINT [DF_dim_Product_IngestionDate]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[DF_dim_Status_IngestionDate]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[dim_Status] ADD  CONSTRAINT [DF_dim_Status_IngestionDate]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[df_fact_Invoices_Ingestion_Date]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[fact_Invoices] ADD  CONSTRAINT [df_fact_Invoices_Ingestion_Date]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[df_fact_orders_Ingestion_Date]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[fact_Orders] ADD  CONSTRAINT [df_fact_orders_Ingestion_Date]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[df_fact_Shipments_Ingestion_Date]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[fact_Shipments] ADD  CONSTRAINT [df_fact_Shipments_Ingestion_Date]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dw].[df_fact_TransactionLineItems_Ingestion_Date]') AND type = 'D')
BEGIN
ALTER TABLE [dw].[fact_TransactionLineItems] ADD  CONSTRAINT [df_fact_TransactionLineItems_Ingestion_Date]  DEFAULT (getdate()) FOR [Ingestion_Date]
END
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_dim_Carriers_dim_Address]') AND parent_object_id = OBJECT_ID(N'[dw].[dim_Carriers]'))
ALTER TABLE [dw].[dim_Carriers]  WITH CHECK ADD  CONSTRAINT [FK_dim_Carriers_dim_Address] FOREIGN KEY([AddressID])
REFERENCES [dw].[dim_Address] ([AddressID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_dim_Carriers_dim_Address]') AND parent_object_id = OBJECT_ID(N'[dw].[dim_Carriers]'))
ALTER TABLE [dw].[dim_Carriers] CHECK CONSTRAINT [FK_dim_Carriers_dim_Address]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_dim_Customer_dim_Address]') AND parent_object_id = OBJECT_ID(N'[dw].[dim_Customer]'))
ALTER TABLE [dw].[dim_Customer]  WITH CHECK ADD  CONSTRAINT [FK_dim_Customer_dim_Address] FOREIGN KEY([AddressID])
REFERENCES [dw].[dim_Address] ([AddressID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_dim_Customer_dim_Address]') AND parent_object_id = OBJECT_ID(N'[dw].[dim_Customer]'))
ALTER TABLE [dw].[dim_Customer] CHECK CONSTRAINT [FK_dim_Customer_dim_Address]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_dim_Entity_dim_Address]') AND parent_object_id = OBJECT_ID(N'[dw].[dim_Entity]'))
ALTER TABLE [dw].[dim_Entity]  WITH CHECK ADD  CONSTRAINT [FK_dim_Entity_dim_Address] FOREIGN KEY([AddressID])
REFERENCES [dw].[dim_Address] ([AddressID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_dim_Entity_dim_Address]') AND parent_object_id = OBJECT_ID(N'[dw].[dim_Entity]'))
ALTER TABLE [dw].[dim_Entity] CHECK CONSTRAINT [FK_dim_Entity_dim_Address]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Invoices_dim_Person]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Invoices]'))
ALTER TABLE [dw].[fact_Invoices]  WITH CHECK ADD  CONSTRAINT [FK_fact_Invoices_dim_Person] FOREIGN KEY([CreatedByID])
REFERENCES [dw].[dim_Person] ([PersonID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Invoices_dim_Person]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Invoices]'))
ALTER TABLE [dw].[fact_Invoices] CHECK CONSTRAINT [FK_fact_Invoices_dim_Person]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_BillerType]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders]  WITH CHECK ADD  CONSTRAINT [FK_fact_Orders_dim_BillerType] FOREIGN KEY([BillerTypeID])
REFERENCES [dw].[dim_BillerType] ([BillerTypeID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_BillerType]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders] CHECK CONSTRAINT [FK_fact_Orders_dim_BillerType]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_City]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders]  WITH CHECK ADD  CONSTRAINT [FK_fact_Orders_dim_City] FOREIGN KEY([ReceiverCityID])
REFERENCES [dw].[dim_City] ([CityID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_City]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders] CHECK CONSTRAINT [FK_fact_Orders_dim_City]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_City1]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders]  WITH CHECK ADD  CONSTRAINT [FK_fact_Orders_dim_City1] FOREIGN KEY([SenderCityID])
REFERENCES [dw].[dim_City] ([CityID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_City1]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders] CHECK CONSTRAINT [FK_fact_Orders_dim_City1]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_City2]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders]  WITH CHECK ADD  CONSTRAINT [FK_fact_Orders_dim_City2] FOREIGN KEY([SenderCityID])
REFERENCES [dw].[dim_City] ([CityID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_City2]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders] CHECK CONSTRAINT [FK_fact_Orders_dim_City2]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_Entity]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders]  WITH CHECK ADD  CONSTRAINT [FK_fact_Orders_dim_Entity] FOREIGN KEY([ReceiverID])
REFERENCES [dw].[dim_Entity] ([EntityID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_Entity]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders] CHECK CONSTRAINT [FK_fact_Orders_dim_Entity]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_Entity1]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders]  WITH CHECK ADD  CONSTRAINT [FK_fact_Orders_dim_Entity1] FOREIGN KEY([SenderID])
REFERENCES [dw].[dim_Entity] ([EntityID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_Entity1]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders] CHECK CONSTRAINT [FK_fact_Orders_dim_Entity1]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_Product]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders]  WITH CHECK ADD  CONSTRAINT [FK_fact_Orders_dim_Product] FOREIGN KEY([ProductID])
REFERENCES [dw].[dim_Product] ([ProductID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Orders_dim_Product]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Orders]'))
ALTER TABLE [dw].[fact_Orders] CHECK CONSTRAINT [FK_fact_Orders_dim_Product]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Shipments_dim_City]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Shipments]'))
ALTER TABLE [dw].[fact_Shipments]  WITH CHECK ADD  CONSTRAINT [FK_fact_Shipments_dim_City] FOREIGN KEY([CarrierCityID])
REFERENCES [dw].[dim_City] ([CityID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Shipments_dim_City]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Shipments]'))
ALTER TABLE [dw].[fact_Shipments] CHECK CONSTRAINT [FK_fact_Shipments_dim_City]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Shipments_dim_Status]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Shipments]'))
ALTER TABLE [dw].[fact_Shipments]  WITH CHECK ADD  CONSTRAINT [FK_fact_Shipments_dim_Status] FOREIGN KEY([StatusID])
REFERENCES [dw].[dim_Status] ([StatusID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_Shipments_dim_Status]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_Shipments]'))
ALTER TABLE [dw].[fact_Shipments] CHECK CONSTRAINT [FK_fact_Shipments_dim_Status]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_TransactionLineItems_dim_Customer]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_TransactionLineItems]'))
ALTER TABLE [dw].[fact_TransactionLineItems]  WITH CHECK ADD  CONSTRAINT [FK_fact_TransactionLineItems_dim_Customer] FOREIGN KEY([CustomerID])
REFERENCES [dw].[dim_Customer] ([CustomerID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_TransactionLineItems_dim_Customer]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_TransactionLineItems]'))
ALTER TABLE [dw].[fact_TransactionLineItems] CHECK CONSTRAINT [FK_fact_TransactionLineItems_dim_Customer]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_TransactionLineItems_dim_DimBank]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_TransactionLineItems]'))
ALTER TABLE [dw].[fact_TransactionLineItems]  WITH CHECK ADD  CONSTRAINT [FK_fact_TransactionLineItems_dim_DimBank] FOREIGN KEY([BankID])
REFERENCES [dw].[dim_DimBank] ([BankID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_TransactionLineItems_dim_DimBank]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_TransactionLineItems]'))
ALTER TABLE [dw].[fact_TransactionLineItems] CHECK CONSTRAINT [FK_fact_TransactionLineItems_dim_DimBank]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_TransactionLineItems_dim_PaymentTypes]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_TransactionLineItems]'))
ALTER TABLE [dw].[fact_TransactionLineItems]  WITH CHECK ADD  CONSTRAINT [FK_fact_TransactionLineItems_dim_PaymentTypes] FOREIGN KEY([PaymentTypeID])
REFERENCES [dw].[dim_PaymentTypes] ([PaymentTypeID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_TransactionLineItems_dim_PaymentTypes]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_TransactionLineItems]'))
ALTER TABLE [dw].[fact_TransactionLineItems] CHECK CONSTRAINT [FK_fact_TransactionLineItems_dim_PaymentTypes]
GO
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_TransactionLineItems_dim_Person]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_TransactionLineItems]'))
ALTER TABLE [dw].[fact_TransactionLineItems]  WITH CHECK ADD  CONSTRAINT [FK_fact_TransactionLineItems_dim_Person] FOREIGN KEY([CreatedByID])
REFERENCES [dw].[dim_Person] ([PersonID])
GO
IF  EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dw].[FK_fact_TransactionLineItems_dim_Person]') AND parent_object_id = OBJECT_ID(N'[dw].[fact_TransactionLineItems]'))
ALTER TABLE [dw].[fact_TransactionLineItems] CHECK CONSTRAINT [FK_fact_TransactionLineItems_dim_Person]
GO
/****** Object:  StoredProcedure [dbo].[get_pipelinelog]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create procedure [dbo].[get_pipelinelog](@batchid int,@PipelineName varchar(50))
as
begin 

declare @pipelinelogid int

Insert Into pipelinelog(BatchID,PipelineName,ExecutionStatus,CreatedOn)
Values(@batchid,@PipelineName,NULL,getdate())

set @pipelinelogid = SCOPE_IDENTITY()

Select @pipelinelogid as PipelineLogID
End
GO
/****** Object:  StoredProcedure [dbo].[update_pipelinelog]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


Create Procedure [dbo].[update_pipelinelog] (@pipelinelogid int, @ExecutionStatus varchar(50))
as
Begin

Update pipelinelog
set ExecutionStatus = @ExecutionStatus,UpdatedOn = getdate()
where id = @pipelinelogid

End
GO
/****** Object:  StoredProcedure [dbo].[update_watermark]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [dbo].[update_watermark] @TableName varchar(50)
as
Begin

If @TableName = 'stg.orders'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max([OrderDate]) from stg.Orders)
where TableName = 'stg.orders'
end
else If @TableName = 'stg.Bank'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max([DataRealisationDate]) from stg.Bank)
where TableName = 'stg.Bank'

end
else If @TableName = 'stg.Carriers'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max(DataRealisationDate) from stg.Carriers)
where TableName = 'stg.Carriers'

end
else If @TableName = 'stg.Customer'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max(DataRealisationDate) from stg.Customer)
where TableName = 'stg.Customer'


end
else If @TableName = 'stg.Entities'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max(DataRealisationDate) from stg.Entities)
where TableName = 'stg.Entities'

end
else If @TableName = 'stg.Invoices'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max(InvoiceDate) from stg.Invoices)
where TableName = 'stg.Invoices'

end
else If @TableName = 'stg.Person'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max(DataRealisationDate) from stg.Person)
where TableName = 'stg.Person'


end
else If @TableName = 'stg.Products'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max(DataRealisationDate) from stg.Products)
where TableName = 'stg.Products'


end
else If @TableName = 'stg.shipmentdetails'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max(ShipmentDate) from stg.ShipmentDetails)
where TableName = 'stg.shipmentdetails'


end
else If @TableName = 'stg.ShipmentLineItems'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max(CreatedOn) from stg.ShipmentLineItems)
where TableName = 'stg.ShipmentLineItems'


end
else If @TableName = 'stg.TransactionLineItems'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max(TransactionDate) from stg.TransactionLineItems)
where TableName = 'stg.TransactionLineItems'

end
End
GO
/****** Object:  StoredProcedure [dw].[Load_Dim_Address]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
  Create Procedure [dw].[Load_Dim_Address]
  as
  Begin

  Merge dw.Dim_Address as Target
  using (Select AddressLine1, AddressLine2, City, [State]
		 from stg.Customer

		 Union
		 Select AddressLine1, AddressLine2, City, [State]
		 from stg.Entities
		 Union
		 Select 'NA', 'NA', City, [State]
		 from stg.Carriers) as Source
On Target.AddressLine1 = Source.AddressLine1 and Target.AddressLine2 = Source.AddressLine2 
and Target.City = Source.City and Target.[State] = Source.[State]

When not Matched then Insert([AddressLine1]
      ,[AddressLine2]
      ,[City]
      ,[State])
Values(Source.AddressLine1, Source.AddressLine2, Source.City, Source.[State]);

End
GO
/****** Object:  StoredProcedure [dw].[Load_Dim_Bank]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

  Create Procedure [dw].[Load_Dim_Bank]
  as
  Begin

  Merge dw.dim_DimBank as Target
  using (Select Distinct BankName
		 from stg.Bank c
		 
		 ) as Source
On Target.BankName = Source.BankName 
When not Matched then Insert(BankName)
Values(Source.BankName);

End
GO
/****** Object:  StoredProcedure [dw].[Load_Dim_Carriers]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
  Create Procedure [dw].[Load_Dim_Carriers]
  as
  Begin

  Merge dw.Dim_Carriers as Target
  using (Select Distinct CarrierName,PhoneNumber,AddressID,VehicleNo
		 From stg.Carriers C
		 Inner join (Select City,[State],max(AddressID) AddressID
					 from dw.dim_address 
					 Group by City,[State])xx on c.City = xx.City and C.[State] = xx.[State]) as Source

On Target.CarrierName = Source.CarrierName and Target.PhoneNumber = Source.PhoneNumber 
and Target.VehicleNo = Source.VehicleNo
When Matched Then
Update 
Set AddressID = Source.AddressID

When not Matched then Insert(CarrierName,PhoneNumber,AddressID,VehicleNo)
Values(Source.CarrierName, Source.PhoneNumber, Source.AddressID, Source.VehicleNo);

End
GO
/****** Object:  StoredProcedure [dw].[Load_Dim_Customer]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

  Create Procedure [dw].[Load_Dim_Customer]
  as
  Begin

  Merge dw.Dim_Customer as Target
  using (Select distinct CustomerName,PhoneNumber,AddressID
		 from stg.Customer c
		 Inner join dw.dim_Address a on C.AddressLine1 = a.AddressLine1 and c.AddressLine2 = a.AddressLine2
					and c.City = a.City and c.[State] = a.[State]
		 ) as Source
On Target.CustomerName = Source.CustomerName and Target.PhoneNumber = Source.PhoneNumber and Target.AddressID = Source.AddressID

When not Matched then Insert(CustomerName,PhoneNumber,AddressID)
Values(Source.CustomerName, Source.PhoneNumber, Source.AddressID);

End
GO
/****** Object:  StoredProcedure [dw].[load_dim_Date]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [dw].[load_dim_Date]
as
Begin
Declare @date Datetime = '2024-01-01'

while @Date <'2026-01-01'
Begin
Insert Into dw.dim_date(DateKey,Date,Year,Month,Day)
Select convert(int,replace(Convert(Varchar(10),Convert(Date,@Date)),'-','')),Convert(Date,@Date),Year(@Date),Month(@Date),Day(@Date)
Set @Date = dateadd(dd,1,@Date)


End
End
GO
/****** Object:  StoredProcedure [dw].[Load_Dim_Entity]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

  Create Procedure [dw].[Load_Dim_Entity]
  as
  Begin

  Merge dw.Dim_Entity as Target
  using (Select Distinct EntityName,PhoneNumber,AddressID
		 from stg.Entities c
		 Inner join dw.dim_Address a on C.AddressLine1 = a.AddressLine1 and c.AddressLine2 = a.AddressLine2
					and c.City = a.City and c.[State] = a.[State]
		 ) as Source
On Target.EntityName = Source.EntityName and Target.PhoneNumber = Source.PhoneNumber and Target.AddressID = Source.AddressID

When not Matched then Insert(EntityName,PhoneNumber,AddressID)
Values(Source.EntityName, Source.PhoneNumber, Source.AddressID);

End
GO
/****** Object:  StoredProcedure [dw].[Load_Dim_Person]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

  Create Procedure [dw].[Load_Dim_Person]
  as
  Begin

  Merge dw.Dim_Person as Target
  using (Select Distinct PersonName
		 from stg.Person
		 ) as Source
On Target.PersonName = Source.PersonName 
When not Matched then Insert(PersonName)
Values(Source.PersonName);

End
GO
/****** Object:  StoredProcedure [dw].[Load_dim_Product]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [dw].[Load_dim_Product]
  as
  Begin

  Merge dw.dim_Product as Target
  using (Select Distinct  [Name] as ProductName, [ProductCategory],[Size] as ProductSize, [ListPrice] as ProductValue
  , [Weight] as ProductWeight
		 from stg.Products
		 ) as Source
On Target.ProductName = Source.ProductName and Target.[ProductCategory] = Source.[ProductCategory] 
and Target.ProductSize = Source.ProductSize and Target.ProductValue = Source.ProductValue
and Target.ProductWeight = Source.ProductWeight

When not Matched then Insert([ProductName], [ProductCategory], [ProductSize], [ProductValue], [ProductWeight])
Values(Source.[ProductName], Source.[ProductCategory], Source.[ProductSize], Source.[ProductValue], Source.[ProductWeight]);

End
GO
/****** Object:  StoredProcedure [dw].[load_fact_Invoices]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create Procedure [dw].[load_fact_Invoices]
as
Begin

Insert Into dw.[fact_Invoices](
[InvoiceNo]
      ,[InvoiceDateKey]
      ,[OrderNo]
      ,[InvoiceAmount]
      ,[TaxAmount]
      ,[TaxRate]
      ,[IsPaid]
      ,[CustomerID]
      ,[CreatedByID]
      ,[Ingestion_Date]
	  )
Select Distinct
InvoiceNo,
DateKey as InvoiceDateKey,
inv.OrderNo,
InvoiceAmount,
TaxAmount,
TaxRate,
Case When tli.TransactionLineItemID is not null then 1 else 0 end as IsPaid,
cust.CustomerID,
PersonID as CreatedByID,
sysdatetime() as Ingestion_Date
From stg.Invoices inv
Inner join dw.dim_date dimdate on dimdate.[Date] = convert(Date,InvoiceDate)
left join stg.transactionLineitems tli on inv.InvoiceNo = tli.ReferenceNo and PaymentType = 'InvoiceBalancePayment'
left join stg.Orders ord on inv.OrderNo = ord.OrderNo
Left join dw.dim_customer cust on 
Case when BillToSender = 'Y' and BillToReceiver = 'N' and BIllToOther = 'N' then SenderName
when BillToSender = 'N' and BillToReceiver = 'Y' and BIllToOther = 'N' then ReceiverName
when BillToSender = 'N' and BillToReceiver = 'N' and BIllToOther = 'Y' then BillerName else 'na' end = cust.CustomerName
left join dw.dim_Person Person on inv.GeneratedBy = Person.PersonName
Where convert(Date,InvoiceDate) >(select WatermarkValue from dw.watermark where tablename = 'dw.fact_invoices')
order by InvoiceNo
End
GO
/****** Object:  StoredProcedure [dw].[update_watermark]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create Procedure [dw].[update_watermark] @TableName varchar(50)
as
Begin

If @TableName = 'dw.fact_invoices'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max([Date]) from dw.fact_invoices i
inner join dw.dim_date d on i.InvoiceDateKey = d.DateKey)
where TableName = 'dw.fact_invoices'
end
else If @TableName = 'dw.fact_Orders'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max([Date]) from dw.fact_Orders i
inner join dw.dim_date d on i.OrderDateKey = d.DateKey)
where TableName = 'dw.fact_Orders'

end
else If @TableName = 'dw.fact_Shipments'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max([Date]) from dw.fact_Shipments i
inner join dw.dim_date d on i.ShipmentDateKey = d.DateKey)
where TableName = 'dw.fact_Shipments'

end
else If @TableName = 'dw.fact_TransactionLineItems'
begin

update stg.watermark
set watermarkvalue = (Select top 1 max([Date]) from dw.fact_TransactionLineItems i
inner join dw.dim_date d on i.TransactionDateKey = d.DateKey)
where TableName = 'dw.fact_TransactionLineItems'


end

End
GO
/****** Object:  StoredProcedure [stg].[load_bank]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create Procedure [stg].[load_bank]
as
Begin


Insert Into stg.bank([BankName], [DataRealisationDate], [Ingestion_Date])

Select Distinct BankName,TransactionDate,getdate()   
From stg.TransactionLineItems
Where TransactionDate >(Select WaterMarkValue from stg.Watermark where TableName = 'stg.Bank')

End
GO
/****** Object:  StoredProcedure [stg].[load_carriers]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [stg].[load_carriers]
as
Begin


Insert Into stg.Carriers([CarrierName], [PhoneNumber], [City], [State], [VehicleNo], [DataRealisationDate], [Ingestion_Date])

Select Distinct [CarrierName], CarrierPhoneNo, CarrierCity, CarrierState, [VehicleNo], ShipmentDate, getdate()  
From stg.ShipmentDetails
Where ShipmentDate >(Select WaterMarkValue from stg.Watermark where TableName = 'stg.Carriers')


End
GO
/****** Object:  StoredProcedure [stg].[load_customers]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [stg].[load_customers]
as
Begin


Insert Into stg.Customer([CustomerName], [PhoneNumber], [AddressLine1], [AddressLine2], [City], [State], [DataRealisationDate], [Ingestion_Date])

Select Distinct BillerName,BillerPhoneNo,BillerAddress,'',BillerCity,BillerState,OrderDate, getdate()  
From stg.Orders
Where OrderDate >(Select WaterMarkValue from stg.Watermark where TableName = 'stg.Customer')
and BIllToOther = 'Y'
Union
Select Distinct SenderName,SenderPhoneNo,SenderAddress,'',SenderCity,SenderState,OrderDate, getdate()  
From stg.Orders
Where OrderDate >(Select WaterMarkValue from stg.Watermark where TableName = 'stg.Customer')
and BillToSender = 'Y'
Union
Select Distinct ReceiverName,ReceiverPhoneNo,ReceiverAddress,'',ReceiverCity,ReceiverState,OrderDate, getdate()  
From stg.Orders
Where OrderDate >(Select WaterMarkValue from stg.Watermark where TableName = 'stg.Customer')
and BillToReceiver = 'Y'


End
GO
/****** Object:  StoredProcedure [stg].[load_entities]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [stg].[load_entities]
as
Begin


Insert Into stg.Entities([EntityName], [PhoneNumber], [AddressLine1], [AddressLine2], [City], [State], [DataRealisationDate], [Ingestion_Date])

Select Distinct SenderName,SenderPhoneNo,SenderAddress,'',SenderCity,SenderState,OrderDate, getdate()  
From stg.Orders
Where OrderDate >(Select WaterMarkValue from stg.Watermark where TableName = 'stg.Entities')

union

Select Distinct ReceiverName,ReceiverPhoneNo,ReceiverAddress,'',ReceiverCity,ReceiverState,OrderDate, getdate()  
From stg.Orders
Where OrderDate >(Select WaterMarkValue from stg.Watermark where TableName = 'stg.Entities')

End
GO
/****** Object:  StoredProcedure [stg].[load_invoices]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [stg].[load_invoices]
as
Begin


Insert Into stg.Invoices([InvoiceNo], [InvoiceDate], [OrderNo], [InvoiceAmount], [TaxRate], [TaxAmount], [PaymentDays], [GeneratedBy], [Ingestion_Date])

Select [InvNo], [InvDate], [Ord], [Amount], [TaxRate], [TaxAmount], [PaymentDays], [GeneratedBy]
, getdate() as ingestion_date
from ship.Invoices
where [InvDate] > (select watermarkvalue From stg.watermark where TableName = 'stg.Invoices')

End
GO
/****** Object:  StoredProcedure [stg].[load_orders]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [stg].[load_orders]
as
Begin


Insert Into stg.Orders([OrderNo], [OrderDate], [OrderWeight], [OrderValue], [ProductCategory], [ProductName]
, [ProductSize], [ProductValue], [ProductWeight], [NoOfItems], [SenderName], [SenderAddress], [SenderCity]
, [SenderState], [SenderPhoneNo], [ReceiverName], [ReceiverAddress], [ReceiverCity], [ReceiverState]
, [ReceiverPhoneNo], [EstimatedCost], [BillToSender], [BillToReceiver], [BillToOther], [BillerName]
, [BillerAddress], [BillerCity], [BillerState], [BillerPhoneNo], [Ingestion_Date])

Select [ord] as [OrderNo]
, convert(Datetime2(7),[Odate]) as [OrderDate]
, [wt] as [OrderWeight]
, [OrderValue]
, [ProductCategory]
, [ProductName]
, [ProductSize]
, [ProductValue]
, [ProductWeight]
, [NoOfItems]
, [SenderName]
, [SenderAddress]
, [SenderCity]
, [SenderState]
, [SenderPhoneNo]
, [ReceiverName]
, [ReceiverAddress]
, [ReceiverCity]
, [ReceiverState]
, [ReceiverPhoneNo]
, [EstimatedCost]
, [BillToSender]
, [BillToReceiver]
, [BillToOther]
, [BillerName]
, [BillerAddress]
, [BillerCity]
, [BillerState]
, [BillerPhoneNo]
, getdate() as ingestion_date
from ship.Orders
where Odate > (select watermarkvalue From stg.watermark where TableName = 'stg.orders')

End
GO
/****** Object:  StoredProcedure [stg].[load_person]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [stg].[load_person]
as
Begin


Insert Into stg.Person([PersonName], [DataRealisationDate], [Ingestion_Date])

Select Distinct GeneratedBy,InvoiceDate, getdate()  
From stg.Invoices
Where InvoiceDate >(Select WaterMarkValue from stg.Watermark where TableName = 'stg.Person')

union

Select Distinct GeneratedBy,TransactionDate, getdate()  
From stg.TransactionLineItems
Where TransactionDate >(Select WaterMarkValue from stg.Watermark where TableName = 'stg.Person')

End
GO
/****** Object:  StoredProcedure [stg].[load_products]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [stg].[load_products]
as
Begin


Insert Into stg.Products([Name], [ProductNumber], [ListPrice], [Size], [Weight], [ProductCategory], [DataRealisationDate], [Ingestion_Date])

Select Distinct ProductName,'',ProductValue,ProductSize,ProductWeight,ProductCategory,OrderDate,Getdate()  
From stg.Orders
Where OrderDate >(Select WaterMarkValue from stg.Watermark where TableName = 'stg.Products')


End
GO
/****** Object:  StoredProcedure [stg].[load_shipmentdetails]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [stg].[load_shipmentdetails]
as
Begin


Insert Into stg.ShipmentDetails([ShipmentNo], [OrderNo], [ShipmentDate], [ShipmentCost], [VehicleNo], [CarrierName], [CarrierPhoneNo], [CarrierCity], [CarrierState], [Ingestion_Date])

Select [ShipmentNo], [ord], [ShipmentDate], [ShipmentCost], [VehicleNo], [CarrierName], [CarrierPhoneNo],
[CarrierCity], [CarrierState]
, getdate() as ingestion_date
from ship.ShipmentDetails
where [ShipmentDate] > (select watermarkvalue From stg.watermark where TableName = 'stg.shipmentdetails')

End
GO
/****** Object:  StoredProcedure [stg].[load_shipmentlineitems]    Script Date: 29-08-2024 19:59:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create Procedure [stg].[load_shipmentlineitems]
as
Begin


Insert Into stg.ShipmentLineItems([ShipmentLineItemID], [ShipmentNo], [ShipmentStatus], [CreatedOn], [Ingestion_Date])

Select [ShipmentLineItemID], [ShipmentNo], [ShipmentStatus], [CreatedOn]
, getdate() as ingestion_date
from ship.ShipmentLineItems
where [CreatedOn] > (select watermarkvalue From stg.watermark where TableName = 'stg.ShipmentLineItems')

End
GO