-- @TBL_GUI_NAME: Client Member Relationship
-- @TBL_GUI_NAME_SHORT: CLI_MEM_RELATIONSHIP
-- @TBL_NAME_AT: CLI_MEM_RELATIONSHIP_AT
-- @TBL_DISPLAY_ORDER: 1
CREATE TABLE "GUI_XMDM_MBSP"."CLI_MEM_RELATIONSHIP" (
  -- @COL_GUI_NAME: MBSP ID
  -- @COL_DESC: SAP MBSP ID
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  "MBSP_ID" CHAR(5 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Date Of Admission MBSP
  -- @COL_DESC: Date of Admission of MBSP contract
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_FORMAT: dd.MM.yyyy
  "MBSP_ADMISSION_DATE" DATE NOT NULL,
  -- @COL_GUI_NAME: Date Of Cancellation MBSP
  -- @COL_DESC: Date of Cancellation of MBSP contract
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_FORMAT: dd.MM.yyyy
  "MBSP_CANCELLATION_DATE" DATE,
  -- @COL_GUI_NAME: Member BU
  -- @COL_DESC: ORS / DMA Provider Member ID
  -- @COL_DISPLAY_ORDER: 4
  "MEMBER_BU" CHAR(5 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Member User ID
  -- @COL_DESC: ORS / DMA Provider ID
  -- @COL_DISPLAY_ORDER: 5
  "MEMBER_UID" CHAR(6 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Client Name
  -- @COL_DESC: Name of Buy Side client
  -- @COL_DISPLAY_ORDER: 6
  "CLIENT_NAME" VARCHAR2(50 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Client LEI
  -- @COL_DESC: LEI of Buy Side client
  -- @COL_DISPLAY_ORDER: 7
  "CLIENT_LEI" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Client ID Code
  -- @COL_DESC: Code used in order flow to identify client
  -- @COL_DISPLAY_ORDER: 8
  "CLIENT_ID_CODE" VARCHAR2(50 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Identifier
  -- @COL_DESC: T7 Field, where the code is applied - either TextField1, 2, 3 or client ID
  -- @COL_DISPLAY_ORDER: 9
  "IDENTIFIER" VARCHAR2(50 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Date Of Admission Client
  -- @COL_DESC: Date of Admission of MBSP setup
  -- @COL_DISPLAY_ORDER: 10
  -- @COL_FORMAT: dd.MM.yyyy
  "CLIENT_ADMISSION_DATE" DATE NOT NULL,
  -- @COL_GUI_NAME: Date Of Cancellation CLient
  -- @COL_DESC: Date of Admission of MBSP setup
  -- @COL_DISPLAY_ORDER: 11
  -- @COL_FORMAT: dd.MM.yyyy
  "CLIENT_CANCELLATION_DATE" DATE,
  -- @COL_GUI_NAME: Eligiable For Rebate
  -- @COL_DESC: Internal notifier for MDS NDIU rebate coverage (Y,N)
  -- @COL_DISPLAY_ORDER: 12
  "REBATE_ELIGIABLE" CHAR(5 BYTE),
  "ITS" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "PK_CLI_MEM_RELATIONSHIP" PRIMARY KEY ("MBSP_ID", "MBSP_ADMISSION_DATE")
) TABLESPACE XMDM_FACT;