-- @TBL_GUI_NAME: Alpha Swap Terminations
-- @TBL_GUI_NAME_SHORT: ALPHA_SWAP_TERMINATIONS
-- @TBL_NAME_AT: ALPHA_SWAP_TERMINATIONS_AT
-- @TBL_DISPLAY_ORDER: 1
CREATE TABLE "GUI_XMDM_DTCC"."ALPHA_SWAP_TERMINATIONS" (
  -- @COL_GUI_NAME: Source System Trade ID
  -- @COL_DESC: Source System Trade ID (Trade ID assigned by the ATS prior to the submission to EurexOTC Clear, e.g. Tradeweb Trade ID)
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  "SOURCE_SYS_TRADE_ID" VARCHAR2(50 BYTE) NOT NULL,
  -- @COL_GUI_NAME: USI Prefix
  -- @COL_DESC: USI Prefix (Namespace of the Unique Swap Identifier is a unique code that identifies the registered entity creating the original alpha swap)
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_FORMAT: 0
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^\d{1,10}$
  "USI_PREFIX" NUMBER(10, 0) NOT NULL,
  -- @COL_GUI_NAME: USI Value
  -- @COL_DESC: USI Value (Transaction Identifier that uniquely identifies the original alpha swap)
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:[a-zA-Z0-9]{32}
  "USI_VALUE" VARCHAR2(32 BYTE) NOT NULL,
  -- @COL_GUI_NAME: LEI SDR
  -- @COL_DESC: LEI SDR (Legal Entity Identifier of the original SDR)
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[a-zA-Z0-9]{20}
  "LEI_SDR" VARCHAR2(50 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Valid from
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_PK: Y
  -- @COL_FORMAT: dd.MM.yyyy
  -- @COL_VALID_INTERVAL_COLUMN: F
  "VALID_FROM" DATE NOT NULL,
  -- @COL_GUI_NAME: Valid to
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_FORMAT: dd.MM.yyyy
  -- @COL_VALID_INTERVAL_COLUMN: T
  "VALID_TO" DATE NOT NULL,
  "ITS" DATE DEFAULT SYSDATE,
  CONSTRAINT "PK_ALPHA_SWAP_TERMINATIONS" PRIMARY KEY ("SOURCE_SYS_TRADE_ID", "USI_PREFIX", "VALID_FROM")
) TABLESPACE XMDM_FACT;