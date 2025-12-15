-- @TBL_GUI_NAME: CAA Liquidation
-- @TBL_GUI_NAME_SHORT: CAA_LIQUIDATION
-- @TBL_NAME_AT: S_STG_CAA_LIQUIDATION_AT
-- @TBL_DISPLAY_ORDER: 7
CREATE TABLE "GUI_XMDM_DMP"."S_STG_CAA_LIQUIDATION" (
  -- @COL_GUI_NAME: Default Date
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_FORMAT: yyyy-MM-dd
  "DEFAULT_DATE" DATE DEFAULT SYSDATE,
  -- @COL_GUI_NAME: Value Date
  -- @COL_DESC: The date on which the transaction is supposed to settle. It can be a future date.
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_FORMAT: yyyy-MM-dd
  "VALUE_DATE" DATE,
  -- @COL_GUI_NAME: Trade Timestamp
  -- @COL_DESC: The time at which liquidation takes place.
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_FORMAT: yyyy-MM-dd HH:mm:ss
  "TRADE_TIMESTAMP" DATE,
  -- @COL_GUI_NAME: Partner GP ID
  -- @COL_DESC: ID of the defaulting  clearer in SAP.
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_FORMAT: 0
  "PARTNER_GP_NO" NUMBER(10, 0),
  -- @COL_GUI_NAME: Partner GP name
  -- @COL_DESC: Name of the defaulting clearer in SAP.
  -- @COL_DISPLAY_ORDER: 5
  "PARTNER_GP_NAME" VARCHAR2(255 BYTE),
  -- @COL_GUI_NAME: CM ID
  -- @COL_DESC: Member ID of the defaulting clearing member.
  -- @COL_DISPLAY_ORDER: 6
  "CM_ID" VARCHAR2(5 BYTE),
  -- @COL_GUI_NAME: POOL ID
  -- @COL_DESC: Unique identifier for pool.
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_PK: Y
  "POOL_ID" VARCHAR2(15 BYTE),
  -- @COL_GUI_NAME: Counterparty Name
  -- @COL_DESC: Name of the  Counterparty the sell transaction has been executed with.
  -- @COL_DISPLAY_ORDER: 8
  "COUNTERPARTY_NAME" VARCHAR2(255 BYTE),
  -- @COL_GUI_NAME: AWV Country Code CP
  -- @COL_DESC: Foreign trade ordinance country code for the counterparty.
  -- @COL_DISPLAY_ORDER: 9
  "AWV_COUNTRY_CODE_CP" VARCHAR2(3 BYTE),
  -- @COL_GUI_NAME: CSD
  -- @COL_DESC: Central Securities Depository - Settlement location for the securities.
  -- @COL_DISPLAY_ORDER: 10
  "CSD" VARCHAR2(10 BYTE),
  -- @COL_GUI_NAME: Target CSD
  -- @COL_DESC: CSD where security is transferred to.
  -- @COL_DISPLAY_ORDER: 11
  "TARGET_CSD" VARCHAR2(3 BYTE),
  -- @COL_GUI_NAME: Target CSD Account
  -- @COL_DESC: Account of the CSD where security is transferred to.
  -- @COL_DISPLAY_ORDER: 12
  "TARGET_CSD_ACCOUNT" VARCHAR2(12 BYTE),
  -- @COL_GUI_NAME: ISIN
  -- @COL_DESC: International Securities Identification Number.
  -- @COL_DISPLAY_ORDER: 13
  -- @COL_PK: Y
  "ISIN" VARCHAR2(12 BYTE),
  -- @COL_GUI_NAME: AWV SECU COD
  -- @COL_DESC: Foreign trade ordinance country code for the security.
  -- @COL_DISPLAY_ORDER: 14
  "AWV_SECU_COD" VARCHAR2(3 BYTE),
  -- @COL_GUI_NAME: Currency
  -- @COL_DESC: Currency of the ISIN.
  -- @COL_DISPLAY_ORDER: 15
  "CURRENCY" VARCHAR2(3 BYTE),
  -- @COL_GUI_NAME: Nominal Amount
  -- @COL_DESC: Number of shares.
  -- @COL_DISPLAY_ORDER: 16
  -- @COL_FORMAT: 0
  "NOMINAL_AMOUNT" NUMBER(15, 0),
  -- @COL_GUI_NAME: Price
  -- @COL_DESC: Current price of the ISIN used for allocation.
  -- @COL_DISPLAY_ORDER: 17
  -- @COL_FORMAT: #.###
  "PRICE" NUMBER(15, 3),
  -- @COL_GUI_NAME: Actual price
  -- @COL_DESC: The price at which ISIN is liquidated.
  -- @COL_DISPLAY_ORDER: 18
  -- @COL_FORMAT: #.###
  "ACTUAL_PRICE" NUMBER(15, 3),
  -- @COL_GUI_NAME: Comments
  -- @COL_DESC: Free text field.
  -- @COL_DISPLAY_ORDER: 19
  "COMMENT_TEXT" VARCHAR2(255 BYTE),
  -- @COL_GUI_NAME: DMP Run Id
  -- @COL_DESC: Unique identifier for whole DMP Process chain. Do not modify please.
  -- @COL_DISPLAY_ORDER: 20
  -- @COL_PK: Y
  -- @COL_FORMAT: 0
  "DMP_RUN_ID" NUMBER,
  "ITS" DATE DEFAULT SYSDATE,
  -- @COL_GUI_NAME: Settlement amount
  -- @COL_DISPLAY_ORDER: 16
  -- @COL_FORMAT: #.###
  "SETTLEMENT_AMNT" NUMBER(15, 3)
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: CAA Parameter
-- @TBL_GUI_NAME_SHORT: CAA_PARAMETER
-- @TBL_NAME_AT: S_STG_CAA_PARAMETER_AT
-- @TBL_DISPLAY_ORDER: 6
CREATE TABLE "GUI_XMDM_DMP"."S_STG_CAA_PARAMETER" (
  -- @COL_GUI_NAME: Min Settlement Quantity Bonds
  -- @COL_DESC: Minimum nominal amount to split fixed income security ISINs - current value 1 million nominal value. This is like the trading unit , min quantity one can buy or sell.
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_FORMAT: 0
  "MIN_STL_QTY_BONDS" NUMBER(15, 0),
  -- @COL_GUI_NAME: Min Remaining Settlement Quantity Bonds
  -- @COL_DESC: Minimum nominal amount for remaining size of fixed income security ISINs - current value 1 million nominal value. Minimum quantity for allocation to happen.
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_FORMAT: 0
  "STL_INTERVAL_BONDS" NUMBER(15, 0),
  -- @COL_GUI_NAME: Concentration limit
  -- @COL_DESC: Maximum nominal value of single fixed income security ISINs - current value 999999999999999. Max quantity which could be allocated.
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_FORMAT: 0
  "CONCENTRATION_LIMIT" NUMBER(15, 0),
  -- @COL_GUI_NAME: Settlement Steps Bonds
  -- @COL_DESC: Minimum steps of allocated nominal value - current value 100,000 nominal value. After allocation of min quantity, allocation can happen in these steps.
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_FORMAT: 0
  "STL_STEP_BONDS" NUMBER(15, 0),
  -- @COL_GUI_NAME: Confirmed
  -- @COL_DESC: This triggers the CAA tool to start calculation. Allowed value is: Y
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:(Y)
  "CONFIRMED" CHAR(1 BYTE),
  -- @COL_GUI_NAME: Record ID
  -- @COL_DESC: Unique identifier given to the CAA parameter record in the table.
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_PK: Y
  -- @COL_FORMAT: 0
  "RECORD_ID" NUMBER,
  "ITS" DATE DEFAULT SYSDATE,
  -- @COL_GUI_NAME: DMP Run Id
  -- @COL_DESC: Unique identifier for whole DMP Process chain. Do not modify please.
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_FORMAT: 0
  "DMP_RUN_ID" NUMBER(15, 0) DEFAULT NULL
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: Recollateralization Booking
-- @TBL_GUI_NAME_SHORT: DMP_RECOLL_BOOK
-- @TBL_NAME_AT: S_STG_DMP_RECOLL_BOOK_AT
-- @TBL_DISPLAY_ORDER: 9
CREATE TABLE "GUI_XMDM_DMP"."S_STG_DMP_RECOLL_BOOK" (
  -- @COL_GUI_NAME: Business Date
  -- @COL_DESC: Date booked payment is related
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy-MM-dd
  "BUSINESS_DATE" DATE,
  -- @COL_GUI_NAME: Default Date
  -- @COL_DESC: Default Date
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy-MM-dd
  "DEFAULT_DATE" DATE,
  -- @COL_GUI_NAME: Clearer ID
  -- @COL_DESC: Clearer ID e.g. HINMA
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_PK: Y
  "CLEARER_ID" VARCHAR2(5 CHAR),
  -- @COL_GUI_NAME: Porting Member ID
  -- @COL_DESC: Porting Member ID e.g. HINMC
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_PK: Y
  "PORTING_MEMBER" VARCHAR2(5 CHAR),
  -- @COL_GUI_NAME: Netting Set ID
  -- @COL_DESC: Netting Set ID e.g. CM_1006399_PROP
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_PK: Y
  "NETTING_SET" VARCHAR2(256 CHAR),
  -- @COL_GUI_NAME: Collateral Pool ID
  -- @COL_DESC: Collateral Pool ID e.g. DBKFRXSTANDARD
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_PK: Y
  "POOL_ID" VARCHAR2(15 CHAR),
  -- @COL_GUI_NAME: Currency
  -- @COL_DESC: Currency e.g. EUR
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_PK: Y
  "CURRENCY" VARCHAR2(3 CHAR),
  -- @COL_GUI_NAME: Recollateralization Value Booked
  -- @COL_DESC: Recollateralization value booked
  -- @COL_DISPLAY_ORDER: 8
  -- @COL_PK: Y
  -- @COL_FORMAT: #.##
  "RECOLL_VALUE_BOOKED" NUMBER(15, 0),
  "ITS" DATE DEFAULT CURRENT_DATE
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: Recollateralization Registration
-- @TBL_GUI_NAME_SHORT: DMP_RECOLL_REGIST
-- @TBL_NAME_AT: S_STG_DMP_RECOLL_REGIST_AT
-- @TBL_DISPLAY_ORDER: 8
CREATE TABLE "GUI_XMDM_DMP"."S_STG_DMP_RECOLL_REGIST" (
  -- @COL_GUI_NAME: Business Date
  -- @COL_DESC: Date registered payment is related
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy-MM-dd
  "BUSINESS_DATE" DATE,
  -- @COL_GUI_NAME: Default Date
  -- @COL_DESC: Default Date
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy-MM-dd
  "DEFAULT_DATE" DATE,
  -- @COL_GUI_NAME: Clearer ID
  -- @COL_DESC: Clearer ID e.g. HINMA
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_PK: Y
  "CLEARER_ID" VARCHAR2(5 CHAR),
  -- @COL_GUI_NAME: Porting Member ID
  -- @COL_DESC: Porting Member ID e.g. HINMC
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_PK: Y
  "PORTING_MEMBER" VARCHAR2(5 CHAR),
  -- @COL_GUI_NAME: Netting Set ID
  -- @COL_DESC: Netting Set ID e.g. CM_1006399_PROP
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_PK: Y
  "NETTING_SET" VARCHAR2(256 CHAR),
  -- @COL_GUI_NAME: Collateral Pool ID
  -- @COL_DESC: Collateral Pool ID e.g. DBKFRXSTANDARD
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_PK: Y
  "POOL_ID" VARCHAR2(15 CHAR),
  -- @COL_GUI_NAME: Currency
  -- @COL_DESC: Currency e.g. EUR
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_PK: Y
  "CURRENCY" VARCHAR2(3 CHAR),
  -- @COL_GUI_NAME: Deadline
  -- @COL_DESC: Deadline by which the customer must provide money
  -- @COL_DISPLAY_ORDER: 8
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy-MM-dd HH:mm:ss
  "DEADLINE" DATE,
  -- @COL_GUI_NAME: Recollateralization Value Registered
  -- @COL_DESC: Recollateralization value registered
  -- @COL_DISPLAY_ORDER: 9
  -- @COL_PK: Y
  -- @COL_FORMAT: #.##
  "RECOLL_VALUE_REGIST" NUMBER(15, 0),
  -- @COL_GUI_NAME: ARID
  -- @COL_DESC: ARID
  -- @COL_DISPLAY_ORDER: 10
  -- @COL_PK: Y
  -- @COL_FORMAT: 0
  "ARID" NUMBER(15, 0),
  "ITS" DATE DEFAULT CURRENT_DATE
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: DMP Default Details
-- @TBL_GUI_NAME_SHORT: PAY_DEFAULT_PAY
-- @TBL_NAME_AT: S_STG_PAY_DEFAULT_DETAILS_AT
-- @TBL_DISPLAY_ORDER: 1
CREATE TABLE "GUI_XMDM_DMP"."S_STG_PAY_DEFAULT_DETAILS" (
  -- @COL_GUI_NAME: Member ID
  -- @COL_DESC: Member ID of the defaulting clearing member.
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.DbColumnValidator:DWH.D_MEMBER_OBJ.BUSINESS_ID
  "MEMBER_ID" VARCHAR2(5 CHAR),
  -- @COL_GUI_NAME: Default timestamp
  -- @COL_DESC: Date Time of CM Default as announced by EXCO.
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy-MM-dd HH:mm:ss
  "DEFAULT_TIMESTAMP" DATE,
  -- @COL_GUI_NAME: Calc date
  -- @COL_DESC: Business day for which the open payment tool is being run.
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy-MM-dd
  "CALC_DATE" DATE,
  "ITS" DATE DEFAULT SYSDATE,
  -- @COL_GUI_NAME: DMP Run Id
  -- @COL_DESC: Leave empty. Unique identifier for whole DMP Process chain.
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_FORMAT: 0
  "DMP_RUN_ID" NUMBER(15, 0) DEFAULT 0
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: OP Settlement Status
-- @TBL_GUI_NAME_SHORT: PAY_INSTC_STATUS
-- @TBL_NAME_AT: S_STG_PAY_INSTC_STATUS_AT
-- @TBL_DISPLAY_ORDER: 2
CREATE TABLE "GUI_XMDM_DMP"."S_STG_PAY_INSTC_STATUS" (
  -- @COL_GUI_NAME: Fact Date
  -- @COL_DESC: The date for which the open payment tool is being run.
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy-MM-dd
  "FACT_DATE" DATE,
  -- @COL_GUI_NAME: Instruction ID
  -- @COL_DESC: Collateral Instruction ID
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_PK: Y
  -- @COL_FORMAT: 0
  "INSTC_ID" NUMBER(15, 0),
  -- @COL_GUI_NAME: STL Status
  -- @COL_DESC: Indicates if the collateral instruction from Carmen have been settled or not ( Valid Values : Y / N )
  -- @COL_DISPLAY_ORDER: 4
  "STL_STATUS" VARCHAR2(1 BYTE),
  "ITS" DATE DEFAULT SYSDATE,
  -- @COL_GUI_NAME: DMP Run Id
  -- @COL_DESC: Unique identifier for whole DMP Process chain. Do not modify please.
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_FORMAT: 0
  "DMP_RUN_ID" NUMBER(15, 0) DEFAULT 0 NOT NULL,
  -- @COL_GUI_NAME: Clearer ID
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.DbColumnValidator:DWH.D_MEMBER_OBJ.BUSINESS_ID
  "CLEARER_ID" VARCHAR2(5 BYTE),
  -- @COL_GUI_NAME: Source
  -- @COL_DISPLAY_ORDER: 5
  "SOURCE" VARCHAR2(10 CHAR)
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: OP Transaction Manual
-- @TBL_GUI_NAME_SHORT: PAY_TRANSACTION_MANUAL
-- @TBL_NAME_AT: S_STG_PAY_TRANSACT_MANUAL_AT
-- @TBL_DISPLAY_ORDER: 4
CREATE TABLE "GUI_XMDM_DMP"."S_STG_PAY_TRANSACTION_MANUAL" (
  -- @COL_GUI_NAME: Fact Date
  -- @COL_DESC: The date for which the open payment tool is being run.
  -- @COL_DISPLAY_ORDER: 10
  -- @COL_FORMAT: yyyy-MM-dd
  "FACT_DATE" DATE,
  -- @COL_GUI_NAME: Default Timestamp
  -- @COL_DESC: Date Time of CM Default as announced by EXCO.
  -- @COL_DISPLAY_ORDER: 11
  -- @COL_FORMAT: yyyy-MM-dd HH:mm:ss
  "DEFAULT_TIMESTAMP" DATE,
  -- @COL_GUI_NAME: Default Date
  -- @COL_DESC: Business day the clearing member is declared to be in default. Same date as the Date mentioned in the default timestamp.
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_FORMAT: yyyy-MM-dd
  "DEFAULT_DATE" DATE,
  -- @COL_GUI_NAME: Transaction Date
  -- @COL_DESC: The date in which the transaction was done and successfully technically accepted.
  -- @COL_DISPLAY_ORDER: 12
  -- @COL_FORMAT: yyyy-MM-dd
  "TRANSACTION_DATE" DATE,
  -- @COL_GUI_NAME: Value Date
  -- @COL_DESC: The date on which the transaction is supposed to settle. It can be a future date.
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_FORMAT: yyyy-MM-dd
  "VALUE_DATE" DATE,
  -- @COL_GUI_NAME: Cash Settlement Run
  -- @COL_DESC: The system run in which the respective cash instruction is processed. Allowed values are: ADD1, NTP
  -- @COL_DISPLAY_ORDER: 13
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:(ADD1|NTP)
  "CASH_SETTLEMENT_RUN" VARCHAR2(4 BYTE),
  -- @COL_GUI_NAME: Clearer ID
  -- @COL_DESC: ID of the defaulting clearing member.
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.DbColumnValidator:DWH.D_MEMBER_OBJ.BUSINESS_ID
  "CLEARER_ID" VARCHAR2(5 BYTE),
  -- @COL_GUI_NAME: Account Holder
  -- @COL_DESC: ID of the trading member/RC.
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.DbColumnValidator:DWH.D_MEMBER_OBJ.BUSINESS_ID
  "ACCOUNT_HOLDER" VARCHAR2(5 BYTE),
  -- @COL_GUI_NAME: Account
  -- @COL_DESC: Member account ( A1, A2 ,  . . . or PP ) for which the transaction has been entered.
  -- @COL_DISPLAY_ORDER: 5
  "ACCOUNT" VARCHAR2(32 BYTE),
  -- @COL_GUI_NAME: Pool ID
  -- @COL_DESC: Unique identifier for pool. Not filled for transactions initiated at CCP.
  -- @COL_DISPLAY_ORDER: 6
  "POOL_ID" VARCHAR2(15 BYTE),
  -- @COL_GUI_NAME: Cash Chain ID
  -- @COL_DESC: Cash account chains are used to select the cash accounts on which the amount has to be booked.
  -- @COL_DISPLAY_ORDER: 14
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.DbColumnValidator:CCP.D_CCP_CASH_ACCOUNT_CHAIN_OBJ.CSH_ACCT_CHAIN_ID
  "CASH_CHAIN_ID" VARCHAR2(15 BYTE),
  -- @COL_GUI_NAME: Cash Transaction Group
  -- @COL_DESC: Group to which this collateral transaction is assigned. This is generally based on the transaction type and can be modified by the user.
  -- @COL_DISPLAY_ORDER: 15
  -- @COL_FORMAT: 0
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.DbColumnValidator:EUREX_DMP.D_DOM_PAY_TRANSACTION_GROUP.TRN_GROUP_NO
  "CSH_TRN_GROUP" NUMBER(15, 0),
  -- @COL_GUI_NAME: Transaction Type
  -- @COL_DESC: Type of cash transaction e.g. premium paid, variation margin paid etc.
  -- @COL_DISPLAY_ORDER: 7
  "TRANSACTION_TYPE" VARCHAR2(3 BYTE),
  -- @COL_GUI_NAME: Currency
  -- @COL_DESC: Currency of the transaction.
  -- @COL_DISPLAY_ORDER: 8
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.DbColumnValidator:DWH.D_CURRENCY.ISO_ALPHA_CODE
  "CURRENCY" VARCHAR2(3 BYTE),
  -- @COL_GUI_NAME: Cash Instruction
  -- @COL_DESC: An instruction which collects surplus cash transactions and is sent out to the cash location for settlement.
  -- @COL_DISPLAY_ORDER: 16
  -- @COL_FORMAT: 0
  "CASH_INSTRUCTION" NUMBER(15, 0),
  -- @COL_GUI_NAME: Cash Transaction
  -- @COL_DESC: Identifier of the transaction to settle cash.
  -- @COL_DISPLAY_ORDER: 17
  "CASH_TRANSACTION" VARCHAR2(15 BYTE),
  -- @COL_GUI_NAME: EXT Cash Transaction
  -- @COL_DESC: Instruction id in the source system.
  -- @COL_DISPLAY_ORDER: 18
  "EXT_CASH_TRANSACTION" VARCHAR2(17 BYTE),
  -- @COL_GUI_NAME: Cash Amount INT
  -- @COL_DESC: Value of the transaction amount.
  -- @COL_DISPLAY_ORDER: 9
  -- @COL_FORMAT: #.##
  "CASH_AMOUNT_INT" NUMBER(19, 2),
  -- @COL_GUI_NAME: Autoincrement id
  -- @COL_DESC: Technical id of the record. Field is automatically filled by GUI.
  -- @COL_DISPLAY_ORDER: 19
  -- @COL_PK: Y
  -- @COL_FORMAT: 0
  "AUTOINCREMENT_ID" NUMBER,
  "ITS" DATE DEFAULT sysdate,
  -- @COL_GUI_NAME: DMP Run Id
  -- @COL_DESC: Leave empty. Unique identifier for whole DMP Process chain.
  -- @COL_DISPLAY_ORDER: 20
  -- @COL_FORMAT: 0
  "DMP_RUN_ID" NUMBER(15, 0) DEFAULT 0
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: OP Transaction Overview
-- @TBL_GUI_NAME_SHORT: PAY_TRANSACTION_OVERVIEW
-- @TBL_NAME_AT: S_STG_PAY_TRANSACT_OVERVIEW_AT
-- @TBL_DISPLAY_ORDER: 3
CREATE TABLE "GUI_XMDM_DMP"."S_STG_PAY_TRANSACTION_OVERVIEW" (
  -- @COL_GUI_NAME: DMP Run ID
  -- @COL_DESC: DMP Run ID
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  -- @COL_FORMAT: 1759395573214
  "DMP_RUN_ID" NUMBER(*, 0),
  -- @COL_GUI_NAME: Fact Date
  -- @COL_DESC: Fact Date
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy-MM-dd
  "FACT_DATE" DATE,
  -- @COL_GUI_NAME: Default Date
  -- @COL_DESC: Default Date
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy-MM-dd
  "DEFAULT_DATE" DATE,
  -- @COL_GUI_NAME: Value Date
  -- @COL_DESC: Value Date
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy-MM-dd
  "VALUE_DATE" DATE,
  -- @COL_GUI_NAME: Transaction Date
  -- @COL_DESC: Transaction Date
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_FORMAT: yyyy-MM-dd
  "TRANSACTION_DATE" DATE,
  -- @COL_GUI_NAME: Account Sponsor
  -- @COL_DESC: Account Sponsor
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_PK: Y
  -- @COL_FORMAT: ABCFR
  "ACCOUNT_SPONSOR" VARCHAR2(5 BYTE),
  -- @COL_GUI_NAME: Account Holder
  -- @COL_DESC: Account Holder
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_PK: Y
  -- @COL_FORMAT: ABCFR
  "ACCOUNT_HOLDER" VARCHAR2(5 BYTE),
  -- @COL_GUI_NAME: Account
  -- @COL_DESC: Account
  -- @COL_DISPLAY_ORDER: 8
  -- @COL_FORMAT: PP
  "ACCOUNT" VARCHAR2(64 BYTE),
  -- @COL_GUI_NAME: Pool Id
  -- @COL_DESC: Pool ID
  -- @COL_DISPLAY_ORDER: 9
  -- @COL_FORMAT: ABCFRXSTANDARD
  "POOL_ID" VARCHAR2(15 BYTE),
  -- @COL_GUI_NAME: Porting Unit
  -- @COL_DESC: Porting unit
  -- @COL_DISPLAY_ORDER: 10
  -- @COL_FORMAT: PP
  "PORTING_UNIT" VARCHAR2(64 BYTE),
  -- @COL_GUI_NAME: Porting Member
  -- @COL_DESC: Porting member
  -- @COL_DISPLAY_ORDER: 11
  -- @COL_FORMAT: ABCFR
  "PORTING_MEMBER" VARCHAR2(5 BYTE),
  -- @COL_GUI_NAME: OP Pool Type
  -- @COL_DESC: pool type e.g. ISA
  -- @COL_DISPLAY_ORDER: 12
  -- @COL_FORMAT: 0
  "OP_POOL_TYPE" VARCHAR2(64 BYTE),
  -- @COL_GUI_NAME: Cash Chain Id
  -- @COL_DESC: Cash chain identifier
  -- @COL_DISPLAY_ORDER: 13
  -- @COL_FORMAT: 0
  "CASH_CHAIN_ID" VARCHAR2(32 BYTE),
  -- @COL_GUI_NAME: Transaction Type
  -- @COL_DESC: Transaction type
  -- @COL_DISPLAY_ORDER: 14
  -- @COL_FORMAT: 123
  "TRANSACTION_TYPE" VARCHAR2(3 BYTE),
  -- @COL_GUI_NAME: Currency
  -- @COL_DESC: Currency code e.g. EUR
  -- @COL_DISPLAY_ORDER: 15
  -- @COL_PK: Y
  -- @COL_FORMAT: ISO code
  "CURRENCY" VARCHAR2(3 BYTE),
  -- @COL_GUI_NAME: Cash Transaction
  -- @COL_DESC: Cash transaction ID
  -- @COL_DISPLAY_ORDER: 16
  -- @COL_PK: Y
  -- @COL_FORMAT: 0
  "CASH_TRANSACTION" VARCHAR2(15 BYTE),
  -- @COL_GUI_NAME: Cash Instruction
  -- @COL_DESC: Cash instruction ID
  -- @COL_DISPLAY_ORDER: 17
  -- @COL_FORMAT: 0
  "CASH_INSTRUCTION" NUMBER(15, 0),
  -- @COL_GUI_NAME: Amount
  -- @COL_DESC: Cash Transaction amount
  -- @COL_DISPLAY_ORDER: 18
  -- @COL_FORMAT: number
  "AMOUNT" NUMBER(18, 2),
  -- @COL_GUI_NAME: CRM GP No
  -- @COL_DESC: CRM GP number
  -- @COL_DISPLAY_ORDER: 19
  -- @COL_FORMAT: 0
  "CRM_GP_NO" VARCHAR2(10 BYTE),
  "ITS" DATE
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: OP Transaction Group Assignment
-- @TBL_GUI_NAME_SHORT: PAY_TRN_GROUP_OVERRULE
-- @TBL_NAME_AT: S_STG_PAY_TRN_GRP_OVERRULE_AT
-- @TBL_DISPLAY_ORDER: 5
CREATE TABLE "GUI_XMDM_DMP"."S_STG_PAY_TRN_GROUP_OVERRULE" (
  -- @COL_GUI_NAME: Fact Date
  -- @COL_DESC: The date for which the open payment tool is being run.
  -- @COL_DISPLAY_ORDER: 11
  -- @COL_FORMAT: yyyy-MM-dd
  "FACT_DATE" DATE,
  -- @COL_GUI_NAME: Default Timestamp
  -- @COL_DESC: Date Time of CM Default as announced by EXCO.
  -- @COL_DISPLAY_ORDER: 12
  -- @COL_FORMAT: yyyy-MM-dd HH:mm:ss
  "DEFAULT_TIMESTAMP" DATE,
  -- @COL_GUI_NAME: Default Date
  -- @COL_DESC: Business day the clearing member is declared to be in default. Same date as the Date mentioned in the default timestamp.
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_FORMAT: yyyy-MM-dd
  "DEFAULT_DATE" DATE,
  -- @COL_GUI_NAME: Transaction Date
  -- @COL_DESC: The date on which the transaction was transmitted and successfully technically accepted.
  -- @COL_DISPLAY_ORDER: 13
  -- @COL_FORMAT: yyyy-MM-dd
  "TRANSACTION_DATE" DATE,
  -- @COL_GUI_NAME: Value Date
  -- @COL_DESC: The date on which the transaction is supposed to settle. It can be a future date.
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_FORMAT: yyyy-MM-dd
  "VALUE_DATE" DATE,
  -- @COL_GUI_NAME: Cash Settlement Run
  -- @COL_DESC: Cash Settlement Run.
  -- @COL_DISPLAY_ORDER: 14
  "CASH_SETTLEMENT_RUN" VARCHAR2(4 BYTE),
  -- @COL_GUI_NAME: OP Relevance Ind
  -- @COL_DESC: Indicates if the transaction should be considered for open payment calculation or not. 1 - indicates that the transaction is relevant , 0 - indicates  that the transaction is irrelevant.
  -- @COL_DISPLAY_ORDER: 15
  -- @COL_FORMAT: 0
  "OP_RELEVANCE_IND" NUMBER(1, 0),
  -- @COL_GUI_NAME: CRM Partner ID
  -- @COL_DESC: Technical ID of the CRM GP number in statistix.
  -- @COL_DISPLAY_ORDER: 16
  -- @COL_FORMAT: 0
  "CRM_PARTNER_ID" NUMBER(15, 0),
  -- @COL_GUI_NAME: CRM GP NO
  -- @COL_DESC: Business partner ID number in SAP.
  -- @COL_DISPLAY_ORDER: 17
  "CRM_GP_NO" VARCHAR2(10 BYTE),
  -- @COL_GUI_NAME: Clearer ID
  -- @COL_DESC: ID of the defaulting clearing member.
  -- @COL_DISPLAY_ORDER: 5
  "CLEARER_ID" VARCHAR2(5 BYTE),
  -- @COL_GUI_NAME: Account Holder
  -- @COL_DESC: ID of the trading member/RC.
  -- @COL_DISPLAY_ORDER: 6
  "ACCOUNT_HOLDER" VARCHAR2(5 BYTE),
  -- @COL_GUI_NAME: Account
  -- @COL_DESC: Member account ( A1, A2 , . . . or PP ) for which the transaction has been entered.
  -- @COL_DISPLAY_ORDER: 18
  "ACCOUNT" VARCHAR2(32 BYTE),
  -- @COL_GUI_NAME: Pool ID
  -- @COL_DESC: Unique identifier for pool. Not filled for transactions initiated at CCP.
  -- @COL_DISPLAY_ORDER: 7
  "POOL_ID" VARCHAR2(15 BYTE),
  -- @COL_GUI_NAME: OP Pool Type
  -- @COL_DESC: Indicates the pool type.
  -- @COL_DISPLAY_ORDER: 19
  "OP_POOL_TYPE" VARCHAR2(256 CHAR),
  -- @COL_GUI_NAME: OP Client Type
  -- @COL_DESC: Type of client like  NCM, DC etc.
  -- @COL_DISPLAY_ORDER: 20
  "OP_CLIENT_TYPE" VARCHAR2(100 BYTE),
  -- @COL_GUI_NAME: Linked account
  -- @COL_DESC: Indicates the direct account to which this indirect client account is linked. E.g. A2 is an indirect client account is linked to A5 which is a direct client account then linked account field will contain A5 and account field will contain A2.
  -- @COL_DISPLAY_ORDER: 21
  "LINKEDACCOUNT" VARCHAR2(60 BYTE),
  -- @COL_GUI_NAME: Porting unit
  -- @COL_DESC: Each NCM, RC, BCM, linked accounts together and funds that are attached to the same fund manager constitute a porting unit.
  -- @COL_DISPLAY_ORDER: 22
  "PORTING_UNIT" VARCHAR2(60 BYTE),
  -- @COL_GUI_NAME: Cash Chain ID
  -- @COL_DESC: Default value EUREX. Indicates cash chain ID for the CCP related transactions.
  -- @COL_DISPLAY_ORDER: 24
  "CASH_CHAIN_ID" VARCHAR2(15 BYTE),
  -- @COL_GUI_NAME: Number of purchases per chain
  -- @COL_DESC: Number of porting unit per chain.
  -- @COL_DISPLAY_ORDER: 25
  -- @COL_FORMAT: 0
  "NBR_OF_PU_PER_CHAIN" NUMBER(15, 0),
  -- @COL_GUI_NAME: Cash Instruction
  -- @COL_DESC: Collateral Instruction ID for the cash transaction.
  -- @COL_DISPLAY_ORDER: 26
  -- @COL_FORMAT: 0
  "CASH_INSTRUCTION" NUMBER(15, 0),
  -- @COL_GUI_NAME: Settlement Flag Instc
  -- @COL_DESC: Status of the settlement instructions.
  -- @COL_DISPLAY_ORDER: 27
  "STL_FLG_INSTC" VARCHAR2(1 BYTE),
  -- @COL_GUI_NAME: Cash Transaction Group
  -- @COL_DESC: Group to which this collateral transaction is assigned. This is generally based on the transaction type and can be modified by the user. Valid Value : 1 to 10
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_FORMAT: 0
  "CSH_TRN_GROUP" NUMBER(2, 0),
  -- @COL_GUI_NAME: Cash Transaction Group Overrule
  -- @COL_DESC: By default contains same value as cash transaction group. This field can be updated to the change the group of the cash transaction.
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_FORMAT: 0
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.DbColumnValidator:EUREX_DMP.D_DOM_PAY_TRANSACTION_GROUP.TRN_GROUP_NO
  "CSH_TRN_GROUP_OVERRULE" NUMBER(2, 0),
  -- @COL_GUI_NAME: Transaction Type
  -- @COL_DESC: Type of the cash transaction.
  -- @COL_DISPLAY_ORDER: 8
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.DbColumnValidator:CCP.D_CCP_CASH_TRN_TYPE.CSH_TRN_TYP
  "TRANSACTION_TYPE" VARCHAR2(3 BYTE),
  -- @COL_GUI_NAME: Currency
  -- @COL_DESC: Currency of the transaction.
  -- @COL_DISPLAY_ORDER: 9
  "CURRENCY" VARCHAR2(3 BYTE),
  -- @COL_GUI_NAME: Cash Transaction
  -- @COL_DESC: Transaction to settle cash.
  -- @COL_DISPLAY_ORDER: 28
  "CASH_TRANSACTION" VARCHAR2(15 BYTE),
  -- @COL_GUI_NAME: EXT Cash Transaction
  -- @COL_DESC: Instruction ID in the source system.
  -- @COL_DISPLAY_ORDER: 29
  "EXT_CASH_TRANSACTION" VARCHAR2(17 BYTE),
  -- @COL_GUI_NAME: Cash Amount INT
  -- @COL_DESC: The information about the transaction amount.
  -- @COL_DISPLAY_ORDER: 10
  -- @COL_PK: Y
  -- @COL_FORMAT: #.##
  "CASH_AMOUNT_INT" NUMBER(19, 2),
  "ITS" DATE DEFAULT sysdate,
  -- @COL_GUI_NAME: Porting member
  -- @COL_DISPLAY_ORDER: 23
  "PORTING_MEMBER" VARCHAR2(5 CHAR),
  "DATA_SRC" VARCHAR2(30 BYTE),
  -- @COL_GUI_NAME: DMP Run Id
  -- @COL_DESC: Unique identifier for whole DMP Process chain. Do not modify please.
  -- @COL_DISPLAY_ORDER: 31
  -- @COL_PK: Y
  -- @COL_FORMAT: 0
  "DMP_RUN_ID" NUMBER(15, 0) DEFAULT 0 NOT NULL
) TABLESPACE XMDM_FACT;