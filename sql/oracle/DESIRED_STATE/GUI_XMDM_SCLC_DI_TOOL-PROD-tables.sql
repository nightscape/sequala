-- @TBL_GUI_NAME: XETR Final Missings
-- @TBL_GUI_NAME_SHORT: XETR_FINAL_MISSING
-- @TBL_NAME_AT: XETR_FINAL_MISSING_AT
-- @TBL_DISPLAY_ORDER: 1
CREATE TABLE "GUI_XMDM_SCLC_DI_TOOL"."XETR_FINAL_MISSING" (
  -- @COL_GUI_NAME: MIC
  -- @COL_DESC: The MIC is the four-digit operating MIC of the trading venue
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^XETR$
  "MIC" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ParticipantID
  -- @COL_DESC: The participant ID is the trading participants five-digit memberID
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[A-Z]{5}$
  "PARTICIPANT_ID" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ShortCode
  -- @COL_DESC: The short code is the numeric code that functions as a placeholder for ClientID, ExecutionID and InvestmentID
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_PK: Y
  -- @COL_FORMAT: ##
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[0-9]{1,20}$
  "SHORT_CODE" VARCHAR2(30 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Reporting date of FM from
  -- @COL_DESC: The date the from when the final missing was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_FROM" DATE NOT NULL,
  -- @COL_GUI_NAME: Reporting date of FM to
  -- @COL_DESC: The date until the final missing was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_TO" DATE NOT NULL,
  -- @COL_GUI_NAME: Entry Date
  -- @COL_DESC: This date indicates when the record is entered into the XMDM GUI (This field will not be used in the reports)
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "ENTRY_DATE" DATE NOT NULL,
  -- @COL_GUI_NAME: Reason
  -- @COL_DESC: The field reason is used for documentation purposes from Business, providing a reason why the SC must be excluded from the respective reports
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^.{0,500}$
  "REASON" VARCHAR2(1000 BYTE) NOT NULL,
  "ITS" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "PK_F_SCLC_DI_TOOL_XETR_FINAL_MISSING" PRIMARY KEY ("MIC", "PARTICIPANT_ID", "SHORT_CODE", "REPORTING_DATE_FROM")
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: XETR Incorrect LongCodes
-- @TBL_GUI_NAME_SHORT: XETR_INCORRECT_LONGCODE
-- @TBL_NAME_AT: XETR_INCORRECT_LONGCODE_AT
-- @TBL_DISPLAY_ORDER: 4
CREATE TABLE "GUI_XMDM_SCLC_DI_TOOL"."XETR_INCORRECT_LONGCODE" (
  -- @COL_GUI_NAME: MIC
  -- @COL_DESC: The MIC is the four-digit operating MIC of the trading venue
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^XETR$
  "MIC" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ParticipantID
  -- @COL_DESC: The participant ID is the trading participants five-digit memberID
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[A-Z]{5}$
  "PARTICIPANT_ID" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ShortCode
  -- @COL_DESC: The short code is the numeric code that functions as a placeholder for ClientID, ExecutionID and InvestmentID
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_PK: Y
  -- @COL_FORMAT: ##
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[0-9]{1,20}$
  "SHORT_CODE" VARCHAR2(30 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Reporting date of Incorrect Longcode from
  -- @COL_DESC: The date the from when the incorrect longcode was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_FROM" DATE NOT NULL,
  -- @COL_GUI_NAME: Reporting date of Incorrect Longcode to
  -- @COL_DESC: The date until the incorrect longcode was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_TO" DATE NOT NULL,
  -- @COL_GUI_NAME: Entry Date
  -- @COL_DESC: This date indicates when the record is entered into the XMDM GUI (This field will not be used in the reports)
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "ENTRY_DATE" DATE NOT NULL,
  -- @COL_GUI_NAME: Reason
  -- @COL_DESC: The field reason is used for documentation purposes from Business, providing a reason why the SC must be excluded from the respective reports
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^.{0,500}$
  "REASON" VARCHAR2(1000 BYTE) NOT NULL,
  "ITS" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "PK_F_SCLC_DI_TOOL_XETR_INCORRECT_LONGCODE" PRIMARY KEY ("MIC", "PARTICIPANT_ID", "SHORT_CODE", "REPORTING_DATE_FROM")
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: XEUR Final Missings
-- @TBL_GUI_NAME_SHORT: XEUR_FINAL_MISSING
-- @TBL_NAME_AT: XEUR_FINAL_MISSING_AT
-- @TBL_DISPLAY_ORDER: 2
CREATE TABLE "GUI_XMDM_SCLC_DI_TOOL"."XEUR_FINAL_MISSING" (
  -- @COL_GUI_NAME: MIC
  -- @COL_DESC: The MIC is the four-digit operating MIC of the trading venue
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^XEUR$
  "MIC" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ParticipantID
  -- @COL_DESC: The participant ID is the trading participants five-digit memberID
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[A-Z]{5}$
  "PARTICIPANT_ID" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ShortCode
  -- @COL_DESC: The short code is the numeric code that functions as a placeholder for ClientID, ExecutionID and InvestmentID
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_PK: Y
  -- @COL_FORMAT: ##
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[0-9]{1,20}$
  "SHORT_CODE" VARCHAR2(30 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Reporting date of FM from
  -- @COL_DESC: The date the from when the final missing was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_FROM" DATE NOT NULL,
  -- @COL_GUI_NAME: Reporting date of FM to
  -- @COL_DESC: The date until the final missing was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_TO" DATE NOT NULL,
  -- @COL_GUI_NAME: Entry Date
  -- @COL_DESC: This date indicates when the record is entered into the XMDM GUI (This field will not be used in the reports)
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "ENTRY_DATE" DATE NOT NULL,
  -- @COL_GUI_NAME: Reason
  -- @COL_DESC: The field reason is used for documentation purposes from Business, providing a reason why the SC must be excluded from the respective reports
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^.{0,500}$
  "REASON" VARCHAR2(1000 BYTE) NOT NULL,
  "ITS" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "PK_F_SCLC_DI_TOOL_XEUR_FINAL_MISSING" PRIMARY KEY ("MIC", "PARTICIPANT_ID", "SHORT_CODE", "REPORTING_DATE_FROM")
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: XEUR Incorrect LongCodes
-- @TBL_GUI_NAME_SHORT: XEUR_INCORRECT_LONGCODE
-- @TBL_NAME_AT: XEUR_INCORRECT_LONGCODE_AT
-- @TBL_DISPLAY_ORDER: 5
CREATE TABLE "GUI_XMDM_SCLC_DI_TOOL"."XEUR_INCORRECT_LONGCODE" (
  -- @COL_GUI_NAME: MIC
  -- @COL_DESC: The MIC is the four-digit operating MIC of the trading venue
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^XEUR$
  "MIC" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ParticipantID
  -- @COL_DESC: The participant ID is the trading participants five-digit memberID
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[A-Z]{5}$
  "PARTICIPANT_ID" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ShortCode
  -- @COL_DESC: The short code is the numeric code that functions as a placeholder for ClientID, ExecutionID and InvestmentID
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_PK: Y
  -- @COL_FORMAT: ##
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[0-9]{1,20}$
  "SHORT_CODE" VARCHAR2(30 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Reporting date of Incorrect Longcode from
  -- @COL_DESC: The date the from when the incorrect longcode was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_FROM" DATE NOT NULL,
  -- @COL_GUI_NAME: Reporting date of Incorrect Longcode to
  -- @COL_DESC: The date until the incorrect longcode was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_TO" DATE NOT NULL,
  -- @COL_GUI_NAME: Entry Date
  -- @COL_DESC: This date indicates when the record is entered into the XMDM GUI (This field will not be used in the reports)
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "ENTRY_DATE" DATE NOT NULL,
  -- @COL_GUI_NAME: Reason
  -- @COL_DESC: The field reason is used for documentation purposes from Business, providing a reason why the SC must be excluded from the respective reports
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^.{0,500}$
  "REASON" VARCHAR2(1000 BYTE) NOT NULL,
  "ITS" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "PK_F_SCLC_DI_TOOL_XEUR_INCORRECT_LONGCODE" PRIMARY KEY ("MIC", "PARTICIPANT_ID", "SHORT_CODE", "REPORTING_DATE_FROM")
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: XFRA Final Missings
-- @TBL_GUI_NAME_SHORT: XFRA_FINAL_MISSING
-- @TBL_NAME_AT: XFRA_FINAL_MISSING_AT
-- @TBL_DISPLAY_ORDER: 3
CREATE TABLE "GUI_XMDM_SCLC_DI_TOOL"."XFRA_FINAL_MISSING" (
  -- @COL_GUI_NAME: MIC
  -- @COL_DESC: The MIC is the four-digit operating MIC of the trading venue
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^XFRA$
  "MIC" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ParticipantID
  -- @COL_DESC: The participant ID is the trading participants five-digit memberID
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[A-Z]{5}$
  "PARTICIPANT_ID" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ShortCode
  -- @COL_DESC: The short code is the numeric code that functions as a placeholder for ClientID, ExecutionID and InvestmentID
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_PK: Y
  -- @COL_FORMAT: ##
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[0-9]{1,20}$
  "SHORT_CODE" VARCHAR2(30 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Reporting date of FM from
  -- @COL_DESC: The date the from when the final missing was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_FROM" DATE NOT NULL,
  -- @COL_GUI_NAME: Reporting date of FM to
  -- @COL_DESC: The date until the final missing was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_TO" DATE NOT NULL,
  -- @COL_GUI_NAME: Entry Date
  -- @COL_DESC: This date indicates when the record is entered into the XMDM GUI (This field will not be used in the reports)
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "ENTRY_DATE" DATE NOT NULL,
  -- @COL_GUI_NAME: Reason
  -- @COL_DESC: The field reason is used for documentation purposes from Business, providing a reason why the SC must be excluded from the respective reports
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^.{0,500}$
  "REASON" VARCHAR2(1000 BYTE) NOT NULL,
  "ITS" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "PK_F_SCLC_DI_TOOL_XFRA_FINAL_MISSING" PRIMARY KEY ("MIC", "PARTICIPANT_ID", "SHORT_CODE", "REPORTING_DATE_FROM")
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: XFRA Incorrect LongCodes
-- @TBL_GUI_NAME_SHORT: XFRA_INCORRECT_LONGCODE
-- @TBL_NAME_AT: XFRA_INCORRECT_LONGCODE_AT
-- @TBL_DISPLAY_ORDER: 6
CREATE TABLE "GUI_XMDM_SCLC_DI_TOOL"."XFRA_INCORRECT_LONGCODE" (
  -- @COL_GUI_NAME: MIC
  -- @COL_DESC: The MIC is the four-digit operating MIC of the trading venue
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^XFRA$
  "MIC" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ParticipantID
  -- @COL_DESC: The participant ID is the trading participants five-digit memberID
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[A-Z]{5}$
  "PARTICIPANT_ID" VARCHAR2(20 BYTE) NOT NULL,
  -- @COL_GUI_NAME: ShortCode
  -- @COL_DESC: The short code is the numeric code that functions as a placeholder for ClientID, ExecutionID and InvestmentID
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_PK: Y
  -- @COL_FORMAT: ##
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[0-9]{1,20}$
  "SHORT_CODE" VARCHAR2(30 BYTE) NOT NULL,
  -- @COL_GUI_NAME: Reporting date of Incorrect Longcode from
  -- @COL_DESC: The date the from when the incorrect longcode was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_PK: Y
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_FROM" DATE NOT NULL,
  -- @COL_GUI_NAME: Reporting date of Incorrect Longcode to
  -- @COL_DESC: The date until the incorrect longcode was reported, on which the short code was used in the trading system
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "REPORTING_DATE_TO" DATE NOT NULL,
  -- @COL_GUI_NAME: Entry Date
  -- @COL_DESC: This date indicates when the record is entered into the XMDM GUI (This field will not be used in the reports)
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_FORMAT: yyyy.MM.dd
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^[1-9]\d{3}\.\d{2}\.\d{2}$
  "ENTRY_DATE" DATE NOT NULL,
  -- @COL_GUI_NAME: Reason
  -- @COL_DESC: The field reason is used for documentation purposes from Business, providing a reason why the SC must be excluded from the respective reports
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_VALIDATOR: de.deutsche_boerse.statistix.xmdm.validators.RegexColumnValidator:^.{0,500}$
  "REASON" VARCHAR2(1000 BYTE) NOT NULL,
  "ITS" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "PK_F_SCLC_DI_TOOL_XFRA_INCORRECT_LONGCODE" PRIMARY KEY ("MIC", "PARTICIPANT_ID", "SHORT_CODE", "REPORTING_DATE_FROM")
) TABLESPACE XMDM_FACT;