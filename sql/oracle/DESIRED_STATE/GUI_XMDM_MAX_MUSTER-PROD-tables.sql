-- @TBL_GUI_NAME: New Max Mustermann Values
-- @TBL_GUI_NAME_SHORT: Max_Muster_Values
-- @TBL_NAME_AT: D_FIXED_VALUES_MAXM_XMDM_AT
-- @TBL_DISPLAY_ORDER: 1
CREATE TABLE "GUI_XMDM_MAX_MUSTER"."D_FIXED_VALUES_MAXM_XMDM" (
  -- @COL_GUI_NAME: Participant ID
  -- @COL_DESC: Enter the Participant ID
  -- @COL_DISPLAY_ORDER: 1
  "PARTICIPANT_ID" VARCHAR2(5 CHAR),
  -- @COL_GUI_NAME: MIC
  -- @COL_DESC: Enter the MIC
  -- @COL_DISPLAY_ORDER: 2
  "MIC" VARCHAR2(4 CHAR),
  -- @COL_GUI_NAME: Status Indicator
  -- @COL_DESC: Enter the Status Indicator
  -- @COL_DISPLAY_ORDER: 3
  "STATUS_INDICATOR" VARCHAR2(1 CHAR),
  -- @COL_GUI_NAME: Valid from
  -- @COL_DESC: Enter the date it is valid from
  -- @COL_DISPLAY_ORDER: 4
  -- @COL_FORMAT: yyyy-MM-dd
  "VALID_FROM" DATE,
  -- @COL_GUI_NAME: Short Code ID
  -- @COL_DESC: Enter the Short Code ID
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_FORMAT: 0
  "SHORT_CODE_ID" VARCHAR2(5 CHAR),
  -- @COL_GUI_NAME: Classification Rule
  -- @COL_DESC: Enter the Classification Rule
  -- @COL_DISPLAY_ORDER: 6
  "CLASSIFICATION_RULE" VARCHAR2(1 CHAR),
  -- @COL_GUI_NAME: National ID Country Code
  -- @COL_DESC: Enter the National ID Country Code
  -- @COL_DISPLAY_ORDER: 7
  "NATIONAL_ID_CC" VARCHAR2(2 CHAR),
  -- @COL_GUI_NAME: National ID Priority
  -- @COL_DESC: Enter the National ID Priority
  -- @COL_DISPLAY_ORDER: 8
  -- @COL_FORMAT: 0
  "NATIONAL_ID_PRI" VARCHAR2(1 CHAR),
  -- @COL_GUI_NAME: Client Long Value
  -- @COL_DESC: Enter the Client Long Value
  -- @COL_DISPLAY_ORDER: 9
  -- @COL_PK: Y
  "CLIENT_LONG_VALUE" VARCHAR2(35 CHAR) NOT NULL,
  -- @COL_GUI_NAME: First Name
  -- @COL_DESC: Enter first name
  -- @COL_DISPLAY_ORDER: 10
  "FIRST_NAME" VARCHAR2(50 CHAR),
  -- @COL_GUI_NAME: Last Name
  -- @COL_DESC: Enter last name
  -- @COL_DISPLAY_ORDER: 11
  "LAST_NAME" VARCHAR2(50 CHAR),
  -- @COL_GUI_NAME: Date of Birth
  -- @COL_DESC: Enter Date of Birth
  -- @COL_DISPLAY_ORDER: 12
  -- @COL_FORMAT: yyyy-MM-dd
  "DATE_OF_BIRTH" DATE,
  CONSTRAINT "PK_D_FIXED_VALUES_MAXM_XMDM" PRIMARY KEY ("CLIENT_LONG_VALUE")
);