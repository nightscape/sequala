-- @TBL_GUI_NAME: Notebooks
-- @TBL_GUI_NAME_SHORT: Notebooks
-- @TBL_NAME_AT: NOTEBOOKS_AT
-- @TBL_DISPLAY_ORDER: 1
CREATE TABLE "GUI_XMDM_ZEPPELIN"."NOTEBOOKS" (
  -- @COL_GUI_NAME: Git path
  -- @COL_DESC: Git path to notebook json file
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  "GIT_PATH" VARCHAR2(200 CHAR) NOT NULL,
  -- @COL_GUI_NAME: Target path
  -- @COL_DESC: Target path in zeppelin. Can contain $USER variable. But $ENV variable is not allowed - it is automatically prepanded to TARGET_PATH
  -- @COL_DISPLAY_ORDER: 2
  "TARGET_PATH" VARCHAR2(300 CHAR) NOT NULL,
  -- @COL_GUI_NAME: Owners
  -- @COL_DESC: Comma separated ldap groups for notebook
  -- @COL_DISPLAY_ORDER: 3
  "OWNERS" VARCHAR2(300 CHAR) NOT NULL,
  -- @COL_GUI_NAME: Writers
  -- @COL_DESC: Comma separated ldap groups for notebook
  -- @COL_DISPLAY_ORDER: 4
  "WRITERS" VARCHAR2(300 CHAR),
  -- @COL_GUI_NAME: Runners
  -- @COL_DESC: Comma separated ldap groups for notebook
  -- @COL_DISPLAY_ORDER: 5
  "RUNNERS" VARCHAR2(300 CHAR),
  -- @COL_GUI_NAME: Readers
  -- @COL_DESC: Comma separated ldap groups for notebook
  -- @COL_DISPLAY_ORDER: 6
  "READERS" VARCHAR2(300 CHAR),
  "ITS" DATE DEFAULT SYSDATE,
  CONSTRAINT "PK_NOTEBOOKS" PRIMARY KEY ("TARGET_PATH")
) TABLESPACE XMDM_FACT;
-- @TBL_GUI_NAME: Note schedule white list
-- @TBL_GUI_NAME_SHORT: Cron job scheduler
-- @TBL_NAME_AT: NOTE_SCHED_WHITE_LIST_AT
-- @TBL_DISPLAY_ORDER: 2
CREATE TABLE "GUI_XMDM_ZEPPELIN"."NOTE_SCHED_WHITE_LIST" (
  -- @COL_GUI_NAME: Target path
  -- @COL_DESC: Target path in zeppelin. Can contain $USER variable.
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  "TARGET_PATH" VARCHAR2(300 CHAR) NOT NULL,
  -- @COL_GUI_NAME: Cron
  -- @COL_DESC: Cron string. Example for each 6 hours: 0 0 0/6 * * ?
  -- @COL_DISPLAY_ORDER: 2
  "CRON" VARCHAR2(100 CHAR),
  "ITS" DATE DEFAULT SYSDATE,
  -- @COL_GUI_NAME: Valid to
  -- @COL_DESC: Max validity of cron notebook
  -- @COL_DISPLAY_ORDER: 3
  -- @COL_FORMAT: dd.MM.yyyy
  -- @COL_VALID_INTERVAL_COLUMN: T
  "BUSINESS_VALID_TO" DATE DEFAULT TO_DATE('31.12.9999', 'DD.MM.YYYY'),
  CONSTRAINT "PK_NOTE_SCHED_WHITE_LIST" PRIMARY KEY ("TARGET_PATH")
) TABLESPACE XMDM_FACT;