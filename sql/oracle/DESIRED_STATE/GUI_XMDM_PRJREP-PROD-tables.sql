-- @TBL_GUI_NAME: Status for Project
-- @TBL_GUI_NAME_SHORT: Status for Project
-- @TBL_NAME_AT: PRJREP_PROJECT_AT
-- @TBL_DISPLAY_ORDER: 1
CREATE TABLE "GUI_XMDM_PRJREP"."PRJREP_PROJECT" (
  -- @COL_GUI_NAME: Project Name
  -- @COL_DESC: Project Name
  -- @COL_DISPLAY_ORDER: 1
  -- @COL_PK: Y
  "PRJ_NAME" VARCHAR2(100 BYTE),
  -- @COL_GUI_NAME: Sub Project Name
  -- @COL_DESC: Sub Project Name
  -- @COL_DISPLAY_ORDER: 2
  -- @COL_PK: Y
  "SUB_PRJ_NAME" VARCHAR2(250 BYTE),
  -- @COL_GUI_NAME: Project Manager
  -- @COL_DESC: Project Manager
  -- @COL_DISPLAY_ORDER: 3
  "PRJ_MANAGER" VARCHAR2(500 BYTE),
  -- @COL_GUI_NAME: Current Status
  -- @COL_DESC: Current Status
  -- @COL_DISPLAY_ORDER: 4
  -- @LKP_SQL_STMT1: select 'Green' as LABEL, 'Green' as VALUE from dual
UNION ALL
select 'Amber' as LABEL, 'Amber' as VALUE from dual
UNION ALL
select 'Red' as LABEL, 'Red' as VALUE from dual
  -- @LKP_SQL_LABEL: LABEL
  -- @LKP_SQL_VALUE: VALUE
  "CURRENT_STATUS" VARCHAR2(250 BYTE),
  -- @COL_GUI_NAME: Status Overview
  -- @COL_DESC: Status Overview
  -- @COL_DISPLAY_ORDER: 5
  -- @COL_MULTILINE: Y
  "STATUS_OVERVIEW" VARCHAR2(4000 BYTE),
  -- @COL_GUI_NAME: Risks
  -- @COL_DESC: Risks
  -- @COL_DISPLAY_ORDER: 6
  -- @COL_MULTILINE: Y
  "RISK" VARCHAR2(4000 BYTE),
  -- @COL_GUI_NAME: Issues
  -- @COL_DESC: Issues
  -- @COL_DISPLAY_ORDER: 7
  -- @COL_MULTILINE: Y
  "ISSUE" VARCHAR2(4000 BYTE),
  -- @COL_GUI_NAME: Delivery TEST
  -- @COL_DESC: Delivery Test
  -- @COL_DISPLAY_ORDER: 8
  -- @COL_FORMAT: dd.MM.yyyy
  "MS_TST" DATE,
  -- @COL_GUI_NAME: Delivery ACC
  -- @COL_DESC: Delivery ACC
  -- @COL_DISPLAY_ORDER: 9
  -- @COL_FORMAT: dd.MM.yyyy
  "MS_ACC" DATE,
  -- @COL_GUI_NAME: Delivery SIM
  -- @COL_DESC: Delivery SIM
  -- @COL_DISPLAY_ORDER: 10
  -- @COL_FORMAT: dd.MM.yyyy
  "MS_SIM" DATE,
  -- @COL_GUI_NAME: Delivery PRD
  -- @COL_DESC: Delivery PRD
  -- @COL_DISPLAY_ORDER: 11
  -- @COL_FORMAT: dd.MM.yyyy
  "MS_PRD" DATE,
  -- @COL_GUI_NAME: State
  -- @COL_DESC: State
  -- @COL_DISPLAY_ORDER: 12
  -- @LKP_SQL_STMT1: select 'Draft' as LABEL, 'Draft' as VALUE from dual
UNION ALL
select 'In Progress' as LABEL, 'In Progress' as VALUE from dual
UNION ALL
select 'On Hold' as LABEL, 'On Hold' as VALUE from dual
UNION ALL
select 'Closed' as LABEL, 'Closed' as VALUE from dual
  -- @LKP_SQL_LABEL: LABEL
  -- @LKP_SQL_VALUE: VALUE
  "STATE" VARCHAR2(250 BYTE),
  "ITS" DATE
) TABLESPACE XMDM_FACT;