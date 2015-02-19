module Hyperion

    class Planning

        module SQL

            DIMENSION_SQL = <<-EOQ
                SELECT
                  o.OBJECT_NAME DIMENSION_NAME, ot.TYPE_NAME DIMENSION_TYPE
                FROM
                  HSP_OBJECT_TYPE ot
                  JOIN HSP_OBJECT o ON ot.OBJECT_TYPE = o.OBJECT_TYPE
                WHERE
                  ot.TYPE_NAME IN ('Dimension', 'Attribute Dimension')
                  AND o.OBJECT_NAME NOT IN ('HSP_Rates', 'HSP_XCRNCY')
                ORDER BY
                  2 DESC, 1
            EOQ

            # Query to extract plan type names (in correct order)
            PLAN_TYPE_SQL = <<-EOQ
                SELECT TYPE_NAME PLAN_TYPE
                FROM HSP_PLAN_TYPE
                ORDER BY PLAN_TYPE
            EOQ


            # Query to extract aliases for all members of a dimension
            ALIAS_SQL = <<-EOQ
                SELECT
                  ALIAS_TBL.OBJECT_NAME ALIAS_TBL_NAME,
                  MBR.MEMBER_ID MBR_ID,
                  ALIAS.OBJECT_NAME ALIAS
                FROM
                  HSP_OBJECT DIM
                  INNER JOIN HSP_MEMBER MBR
                     ON DIM.OBJECT_ID = MBR.DIM_ID
                  JOIN HSP_ALIAS A
                    ON MBR.MEMBER_ID = A.MEMBER_ID
                  JOIN HSP_OBJECT ALIAS_TBL
                    ON A.ALIASTBL_ID = ALIAS_TBL.OBJECT_ID
                  JOIN HSP_OBJECT ALIAS
                    ON A.ALIAS_ID = ALIAS.OBJECT_ID
                WHERE
                  DIM.OBJECT_NAME = ?
                ORDER BY A.ALIASTBL_ID, MBR.MEMBER_ID
            EOQ


            # Query to extract UDAs for all members of a dimension
            UDA_SQL = <<-EOQ
                SELECT DISTINCT
                    MBRNAME.OBJECT_ID, UDA.UDA_VALUE
                FROM
                    HSP_OBJECT DIM
                    INNER JOIN HSP_MEMBER MBR
                       ON DIM.OBJECT_ID = MBR.DIM_ID
                    JOIN HSP_OBJECT MBRNAME
                      ON MBR.MEMBER_ID = MBRNAME.OBJECT_ID
                    JOIN HSP_MEMBER_TO_UDA UL
                      ON UL.MEMBER_ID = MBR.MEMBER_ID
                    JOIN HSP_UDA UDA
                      ON UDA.UDA_ID = UL.UDA_ID
                WHERE
                    DIM.OBJECT_NAME = ?
                ORDER BY
                    1, 2
            EOQ


            # Query to extract attribute associations for all members of a dimension
            ATTR_SQL = <<-EOQ
                SELECT
                  ATTDIM.OBJECT_NAME ATTR_DIM_NAME,
                  MBRNAME.OBJECT_ID MBR_ID,
                  ATTR.OBJECT_NAME ATTR_NAME
                FROM
                  HSP_OBJECT DIM
                  INNER JOIN HSP_MEMBER MBR
                     ON DIM.OBJECT_ID = MBR.DIM_ID
                  JOIN HSP_OBJECT MBRNAME
                    ON MBR.MEMBER_ID = MBRNAME.OBJECT_ID
                  JOIN HSP_ATTRIBUTE_DIM AD
                    ON AD.DIM_ID = DIM.OBJECT_ID
                  JOIN HSP_OBJECT ATTDIM
                    ON AD.ATTR_ID = ATTDIM.OBJECT_ID
                  JOIN HSP_MEMBER_TO_ATTRIBUTE M2A
                    ON AD.ATTR_ID = M2A.ATTR_ID
                   AND MBR.MEMBER_ID = M2A.MEMBER_ID
                  JOIN HSP_OBJECT ATTR
                    ON M2A.ATTR_MEM_ID = ATTR.OBJECT_ID
                WHERE
                  DIM.OBJECT_NAME = ?
                ORDER BY 1, 2
            EOQ


            ACCOUNT_FIELDS = <<-EOQ
                    CASE ACC.ACCOUNT_TYPE
                        WHEN 1 THEN 'Expense'
                        WHEN 2 THEN 'Revenue'
                        WHEN 3 THEN 'Asset'
                        WHEN 4 THEN 'Liability'
                        WHEN 5 THEN 'Equity'
                        WHEN 6 THEN 'Statistical'
                        WHEN 7 THEN 'Saved Assumption'
                        ELSE 'Unknow Account Type!'
                    END AS ACCOUNT_TYPE,
                    CASE ACC.TIME_BALANCE
                        WHEN 0 THEN 'Flow'
                        WHEN 1 THEN 'First'
                        WHEN 2 THEN 'Balance'
                        WHEN 3 THEN 'Average'
                        WHEN 4 THEN 'Avg_Actual'
                        WHEN 5 THEN 'Avg_365'
                        WHEN 6 THEN 'Fill'
                        ELSE 'Unknow Time Balance!'
                    END AS TIME_BALANCE,
                    CASE ACC.SKIP_VALUE
                        WHEN 0 THEN 'None'
                        WHEN 1 THEN 'Missing'
                        WHEN 2 THEN 'Zeros'
                        WHEN 3 THEN 'Missing and Zeros'
                        ELSE 'Unknown Skip Value!'
                    END SKIP_VALUE,
                    CASE ACC.VARIANCE_REP
                        WHEN 0 THEN 'Non-Expense'
                        WHEN 1 THEN 'Expense'
                        ELSE 'Unknown Variance Reporting!'
                    END AS VARIANCE_REPORTING,
                    PT.TYPE_NAME AS SOURCE_PLAN_TYPE,
                    ACC.USED_IN,
            EOQ


            ENTITY_FIELDS = <<-EOQ
                    ENT.USED_IN,
            EOQ


            PERIOD_FIELDS = <<-EOQ
                    CASE PER.TYPE
                    WHEN 0 THEN 'base'
                    WHEN 1 THEN 'rollup'
                    WHEN 2 THEN 'year'
                    WHEN 3 THEN 'alternate'
                    WHEN 4 THEN 'DTS'
                    END "Type",
            EOQ


            SCENARIO_FIELDS = <<-EOQ
                    STYR.OBJECT_NAME START_YEAR,
                    ENYR.OBJECT_NAME END_YEAR,
                    STTP.OBJECT_NAME START_PERIOD,
                    ENTP.OBJECT_NAME END_PERIOD,
            EOQ


            VERSION_FIELDS = <<-EOQ
                    CASE VER.VERSION_TYPE
                    WHEN 1 THEN 'Bottom Up'
                    WHEN 2 THEN 'Target'
                    END "Version Type",
            EOQ


            # Returns the member id(s) for the named +member+ in +dimension+.
            # Ensures that shared members are returned after non-shared members.
            def get_member_ids_sql(dim_name, member)
                sql = <<-EOQ
                    SELECT M.MEMBER_ID
                    FROM HSP_MEMBER M
                    JOIN HSP_OBJECT D
                      ON M.DIM_ID = D.OBJECT_ID
                    JOIN HSP_OBJECT N
                      ON M.MEMBER_ID = N.OBJECT_ID
                    WHERE D.OBJECT_NAME = '#{dim_name}'
                      AND N.OBJECT_NAME = '#{member}'
                    ORDER BY CASE M.DATA_STORAGE WHEN 3 THEN 99 ELSE M.DATA_STORAGE END
                EOQ
            end


            def get_attr_dimension_sql(dim_name, top_mbr_id)
                sql = <<-EOQ
                    SELECT
                        CHILD.OBJECT_ID OBJECT_ID,
                        CHILD.OBJECT_NAME AS "#{dim_name}",
                        PARENT.OBJECT_NAME AS PARENT,
                        NVL(ALIAS.OBJECT_NAME, '') "Alias: Default",
                        'Update' AS OPERATION
                    FROM
                        HSP_MEMBER MBR
                        INNER JOIN HSP_OBJECT DIM
                           ON MBR.DIM_ID = DIM.OBJECT_ID
                        INNER JOIN HSP_OBJECT CHILD
                           ON MBR.MEMBER_ID = CHILD.OBJECT_ID
                        INNER JOIN HSP_OBJECT PARENT
                           ON CHILD.PARENT_ID = PARENT.OBJECT_ID
                        LEFT JOIN HSP_ALIAS AL
                           ON CHILD.OBJECT_ID = AL.MEMBER_ID
                        LEFT JOIN HSP_OBJECT ALIAS
                           ON AL.ALIAS_ID = ALIAS.OBJECT_ID
                        LEFT JOIN HSP_OBJECT ALIASTABLE
                           ON AL.ALIASTBL_ID = ALIASTABLE.OBJECT_ID
                    WHERE CHILD.OBJECT_TYPE != 2
                    CONNECT BY CHILD.PARENT_ID = PRIOR CHILD.OBJECT_ID
                    START WITH PARENT.OBJECT_ID = #{top_mbr_id}
                    ORDER SIBLINGS BY CHILD.POSITION
                EOQ
            end


            def get_dimension_sql(dim_name, top_mbr_id)
                sql = <<-EOQ
                    SELECT
                        CHILD.OBJECT_ID OBJECT_ID,
                        CHILD.OBJECT_NAME AS "#{dim_name}",
                        PARENT.OBJECT_NAME AS PARENT,
                        CASE MBR.DATA_STORAGE
                            WHEN 0 THEN 'Store'
                            WHEN 1 THEN 'Never Share'
                            WHEN 2 THEN 'Label Only'
                            WHEN 3 THEN 'Shared'
                            WHEN 4 THEN 'Dynamic Calc and Store'
                            WHEN 5 THEN 'Dynamic Calc'
                        END AS DATA_STORAGE,
                        TWOPASS_CALC AS TWO_PASS_CALCULATION,
                        '' AS DESCRIPTION,
                        NVL(MF.FORMULA, '') FORMULA,
                        NVL(ENUM.NAME, '') AS SMART_LIST,
                        CASE NVL(ACC.DATA_TYPE, MBR.DATA_TYPE)
                            WHEN 1 THEN 'Currency'
                            WHEN 2 THEN 'Non-currency'
                            WHEN 3 THEN 'Percentage'
                            WHEN 4 THEN 'Smart List'
                            WHEN 5 THEN 'Date'
                            WHEN 6 THEN 'Text'
                            ELSE 'Unspecified'
                        END AS DATA_TYPE,
                        'Update' AS OPERATION,
                        #{case dim_name
                          when /Scenario/ then SCENARIO_FIELDS
                          when /Version/ then VERSION_FIELDS
                          when /Account/ then ACCOUNT_FIELDS
                          when /Entity/ then ENTITY_FIELDS
                          when /Period/ then PERIOD_FIELDS
                          end}
                        MBR.CONSOL_OP,
                        'UDA_Placeholder' AS UDA
                    FROM
                        HSP_MEMBER MBR
                        INNER JOIN HSP_OBJECT DIM
                          ON MBR.DIM_ID = DIM.OBJECT_ID
                        INNER JOIN HSP_OBJECT CHILD
                          ON MBR.MEMBER_ID = CHILD.OBJECT_ID
                        INNER JOIN HSP_OBJECT PARENT
                          ON CHILD.PARENT_ID = PARENT.OBJECT_ID
                        LEFT JOIN HSP_MEMBER_FORMULA MF
                          ON MF.MEMBER_ID = CHILD.OBJECT_ID
                        LEFT JOIN HSP_SCENARIO SCEN
                          ON SCEN.SCENARIO_ID = CHILD.OBJECT_ID
                        LEFT JOIN HSP_OBJECT STYR
                          ON SCEN.START_YR_ID = STYR.OBJECT_ID
                        LEFT JOIN HSP_OBJECT ENYR
                          ON SCEN.END_YR_ID = ENYR.OBJECT_ID
                        LEFT JOIN HSP_OBJECT STTP
                          ON SCEN.START_TP_ID = STTP.OBJECT_ID
                        LEFT JOIN HSP_OBJECT ENTP
                          ON SCEN.END_TP_ID = ENTP.OBJECT_ID
                        LEFT JOIN HSP_VERSION VER
                          ON VER.VERSION_ID = CHILD.OBJECT_ID
                        LEFT JOIN HSP_ACCOUNT ACC
                          ON ACC.ACCOUNT_ID = CHILD.OBJECT_ID
                        LEFT JOIN HSP_ENTITY ENT
                          ON ENT.ENTITY_ID = CHILD.OBJECT_ID
                        LEFT JOIN HSP_TIME_PERIOD PER
                          ON PER.TP_ID = CHILD.OBJECT_ID
                        LEFT JOIN HSP_PLAN_TYPE PT
                          ON PT.PLAN_TYPE = ACC.SRC_PLAN_TYPE
                        LEFT JOIN HSP_ENUMERATION ENUM
                          ON MBR.ENUMERATION_ID = ENUM.ENUMERATION_ID
                    WHERE CHILD.OBJECT_TYPE != 2
                      AND NOT (NVL(PER.TYPE, -1) = 4 AND NVL(PER.DTS_GENERATION, -1) = 0)
                    CONNECT BY CHILD.PARENT_ID = PRIOR CHILD.OBJECT_ID
                    START WITH CHILD.OBJECT_ID = #{top_mbr_id}
                    ORDER SIBLINGS BY CHILD.POSITION
                EOQ
            end


            SECURITY_ACCESS_SQL = <<-EOQ
                SELECT
                  U.OBJECT_NAME NAME,
                  O.OBJECT_NAME,
                  CASE AC.ACCESS_MODE
                    WHEN 0 THEN 'NONE'
                    WHEN 1 THEN 'READ'
                    WHEN 2 THEN 'READWRITE'
                    WHEN 3 THEN 'READWRITE'
                    ELSE TO_CHAR(AC.ACCESS_MODE)
                  END ACCESS_MODE,
                  CASE AC.FLAGS
                    WHEN 0 THEN 'MEMBER'
                    WHEN 5 THEN '@CHILDREN'
                    WHEN 6 THEN '@ICHILDREN'
                    WHEN 8 THEN '@DESCENDANTS'
                    WHEN 9 THEN '@IDESCENDANTS'
                    ELSE '*** Unknown flag ' || TO_CHAR(AC.FLAGS)
                  END FLAGS,
                  CASE
                    WHEN O.OBJECT_TYPE = 1 THEN 'SL_FORMFOLDER'
                    WHEN O.OBJECT_TYPE = 7 THEN 'SL_FORM'
                    WHEN O.OBJECT_TYPE = 24 THEN 'SL_TASKLIST'
                    WHEN O.OBJECT_TYPE BETWEEN 30 AND 50 THEN NULL
                    WHEN O.OBJECT_TYPE = 107 THEN 'SL_COMPOSITE'
                    ELSE '*** Unknown object type ' || TO_CHAR(O.OBJECT_TYPE) || ' (' || OT.TYPE_NAME || ')'
                  END OBJECT_TYPE
                FROM
                  HSP_ACCESS_CONTROL AC
                  JOIN HSP_OBJECT O
                    ON AC.OBJECT_ID = O.OBJECT_ID
                  LEFT JOIN HSP_OBJECT_TYPE OT
                    ON O.OBJECT_TYPE = OT.OBJECT_TYPE
                  JOIN HSP_OBJECT U
                    ON AC.USER_ID = U.OBJECT_ID
                ORDER BY
                  1, 2, 3
            EOQ


            TASK_LIST_SQL = <<-EOQ
                SELECT
                  otl.OBJECT_NAME TASK_LIST,
                  LPAD(' ', LEVEL*4-8) || ot.OBJECT_NAME TASK_NAME,
                  otp.OBJECT_NAME PARENT_TASK,
                  CASE
                    WHEN t.TASK_TYPE = 0 THEN 'Descriptive'
                    WHEN t.TASK_TYPE = 1 THEN 'URL'
                    WHEN t.TASK_TYPE = 2 THEN 'Data Form'
                    WHEN t.TASK_TYPE = 3 THEN 'Business Rule'
                    WHEN t.TASK_TYPE = 4 THEN 'Manage Process'
                  END TASK_TYPE,
                  aop.OBJECT_NAME FORM_FOLDER,
                  ao.OBJECT_NAME FORM_NAME,
                  CASE WHEN t.TASK_TYPE = 3 THEN t.STR_PROP1 END BUSINESS_RULE_NAME,
                  pt.TYPE_NAME PLAN_TYPE,
                  CASE WHEN t.TASK_TYPE = 1 THEN t.STR_PROP1 END URL,
                  TRIM(t.INSTRUCTIONS) INSTRUCTIONS
                FROM
                  HSP_TASK t
                  JOIN HSP_OBJECT ot ON t.TASK_ID = ot.OBJECT_ID
                  JOIN HSP_OBJECT otp ON ot.PARENT_ID = otp.OBJECT_ID
                  JOIN HSP_OBJECT otl ON t.TASK_LIST_ID = otl.OBJECT_ID
                  LEFT JOIN HSP_OBJECT ao ON t.TASK_TYPE = 2 AND t.INT_PROP1 = ao.OBJECT_ID
                  LEFT JOIN HSP_OBJECT aop ON ao.PARENT_ID = aop.OBJECT_ID
                  LEFT JOIN HSP_PLAN_TYPE pt ON t.TASK_TYPE = 3 AND t.INT_PROP1 = pt.PLAN_TYPE
                WHERE
                  ot.OBJECT_ID <> t.TASK_LIST_ID
                CONNECT BY ot.PARENT_ID = PRIOR ot.OBJECT_ID
                START WITH ot.OBJECT_ID = t.TASK_LIST_ID
                ORDER SIBLINGS BY ot.POSITION
            EOQ


            FORMS_SQL = <<-EOQ
                WITH Path AS (
                  SELECT PRIOR SUBSTR(SYS_CONNECT_BY_PATH(o.OBJECT_NAME, '/'), 7) FOLDER,
                    o.OBJECT_ID, o.OBJECT_NAME FORM_NAME
                  FROM HSP_OBJECT o
                  WHERE o.OBJECT_TYPE != 1
                    AND UPPER(o.OBJECT_NAME) LIKE UPPER(?)
                  CONNECT BY NOCYCLE o.PARENT_ID = PRIOR o.OBJECT_ID
                  START WITH o.OBJECT_ID = 9
                )
                SELECT
                  p.FORM_NAME, 'Simple' FORM_TYPE, p.FOLDER,
                  co.OBJECT_NAME CUBE_NAME,
                  f.FORM_OPT, f.SCALING, f.FMT_PRECEDENCE,
                  CASE f.COLUMN_WIDTH
                  WHEN 0 THEN 'Size-to-Fit'
                  WHEN 50 THEN 'Small'
                  WHEN 75 THEN 'Medium'
                  WHEN 100 THEN 'Large'
                  ELSE 'Custom: '||f.COLUMN_WIDTH
                  END COLUMN_WIDTH,
                  f.PRECISION_MIN1 CURRENCY_PRECISION_MIN, f.PRECISION_MAX1 CURRENCY_PRECISION_MAX,
                  f.PRECISION_MIN2 NON_CURRENCY_PRECISION_MIN, f.PRECISION_MAX2 NON_CURRENCY_PRECISION_MAX,
                  f.PRECISION_MIN3 PERCENTAGE_PRECISION_MIN, f.PRECISION_MAX3 PERCENTAGE_PRECISION_MAX,
                  f.ROW_LABEL MSG_NO_DATA,
                  CAST(NULL AS NUMBER(38)) GLOBAL_SCOPE
                FROM
                  HSP_FORM f
                  JOIN Path p ON f.FORM_ID = p.OBJECT_ID
                  JOIN HSP_CUBES c ON f.CUBE_ID = c.CUBE_ID
                  JOIN HSP_OBJECT co ON c.CUBE_ID = co.OBJECT_ID
                UNION ALL
                SELECT
                  p.FORM_NAME, 'Composite', p.FOLDER,
                  NULL, cf.FORM_OPT, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                  cf.GLOBAL_SCOPE
                FROM
                  HSP_COMPOSITE_FORM cf
                  JOIN Path p ON cf.FORM_ID = p.OBJECT_ID
                ORDER BY
                  1
            EOQ


            FORM_PANES_SQL = <<-EOQ
                SELECT
                  cfo.OBJECT_NAME COMPOSITE_FORM, cb.PANE_ID PANE,
                  NVL(cb.RESOURCE_LABEL, r.OBJECT_NAME) TAB_LABEL,
                  CASE cb.RESOURCE_TYPE
                    WHEN 2 THEN 'Form'
                    ELSE 'Unknown resource type: ' || RESOURCE_TYPE
                  END RESOURCE_TYPE,
                  r.OBJECT_NAME RESOURCE_NAME
                FROM HSP_COMPOSITE_FORM cf
                JOIN HSP_OBJECT cfo
                  ON cf.FORM_ID = cfo.OBJECT_ID
                JOIN HSP_COMPOSITE_FORM_PANE cfp
                  ON cf.FORM_ID = cfp.FORM_ID
                JOIN HSP_COMPOSITE_BLOCK cb
                  ON cfp.FORM_ID = cb.FORM_ID
                 AND cfp.PANE_ID = cb.PANE_ID
                LEFT JOIN HSP_OBJECT r
                  ON cb.RESOURCE_ID = r.OBJECT_ID
                WHERE
                  UPPER(cfo.OBJECT_NAME) LIKE UPPER(?)
                ORDER BY cfo.OBJECT_NAME, cb.PANE_ID, cb.POSITION
            EOQ


            FORM_LAYOUT_SQL = <<-EOQ
                SELECT
                  fo.OBJECT_NAME FORM_NAME,
                  CASE l.LAYOUT_TYPE
                  WHEN 0 THEN 'POV'
                  WHEN 1 THEN 'Page'
                  WHEN 2 THEN 'Rows'
                  WHEN 3 THEN 'Columns'
                  ELSE 'UNKNOWN AXIS'
                  END AXIS,
                  do.OBJECT_NAME DIM_NAME,
                  DECODE(BITAND(l.STYLE, 1), 1, 1, 0) START_EXPANDED,
                  DECODE(BITAND(l.STYLE, 2), 2, 1, 0) DISPLAY_NAME,
                  DECODE(BITAND(l.STYLE, 4), 4, 1, 0) DISPLAY_ALIAS,
                  DECODE(BITAND(l.STYLE, 8), 8, 1, 0) BIT4,
                  DECODE(BITAND(l.STYLE, 16), 16, 1, 0) HIDE_DIMENSION,
                  DECODE(BITAND(l.STYLE, 32), 32, 1, 0) DISPLAY_FORMULA,
                  DECODE(BITAND(l.STYLE, 64), 64, 1, 0) SHOW_CONSOL_OPERATORS
                FROM
                  HSP_FORM f
                  JOIN HSP_OBJECT fo ON f.FORM_ID = fo.OBJECT_ID
                  JOIN HSP_FORM_LAYOUT l ON f.FORM_ID = l.FORM_ID
                  JOIN HSP_OBJECT do ON l.DIM_ID = do.OBJECT_ID
                WHERE
                  UPPER(fo.OBJECT_NAME) LIKE UPPER(?)
                ORDER BY
                  fo.OBJECT_NAME, l.LAYOUT_TYPE, l.ORDINAL
            EOQ


            FORM_MEMBERS_SQL = <<-EOQ
                SELECT
                  fo.OBJECT_NAME FORM_NAME,
                  CASE fd.OBJDEF_TYPE
                    WHEN 0 THEN 'POV'
                    WHEN 1 THEN 'Page'
                    WHEN 2 THEN 'Row'
                    WHEN 3 THEN 'Column'
                    ELSE 'Unknown: '||fd.OBJDEF_TYPE
                  END AXIS,
                  DENSE_RANK() OVER (PARTITION BY fo.OBJECT_NAME, fd.OBJDEF_TYPE ORDER BY fd.LOCATION) NUM,
                  '' || fdm.ORDINAL || '.' || fdm.SEQUENCE SEQ,
                  CASE
                    WHEN FORMULA IS NOT NULL THEN LABEL
                    WHEN SUBST_VAR IS NOT NULL THEN '&'||SUBST_VAR
                    ELSE mo.OBJECT_NAME
                  END MEMBER_NAME,
                  CASE fdm.QUERY_TYPE
                    WHEN 8 THEN 'Descendants'
                    WHEN 9 THEN 'IDescendants'
                    WHEN 3 THEN 'Ancestors'
                    WHEN 4 THEN 'IAncestors'
                    WHEN 12 THEN 'Siblings'
                    WHEN 21 THEN 'Parents'
                    WHEN 22 THEN 'IParents'
                    WHEN 5 THEN 'Children'
                    WHEN 6 THEN 'IChildren'
                    WHEN -9 THEN 'ILvl0Descendants'
                    WHEN -1002 THEN 'Formula'
                    WHEN 0 THEN 'Member'
                    ELSE 'Unknown: '|| fdm.QUERY_TYPE
                  END QUERY_TYPE,
                  fd.FORMULA,
                  fd.STYLE
                FROM
                  HSP_FORMOBJ_DEF fd
                  JOIN HSP_OBJECT fo ON fd.form_id = fo.OBJECT_ID
                  JOIN HSP_FORMOBJ_DEF_MBR fdm ON fd.OBJDEF_ID = fdm.OBJDEF_ID
                  JOIN HSP_OBJECT mo ON fdm.mbr_id = mo.OBJECT_ID
                WHERE
                  UPPER(fo.OBJECT_NAME) LIKE UPPER(?)
                ORDER BY fo.OBJECT_NAME, fd.OBJDEF_TYPE, fd.LOCATION, fdm.ORDINAL, fdm.SEQUENCE
            EOQ


            FORM_CALCS_SQL = <<-EOQ
                SELECT
                  fo.OBJECT_NAME FORM_NAME,
                  CASE
                  WHEN c.CALC_TYPE = 0 THEN c.CALC_NAME
                  WHEN c.CALC_NAME = 'DEFAULT' THEN '<Calculate Data Form>'
                  WHEN c.CALC_NAME LIKE 'COMPONENT_%' AND cfo.OBJECT_NAME IS NOT NULL
                  THEN '<Business rules for ' || cfo.OBJECT_NAME || '>'
                  ELSE c.CALC_NAME
                  END CALC_NAME,
                  c.RUN_ON_LOAD, c.RUN_ON_SAVE, c.USE_MRU USE_FORM_MBRS, c.HIDE_PROMPT
                FROM
                  HSP_FORM_CALCS c
                  JOIN HSP_OBJECT fo ON c.FORM_ID = fo.OBJECT_ID
                  LEFT JOIN HSP_FORM f ON c.FORM_ID = f.FORM_ID
                  LEFT JOIN HSP_COMPOSITE_BLOCK cb ON c.FORM_ID = cb.FORM_ID
                   AND 'COMPONENT_'||cb.POSITION = c.CALC_NAME
                  LEFT JOIN HSP_OBJECT cfo ON cb.RESOURCE_ID = cfo.OBJECT_ID
                WHERE
                  UPPER(fo.OBJECT_NAME) LIKE UPPER(?)
                ORDER BY
                  fo.OBJECT_NAME, c.CALC_ID
            EOQ


            FORM_MENUS_SQL = <<-EOQ
                SELECT
                  fo.OBJECT_NAME FORM_NAME,
                  mo.OBJECT_NAME MENU_NAME
                FROM
                  HSP_FORM_MENUS fm
                  JOIN HSP_OBJECT fo ON fm.FORM_ID = fo.OBJECT_ID
                  JOIN HSP_OBJECT mo ON fm.MENU_ID = mo.OBJECT_ID
                WHERE
                  UPPER(fo.OBJECT_NAME) LIKE UPPER(?)
                ORDER BY
                  fo.OBJECT_NAME, fm.POSITION
            EOQ


            FORM_USAGE_SQL = <<-EOQ
                WITH Path AS (
                  SELECT PRIOR SUBSTR(SYS_CONNECT_BY_PATH(o.OBJECT_NAME, '/'), 7) FOLDER,
                    o.OBJECT_ID, o.OBJECT_NAME FORM_NAME
                  FROM HSP_OBJECT o
                  WHERE o.OBJECT_TYPE != 1
                  CONNECT BY NOCYCLE o.PARENT_ID = PRIOR o.OBJECT_ID
                  START WITH o.OBJECT_ID = 9
                )
                SELECT DISTINCT
                  ot.OBJECT_NAME TASK_LIST, NULL COMPOSITE_FORM, NULL FOLDER
                FROM HSP_OBJECT fo
                LEFT JOIN HSP_COMPOSITE_BLOCK cb
                  ON fo.OBJECT_ID = cb.RESOURCE_ID
                 AND cb.RESOURCE_TYPE = 2
                LEFT JOIN HSP_OBJECT cfo
                  ON cb.FORM_ID = cfo.OBJECT_ID
                JOIN HSP_TASK t
                  ON (fo.OBJECT_ID = t.INT_PROP1 OR cb.RESOURCE_ID = t.INT_PROP1)
                 AND t.TASK_TYPE = 2
                LEFT JOIN HSP_OBJECT ot
                  ON t.TASK_LIST_ID = ot.OBJECT_ID
                WHERE fo.OBJECT_NAME LIKE UPPER(?)
                  AND fo.OBJECT_TYPE IN (7, 107)
                UNION ALL
                SELECT DISTINCT
                  NULL TASK_LIST, p.FORM_NAME, p.FOLDER
                FROM HSP_OBJECT f
                JOIN HSP_COMPOSITE_BLOCK cb
                  ON f.OBJECT_ID = cb.RESOURCE_ID
                 AND cb.RESOURCE_TYPE = 2
                JOIN Path p
                  ON cb.FORM_ID = p.OBJECT_ID
                WHERE
                  f.OBJECT_TYPE = 7
                  AND UPPER(f.OBJECT_NAME) LIKE UPPER(?)
            EOQ


            DIMENSION_BY_LEVEL_SQL = <<-EOQ
                WITH PC AS (
                    SELECT
                      MBRNAME.OBJECT_ID MEMBER_ID,
                      MBRNAME.PARENT_ID PARENT_ID,
                      MBRNAME.OBJECT_NAME MEMBER_NAME,
                      ALIAS.OBJECT_NAME DEFAULT_ALIAS,
                      MBRNAME.POSITION
                    FROM
                      HSP_OBJECT DIM
                      INNER JOIN HSP_MEMBER MBR
                         ON DIM.OBJECT_ID = MBR.DIM_ID
                       JOIN HSP_OBJECT MBRNAME
                         ON MBR.MEMBER_ID = MBRNAME.OBJECT_ID
                       LEFT JOIN HSP_ALIAS AL
                         ON MBR.MEMBER_ID = AL.MEMBER_ID
                       LEFT JOIN HSP_OBJECT ALIASTABLE
                         ON AL.ALIASTBL_ID = ALIASTABLE.OBJECT_ID
                        AND ALIASTABLE.OBJECT_NAME = 'Default'
                       LEFT JOIN HSP_OBJECT ALIAS
                         ON AL.ALIAS_ID = ALIAS.OBJECT_ID
                    WHERE
                      DIM.OBJECT_NAME = ?
                )
                SELECT
                  LPAD(MEMBER_NAME, LENGTH(MEMBER_NAME) + (LEVEL-1) * 4) MEMBER_NAME,
                  DEFAULT_ALIAS,
                  LEVEL AS GEN,
                  SUBSTR(SYS_CONNECT_BY_PATH(MEMBER_NAME, '|'), 2) PATH
                FROM PC
                START WITH MEMBER_ID = ?
                CONNECT BY NOCYCLE PARENT_ID = PRIOR MEMBER_ID
                ORDER SIBLINGS BY POSITION
            EOQ


            SMART_LISTS_SQL = <<-EOQ
                SELECT
                  e.NAME SMARTLIST_NAME,
                  'addSmartList' OPERATION,
                  e.LABEL,
                  CASE e.DISPLAY_ORDER
                    WHEN 0 THEN  'ID'
                    WHEN 1 THEN 'Name'
                    WHEN 2 THEN 'Label'
                  END DISPLAY_ORDER,
                  NVL(e.MISSING_LABEL, 'LABEL_NONE') MISSING_LABEL,
                  DECODE(e.OVERRIDE_GRID_MISSING, 0, 'FALSE', 1, 'TRUE') USE_FORM_MISSING_LABEL
                FROM HSP_ENUMERATION e
                ORDER BY e.NAME
            EOQ


            SMART_LIST_ITEMS_SQL = <<-EOQ
                SELECT sl.NAME SMARTLIST_NAME,
                  'addEntry' OPERATION,
                  sle.ENTRY_ID,
                  sle.NAME ENTRY_NAME,
                  sle.LABEL ENTRY_LABEL
                FROM HSP_ENUMERATION sl
                JOIN HSP_ENUMERATION_ENTRY sle
                  ON sl.ENUMERATION_ID = sle.ENUMERATION_ID
                ORDER BY sl.NAME, sle.ENTRY_ID
            EOQ


            MENU_ITEMS_SQL = <<-EOQ
                SELECT
                  mo.OBJECT_NAME MENU, mi.MENU_ITEM, mi.LABEL, mi.ICON,
                  CASE mi.MENU_ITEM_TYPE
                    WHEN 1 THEN 'URL'
                    WHEN 2 THEN 'Data Form'
                    WHEN 3 THEN 'Business Rule'
                    WHEN 4 THEN 'Manage Process'
                    WHEN 0 THEN 'Menu Header'
                    WHEN 5 THEN 'Previous Form'
                    ELSE TO_CHAR(mi.MENU_ITEM_TYPE)
                  END MENU_ITEM_TYPE,
                  CASE mi.REQUIRED_DIM_ID
                    WHEN 1 THEN 'Page'
                    WHEN 2 THEN 'Row'
                    WHEN 3 THEN 'Column'
                    WHEN 4 THEN 'Point of View'
                    WHEN -1 THEN 'Members only'
                    WHEN -2 THEN 'Cell Only'
                    ELSE do.OBJECT_NAME
                  END REQUIRED_PARAMETERS,
                  mi.OPEN_IN_NEW_WINDOW,
                  DECODE(mi.MENU_ITEM_TYPE, 2, o1.OBJECT_NAME) FORM_NAME,
                  DECODE(mi.MENU_ITEM_TYPE, 3, pt.TYPE_NAME) PLAN_TYPE,
                  DECODE(mi.MENU_ITEM_TYPE, 3, mi.STR_PROP1) BUSINESS_RULE,
                  DECODE(mi.MENU_ITEM_TYPE, 3,
                         DECODE(mi.INT_PROP3, 0, 'Classic View', 1, 'Streamline View')) VIEW_TYPE,
                  DECODE(mi.MENU_ITEM_TYPE, 3, mi.STR_PROP2) WINDOW_TITLE,
                  DECODE(mi.MENU_ITEM_TYPE, 3, mi.STR_PROP3) OK_BUTTON_LABEL,
                  DECODE(mi.MENU_ITEM_TYPE, 3, mi.STR_PROP4) CANCEL_BUTTON_LABEL,
                  DECODE(mi.MENU_ITEM_TYPE, 4, o1.OBJECT_NAME) SCENARIO,
                  DECODE(mi.MENU_ITEM_TYPE, 4, o2.OBJECT_NAME) VERSION
                FROM (
                    SELECT mi.*, mio.OBJECT_NAME MENU_ITEM
                    FROM HSP_MENU_ITEM mi
                    JOIN HSP_OBJECT mio ON mi.MENU_ITEM_ID = mio.OBJECT_ID
                    WHERE mi.MENU_ID <> mi.MENU_ITEM_ID
                    CONNECT BY mio.PARENT_ID = PRIOR mio.OBJECT_ID
                    START WITH mi.MENU_ITEM_ID = mi.MENU_ID
                    ORDER SIBLINGS BY mio.POSITION
                ) mi
                JOIN HSP_OBJECT mo ON mi.MENU_ID = mo.OBJECT_ID
                LEFT JOIN HSP_OBJECT do ON mi.REQUIRED_DIM_ID = do.OBJECT_ID
                LEFT JOIN HSP_PLAN_TYPE pt ON mi.INT_PROP1 = pt.PLAN_TYPE
                LEFT JOIN HSP_OBJECT o1 ON mi.INT_PROP1 = o1.OBJECT_ID
                LEFT JOIN HSP_OBJECT o2 ON mi.INT_PROP2 = o2.OBJECT_ID
            EOQ


            USER_VARIABLE_SQL = <<-EOQ
                SELECT uv.VARIABLE_NAME, do.OBJECT_NAME DIMENSION_NAME
                FROM HSP_USER_VARIABLE uv
                JOIN HSP_OBJECT do on uv.DIM_ID = do.OBJECT_ID
                ORDER BY 1
            EOQ

        end

    end

end

