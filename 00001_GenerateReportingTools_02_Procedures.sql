USE some_warehouse;

DROP PROCEDURE IF EXISTS Tools_GenerateTimeseries;
CREATE PROCEDURE Tools_GenerateTimeseries(
  startDate TIMESTAMP,
  endDate TIMESTAMP,
  step INTEGER,
  resolution VARCHAR(10)
)
BEGIN

  /*
  Usage:

  CALL Tools_GenerateTimeseries('2009-01-01 00:00:00','2009-01-01 02:00:00',10,'MINUTE');
  SELECT * FROM Timeseries;

  */

  CALL Tools_GenerateTimeseriesWithTimezone('UTC', startDate, endDate, step, resolution);

END;


DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesWithTimezone;
CREATE PROCEDURE Tools_GenerateTimeseriesWithTimezone(
  dateNamedTimezone VARCHAR(128),
  startDate TIMESTAMP(6),
  endDate TIMESTAMP(6),
  step INTEGER,
  resolution VARCHAR(10)
)
BEGIN

  /*
  Usage:
  CALL Tools_GenerateTimeseriesWithTimezone('UTC', '2009-01-01 00:00:00','2009-01-01 02:00:00',10,'MINUTE');
  SELECT * FROM Timeseries;
  */

  /*
  Adapted from http://stackoverflow.com/questions/510012/get-a-list-of-dates-between-two-dates
  */

  DECLARE thisDateInTimezone TIMESTAMP(6);
  DECLARE nextDateInTimezone TIMESTAMP(6);
  DECLARE endDateInTimezone TIMESTAMP(6);

  SET thisDateInTimezone = startDate;
  SET endDateInTimezone = endDate;

  DROP TEMPORARY TABLE IF EXISTS Timeseries;
  CREATE TEMPORARY TABLE IF NOT EXISTS Timeseries (
    IntervalStart TIMESTAMP(6),
    IntervalEnd TIMESTAMP(6),
    IntervalStartUtc TIMESTAMP(6),
    IntervalEndUtc TIMESTAMP(6),
    IntervalStartUtcIso8601 VARCHAR(32),
    IntervalEndUtcIso8601 VARCHAR(32),
    IntervalStartInTimezone TIMESTAMP(6),
    IntervalEndInTimezone TIMESTAMP(6),
    IntervalStartInTimezoneIso8601 VARCHAR(32),
    IntervalEndInTimezoneIso8601 VARCHAR(32),
    INDEX IntervalStart_IntervalEnd (IntervalStart, IntervalEnd)
  );

  REPEAT
    SELECT
      (CASE UPPER(resolution)
       WHEN 'MICROSECOND' THEN TIMESTAMPADD(MICROSECOND, step, thisDateInTimezone)
       WHEN 'SECOND' THEN TIMESTAMPADD(SECOND, step, thisDateInTimezone)
       WHEN 'MINUTE' THEN TIMESTAMPADD(MINUTE, step, thisDateInTimezone)
       WHEN 'HOUR' THEN TIMESTAMPADD(HOUR, step, thisDateInTimezone)
       WHEN 'DAY' THEN TIMESTAMPADD(DAY, step, thisDateInTimezone)
       WHEN 'WEEK' THEN TIMESTAMPADD(WEEK, step, thisDateInTimezone)
       WHEN 'MONTH' THEN TIMESTAMPADD(MONTH, step, thisDateInTimezone)
       WHEN 'QUARTER' THEN TIMESTAMPADD(QUARTER, step, thisDateInTimezone)
       WHEN 'YEAR' THEN TIMESTAMPADD(YEAR, step, thisDateInTimezone)
       WHEN 'ALL' THEN endDateInTimezone
     END)
    INTO nextDateInTimezone;

    INSERT INTO
      Timeseries
    SELECT
      CONVERT_TZ(thisDateInTimezone, dateNamedTimezone, 'UTC'),
      TIMESTAMPADD(MICROSECOND, -1, CONVERT_TZ(nextDateInTimezone, dateNamedTimezone, 'UTC')),
      CONVERT_TZ(thisDateInTimezone, dateNamedTimezone, 'UTC'),
      TIMESTAMPADD(MICROSECOND, -1, CONVERT_TZ(nextDateInTimezone, dateNamedTimezone, 'UTC')),
      Tools_GetIso8601StringFromDate(CONVERT_TZ(thisDateInTimezone, dateNamedTimezone, 'UTC'), 'UTC'),
      Tools_GetIso8601StringFromDate(TIMESTAMPADD(MICROSECOND, -1, CONVERT_TZ(nextDateInTimezone, dateNamedTimezone, 'UTC')), 'UTC'),

      thisDateInTimezone,
      TIMESTAMPADD(MICROSECOND, -1, nextDateInTimezone),
      Tools_GetIso8601StringFromDate(thisDateInTimezone, dateNamedTimezone),
      Tools_GetIso8601StringFromDate(TIMESTAMPADD(MICROSECOND, -1, nextDateInTimezone), dateNamedTimezone)
    ;

    SET thisDateInTimezone = nextDateInTimezone;
  UNTIL thisDateInTimezone >= endDateInTimezone
  END REPEAT;

END;


DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesFor;
CREATE PROCEDURE Tools_GenerateTimeseriesFor (
  sql_select TEXT,
  sql_from TEXT,
  sql_group TEXT,
  sql_from_date_column VARCHAR(1024),
  startDate TIMESTAMP,
  endDate TIMESTAMP,
  step INTEGER,
  resolution VARCHAR(10),
  include_partial_steps BOOL
)
BEGIN

  # Usage:
  # CALL some_warehouse.Tools_GenerateTimeseriesFor(
  #   /* sql_select = */             'COUNT(EventId) AS Fact_SomeEventHappened_Total',
  #   /* sql_from = */               'some_warehouse.Fact_SomeEventHappened',
  #   /* sql_group = */              NULL,
  #   /* sql_from_date_column = */   'Date',
  #   /* startDate = */              NULL,
  #   /* endDate = */                NULL,
  #   /* step = */                   1,
  #   /* resolution = */             'MONTH',
  #   /* include_partial_steps = */  FALSE
  # );

  SET @temporaryTableName = CONCAT('`', UUID(), '`');

  CALL Tools_GenerateTimeseriesFor_AndDepositToTempTable(
    @temporaryTableName,
    sql_select,
    sql_from,
    sql_group,
    sql_from_date_column,
    startDate,
    endDate,
    step,
    resolution,
    include_partial_steps
  );

  SET @sql_ReadTemporaryTable = CONCAT('SELECT * FROM ', @temporaryTableName, ';');
  PREPARE stmt FROM @sql_ReadTemporaryTable; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END;


DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesWithTimezoneFor;
CREATE PROCEDURE Tools_GenerateTimeseriesWithTimezoneFor (
  sql_select TEXT,
  sql_from TEXT,
  sql_group TEXT,
  sql_from_date_column VARCHAR(1024),
  dateNamedTimezone VARCHAR(128),
  startDate TIMESTAMP(6),
  endDate TIMESTAMP(6),
  step INTEGER,
  resolution VARCHAR(10),
  include_partial_steps BOOL
)
BEGIN

  # Usage:
  # CALL some_warehouse.Tools_GenerateTimeseriesFor(
  #   /* sql_select = */             'COUNT(EventId) AS Fact_SomeEventHappened_Total',
  #   /* sql_from = */               'some_warehouse.Fact_SomeEventHappened',
  #   /* sql_group = */              NULL,
  #   /* sql_from_date_column = */   'Date',
  #   /* dateNamedTimezone = */      'UTC',
  #   /* startDate = */              NULL,
  #   /* endDate = */                NULL,
  #   /* step = */                   1,
  #   /* resolution = */             'MONTH',
  #   /* include_partial_steps = */  FALSE
  # );

  SET @temporaryTableName = CONCAT('`', UUID(), '`');

  CALL Tools_GenerateTimeseriesWithTimezoneFor_AndDepositToTempTable(
    @temporaryTableName,
    sql_select,
    sql_from,
    sql_group,
    sql_from_date_column,
    dateNamedTimezone,
    startDate,
    endDate,
    step,
    resolution,
    include_partial_steps
  );

  SET @sql_ReadTemporaryTable = CONCAT('SELECT * FROM ', @temporaryTableName, ';');
  PREPARE stmt FROM @sql_ReadTemporaryTable; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END;


DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesWithTimezoneAndRunningAggFor;
CREATE PROCEDURE Tools_GenerateTimeseriesWithTimezoneAndRunningAggFor (
  sql_select TEXT,
  sql_from TEXT,
  sql_group TEXT,
  sql_from_date_column VARCHAR(1024),
  dateNamedTimezone VARCHAR(128),
  startDate TIMESTAMP(6),
  endDate TIMESTAMP(6),
  step INTEGER,
  resolution VARCHAR(10),
  include_partial_steps BOOL,
  sql_column_for_running_aggregate VARCHAR(1024)
)
BEGIN

  # Usage:
  # CALL some_warehouse.Tools_GenerateTimeseriesWithTimezoneAndRunningAggFor(
  #   /* sql_select = */             'COUNT(EventId) AS Fact_SomeEventHappened_Total',
  #   /* sql_from = */               'some_warehouse.Fact_SomeEventHappened',
  #   /* sql_group = */              NULL,
  #   /* sql_from_date_column = */   'Date',
  #   /* dateNamedTimezone = */      'UTC',
  #   /* startDate = */              NULL,
  #   /* endDate = */                NULL,
  #   /* step = */                   1,
  #   /* resolution = */             'MONTH',
  #   /* include_partial_steps = */  FALSE,
  #   /* sql_column_for_running_aggregate = */ 'Fact_SomeEventHappened_Total'
  # );

  SET @temporaryTableName = CONCAT('`', UUID(), '`');

  CALL Tools_GenerateTimeseriesWithTimezoneAndRunningAggFor_DepositTo(
    @temporaryTableName,
    sql_select,
    sql_from,
    sql_group,
    sql_from_date_column,
    dateNamedTimezone,
    startDate,
    endDate,
    step,
    resolution,
    include_partial_steps,
    sql_column_for_running_aggregate
  );

  SET @sql_ReadTemporaryTable = CONCAT('SELECT * FROM ', @temporaryTableName, ';');
  PREPARE stmt FROM @sql_ReadTemporaryTable; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END;


DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesFor_AndDepositToTempTable;
CREATE PROCEDURE Tools_GenerateTimeseriesFor_AndDepositToTempTable (
  deposit_in_table VARCHAR(64),
  sql_select TEXT,
  sql_from TEXT,
  sql_group TEXT,
  sql_from_date_column VARCHAR(1024),
  startDate TIMESTAMP,
  endDate TIMESTAMP,
  step INTEGER,
  resolution VARCHAR(10),
  include_partial_steps BOOL
)
this_proc:BEGIN

  # Usage:
  #   CALL some_warehouse.Tools_GenerateTimeseriesFor_AndDepositToTempTable(
  #     'SomeEventHappened_BySomeDimension_PerMonth',
  #     'SomeDimension, COUNT(EventId) AS SomeEventHappened_Total',
  #     'some_warehouse.Fact_SomeEventHappened',
  #     'SomeDimension',
  #     'Date',
  #     NULL,
  #     NULL,
  #     1,
  #     'MONTH',
  #     FALSE
  #   );

  CALL Tools_GenerateTimeseriesWithTimezoneFor_AndDepositToTempTable(
    deposit_in_table,
    sql_select,
    sql_from,
    sql_group,
    sql_from_date_column,
    'UTC',
    startDate,
    endDate,
    step,
    resolution,
    include_partial_steps
  );

END;


DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesWithTimezoneFor_AndDepositToTempTable;
CREATE PROCEDURE Tools_GenerateTimeseriesWithTimezoneFor_AndDepositToTempTable (
  deposit_in_table VARCHAR(64),
  sql_select TEXT,
  sql_from TEXT,
  sql_group TEXT,
  sql_from_date_column VARCHAR(1024),
  dateNamedTimezone VARCHAR(128),
  startDate TIMESTAMP(6),
  endDate TIMESTAMP(6),
  step INTEGER,
  resolution VARCHAR(10),
  include_partial_steps BOOL
)
this_proc:BEGIN

  # Usage:
  #   CALL some_warehouse.Tools_GenerateTimeseriesWithTimezoneFor_AndDepositToTempTable(
  #     /* deposit_in_table = */ 'SomeEventHappened_BySomeDimension_PerMonth',
  #     /* sql_select = */ 'SomeDimension, COUNT(EventId) AS SomeEventHappened_Total',
  #     /* sql_from = */ 'some_warehouse.Fact_SomeEventHappened',
  #     /* sql_group = */ 'SomeDimension',
  #     /* sql_from_date_column = */ 'Date',
  #     /* dateNamedTimezone = */ 'UTC',
  #     /* startDate = */ NULL,
  #     /* endDate = */ NULL,
  #     /* step = */ 1,
  #     /* resolution = */ 'MONTH',
  #     /* include_partial_steps = */ FALSE
  #   );

  DECLARE timestampColumnDateFormat VARCHAR(32);
  DECLARE timestampColumnName VARCHAR(32);
  DECLARE timestampColumnGroupByDateFormat VARCHAR(32);
  DECLARE sqlWhereClauseIncludePartialSteps VARCHAR(1024);
  DECLARE startDateInTimezone TIMESTAMP(6);
  DECLARE endDateInTimezone TIMESTAMP(6);
  DECLARE startDateInUtc TIMESTAMP(6);
  DECLARE endDateInUtc TIMESTAMP(6);

  IF sql_select IS NULL OR sql_select = '' THEN LEAVE this_proc; END IF;
  IF sql_from IS NULL OR sql_from = '' THEN LEAVE this_proc; END IF;

  SET startDateInTimezone = startDate;
  SET endDateInTimezone = endDate;

  IF startDateInTimezone IS NULL OR startDateInTimezone = '' THEN SET startDateInTimezone = STR_TO_DATE('2010-01-01 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f'); END IF;
  IF endDateInTimezone IS NULL OR endDateInTimezone = '' THEN SET endDateInTimezone = CONVERT_TZ(UTC_TIMESTAMP(), 'UTC', dateNamedTimezone); END IF;

  SET startDateInUtc = CONVERT_TZ(startDateInTimezone, dateNamedTimezone, 'UTC');
  SET endDateInUtc = CONVERT_TZ(endDateInTimezone, dateNamedTimezone, 'UTC');

  IF include_partial_steps IS NULL THEN SET include_partial_steps = TRUE; END IF;


  CALL Tools_GenerateTimeseriesWithTimezone(dateNamedTimezone, startDateInTimezone, endDateInTimezone, step, resolution);


  SELECT
    (CASE UPPER(resolution)
      WHEN 'MICROSECOND' THEN '%Y-%m-%d %H:%i:%s.%f'
      WHEN 'SECOND' THEN '%Y-%m-%d %H:%i:%s.000000'
      WHEN 'MINUTE' THEN '%Y-%m-%d %H:%i:00.000000'
      WHEN 'HOUR' THEN '%Y-%m-%d %H:00:00.000000'
      WHEN 'DAY' THEN '%Y-%m-%d 00:00:00.000000'
      WHEN 'WEEK' THEN 'WEEK resolution is not supported'
      WHEN 'MONTH' THEN '%Y-%m-01 00:00:00.000000'
      WHEN 'QUARTER' THEN 'QUARTER resolution is not supported'
      WHEN 'YEAR' THEN '%Y-01-01 00:00:00.000000'
      WHEN 'ALL' THEN '%Y-%m-%d %H:%i:%s.%f'
    END)
  INTO timestampColumnDateFormat;

  /*
  SELECT
    (CASE UPPER(resolution)
      WHEN 'MICROSECOND' THEN 'Date'
      WHEN 'SECOND' THEN 'Date'
      WHEN 'MINUTE' THEN 'Date'
      WHEN 'HOUR' THEN 'Date_Hour'
      WHEN 'DAY' THEN 'Date_Day'
      WHEN 'WEEK' THEN 'WEEK resolution is not supported'
      WHEN 'MONTH' THEN 'Date_Month'
      WHEN 'QUARTER' THEN 'Date_Quarter'
      WHEN 'YEAR' THEN 'Date_Year'
    END)
  INTO timestampColumnName;
  */
  SELECT
    'Date'
  INTO timestampColumnName;

  SELECT
    (CASE UPPER(resolution)
      WHEN 'MICROSECOND' THEN '%Y-%m-%d %H:%i:%s.%f'
      WHEN 'SECOND' THEN '%Y-%m-%d %H:%i:%s'
      WHEN 'MINUTE' THEN '%Y-%m-%d %H:%i'
      WHEN 'HOUR' THEN '%Y-%m-%d %H'
      WHEN 'DAY' THEN '%Y-%m-%d'
      WHEN 'WEEK' THEN 'WEEK resolution is not supported'
      WHEN 'MONTH' THEN '%Y-%m'
      WHEN 'QUARTER' THEN 'QUARTER resolution is not supported'
      WHEN 'YEAR' THEN '%Y'
      WHEN 'ALL' THEN '%Y-%m-%d %H:%i:%s.%f'
    END)
  INTO timestampColumnGroupByDateFormat;

  IF include_partial_steps = FALSE THEN
    # TODO: Also trunc the leading/first interval
    SET sqlWhereClauseIncludePartialSteps = CONCAT(' AND Timeseries.IntervalStartInTimezone < STR_TO_DATE("', endDateInTimezone, '", "%Y-%m-%d %H:%i:%s.%f") AND Timeseries.IntervalEndInTimezone < STR_TO_DATE("', endDateInTimezone, '", "%Y-%m-%d %H:%i:%s.%f")');
  ELSE
    SET sqlWhereClauseIncludePartialSteps = '';
  END IF;

  SET @sql_PrepareDepositTable = CONCAT(
      ' DROP TEMPORARY TABLE IF EXISTS ', deposit_in_table, ';'
  );
  PREPARE stmt FROM @sql_PrepareDepositTable; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  SET @sqlJoinOnTimeseriesAndAggregate = CONCAT(
    ' CREATE TEMPORARY TABLE IF NOT EXISTS ', deposit_in_table, ' AS (',
    '   SELECT ',
    '     Timeseries.*, ',
    '     Timeseries.IntervalStart AS ', timestampColumnName, ', ',
          sql_select,
    '   FROM ',
    '     Timeseries AS Timeseries ',
    '     LEFT JOIN ', sql_from ,' x ',
    '       ON x.', sql_from_date_column, ' >= STR_TO_DATE("', startDateInUtc, '", "%Y-%m-%d %H:%i:%s.%f") ',
    '         AND x.', sql_from_date_column, ' < STR_TO_DATE("', endDateInUtc, '", "%Y-%m-%d %H:%i:%s.%f") ',
    '         AND x.', sql_from_date_column, ' >= Timeseries.IntervalStart ',
    '         AND x.', sql_from_date_column, ' <= Timeseries.IntervalEnd ',
    '   WHERE ',
    '     1 = 1 ',
          sqlWhereClauseIncludePartialSteps,
    '   GROUP BY ',
    '     DATE_FORMAT(Timeseries.IntervalStart, "', timestampColumnGroupByDateFormat, '")',
          IF(sql_group IS NOT NULL AND sql_group != '', CONCAT(', ', sql_group), ''),
    '   ORDER BY ',
          timestampColumnName,
    ' ); '
  );
  # DEBUG
  # SELECT @sqlJoinOnTimeseriesAndAggregate;

  PREPARE stmt FROM @sqlJoinOnTimeseriesAndAggregate; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END;



DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesWithTimezoneAndRunningAggFor_DepositTo;
CREATE PROCEDURE Tools_GenerateTimeseriesWithTimezoneAndRunningAggFor_DepositTo (
  deposit_in_table VARCHAR(64),
  sql_select TEXT,
  sql_from TEXT,
  sql_group TEXT,
  sql_from_date_column VARCHAR(1024),
  dateNamedTimezone VARCHAR(128),
  startDate TIMESTAMP(6),
  endDate TIMESTAMP(6),
  step INTEGER,
  resolution VARCHAR(10),
  include_partial_steps BOOL,
  sql_column_for_running_aggregate VARCHAR(1024)
)
this_proc:BEGIN

  # Usage:
  #   CALL some_warehouse.Tools_GenerateTimeseriesWithTimezoneFor_AndDepositToTempTable(
  #     /* deposit_in_table = */       'SomeEventHappened_BySomeDimension_PerMonth',
  #     /* sql_select = */             'SomeDimension, COUNT(EventId) AS SomeEventHappened_Total',
  #     /* sql_from = */               'some_warehouse.Fact_SomeEventHappened',
  #     /* sql_group = */              'SomeDimension',
  #     /* sql_from_date_column = */   'Date',
  #     /* dateNamedTimezone = */      'UTC',
  #     /* startDate = */              NULL,
  #     /* endDate = */                NULL,
  #     /* step = */                   1,
  #     /* resolution = */             'MONTH',
  #     /* include_partial_steps = */  FALSE,
  #     /* sql_column_for_running_aggregate = */ NULL
  #   );

  DECLARE timestampColumnDateFormat VARCHAR(32);
  DECLARE timestampColumnName VARCHAR(32);
  DECLARE timestampColumnGroupByDateFormat VARCHAR(32);
  DECLARE sqlWhereClauseIncludePartialSteps VARCHAR(1024);
  DECLARE sqlRunningAggregateGroupSelector VARCHAR(1024);
  DECLARE startDateInTimezone TIMESTAMP(6);
  DECLARE endDateInTimezone TIMESTAMP(6);
  DECLARE startDateInUtc TIMESTAMP(6);
  DECLARE endDateInUtc TIMESTAMP(6);

  IF sql_select IS NULL OR sql_select = '' THEN LEAVE this_proc; END IF;
  IF sql_from IS NULL OR sql_from = '' THEN LEAVE this_proc; END IF;

  SET startDateInTimezone = startDate;
  SET endDateInTimezone = endDate;

  IF startDateInTimezone IS NULL OR startDateInTimezone = '' THEN SET startDateInTimezone = STR_TO_DATE('2010-01-01 00:00:00.000000', '%Y-%m-%d %H:%i:%s.%f'); END IF;
  IF endDateInTimezone IS NULL OR endDateInTimezone = '' THEN SET endDateInTimezone = CONVERT_TZ(UTC_TIMESTAMP(), 'UTC', dateNamedTimezone); END IF;

  SET startDateInUtc = CONVERT_TZ(startDateInTimezone, dateNamedTimezone, 'UTC');
  SET endDateInUtc = CONVERT_TZ(endDateInTimezone, dateNamedTimezone, 'UTC');

  IF include_partial_steps IS NULL THEN SET include_partial_steps = TRUE; END IF;

  IF sql_column_for_running_aggregate IS NULL THEN SET sql_column_for_running_aggregate = 'Total'; END IF;


  CALL Tools_GenerateTimeseriesWithTimezone(dateNamedTimezone, startDateInTimezone, endDateInTimezone, step, resolution);

  # DEBUG
  # SELECT * FROM Timeseries;

  SELECT
    (CASE UPPER(resolution)
      WHEN 'MICROSECOND' THEN '%Y-%m-%d %H:%i:%s.%f'
      WHEN 'SECOND' THEN '%Y-%m-%d %H:%i:%s.000000'
      WHEN 'MINUTE' THEN '%Y-%m-%d %H:%i:00.000000'
      WHEN 'HOUR' THEN '%Y-%m-%d %H:00:00.000000'
      WHEN 'DAY' THEN '%Y-%m-%d 00:00:00.000000'
      WHEN 'WEEK' THEN 'WEEK resolution is not supported'
      WHEN 'MONTH' THEN '%Y-%m-01 00:00:00.000000'
      WHEN 'QUARTER' THEN 'QUARTER resolution is not supported'
      WHEN 'YEAR' THEN '%Y-01-01 00:00:00.000000'
      WHEN 'ALL' THEN '%Y-%m-%d %H:%i:%s.%f'
    END)
  INTO timestampColumnDateFormat;

  /*
  SELECT
    (CASE UPPER(resolution)
      WHEN 'MICROSECOND' THEN 'Date'
      WHEN 'SECOND' THEN 'Date'
      WHEN 'MINUTE' THEN 'Date'
      WHEN 'HOUR' THEN 'Date_Hour'
      WHEN 'DAY' THEN 'Date_Day'
      WHEN 'WEEK' THEN 'WEEK resolution is not supported'
      WHEN 'MONTH' THEN 'Date_Month'
      WHEN 'QUARTER' THEN 'Date_Quarter'
      WHEN 'YEAR' THEN 'Date_Year'
    END)
  INTO timestampColumnName;
  */
  SELECT
    'Date'
  INTO timestampColumnName;

  SELECT
    (CASE UPPER(resolution)
      WHEN 'MICROSECOND' THEN '%Y-%m-%d %H:%i:%s.%f'
      WHEN 'SECOND' THEN '%Y-%m-%d %H:%i:%s'
      WHEN 'MINUTE' THEN '%Y-%m-%d %H:%i'
      WHEN 'HOUR' THEN '%Y-%m-%d %H'
      WHEN 'DAY' THEN '%Y-%m-%d'
      WHEN 'WEEK' THEN 'WEEK resolution is not supported'
      WHEN 'MONTH' THEN '%Y-%m'
      WHEN 'QUARTER' THEN 'QUARTER resolution is not supported'
      WHEN 'YEAR' THEN '%Y'
      WHEN 'ALL' THEN '%Y-%m-%d %H:%i:%s.%f'
    END)
  INTO timestampColumnGroupByDateFormat;

  IF include_partial_steps = FALSE THEN
    # TODO: Also trunc the leading/first interval
    SET sqlWhereClauseIncludePartialSteps = CONCAT(' AND Timeseries.IntervalStartInTimezone < STR_TO_DATE("', endDateInTimezone, '", "%Y-%m-%d %H:%i:%s.%f") AND Timeseries.IntervalEndInTimezone < STR_TO_DATE("', endDateInTimezone, '", "%Y-%m-%d %H:%i:%s.%f")');
  ELSE
    SET sqlWhereClauseIncludePartialSteps = '';
  END IF;

  IF sql_group IS NOT NULL THEN
    SET sqlRunningAggregateGroupSelector = CONCAT(
      ' (CASE ',
      '   WHEN ', SUBSTRING_INDEX(sql_group, ',', 1), ' IS NULL THEN "NULL"',
      '   WHEN ', SUBSTRING_INDEX(sql_group, ',', 1), ' = "" THEN "BLANK"',
      '   ELSE ', SUBSTRING_INDEX(sql_group, ',', 1),
      ' END) '
    );
  ELSE
    SET sqlRunningAggregateGroupSelector = '""';
  END IF;

  SET @sql_PrepareDepositTable = CONCAT(
      ' DROP TEMPORARY TABLE IF EXISTS ', deposit_in_table, ';'
  );
  PREPARE stmt FROM @sql_PrepareDepositTable; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  SET @sqlJoinOnTimeseriesAndAggregate = CONCAT(
    ' CREATE TEMPORARY TABLE IF NOT EXISTS ', deposit_in_table, ' AS (',

    ' SELECT ',
    '   AggregatedTimeseriesWithRunningAggs.* ',
    ' FROM ( ',

    '   SELECT ',
    '     AggregatedTimeseries.*, ',
    '     @runningAgg_Total AS BaseRunningTotal, ',
    # TODO: Add PercentIncrease
    '     @runningAgg_Total := IF(@runningAgg_Group = ', sqlRunningAggregateGroupSelector ,', @runningAgg_Total, 0) + AggregatedTimeseries.', sql_column_for_running_aggregate ,' AS RunningTotal, ',
    '     @runningAgg_Group := ', sqlRunningAggregateGroupSelector ,' AS RunningAggregate_Group ',
    '   FROM ( ',

    '     SELECT ',
    '       Timeseries.*, ',
    '       Timeseries.IntervalStart AS ', timestampColumnName, ', ',
            sql_select,
    '     FROM ',
    '       Timeseries AS Timeseries ',
    '       LEFT JOIN ', sql_from ,' x ',
    '         ON x.', sql_from_date_column, ' >= STR_TO_DATE("', startDateInUtc, '", "%Y-%m-%d %H:%i:%s.%f") ',
    '           AND x.', sql_from_date_column, ' < STR_TO_DATE("', endDateInUtc, '", "%Y-%m-%d %H:%i:%s.%f") ',
    '           AND x.', sql_from_date_column, ' >= Timeseries.IntervalStart ',
    '           AND x.', sql_from_date_column, ' <= Timeseries.IntervalEnd ',
    '     WHERE ',
    '       1 = 1 ',
            sqlWhereClauseIncludePartialSteps,
    '     GROUP BY ',
    '       DATE_FORMAT(Timeseries.IntervalStart, "', timestampColumnGroupByDateFormat, '")',
            IF(sql_group IS NOT NULL AND sql_group != '', CONCAT(', ', sql_group), ''),
    '     ORDER BY ',
            timestampColumnName,
            IF(sql_group IS NOT NULL AND sql_group != '', CONCAT(', ', sql_group), ''),

    '    ) AggregatedTimeseries ',
    '    CROSS JOIN ',
    '      (SELECT @runningAgg_Group := "", @runningAgg_Total := 0) AS RunningAggregates ',
    '    ORDER BY ',
           IF(sql_group IS NOT NULL AND sql_group != '', CONCAT(sql_group, ', '), ''),
    '      Date ',

    ' ) AS AggregatedTimeseriesWithRunningAggs ',
    ' ORDER BY ',
    '  Date ',


    ' ); '
  );
  # DEBUG
  # SELECT @sqlJoinOnTimeseriesAndAggregate;

  PREPARE stmt FROM @sqlJoinOnTimeseriesAndAggregate; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END;


DROP PROCEDURE IF EXISTS Tools_PivotDataset_WithDefaultMissingValue;
CREATE PROCEDURE Tools_PivotDataset_WithDefaultMissingValue (
   from_table VARCHAR(64),
   columns_to_anchor_on TEXT,
   column_to_pivot_on VARCHAR(1024),
   sql_aggregate_function VARCHAR(32),
   column_to_aggregate VARCHAR(1024),
   columns_to_order_by TEXT,
   pivot_missing_column_default_value VARCHAR(1024)
)
BEGIN

  /*
  USAGE:

  CALL some_warehouse.Tools_GenerateTimeseriesFor_AndDepositToTempTable(
    'SomeEventHappened_ByMonth',
    'SomeDimension, COUNT(EventId) AS SomeEventHappened_Total',
    'some_warehouse.Fact_SomeEventHappened',
    'SomeDimension',
    'Date',
    NULL,
    NULL,
    1,
    'MONTH',
    FALSE
  );

  CALL Tools_PivotDataset_WithDefaultMissingValue(
    'SomeEventHappened_ByMonth',
    'Date',
    'SomeDimension',
    'SUM',
    'SomeEventHappened_Total',
    NULL,
    '0'
  );
   */

  SET SESSION group_concat_max_len = 1000000;

  IF columns_to_order_by IS NULL OR columns_to_order_by = '' THEN SET columns_to_order_by = columns_to_anchor_on; END IF;

  SET @sql_AggregateOnPivotColumn = 'UNINITIALIZED';

  # TODO: column_to_pivot_on value needs to be sanitized to only contain valid sql alias characters
  SET @sql_GenerateSqlFor_sql_AggregateOnPivotColumn = CONCAT(
    'SELECT
      GROUP_CONCAT(DISTINCT
        CONCAT(
          \' ', sql_aggregate_function, '(CASE \',
            \' WHEN ', column_to_pivot_on, ' = "\', ', column_to_pivot_on, ', \'" \', \' THEN ', column_to_aggregate, ' \',
          \' ELSE ', pivot_missing_column_default_value ,' END) AS "\',
          (CASE WHEN ', column_to_pivot_on, ' = \'\' THEN \'blank\' ELSE ', column_to_pivot_on, ' END),
          \'"\'
        )
      )
    INTO
      @sql_AggregateOnPivotColumn
    FROM
      ', from_table, ';'
  );
  PREPARE stmt FROM @sql_GenerateSqlFor_sql_AggregateOnPivotColumn; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  SET @sqlPivot = CONCAT(
  'SELECT
    ', columns_to_anchor_on, ', ',
    @sql_AggregateOnPivotColumn,
    ', ', sql_aggregate_function, '(CASE WHEN ', column_to_pivot_on, ' IS NOT NULL THEN ', column_to_aggregate, ' ELSE 0 END) AS "Total" ',
    ', ', sql_aggregate_function, '(CASE WHEN ', column_to_pivot_on, ' IS NULL THEN ', column_to_aggregate, ' ELSE 0 END) AS "NullValue" ',
  ' FROM
    ', from_table, '
  GROUP BY
    ', columns_to_anchor_on, '
  ORDER BY
    ', columns_to_order_by, ';'
  );
  PREPARE stmt FROM @sqlPivot; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END;


DROP PROCEDURE IF EXISTS Tools_PivotDataset;
CREATE PROCEDURE Tools_PivotDataset (
   from_table VARCHAR(64),
   columns_to_anchor_on TEXT,
   column_to_pivot_on VARCHAR(1024),
   sql_aggregate_function VARCHAR(32),
   column_to_aggregate VARCHAR(1024),
   columns_to_order_by TEXT
)
BEGIN

  /*
  USAGE:

  CALL some_warehouse.Tools_GenerateTimeseriesFor_AndDepositToTempTable(
    'SomeEventHappened_ByMonth',
    'SomeDimension, COUNT(EventId) AS SomeEventHappened_Total',
    'some_warehouse.Fact_SomeEventHappened',
    'SomeDimension',
    'Date',
    NULL,
    NULL,
    1,
    'MONTH',
    FALSE
  );

  CALL Tools_PivotDataset(
    'SomeEventHappened_ByMonth',
    'Date',
    'SomeDimension',
    'SUM',
    'SomeEventHappened_Total',
    NULL
  );
   */

  CALL Tools_PivotDataset_WithDefaultMissingValue(
   from_table,
   columns_to_anchor_on,
   column_to_pivot_on,
   sql_aggregate_function,
   column_to_aggregate,
   columns_to_order_by,
   0
  );

END;



DROP PROCEDURE IF EXISTS Tools_PivotDataset_FromQuery;
CREATE PROCEDURE Tools_PivotDataset_FromQuery (
   sql_query TEXT,
   columns_to_anchor_on TEXT,
   column_to_pivot_on VARCHAR(1024),
   sql_aggregate_function VARCHAR(32),
   column_to_aggregate VARCHAR(1024),
   columns_to_order_by TEXT,
   pivot_missing_column_default_value VARCHAR(1024)
)
BEGIN

#   USAGE:
#
#   CALL some_warehouse.Tools_GenerateTimeseriesFor_AndDepositToTempTable(
#     'SomeEventHappened_ByMonth',
#     'SomeDimension, COUNT(EventId) AS SomeEventHappened_Total',
#     'some_warehouse.Fact_SomeEventHappened',
#     'SomeDimension',
#     'Date',
#     NULL,
#     NULL,
#     1,
#     'MONTH',
#     FALSE
#   );
#
#   CALL Tools_PivotDataset_FromQuery(
#     /* sql_query = */ "(SELECT * FROM SomeEventHappened_ByMonth)",
#     /* columns_to_anchor_on = */ 'Date',
#     /* column_to_pivot_on = */ 'SomeDimension',
#     /* sql_aggregate_function = */ 'SUM',
#     /* column_to_aggregate = */ 'SomeEventHappened_Total',
#     /* columns_to_order_by = */ NULL,
#     /* pivot_missing_column_default_value = */ '0'
#   );

  SET @temporaryTableName = CONCAT('`', UUID(), '`');


  SET @sql_DropTempTable = CONCAT(
      ' DROP TEMPORARY TABLE IF EXISTS ', @temporaryTableName, ';'
  );
  PREPARE stmt FROM @sql_DropTempTable; EXECUTE stmt; DEALLOCATE PREPARE stmt;


  SET @sql_CreateTempTable = CONCAT(
    ' CREATE TEMPORARY TABLE ', @temporaryTableName, ' AS (',
        sql_query,
    ' ); '
  );

  PREPARE stmt FROM @sql_CreateTempTable; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  CALL Tools_PivotDataset_WithDefaultMissingValue(
    @temporaryTableName,
    columns_to_anchor_on,
    column_to_pivot_on,
    sql_aggregate_function,
    column_to_aggregate,
    columns_to_order_by,
    pivot_missing_column_default_value
  );

END;


DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesFor_AndPivot;
CREATE PROCEDURE Tools_GenerateTimeseriesFor_AndPivot (
  Timeseries_sql_select TEXT,
  Timeseries_sql_from TEXT,
  Timeseries_sql_group TEXT,
  Timeseries_sql_from_date_column VARCHAR(1024),
  Timeseries_startDate TIMESTAMP,
  Timeseries_endDate TIMESTAMP,
  Timeseries_step INTEGER,
  Timeseries_resolution VARCHAR(10),
  Timeseries_include_partial_steps BOOL,

  Pivot_columns_to_anchor_on TEXT,
  Pivot_column_to_pivot_on VARCHAR(1024),
  Pivot_sql_aggregate_function VARCHAR(32),
  Pivot_column_to_aggregate VARCHAR(1024),
  Pivot_columns_to_order_by TEXT
)
BEGIN

  # Usage:
  # CALL Tools_GenerateTimeseriesFor_AndPivot(
  #   /* Timeseries_sql_select = */             'SomeDimension, COUNT(EventId) AS SomeEventHappened_Total',
  #   /* Timeseries_sql_from = */               'some_warehouse.Fact_SomeEventHappened',
  #   /* Timeseries_sql_group = */              'SomeDimension',
  #   /* Timeseries_sql_from_date_column = */   'Date',
  #   /* Timeseries_startDate = */              NULL,
  #   /* Timeseries_endDate = */                NULL,
  #   /* Timeseries_step = */                   1,
  #   /* Timeseries_resolution = */             'MONTH',
  #   /* Timeseries_include_partial_steps = */  FALSE,
  #
  #   /* Pivot_columns_to_anchor_on = */        'Date',
  #   /* Pivot_column_to_pivot_on = */          'SomeDimension',
  #   /* Pivot_sql_aggregate_function = */      'SUM',
  #   /* Pivot_column_to_aggregate = */         'SomeEventHappened_Total',
  #   /* Pivot_columns_to_order_by = */         NULL
  # )

  CALL Tools_GenerateTimeseriesWithTimezoneFor_AndPivot(
    Timeseries_sql_select,
    Timeseries_sql_from,
    Timeseries_sql_group,
    Timeseries_sql_from_date_column,
    'UTC',
    Timeseries_startDate,
    Timeseries_endDate,
    Timeseries_step,
    Timeseries_resolution,
    Timeseries_include_partial_steps,
    Pivot_columns_to_anchor_on,
    Pivot_column_to_pivot_on,
    Pivot_sql_aggregate_function,
    Pivot_column_to_aggregate,
    Pivot_columns_to_order_by
  );

END;


DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesWithTimezoneFor_AndPivot;
CREATE PROCEDURE Tools_GenerateTimeseriesWithTimezoneFor_AndPivot (
  Timeseries_sql_select TEXT,
  Timeseries_sql_from TEXT,
  Timeseries_sql_group TEXT,
  Timeseries_sql_from_date_column VARCHAR(1024),
  Timeseries_dateNamedTimezone VARCHAR(128),
  Timeseries_startDate TIMESTAMP(6),
  Timeseries_endDate TIMESTAMP(6),
  Timeseries_step INTEGER,
  Timeseries_resolution VARCHAR(10),
  Timeseries_include_partial_steps BOOL,

  Pivot_columns_to_anchor_on TEXT,
  Pivot_column_to_pivot_on VARCHAR(1024),
  Pivot_sql_aggregate_function VARCHAR(32),
  Pivot_column_to_aggregate VARCHAR(1024),
  Pivot_columns_to_order_by TEXT
)
BEGIN

  # Usage:
  # CALL Tools_GenerateTimeseriesWithTimezoneFor_AndPivot(
  #   /* Timeseries_sql_select = */             'SomeDimension, COUNT(EventId) AS SomeEventHappened_Total',
  #   /* Timeseries_sql_from = */               'some_warehouse.Fact_SomeEventHappened',
  #   /* Timeseries_sql_group = */              'SomeDimension',
  #   /* Timeseries_sql_from_date_column = */   'Date',
  #   /* Timeseries_dateNamedTimezone = */      'UTC',
  #   /* Timeseries_startDate = */              NULL,
  #   /* Timeseries_endDate = */                NULL,
  #   /* Timeseries_step = */                   1,
  #   /* Timeseries_resolution = */             'MONTH',
  #   /* Timeseries_include_partial_steps = */  FALSE,
  #
  #   /* Pivot_columns_to_anchor_on = */        'Date',
  #   /* Pivot_column_to_pivot_on = */          'SomeDimension',
  #   /* Pivot_sql_aggregate_function = */      'SUM',
  #   /* Pivot_column_to_aggregate = */         'SomeEventHappened_Total',
  #   /* Pivot_columns_to_order_by = */         NULL
  # )

  SET @temporaryTableName = CONCAT('`', UUID(), '`');

  CALL Tools_GenerateTimeseriesWithTimezoneFor_AndDepositToTempTable(
    @temporaryTableName,
    Timeseries_sql_select,
    Timeseries_sql_from,
    Timeseries_sql_group,
    Timeseries_sql_from_date_column,
    Timeseries_dateNamedTimezone,
    Timeseries_startDate,
    Timeseries_endDate,
    Timeseries_step,
    Timeseries_resolution,
    Timeseries_include_partial_steps
  );

  CALL Tools_PivotDataset(@temporaryTableName, Pivot_columns_to_anchor_on, Pivot_column_to_pivot_on, Pivot_sql_aggregate_function, Pivot_column_to_aggregate, Pivot_columns_to_order_by);

END;


DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesFor_AndTimeshift;
CREATE PROCEDURE Tools_GenerateTimeseriesFor_AndTimeshift (
  Timeseries_sql_select TEXT,
  Timeseries_sql_from TEXT,
  Timeseries_sql_group TEXT,
  Timeseries_sql_from_date_column VARCHAR(1024),
  Timeseries_step INTEGER,
  Timeseries_resolution VARCHAR(10),
  Timeseries_include_partial_steps BOOL,

  Timeshift_period1_Name VARCHAR(256),
  Timeshift_period1_StartDate TIMESTAMP,
  Timeshift_period1_EndDate TIMESTAMP,

  Timeshift_period2_Name VARCHAR(256),
  Timeshift_period2_StartDate TIMESTAMP,
  Timeshift_period2_EndDate TIMESTAMP,

  Join_data_column_name VARCHAR(256)
)
BEGIN

  # Usage:
  # CALL Tools_GenerateTimeseriesFor_AndTimeshift (
  #   /* Timeseries_sql_select = */             'COUNT(EventId) AS SomeEventHappened_Total',
  #   /* Timeseries_sql_from = */               'some_warehouse.Fact_SomeEventHappened',
  #   /* Timeseries_sql_group = */              NULL,
  #   /* Timeseries_sql_from_date_column = */   'Date',
  #   /* Timeseries_step = */                   1,
  #   /* Timeseries_resolution = */             'DAY',
  #   /* Timeseries_include_partial_steps = */  FALSE,
  #
  #   /* Timeshift_period1_Name = */           'PastMonth',
  #   /* Timeshift_period1_StartDate = */      DATE_SUB(DATE(NOW()), INTERVAL 4 WEEK),
  #   /* Timeshift_period1_EndDate = */        DATE(NOW()),
  #
  #   /* Timeshift_period2_Name = */           'PastMonthTimeshiftFourWeeks',
  #   /* Timeshift_period2_StartDate = */      DATE_SUB(DATE(NOW()), INTERVAL 8 WEEK),
  #   /* Timeshift_period2_EndDate = */        DATE_SUB(DATE(NOW()), INTERVAL 4 WEEK),
  #
  #   /* Join_data_column_name = */            'SomeEventHappened_Total'
  # );

  CALL Tools_GenerateTimeseriesWithTimezoneFor_AndTimeshift (
    /* Timeseries_sql_select = */             Timeseries_sql_select,
    /* Timeseries_sql_from = */               Timeseries_sql_from,
    /* Timeseries_sql_group = */              Timeseries_sql_group,
    /* Timeseries_sql_from_date_column = */   Timeseries_sql_from_date_column,
    /* Timeseries_periodNamedTimezone = */    'UTC',
    /* Timeseries_step = */                   Timeseries_step,
    /* Timeseries_resolution = */             Timeseries_resolution,
    /* Timeseries_include_partial_steps = */  Timeseries_include_partial_steps,

    /* Timeshift_period1_Name = */           Timeshift_period1_Name,
    /* Timeshift_period1_StartDate = */      Timeshift_period1_StartDate,
    /* Timeshift_period1_EndDate = */        Timeshift_period1_EndDate,

    /* Timeshift_period2_Name = */           Timeshift_period2_Name,
    /* Timeshift_period2_StartDate = */      Timeshift_period2_StartDate,
    /* Timeshift_period2_EndDate = */        Timeshift_period2_EndDate,

    /* Join_data_column_name = */            Join_data_column_name
  );

END;


DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesWithTimezoneFor_AndTimeshift;
CREATE PROCEDURE Tools_GenerateTimeseriesWithTimezoneFor_AndTimeshift (
  Timeseries_sql_select TEXT,
  Timeseries_sql_from TEXT,
  Timeseries_sql_group TEXT,
  Timeseries_sql_from_date_column VARCHAR(1024),
  Timeseries_periodNamedTimezone VARCHAR(128),
  Timeseries_step INTEGER,
  Timeseries_resolution VARCHAR(10),
  Timeseries_include_partial_steps BOOL,

  Timeshift_period1_Name VARCHAR(256),
  Timeshift_period1_StartDate TIMESTAMP,
  Timeshift_period1_EndDate TIMESTAMP,

  Timeshift_period2_Name VARCHAR(256),
  Timeshift_period2_StartDate TIMESTAMP,
  Timeshift_period2_EndDate TIMESTAMP,

  Join_data_column_name VARCHAR(256)
)
BEGIN

  # Usage:
  # CALL Tools_GenerateTimeseriesWithTimezoneFor_AndTimeshift (
  #   /* Timeseries_sql_select = */             'COUNT(EventId) AS SomeEventHappened_Total',
  #   /* Timeseries_sql_from = */               'some_warehouse.Fact_SomeEventHappened',
  #   /* Timeseries_sql_group = */              NULL,
  #   /* Timeseries_sql_from_date_column = */   'Date',
  #   /* Timeseries_periodNamedTimezone = */    'UTC',
  #   /* Timeseries_step = */                   1,
  #   /* Timeseries_resolution = */             'DAY',
  #   /* Timeseries_include_partial_steps = */  FALSE,
  #
  #   /* Timeshift_period1_Name = */           'PastMonth',
  #   /* Timeshift_period1_StartDate = */      DATE_SUB(DATE(NOW()), INTERVAL 4 WEEK),
  #   /* Timeshift_period1_EndDate = */        DATE(NOW()),
  #
  #   /* Timeshift_period2_Name = */           'PastMonthTimeshiftFourWeeks',
  #   /* Timeshift_period2_StartDate = */      DATE_SUB(DATE(NOW()), INTERVAL 8 WEEK),
  #   /* Timeshift_period2_EndDate = */        DATE_SUB(DATE(NOW()), INTERVAL 4 WEEK),
  #
  #   /* Join_data_column_name = */            'SomeEventHappened_Total'
  # );

  CALL some_warehouse.Tools_GenerateTimeseriesWithTimezoneFor_AndDepositToTempTable(
    /* deposit_in_table = */       Timeshift_period1_Name,
    /* sql_select = */             Timeseries_sql_select,
    /* sql_from = */               Timeseries_sql_from,
    /* sql_group = */              Timeseries_sql_group,
    /* sql_from_date_column = */   Timeseries_sql_from_date_column,
    /* dateNamedTimezone = */      Timeseries_periodNamedTimezone,
    /* startDate = */              Timeshift_period1_StartDate,
    /* endDate = */                Timeshift_period1_EndDate,
    /* step = */                   Timeseries_step,
    /* resolution = */             Timeseries_resolution,
    /* include_partial_steps = */  Timeseries_include_partial_steps
  );

  CALL some_warehouse.Tools_GenerateTimeseriesWithTimezoneFor_AndDepositToTempTable(
    /* deposit_in_table = */       Timeshift_period2_Name,
    /* sql_select = */             Timeseries_sql_select,
    /* sql_from = */               Timeseries_sql_from,
    /* sql_group = */              Timeseries_sql_group,
    /* sql_from_date_column = */   Timeseries_sql_from_date_column,
    /* dateNamedTimezone = */      Timeseries_periodNamedTimezone,
    /* startDate = */              Timeshift_period2_StartDate,
    /* endDate = */                Timeshift_period2_EndDate,
    /* step = */                   Timeseries_step,
    /* resolution = */             Timeseries_resolution,
    /* include_partial_steps = */  Timeseries_include_partial_steps
  );


SET @sql_JoinTimeshiftPeriods = CONCAT(
'
SELECT
  ', Timeshift_period1_Name, '.Date AS ', Timeshift_period1_Name, '_Interval_Start_Date,
  ', Timeshift_period1_Name, '.IntervalStartInTimezoneIso8601 AS ', Timeshift_period1_Name, '_Interval_Start_Date_InTimezoneIso8601,
  ', Timeshift_period1_Name, '.', Join_data_column_name, ' AS ', Join_data_column_name, ',
  ', Timeshift_period2_Name, '.Date AS ', Timeshift_period2_Name, '_Interval_Start_Date,
  ', Timeshift_period2_Name, '.IntervalStartInTimezoneIso8601 AS ', Timeshift_period2_Name, '_Interval_Start_Date_InTimezoneIso8601,
  ', Timeshift_period2_Name, '.', Join_data_column_name, ' AS ', Join_data_column_name, '_', Timeshift_period2_Name, ',
  (', Timeshift_period1_Name, '.', Join_data_column_name, ' - ', Timeshift_period2_Name, '.', Join_data_column_name, ') AS Delta_Quantity,
  (CASE
    WHEN ', Timeshift_period1_Name, '.', Join_data_column_name, ' = 0 AND ', Timeshift_period2_Name, '.', Join_data_column_name, ' = 0 THEN 0
    WHEN ', Timeshift_period2_Name, '.', Join_data_column_name, ' = 0 THEN 100
    ELSE
    ROUND(
      (100 * (', Timeshift_period1_Name, '.', Join_data_column_name, ' - ', Timeshift_period2_Name, '.', Join_data_column_name, ')) / ', Timeshift_period2_Name, '.', Join_data_column_name, ',
      2
    )
  END
  ) AS Delta_Percent
FROM
  (SELECT @', Timeshift_period1_Name, '_RowNum := @', Timeshift_period1_Name, '_RowNum + 1 AS TimeseriesBucketIndex, x.* FROM (SELECT * FROM ', Timeshift_period1_Name, ') AS x, (SELECT @', Timeshift_period1_Name, '_RowNum := 0) AS r ORDER BY Date) AS ', Timeshift_period1_Name, '
  LEFT JOIN (SELECT @', Timeshift_period2_Name, '_RowNum := @', Timeshift_period2_Name, '_RowNum + 1 AS TimeseriesBucketIndex, x.* FROM (SELECT * FROM ', Timeshift_period2_Name, ') AS x, (SELECT @', Timeshift_period2_Name, '_RowNum := 0) AS r ORDER BY Date) AS ', Timeshift_period2_Name, '
    ON ', Timeshift_period1_Name, '.TimeseriesBucketIndex = ', Timeshift_period2_Name, '.TimeSeriesBucketIndex
ORDER BY
  ', Timeshift_period1_Name, '.Date;
'
);

PREPARE stmt FROM @sql_JoinTimeshiftPeriods; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END;



DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesWithTargetComparisonFor;
CREATE PROCEDURE Tools_GenerateTimeseriesWithTargetComparisonFor (
  dateNamedTimezone VARCHAR(128),
  current_sql_select TEXT,
  sql_column_for_current_join VARCHAR(64),
  base_sql_select TEXT,
  sql_column_for_base_join VARCHAR(64),
  target_sql_select TEXT,
  sql_column_for_target_join VARCHAR(64)
)
BEGIN

  # Usage:
  #
  # CALL Tools_GenerateTimeseriesWithTargetComparisonFor(
  #   /* dateNamedTimezone = */            'UTC',
  #   /* current_sql_select = */           "",
  #   /* sql_column_for_current_join = */  'AnchorDate',
  #   /* base_sql_select = */              "",
  #   /* sql_column_for_base_join = */     'AnchorDate',
  #   /* target_sql_select = */            "",
  #   /* sql_column_for_target_join = */   'AnchorDate'
  # );

  SET @temporaryTableName = CONCAT('`', UUID(), '`');

  CALL Tools_GenerateTimeseriesWithTargetComparisonFor_DepositTo(
    @temporaryTableName,
    dateNamedTimezone,
    current_sql_select,
    sql_column_for_current_join,
    base_sql_select,
    sql_column_for_base_join,
    target_sql_select,
    sql_column_for_target_join
  );

  SET @sql_ReadTemporaryTable = CONCAT('SELECT * FROM ', @temporaryTableName, ';');
  PREPARE stmt FROM @sql_ReadTemporaryTable; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END;

DROP PROCEDURE IF EXISTS Tools_GenerateTimeseriesWithTargetComparisonFor_DepositTo;
CREATE PROCEDURE Tools_GenerateTimeseriesWithTargetComparisonFor_DepositTo (
  deposit_in_table VARCHAR(64),
  dateNamedTimezone VARCHAR(128),
  current_sql_select TEXT,
  sql_column_for_current_join VARCHAR(64),
  base_sql_select TEXT,
  sql_column_for_base_join VARCHAR(64),
  target_sql_select TEXT,
  sql_column_for_target_join VARCHAR(64)
)
BEGIN

  SET @sql_PrepareDepositTable = CONCAT(
      ' DROP TEMPORARY TABLE IF EXISTS ', deposit_in_table, ';'
  );
  PREPARE stmt FROM @sql_PrepareDepositTable; EXECUTE stmt; DEALLOCATE PREPARE stmt;

  SET @sql_JoinCurrentToBaseToTarget = CONCAT(
    ' CREATE TEMPORARY TABLE IF NOT EXISTS ', deposit_in_table, ' AS (',

    '    SELECT ',
    '      Tools_TruncateDate(Current.Date, "MONTH") AS DisplayDate, ',
    '      Current.Date AS CurrentDate, ',
    '      Current.Value AS CurrentValue, ',
    '      Current.Title AS CurrentTitle, ',

    '      Base.Date AS BaseDate, ',
    '      Base.Value AS BaseValue, ',
    '      Base.Title AS BaseTitle, ',

    '      Target.Date AS TargetDate, ',
    '      Target.Value AS TargetValue, ',
    '      Target.Title AS TargetTitle, ',

    '      TIMESTAMPDIFF(SECOND, Base.Date, Current.Date) AS CurrentDate_ElapsedTimeInSeconds_FromBase, ',
    '      TIMESTAMPDIFF(SECOND, Current.Date, Target.Date) AS CurrentDate_RemainingTimeInSeconds_ToTarget, ',

    '      (TIMESTAMPDIFF(SECOND, Base.Date, Current.Date) / TIMESTAMPDIFF(SECOND, Base.Date, Target.Date)) AS CurrentDate_ElapsedTimeInPercent_FromBase, ',
    '      (TIMESTAMPDIFF(SECOND, Current.Date, Target.Date) / TIMESTAMPDIFF(SECOND, Base.Date, Target.Date)) AS CurrentDate_RemainingTimeInPercent_ToTarget, ',

    '      (Target.Value - Base.Value) AS GoalQuantity, ',
    '      1.00 AS GoalPercent, ',
    '      0.00 AS BasePercent, ',

    '      (Current.Value - Base.Value) AS CurrentVsBase_DeltaQuantity, ',
    '      (CASE ',
    '        WHEN (Target.Value - Base.Value) = 0 THEN 0 ',
    '        ELSE ((Current.Value - Base.Value) / (Target.Value - Base.Value)) ',
    '      END) CurrentVsBase_DeltaPercent, ',

    '      (Target.Value - Current.Value) CurrentVsTarget_DeltaQuantity, ',
    '      (CASE ',
    '        WHEN (Target.Value - Base.Value) = 0 THEN 0 ',
    '        WHEN ((Target.Value - Current.Value) / (Target.Value - Base.Value)) < 0 THEN 0 ',
    '        ELSE ((Target.Value - Current.Value) / (Target.Value - Base.Value)) ',
    '      END) CurrentVsTarget_DeltaPercent ',

    '    FROM ',
    '      (', current_sql_select ,') AS Current ',
    '      LEFT JOIN (', base_sql_select ,') AS Base ',
    '        ON Current.',sql_column_for_current_join,' = Base.',sql_column_for_base_join,' ',
    '      LEFT JOIN (', target_sql_select ,') AS Target ',
    '        ON Current.',sql_column_for_current_join,' = Target.',sql_column_for_target_join,' ',

    ' ); '
  );
  # DEBUG
  # SELECT @sql_JoinCurrentToBaseToTarget;

  PREPARE stmt FROM @sql_JoinCurrentToBaseToTarget; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END;
