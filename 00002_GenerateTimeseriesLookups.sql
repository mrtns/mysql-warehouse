USE some_warehouse;

SET @dateMin = STR_TO_DATE('2010-01-01', '%Y-%m-%d');
SET @dateMax = STR_TO_DATE('2020-01-01', '%Y-%m-%d');

CALL Tools_GenerateTimeseriesWithTimezone('UTC', @dateMin, @dateMax, 1, 'MONTH');
DROP TABLE IF EXISTS Lookup_Timeseries_Months;
CREATE TABLE IF NOT EXISTS Lookup_Timeseries_Months (
  INDEX IntervalStart (IntervalStart),
  INDEX IntervalEnd (IntervalEnd),
  INDEX IntervalStartUtc (IntervalStartUtc),
  INDEX IntervalEndUtc (IntervalEndUtc),
  INDEX IntervalStartInTimezone (IntervalStartInTimezone),
  INDEX IntervalEndInTimezone (IntervalEndInTimezone)
) AS (
  SELECT
    *
  FROM
    Timeseries
);

CALL Tools_GenerateTimeseriesWithTimezone('US/Pacific', @dateMin, @dateMax, 1, 'MONTH');
DROP TABLE IF EXISTS Lookup_Timeseries_Months_TzPt;
CREATE TABLE IF NOT EXISTS Lookup_Timeseries_Months_TzPt (
  INDEX IntervalStart (IntervalStart),
  INDEX IntervalEnd (IntervalEnd),
  INDEX IntervalStartUtc (IntervalStartUtc),
  INDEX IntervalEndUtc (IntervalEndUtc),
  INDEX IntervalStartInTimezone (IntervalStartInTimezone),
  INDEX IntervalEndInTimezone (IntervalEndInTimezone)
) AS (
  SELECT
    *
  FROM
    Timeseries
);


CALL Tools_GenerateTimeseriesWithTimezone('UTC', @dateMin, @dateMax, 1, 'DAY');
DROP TABLE IF EXISTS Lookup_Timeseries_Days;
CREATE TABLE IF NOT EXISTS Lookup_Timeseries_Days (
  INDEX IntervalStart (IntervalStart),
  INDEX IntervalEnd (IntervalEnd),
  INDEX IntervalStartUtc (IntervalStartUtc),
  INDEX IntervalEndUtc (IntervalEndUtc),
  INDEX IntervalStartInTimezone (IntervalStartInTimezone),
  INDEX IntervalEndInTimezone (IntervalEndInTimezone)
) AS (
  SELECT
    *
  FROM
    Timeseries
);

CALL Tools_GenerateTimeseriesWithTimezone('US/Pacific', @dateMin, @dateMax, 1, 'DAY');
DROP TABLE IF EXISTS Lookup_Timeseries_Days_TzPt;
CREATE TABLE IF NOT EXISTS Lookup_Timeseries_Days_TzPt (
  INDEX IntervalStart (IntervalStart),
  INDEX IntervalEnd (IntervalEnd),
  INDEX IntervalStartUtc (IntervalStartUtc),
  INDEX IntervalEndUtc (IntervalEndUtc),
  INDEX IntervalStartInTimezone (IntervalStartInTimezone),
  INDEX IntervalEndInTimezone (IntervalEndInTimezone)
) AS (
  SELECT
    *
  FROM
    Timeseries
);


CALL Tools_GenerateTimeseriesWithTimezone('UTC', @dateMin, @dateMax, 1, 'HOUR');
DROP TABLE IF EXISTS Lookup_Timeseries_Hours;
CREATE TABLE IF NOT EXISTS Lookup_Timeseries_Hours (
  INDEX IntervalStart (IntervalStart),
  INDEX IntervalEnd (IntervalEnd),
  INDEX IntervalStartUtc (IntervalStartUtc),
  INDEX IntervalEndUtc (IntervalEndUtc),
  INDEX IntervalStartInTimezone (IntervalStartInTimezone),
  INDEX IntervalEndInTimezone (IntervalEndInTimezone)
) AS (
  SELECT
    *
  FROM
    Timeseries
);

CALL Tools_GenerateTimeseriesWithTimezone('US/Pacific', @dateMin, @dateMax, 1, 'HOUR');
DROP TABLE IF EXISTS Lookup_Timeseries_Hours_TzPt;
CREATE TABLE IF NOT EXISTS Lookup_Timeseries_Hours_TzPt (
  INDEX IntervalStart (IntervalStart),
  INDEX IntervalEnd (IntervalEnd),
  INDEX IntervalStartUtc (IntervalStartUtc),
  INDEX IntervalEndUtc (IntervalEndUtc),
  INDEX IntervalStartInTimezone (IntervalStartInTimezone),
  INDEX IntervalEndInTimezone (IntervalEndInTimezone)
) AS (
  SELECT
    *
  FROM
    Timeseries
);
