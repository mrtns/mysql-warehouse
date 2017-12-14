USE some_warehouse;


DROP FUNCTION IF EXISTS Tools_GetIso8601StringFromDate;
DELIMITER $$
CREATE FUNCTION Tools_GetIso8601StringFromDate(
    theDate TIMESTAMP(6),
    theNamedTimezoneOfTheDate VARCHAR(128)
  )
  RETURNS VARCHAR(32)
  DETERMINISTIC
BEGIN

  /*
    Usage:

    SELECT Tools_GetIso8601StringFromDate(STR_TO_DATE('2015-03-08 03:02:00', '%Y-%m-%d %H:%i:%s'), 'US/Pacific');
  */

  DECLARE utcOffsetString VARCHAR(6);
  DECLARE utcOffsetStringWithSign VARCHAR(6);

  # Get the offset at the point in time of 'theDate'

  SET utcOffsetString = (
    CASE
      WHEN LOWER(theNamedTimezoneOfTheDate) = 'utc' THEN 'Z'
      ELSE TIME_FORMAT(TIMEDIFF(theDate, CONVERT_TZ(theDate, theNamedTimezoneOfTheDate, 'UTC')), '%H%i')
    END
  );

  SET utcOffsetStringWithSign = (
   CASE
     WHEN utcOffsetString LIKE '-%' THEN utcOffsetString
     WHEN utcOffsetString = 'Z' THEN utcOffsetString
     ELSE CONCAT('+', utcOffsetString)
   END
  );

  RETURN
    CONCAT(
      DATE_FORMAT(theDate, '%Y-%m-%dT%H:%i:%s.%f'),
      utcOffsetStringWithSign
    );

END;
$$
DELIMITER ;


DROP FUNCTION IF EXISTS Tools_TruncateDate;
DELIMITER $$
CREATE FUNCTION Tools_TruncateDate(
    theDate TIMESTAMP,
    truncateToResolution VARCHAR(10)
  )
  RETURNS TIMESTAMP
  DETERMINISTIC
BEGIN
  DECLARE timestampDateFormat VARCHAR(32);

  SELECT
    (CASE UPPER(truncateToResolution)
      WHEN 'MICROSECOND' THEN '%Y-%m-%d %H:%i:%s.%f'
      WHEN 'SECOND' THEN '%Y-%m-%d %H:%i:%s.000000'
      WHEN 'MINUTE' THEN '%Y-%m-%d %H:%i:00.000000'
      WHEN 'HOUR' THEN '%Y-%m-%d %H:00:00.000000'
      WHEN 'DAY' THEN '%Y-%m-%d 00:00:00.000000'
      WHEN 'WEEK' THEN '%Y-%m-%d 00:00:00.000000'
      WHEN 'MONTH' THEN '%Y-%m-01 00:00:00.000000'
      WHEN 'QUARTER' THEN 'QUARTER resolution is not supported'
      WHEN 'YEAR' THEN '%Y-01-01 00:00:00.000000'
    END)
  INTO timestampDateFormat;

  RETURN STR_TO_DATE(
      DATE_FORMAT(
          (CASE UPPER(truncateToResolution)
             WHEN 'WEEK' THEN DATE_ADD(theDate, INTERVAL(1-DAYOFWEEK(theDate)) DAY)
             ELSE theDate
          END),
          timestampDateFormat
      ),
      '%Y-%m-%d %H:%i:%s.%f'
  );
END;
$$
DELIMITER ;


DROP FUNCTION IF EXISTS Tools_GetConfigurationValue;
DELIMITER $$
CREATE FUNCTION Tools_GetConfigurationValue(
    configurationKey VARCHAR(1024)
  )
  RETURNS VARCHAR(1024)
  DETERMINISTIC
BEGIN

  /*
    Usage:

    SELECT Tools_GetConfigurationValue('ReportingTimezone');
  */

  DECLARE result VARCHAR(1024);

    SELECT (
      CASE configurationKey
        WHEN 'ReportingTimezone' THEN 'US/Pacific'
        WHEN 'CompanyEpochStart_%Y-%m-%d' THEN '2010-01-01'
      END
    )
    INTO result;

  RETURN result;

END;
$$
DELIMITER ;


DROP FUNCTION IF EXISTS Tools_GetNowInReportingTimezone;
DELIMITER $$
CREATE FUNCTION Tools_GetNowInReportingTimezone()
  RETURNS TIMESTAMP(6)
  DETERMINISTIC
BEGIN

  /*
    Usage:

    SELECT Tools_GetNowInReportingTimezone();
  */

  RETURN CONVERT_TZ(UTC_TIMESTAMP(), 'UTC', Tools_GetConfigurationValue('ReportingTimezone'));

END;
$$
DELIMITER ;
