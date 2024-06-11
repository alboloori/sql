--Source: https://www.mssqltips.com/sqlservertip/4054/creating-a-date-dimension-or-calendar-table-in-sql-server/
--https://community.snowflake.com/s/question/0D50Z00008AH87aSAD/attached-dimdate-and-dimtimeofday-ddldml
--=-=-=-=-=-=-=-=-=-=-=-=-=-A DimDate for MSSQL Server=-=-=-=-=-=-=-=-=-=-=-=-=-
DECLARE @StartDate  date = '2010-01-01';
DECLARE @EndDate date = DATEADD(DAY, -1, DATEADD(YEAR, 30, @StartDate));
--=-=-=-=-=-=-=-=-=-=-=-=-=-CTE To Generate dates
WITH seq(n) AS 
(
  SELECT 0 UNION ALL SELECT n + 1 FROM seq
  WHERE n < DATEDIFF(DAY, @StartDate, @EndDate)
)
, dates_seq(date_actual) AS 
(
  SELECT DATEADD(DAY, n, @StartDate) FROM seq
),
--=-=-=-=-=-=-=-=-=-=-=-=-=-CTE generating dates and basic date features between @StartDate and @CutOffDate
dates as
(
SELECT 
    [DateKey]		= Year(date_actual)*10000+Month(date_actual)*100+Day(date_actual) ,
	[CalendarDate]	= CONVERT(date, date_actual),
    [CalendarYear]	= Year(date_actual) ,
    [CalendarMonth]	= Month(date_actual) ,
    [DayOfMonth]	= DATEPART(DAY,date_actual) ,
    [DayName]      = DATENAME(WEEKDAY,   date_actual),
    [WeekOfYear ]  = DATEPART(WEEK,      date_actual),
    [ISOWeekOfYear]      = DATEPART(ISO_WEEK,  date_actual),
    [DayOfWeek]   = DATEPART(WEEKDAY,date_actual),
    [MonthName]    = DATENAME(MONTH,     date_actual),
    [Quarter]      = DATEPART(Quarter,   date_actual),
    [FirstDayOfMonth] = DATEFROMPARTS(YEAR(date_actual), MONTH(date_actual), 1),
    [LastDayOfYear]   = DATEFROMPARTS(YEAR(date_actual), 12, 31),
    [DayOfYear]    = DATEPART(DAYOFYEAR, date_actual)
FROM dates_seq
),
/*SELECT * FROM dates
ORDER BY CalendarDate
OPTION (MAXRECURSION 0);*/
--=-=-=-=-=-=-=-=-=-=-=-=-=-generating more calendar features for previous CTE
calendar_dates AS
(
  SELECT
    [CalendarDate], 
    [DateKey],
	[DayOfMonth],
	[DayName],
    [DayOfWeek],
	[DayOfWeek_AU]        = CASE WHEN (DATEPART(WEEKDAY,[CalendarDate])+6)%7=0 
								THEN 7 ELSE (DATEPART(WEEKDAY,[CalendarDate])+6)%7 END,
    [DayOfWeekInMonth] = CONVERT(tinyint, ROW_NUMBER() OVER (PARTITION BY [FirstDayOfMonth], [DayOfWeek] ORDER BY CalendarDate)),
    [DayOfYear],
    [IsWeekend]        = CASE WHEN [DayOfWeek] IN (CASE @@DATEFIRST WHEN 1 THEN 6 WHEN 7 THEN 1 END,7) 
                            THEN 1 ELSE 0 END,
    [ISOWeekOfYear],
    [FirstDayOfWeek]   = DATEADD(DAY, 1 - [DayOfWeek], [CalendarDate]),
    [LastDayOfWeek]       = DATEADD(DAY, 6, DATEADD(DAY, 1 - [DayOfWeek], [CalendarDate])),
    [WeekOfMonth]      = CONVERT(tinyint, DENSE_RANK() OVER(PARTITION BY [CalendarYear], [CalendarMonth] ORDER BY [WeekOfYear])),
    [CalendarMonth],
    [MonthName],
    [FirstDayOfMonth],
    [LastDayOfMonth]      = MAX([CalendarDate]) OVER (PARTITION BY [CalendarYear], [CalendarMonth]),
    [FirstDayOfNextMonth] = DATEADD(MONTH, 1, [FirstDayOfMonth]),
    [LastDayOfNextMonth]  = DATEADD(DAY, -1, DATEADD(MONTH, 2, [FirstDayOfMonth])),
    [Quarter],
    [FirstOfQuarter]    = MIN([CalendarDate]) OVER (PARTITION BY [CalendarYear], [Quarter]),
    [LastOfQuarter]     = MAX([CalendarDate]) OVER (PARTITION BY [CalendarYear], [Quarter]),
    [CalendarYear],
    [ISOYear]           = [CalendarYear] - CASE WHEN [CalendarMonth] = 1 AND [ISOWeekOfYear] > 51 THEN 1 
                            WHEN [CalendarMonth] = 12 AND [ISOWeekOfYear] = 1  THEN -1 ELSE 0 END,      
    [FirstDayOfYear]    = DATEFROMPARTS([CalendarYear], 1,  1),
    [LastDayOfYear],
    [IsLeapYear]        = CONVERT(bit, CASE WHEN ([CalendarYear] % 400 = 0) 
                            OR ([CalendarYear] % 4 = 0 AND [CalendarYear] % 100 <> 0) 
                            THEN 1 ELSE 0 END),
    [Has53Weeks]        = CASE WHEN DATEPART(WEEK,     [LastDayOfYear]) = 53 THEN 1 ELSE 0 END,
	[Has53ISOWeeks]     = CASE WHEN DATEPART(ISO_WEEK, [LastDayOfYear]) = 53 THEN 1 ELSE 0 END,
    [MMYYYY]            = CONVERT(char(2), CONVERT(char(8), [CalendarDate], 101)) + CONVERT(char(4), [CalendarYear]),
	[YYYYMM]            = CONVERT(char(4), CONVERT(char(8), [CalendarDate], 112)) + CONVERT(char(2), [CalendarDate],101),
	[Style100]= convert(VARCHAR(11), [CalendarDate], 100),
    [Style101]            = CONVERT(char(10), [CalendarDate], 101),
    [Style103]            = CONVERT(char(10), [CalendarDate], 103),
    [Style112]            = CONVERT(char(8),  [CalendarDate], 112),
    [Style120]            = CONVERT(char(10), [CalendarDate], 120)
  FROM dates
)
/*
SELECT * FROM calendar_dates
ORDER BY CalendarDate
  OPTION (MAXRECURSION 0);
*/
, x AS 
(
  SELECT
    [CalendarDate],
    [FirstDayOfYear],
	[DayOfWeek],
    [DayOfWeekInMonth], 
	[DayOfMonth],
    [CalendarMonth], 
    [DayName], 
    [LastDayOfWeekInMonth] = ROW_NUMBER() OVER 
    (
      PARTITION BY [FirstDayOfMonth], [DayOfWeek]
      ORDER BY [CalendarDate] DESC
    )
  FROM calendar_dates
),
/*SELECT * FROM calendar_dates
ORDER BY CalendarDate
  OPTION (MAXRECURSION 0)*/
--=-=-=-=-=-=-=-=-=-=-=-=-=-Public Holidays (TBC)-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
au_public_holidays AS
(
  SELECT [CalendarDate],
		 [DayName],
		 [HolidayDescription] = CASE
							WHEN (([CalendarDate] = [FirstDayOfYear])  OR ([DayOfMonth]=2 AND [CalendarMonth]=1 AND [DayName] = 'Monday') OR ([DayOfMonth]=3 AND [CalendarMonth]=1  AND [DayName] = 'Monday'))
								 THEN 'New Year''s Day'
							WHEN (([DayOfMonth]=25 AND [CalendarMonth]=12) OR ([DayOfMonth]=26 AND [CalendarMonth]=12 AND [DayName] = 'Monday'))
								 THEN 'Christmas Day'
							WHEN ([DayOfMonth]=26 AND [CalendarMonth]=12 OR ([DayOfMonth]=27 AND [CalendarMonth]=12 AND [DayName] = 'Monday') )
								 THEN 'Boxing Day'
							WHEN ([DayOfMonth]=26 AND [CalendarMonth]=1)
								 THEN 'Australian Day'
							WHEN ([DayOfMonth]=25 AND [CalendarMonth]=4)
								 THEN 'Anzac Day'
							--WHEN ([DayOfMonth]=27 AND [CalendarMonth]=1 AND DayName='Monday')--DATENAME(WEEKDAY,DATEADD(day,-1,[CalendarDate])) IN(N'Saturday',N'Sunday'))
							--	 THEN 'Australian Day-Additional Day'
							WHEN ([DayOfWeekInMonth] = 2 AND [CalendarMonth] = 6 AND [DayName] = 'Monday')
								THEN 'King''s Birthday'    -- (2nd Monday in June)
							WHEN ([LastDayOfWeekInMonth] = 1 AND [CalendarMonth] = 5 AND [DayName] = 'Monday')
								THEN ''              -- (last Monday in May)
							  END
  FROM x
  WHERE 
    ([CalendarDate] = [FirstDayOfYear])
	OR ([DayOfMonth]=2 AND [CalendarMonth]=1  AND [DayName] = 'Monday')
	OR ([DayOfMonth]=3 AND [CalendarMonth]=1  AND [DayName] = 'Monday')
	OR([DayOfMonth]=25 AND [CalendarMonth]=12)
	OR ([DayOfMonth]=26 AND [CalendarMonth]=12)-- AND [DayName] = 'Monday')
    OR ([DayOfMonth]=27 AND [CalendarMonth]=12 AND [DayName] = 'Monday')
	--OR ([DayOfMonth]=27 AND [CalendarMonth]=12 AND DATENAME(WEEKDAY,DATEADD(day,-1,[CalendarDate])) IN(N'Saturday',N'Sunday'))
	OR ([DayOfMonth]=26 AND [CalendarMonth]=1 )
    OR ([DayOfWeekInMonth] = 2 AND [CalendarMonth] = 6)--King's Birthday
    OR ([DayOfMonth]=25 AND [CalendarMonth]=4)--Anzac Day
	--OR ([DayOfMonth]=25 AND [CalendarMonth]=12)
	--OR ([DayOfMonth]=26 AND [CalendarMonth]=12)
   -- OR ([DayOfWeekInMonth] = 2     AND [CalendarMonth] = 10 AND [DayName] = 'Monday')
   -- OR ([CalendarMonth] = 11 AND [DayOfMonth] = 11)
   -- OR ([DayOfWeekInMonth] = 4     AND [CalendarMonth] = 11 AND [DayName] = 'Thursday')
  
)
/*SELECT * FROM au_public_holidays
WHERE YEAR([CalendarDate]) IN(2024,2025,2026)
ORDER BY CalendarDate
  OPTION (MAXRECURSION 0)*/
--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
--CREATE VIEW Dim_Date_AU
--AS 
--TBC: Adjust calendar first day of week, last day of week and week of year considering Mondays as first day of week
  SELECT
    d.*,
    IsPublicHoliday = CASE 
		WHEN h.[CalendarDate] IS NOT NULL THEN 1 ELSE 0 END,
    h.HolidayDescription
  FROM calendar_dates AS d
  LEFT OUTER JOIN au_public_holidays AS h
  ON d.[CalendarDate] = h.[CalendarDate]
  --WHERE HolidayDescription IS NOT NULL --AND CalendarYear BETWEEN 2019 AND 2025
  ORDER BY [CalendarDate]
	OPTION (MAXRECURSION 0);
