--This Impala SQL code creates an impala table for dim data,using hue web interface
CREATE TABLE IF NOT EXISTS dim_date
AS
with dates as (
select date_add("${start_date=2016-01-01}", a.pos) as date_actual
from (select posexplode(split(repeat("o", datediff("${end_date=2030-12-31}", "${start_date=2016-01-01}")), "o"))) a
)
select
    date_actual as date_actual,
    year(date_actual)*10000+month(date_actual)*100+day(date_actual) as date_key,
    year(date_actual) as year_actual,
    month(date_actual) as month_actual,
    day(date_actual) as day_actual,
   -- quarter(date_actual) as quarter_actual,
    IF((dayofweek(date_actual)-1=0),7,dayofweek(date_actual)-1) AS day_of_week,
    date_format(date_actual, 'EEEE') as day_name,
    date_format(date_actual, 'EEE') as day_name_abbr,
    date_format(date_actual,'MMMM') as month_name,
    date_format(date_actual,'MMM') as month_name_abbr,
	dayofmonth(date_actual) AS day_of_month,
    date_format(date_actual, 'D') as day_of_year, 
    datediff(date_actual, "1970-01-01") as day_of_epoch,
    IF(dayofweek(date_actual)=1,CAST(date_format(date_actual, 'W')-1 AS INT),date_format(date_actual, 'W')) as week_of_month, 
    year(date_actual)*100+month(date_actual) AS month_year,
	date_format(date_actual, 'Y') AS calendar_year,
    weekofyear(date_actual) as  week_of_year, 
     year(date_actual)*100+weekofyear(date_actual)  as year_week,
  --  date_format(date_actual, 'Yww') as calendar_year_week,
    date_sub(date_actual,pmod(datediff(date_actual,'1900-01-01'),7)) as first_day_of_week,
    date_add(date_actual,6 - pmod(datediff(date_actual,'1900-01-01'),7)) as last_day_of_week,
    concat(substr(date_actual,1,8),'01') as first_day_of_month,
    last_day(date_actual) AS last_day_of_month,
    concat(cast(year(date_actual) as string),'-','01-01') AS first_day_of_year,
    concat(cast(year(date_actual) as string),'-','12-31') AS last_day_of_year,
    date_format(date_actual,"dd/MM/yyyy") as date_au_format,
    date_format(date_actual,"MM/dd/yyyy") as date_us_format,
    if((dayofweek(date_actual)=1 OR dayofweek(date_actual)=7), 1, 0) as weekend
    FROM dates
sort by date_actual
;