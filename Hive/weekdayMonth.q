drop table noQuoWeekday;

create external table noQuoWeekday (duration string, start_week_day string, start_month string, start_month_day int, start_year int, start_hour int, stop_week_day string, stop_month string, stop_month_day int, stop_year int, stop_hour int, start_id string, start_name string, start_lat string, start_long string, stop_id string, stop_name string, stop_lat string, sopt_long string, bike_id string, user_type string, birth_year string, genger string) row format delimited fields terminated by ',' location '/user/cyc391/CitiBike/noQuoWeekday';

insert overwrite local directory 'noQuoWeekday_2013_Jul' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2013 AND start_month = 'Jul';

insert overwrite local directory 'noQuoWeekday_2013_Aug' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2013 AND start_month = 'Aug';

insert overwrite local directory 'noQuoWeekday_2013_Sep' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2013 AND start_month = 'Sep';

insert overwrite local directory 'noQuoWeekday_2013_Oct' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2013 AND start_month = 'Oct';

insert overwrite local directory 'noQuoWeekday_2013_Nov' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2013 AND start_month = 'Nov';

insert overwrite local directory 'noQuoWeekday_2013_Dec' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2013 AND start_month = 'Dec';

insert overwrite local directory 'noQuoWeekday_2014_Jan' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2014 AND start_month = 'Jan';

insert overwrite local directory 'noQuoWeekday_2014_Feb' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2014 AND start_month = 'Feb';

insert overwrite local directory 'noQuoWeekday_2014_Mar' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2014 AND start_month = 'Mar';

insert overwrite local directory 'noQuoWeekday_2014_Apr' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2014 AND start_month = 'Apr';

insert overwrite local directory 'noQuoWeekday_2014_May' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2014 AND start_month = 'May';

insert overwrite local directory 'noQuoWeekday_2014_Jun' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2014 AND start_month = 'Jun';

insert overwrite local directory 'noQuoWeekday_2014_Jul' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2014 AND start_month = 'Jul';

insert overwrite local directory 'noQuoWeekday_2014_Aug' row format delimited fields terminated by ',' select * from noQuoWeekday where start_year = 2014 AND start_month = 'Aug';