create external table timeWindow (week_day string, month string, month_day int, year int, hour int, loc_lat string, loc_long string, arrival_num int, departure_num int) row format delimited fields terminated by ',' location '/user/cyc391/CitiBike/timeWindow';

insert overwrite local directory 'timeWindow_2013_Jul' row format delimited fields terminated by ',' select * from timeWindow where year = 2013 AND month = 'Jul';

insert overwrite local directory 'timeWindow_2013_Aug' row format delimited fields terminated by ',' select * from timeWindow where year = 2013 AND month = 'Aug';

insert overwrite local directory 'timeWindow_2013_Sep' row format delimited fields terminated by ',' select * from timeWindow where year = 2013 AND month = 'Sep';

insert overwrite local directory 'timeWindow_2013_Oct' row format delimited fields terminated by ',' select * from timeWindow where year = 2013 AND month = 'Oct';

insert overwrite local directory 'timeWindow_2013_Nov' row format delimited fields terminated by ',' select * from timeWindow where year = 2013 AND month = 'Nov';

insert overwrite local directory 'timeWindow_2013_Dec' row format delimited fields terminated by ',' select * from timeWindow where year = 2013 AND month = 'Dec';

insert overwrite local directory 'timeWindow_2014_Jan' row format delimited fields terminated by ',' select * from timeWindow where year = 2014 AND month = 'Jan';

insert overwrite local directory 'timeWindow_2014_Feb' row format delimited fields terminated by ',' select * from timeWindow where year = 2014 AND month = 'Feb';

insert overwrite local directory 'timeWindow_2014_Mar' row format delimited fields terminated by ',' select * from timeWindow where year = 2014 AND month = 'Mar';

insert overwrite local directory 'timeWindow_2014_Apr' row format delimited fields terminated by ',' select * from timeWindow where year = 2014 AND month = 'Apr';

insert overwrite local directory 'timeWindow_2014_May' row format delimited fields terminated by ',' select * from timeWindow where year = 2014 AND month = 'May';

insert overwrite local directory 'timeWindow_2014_Jun' row format delimited fields terminated by ',' select * from timeWindow where year = 2014 AND month = 'Jun';

insert overwrite local directory 'timeWindow_2014_Jul' row format delimited fields terminated by ',' select * from timeWindow where year = 2014 AND month = 'Jul';

insert overwrite local directory 'timeWindow_2014_Aug' row format delimited fields terminated by ',' select * from timeWindow where year = 2014 AND month = 'Aug';