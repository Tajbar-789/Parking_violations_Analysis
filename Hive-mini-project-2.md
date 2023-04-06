Objective - The assignment is meant for you to apply learnings of the module on Hive on a real-life dataset. One of the major objectives of this assignment is gaining familiarity with how an analysis works in Hive and how you can gain insights from large datasets.
 
Problem Statement - New York City is a thriving metropolis and just like most other cities of similar size, one of the biggest problems its residents face is parking. The classic combination of a huge number of cars and a cramped geography is the exact recipe that leads to a large number of parking tickets.
 
In an attempt to scientifically analyse this phenomenon, the NYC Police Department regularly collects data related to parking tickets. This data is made available by NYC Open Data portal. We will try and perform some analysis on this data.

Download Dataset - https://data.cityofnewyork.us/browse?q=parking+tickets

Note: Consider only the year 2017 for analysis and not the Fiscal year.

The analysis can be divided into two parts:

```
load data from local to hdfs loaction 

hdfs dfs -put /config/workspace/Parking_Violations_Issued_-_Fiscal_Year_2017.csv /data/



create table parking_violations_issued
(
      Summons_Number bigint,
      Plate_ID string,
      Registration_State string,
      Plate_Type string,
      Issue_Date string,
      Violation_Code int,
      Vehicle_Body_Type string,
      Vehicle_Make string,
      Issuing_Agency string,
      Street_Code1 int,
      Street_Code2 int,
      Street_Code3 int,
      Vehicle_Expiration Date,
      Violation_Location int,
      Violation_Precinct int,
      Issuer_Precinct int,
      Issuer_Code int,
      Issuer_Command string,
      Issuer_Squad string,
      Violation_Time string,
      Time_First_Observed string,
      Violation_County string,
      Violation_In_Front_Of_Or_Opposite string,
      House_Number string,
      Street_Name string,
      Intersecting_Street string,
      Date_First_Observed int,
      Law_Section int,
      Sub_Division string,
      Violation_Legal_Code string,
      Days_Parking_In_Effect string,
      From_Hours_In_Effect string,
      To_Hours_In_Effect string,
      Vehicle_Color string,
      Unregistered_Vehicle int,
      Vehicle_Year string,
      Meter_Number string,
      Feet_From_Curb int,
      Violation_Post_Code string,
      Violation_Description string,
      No_Standing_or_Stopping_Violation string,
      Hydrant_Violation string,
      Double_Parking_Violation string
)
row format delimited
fields terminated by ','
tblproperties ("skip.header.line.count" = "1");

```


loading into the table 

```
load data  inpath '/data/Parking_Violations_Issued_-_Fiscal_Year_2017.csv' into table parking_violations_issued;



create table parking_violations_issued_2017
(
      Summons_Number bigint,
      Plate_ID string,
      Plate_Type string,
      Issue_Date string,
      Violation_Code int,
      Vehicle_Body_Type string,
      Vehicle_Make string,
      Issuing_Agency string,
      Street_Code1 int,
      Street_Code2 int,
      Street_Code3 int,
      Vehicle_Expiration Date,
      Violation_Location int,
      Violation_Precinct int,
      Issuer_Precinct int,
      Issuer_Code int,
      Issuer_Command string,
      Issuer_Squad string,
      Violation_Time string,
      Time_First_Observed string,
      Violation_County string,
      Violation_In_Front_Of_Or_Opposite string,
      House_Number string,
      Street_Name string,
      Intersecting_Street string,
      Date_First_Observed int,
      Law_Section int,
      Sub_Division string,
      Violation_Legal_Code string,
      Days_Parking_In_Effect string,
      From_Hours_In_Effect string,
      To_Hours_In_Effect string,
      Vehicle_Color string,
      Unregistered_Vehicle int,
      Vehicle_Year string,
      Meter_Number string,
      Feet_From_Curb int,
      Violation_Post_Code string,
      Violation_Description string,
      No_Standing_or_Stopping_Violation string,
      Hydrant_Violation string,
      Double_Parking_Violation string
)
partitioned by (Registration_State string )
clustered by(violation_code) into 10 buckets
row format delimited
fields terminated by ',';



set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict; 
set hive.enforce.bucketing=true;


insert into table parking_violations_issued_2017 partition (Registration_State ) select Summons_Number,Plate_ID,
	            Plate_Type,
	            Issue_Date,
	            Violation_Code,
	            Vehicle_Body_Type,
	            Vehicle_Make,
	            Issuing_Agency,
	            Street_Code1,
	            Street_Code2,
	            Street_Code3,
	            Vehicle_Expiration,
	            Violation_Location,
	            Violation_Precinct,
	            Issuer_Precinct,
	            Issuer_Code,
	            Issuer_Command,
	            Issuer_Squad,
	            Violation_Time,
	            Time_First_Observed,
		    Violation_County,
	            Violation_In_Front_Of_Or_Opposite,
	            House_Number,
	            Street_Name,
	            Intersecting_Street,
	            Date_First_Observed,
	            Law_Section,
	            Sub_Division,
	            Violation_Legal_Code,
	            Days_Parking_In_Effect,
	            From_Hours_In_Effect,
	            To_Hours_In_Effect,
	            Vehicle_Color,
	            Unregistered_Vehicle,
	            Vehicle_Year,
	            Meter_Number,
	            Feet_From_Curb,
	            Violation_Post_Code,
	            Violation_Description,
	            No_Standing_or_Stopping_Violation,
	            Hydrant_Violation,
	            Double_Parking_Violation,
	            Registration_State
from parking_violations_issued where year(from_unixtime(unix_timestamp(issue_date,'MM/dd/yyyy'),'yyyy-MM-dd'))=2017;

```
 
Part-I: Examine the data

1.) Find the total number of tickets for the year.

```
select year(from_unixtime(unix_timestamp(issue_date,'MM/dd/yyyy'),'yyyy-MM-dd')) as issued_year,count(1) as total_tickets_issued from parking_violations_issued_2017 group by year(from_unixtime(unix_timestamp(issue_date,'MM/dd/yyyy'),'yyyy-MM-dd'));
```

2.) Find out how many unique states the cars which got parking tickets came from.

```
select Registration_state,count(1) as total_count from parking_violations_issued_2017 group by Registration_state;
```
3.) Some parking tickets don’t have addresses on them, which is cause for concern. Find out how many such tickets there are(i.e. tickets where either "Street Code 1" or "Street Code 2" or "Street Code 3" is empty )

```
select count(distinct summons_number) as No_of_Tickets_without_address from parking_violations_issued where Street_code1 = 0 or Street_code2 = 0 or Street_code3 = 0;

```

Part-II: Aggregation tasks

1.) How often does each violation code occur? (frequency of violation codes - find the top 5)

```
select violation_code,count(1) as total_number_of_violations from parking_violations_issued_2017 group by violation_code order by total_number_of_violations desc limit 5;
```

2.) How often does each vehicle body type get a parking ticket? How about the vehicle make? (find the top 5 for both)

```
select vehicle_body_type,count(1) as total_number_of_violations from parking_violations_issued_2017 group by vehicle_body_type order by total_number_of_violations desc limit 5;

select vehicle_make,count(1) as total_number_of_violations from parking_violations_issued_2017 group by vehicle_make order by total_number_of_violations desc limit 5;

```
3.) A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of: 

a.) Violating Precincts (this is the precinct of the zone where the violation occurred)

```
select Violation_Precinct,count(1) as total_number_of_violations from parking_violations_issued_2017 group       by Violation_Precinct order by total_number_of_violations desc limit 5;
```    
b.) Issuer Precincts (this is the precinct that issued the ticket)

```
select Issuer_Precinct,count(1) as total_number_of_violations from parking_violations_issued_2017 group by Issuer_Precinct order by total_number_of_violations desc limit 5;

```


4.) Find the violation code frequency across 3 precincts which have issued the most number of tickets - do these precinct zones have an exceptionally high frequency of certain violation codes?

```
create table top_3_violation_precinct as select violation_precinct,count(1) as sum from parking_violations_issued_2017 group by Violation_Precinct order by sum desc limit 3;


SET hive.auto.convert.join=true;

select table1.Violation_Precinct as violation_precinct,table1.violation_code as violation_code,count(*) as total_violations from parking_violations_issued_2017 table1 inner join top_3_violation_precinct  table2 on table1.violation_precinct=table2.violation_precinct group by table1.violation_precinct,table1.violation_code order by total_violations desc; 

```

5.) Find out the properties of parking violations across different times of the day: The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.

Their are two ways to do this 

1- To Convert violation_time string to unix epoch counter bigint as follows 

```
select violation_time,case when substr(violation_time,5,1)='P' then unix_timestamp(substr(violation_time,0,4),'HHmm')+12*60*60 else unix_timestamp(substr(violation_time,0,4),'HHmm') END as new_time ,from_unixtime(case when substr(violation_time,5,1)='P' then unix_timestamp(substr(violation_time,0,4),'HHmm')+12*60*60 else unix_timestamp(substr(violation_time,0,4),'HHmm') END,'HH:mm:ss') from parking_violations_issued_2017 ;

```
2- To divide the 24 hours format into different bins of certain range as follows 

```
select case when substr(violation_time,5,1)='P' then unix_timestamp(substr(violation_time,0,4),'HHmm')+12*60*60 else unix_timestamp(substr(violation_time,0,4),'HHmm') END ,
case when substring(Violation_Time,1,2) in ('00','01','02','03','12') and upper(substring(Violation_Time,-1))='A' then 1 
when substring(Violation_Time,1,2) in ('04','05','06','07') and upper(substring(Violation_Time,-1))='A' then 2 
when substring(Violation_Time,1,2) in ('08','09','10','11') and upper(substring(Violation_Time,-1))='A' then 3 
when substring(Violation_Time,1,2) in ('12','00','01','02','03') and upper(substring(Violation_Time,-1))='P' then 4 
when substring(Violation_Time,1,2) in ('04','05','06','07') and upper(substring(Violation_Time,-1))='P' then 5 
when substring(Violation_Time,1,2) in ('08','09','10','11') and upper(substring(Violation_Time,-1))='P'then 6 
END as violation_time_bin from parking_violations_issued_2017 ;

```



6.) Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. For each of these groups, find the 3 most commonly occurring violations

---creating a new table with 2 extra columns violation_time_bin and season 
```

create table parking_violations_issued_bucket_table
(
      Summons_Number bigint,
      Plate_ID string,
      Registration_State string,
      Plate_Type string,
      Issue_Date string,
      Violation_Code int,
      Vehicle_Body_Type string,
      Vehicle_Make string,
      Issuing_Agency string,
      Street_Code1 int,
      Street_Code2 int,
      Street_Code3 int,
      Vehicle_Expiration Date,
      Violation_Location int,
      Violation_Precinct int,
      Issuer_Precinct int,
      Issuer_Code int,
      Issuer_Command string,
      Issuer_Squad string,
      Violation_Time string,
      Time_First_Observed string,
      Violation_County string,
      Violation_In_Front_Of_Or_Opposite string,
      House_Number string,
      Street_Name string,
      Intersecting_Street string,
      Date_First_Observed int,
      Law_Section int,
      Sub_Division string,
      Violation_Legal_Code string,
      Days_Parking_In_Effect string,
      From_Hours_In_Effect string,
      To_Hours_In_Effect string,
      Vehicle_Color string,
      Unregistered_Vehicle int,
      Vehicle_Year string,
      Meter_Number string,
      Feet_From_Curb int,
      Violation_Post_Code string,
      Violation_Description string,
      No_Standing_or_Stopping_Violation string,
      Hydrant_Violation string,
      Double_Parking_Violation string,
      new_violation_time bigint,
      violation_time_bin int,
      season string
)
clustered by (violation_time) 
sorted by (violation_time)
into 6 buckets 
row format delimited
fields terminated by ',';



insert overwrite table parking_violations_issued_bucket_table  select Summons_Number,Plate_ID,Registration_State,Plate_Type,
Issue_Date,
Violation_Code,
Vehicle_Body_Type,
Vehicle_Make,
Issuing_Agency,
Street_Code1,
Street_Code2,
Street_Code3,
Vehicle_Expiration,
Violation_Location,
Violation_Precinct,
Issuer_Precinct,
Issuer_Code,
Issuer_Command,
Issuer_Squad,
violation_time ,
Time_First_Observed,
Violation_County,
Violation_In_Front_Of_Or_Opposite,
House_Number,
Street_Name,
Intersecting_Street,
Date_First_Observed,
Law_Section,
Sub_Division,
Violation_Legal_Code,
Days_Parking_In_Effect,
From_Hours_In_Effect,
To_Hours_In_Effect,
Vehicle_Color,
Unregistered_Vehicle,
Vehicle_Year,
Meter_Number,
Feet_From_Curb,
Violation_Post_Code,
Violation_Description,
No_Standing_or_Stopping_Violation,
Hydrant_Violation,
Double_Parking_Violation,
case when substr(violation_time,5,1)='P' then unix_timestamp(substr(violation_time,0,4),'HHmm')+12*60*60 else unix_timestamp(substr(violation_time,0,4),'HHmm') END ,
case when substring(Violation_Time,1,2) in ('00','01','02','03','12') and upper(substring(Violation_Time,-1))='A' then 1 
when substring(Violation_Time,1,2) in ('04','05','06','07') and upper(substring(Violation_Time,-1))='A' then 2 
when substring(Violation_Time,1,2) in ('08','09','10','11') and upper(substring(Violation_Time,-1))='A' then 3 
when substring(Violation_Time,1,2) in ('12','00','01','02','03') and upper(substring(Violation_Time,-1))='P' then 4 
when substring(Violation_Time,1,2) in ('04','05','06','07') and upper(substring(Violation_Time,-1))='P' then 5 
when substring(Violation_Time,1,2) in ('08','09','10','11') and upper(substring(Violation_Time,-1))='P'then 6 
END,
case when month(from_unixtime(unix_timestamp(issue_date,'MM/dd/yyyy'),'yyyy-MM-dd')) between 03 and 05 then 'spring' 
when month(from_unixtime(unix_timestamp(issue_date,'MM/dd/yyyy'),'yyyy-MM-dd')) between 06 and 08 then 'summer' 
when month(from_unixtime(unix_timestamp(issue_date,'MM/dd/yyyy'),'yyyy-MM-dd')) between 09 and 11 then 'autumn' 
when month(from_unixtime(unix_timestamp(issue_date,'MM/dd/yyyy'),'yyyy-MM-dd')) in (1,2,12) then 'winter' 
else 'unknown' end  from parking_violations_issued_2017 ;


```


---for each group the top three violations code   are as follows 

```
create table top_violations_per_bin as select * ,DENSE_RANK () OVER (partition by violation_time_bin order by total_violations desc) as rank  from (select violation_time_bin,violation_code,count(*) as total_violations from parking_violations_issued_bucket_table group by violation_time_bin,violation_code) as n  ; 


select violation_time_bin,violation_code,total_violations from top_violations_per_bin where rank<=3 and violation_time_bin is not null;

```


7.) Now, try another direction. For the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)


```
create table top_3_violation_code as select violation_code,count(1) as total_violations from parking_violations_issued_bucket_table group by violation_code order by total_violations desc limit 3;


create table most_commont_time_for_top_3_violation_code as select *,RANK() OVER(PARTITION BY violation_code order by total_violations desc) as x  from (select table1.Violation_Code as violation_code,table1.violation_time_bin as violation_time_bin,count(*) as total_violations from parking_violations_issued_bucket_table table1 inner join top_3_violation_code  table2 on table1.violation_code=table2.violation_code group by table1.violation_code,table1.violation_time_bin order by total_violations desc) as n;


select violation_code,violation_time_bin,total_violations from most_commont_time_for_top_3_violation_code where x=1;


```

8.) Let’s try and find some seasonality in this data

a.) First, divide the year into some number of seasons, and find frequencies of tickets for each season. (Hint: A quick Google search reveals the following seasons in NYC: Spring(March, April, March); Summer(June, July, August); Fall(September, October, November); Winter(December, January, February))

```
select season,count(1) from parking_violations_issued_bucket_table group by season;
```

 b.)Then, find the 3 most common violations for each of these seasons.

```

create table top_violations_code_per_season as select * ,DENSE_RANK () OVER (partition by season order by total_violations desc) as rank  from (select season,violation_code,count(*) as total_violations from parking_violations_issued_bucket_table group by violation_time_bin,violation_code) as n  ; 


select season,violation_code,total_violations from top_violations_code_per_season where rank<=3;

```

Note: Please ensure you make necessary optimizations to your queries like selecting the appropriate table format, using partitioned/bucketed tables. Marks will be awarded for keeping the performance also in mind.
