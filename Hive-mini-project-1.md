This is a real time dataset of the ineuron technical consultant team. You have to perform hive analysis on this given dataset.

Download Dataset 1 - https://drive.google.com/file/d/1WrG-9qv6atP-W3P_-gYln1hHyFKRKMHP/view

Download Dataset 2 - https://drive.google.com/file/d/1-JIPCZ34dyN6k9CqJa-Y8yxIGq6vTVXU/view

Note: both files are csv files. 

placing the files in hdfs location 

hdfs dfs -put /config/workspace/AgentLogingReport.csv /data/

hdfs dfs -put /config/workspace/AgentPerformance.csv /data/


1. Create a schema based on the given dataset

--AgentLogingReport backup table

create table if not exists agent_loging_report_bkp
(sl_no int,agent_name string,ag_date string,login_time string,logout_time string,duration string)row format delimited fields terminated by ','tblproperties("skip.header.line.count"="1");


create table if not exists agent_loging_report
(sl_no int,agent_name string,ag_date date,login_time bigint,logout_time bigint,duration bigint)row format delimited fields terminated by ','tblproperties;

--AgentPerformance backup table

create table if not exists agent_performance_bkp
(sl_no int,ag_date string,agent_name string,total_chats int,avg_response_time string,avg_resolution_time string,avg_rating float,total_feedbacks int)row format delimited fields terminated by ','tblproperties("skip.header.line.count"="1");

create table if not exists agent_performance
(sl_no int,ag_date date,agent_name string,total_chats int,avg_response_time bigint,avg_resolution_time bigint,avg_rating float,total_feedbacks int)row format delimited fields terminated by ',';


2. Dump the data inside the hdfs in the given schema location.

load data inpath '/data/AgentLogingReport.csv' into table agent_loging_report_bkp ;

load data inpath '/data/AgentPerformace.csv' into table agent_performance_bkp ;


insert into table agent_loging_report select sl_no,agent_name ,from_unixtime(unix_timestamp(ag_date,'dd-MMM-yy'),'yyyy-MM-dd'),case when login_time like '%:%:%' then unix_timestamp(login_time,'HH:mm:ss')
when login_time like '%:%' then unix_timestamp(login_time,'mm:ss')
else login_time 
end as login_time,
case when logout_time like '%:%:%' then unix_timestamp(logout_time,'HH:mm:ss')
when logout_time like '%:%' then unix_timestamp(logout_time,'mm:ss')
else logout_time 
end as logout_time,
case when duration like '%:%:%' then unix_timestamp(duration,'HH:mm:ss')
when duration like '%:%' then unix_timestamp(duration,'mm:ss')
else duration 
end as duration from agent_loging_report_bkp;




insert into agent_performance select sl_no,from_unixtime(unix_timestamp(ag_date,'M/d/yyyy'),'yyyy-MM-dd'),agent_name,total_chats,
case when avg_response_time like '%:%:%' then unix_timestamp(avg_response_time,'HH:mm:ss') when avg_response_time like '%:%:%' then unix_timestamp(avg_response_time,'mm:ss')else avg_response_time end as avg_response_time,
case when avg_resolution_time like '%:%:%' then unix_timestamp(avg_resolution_time,'HH:mm:ss') when avg_resolution_time like '%:%:%' then unix_timestamp(avg_resolution_time,'mm:ss')else avg_resolution_time end as avg_resolution_time,
avg_rating,total_feedbacks from agent_performance_bkp;


3. List of all agents' names. 

select distinct(agent_name) as agent_name from agent_performance;

4. Find out agent average rating.

select agent_name as agent_name,round(avg(avg_rating),3) as average_rating from agent_performance group by agent_name ;

5. Total working days for each agents 

select agent_name as agent_name , count(date) as number_of_working_days from agent_performance group by agent_name;

6. Total query that each agent have taken 

select agent_name as agent_name ,sum(total_chats) as total_query_taken from agent_performance group by agent_name;

7. Total Feedback that each agent have received 

select agent_name as agent_name ,sum(total_feedbacks) as total_feedback from agent_performance group by agent_name;

8. Agent name who have average rating between 3.5 to 4 


select agent_name as agent_name,round(avg(avg_rating),3) as average_rating from agent_performance group by agent_name having round(avg(avg_rating),3) between 3.5 and 4;

9. Agent name who have rating less than 3.5 

select agent_name as agent_name,round(avg(avg_rating),3) as average_rating from agent_performance group by agent_name having round(avg(avg_rating),3) <3.5 ;

10. Agent name who have rating more than 4.5 

select agent_name as agent_name,round(avg(avg_rating),3) as average_rating from agent_performance group by agent_name having round(avg(avg_rating),3) >4 ;

11. How many feedback agents have received more than 4.5 average

select agent_name as agent_name,count(avg_rating) as count_of_average_rating from agent_performance where avg_rating>4.5  group by agent_name  ;

12. average weekly response time for each agent 

select agent_name,weekofyear(ag_date) as week_of_year,avg(avg_response_time) from agent_performance group by agent_name,weekofyear(ag_date);

13. average weekly resolution time for each agents 

select agent_name,weekofyear(ag_date) as week_of_year,round(avg(avg_resolution_time),3) from agent_performance group by agent_name,weekofyear(ag_date); 

14. Find the number of chat on which they have received a feedback 

select agent_name,count(total_chats) as calls_with_feedback  from agent_performance where total_chats> 0 group by agent_name;

15. Total contribution hour for each and every agents weekly basis 

select agent_name, weekofyear(ag_date) as week_no, round(sum(duration)/3600,0) as hours_worked from agent_loging_report group by agent_name, weekofyear(ag_date);

16. Perform inner join, left join and right join based on the agent column and after joining the table export that data into your local system.

insert overwrite local directory '/config/workspace/inner_join' select * from agent_loging_report as alr inner join agent_performance as ap on alr.agent_name=ap.agent_name;


insert overwrite local directory '/config/workspace/left_join' select * from agent_loging_report as alr left join agent_performance as ap on alr.agent_name=ap.agent_name;


insert overwrite local directory '/config/workspace/right_join' select * from agent_loging_report as alr right join agent_performance as ap on alr.agent_name=ap.agent_name;


17. Perform partitioning on top of the agent column and then on top of that perform bucketing for each partitioning.



set hive.exec.dynamic.partition =true;

set hive.exec.dynamic.partition.mode =nonstrict;


create table if not exists agent_loging_report_part(sr_no int ,ag_date date ,login_time bigint ,logout_time bigint,duration bigint) partitioned by(agent_name string) clustered by (ag_date)  into 10 buckets row format delimited fields terminated by ',' tblproperties("skip.header.line.count"="1");


insert overwrite table  agent_loging_report_part partition (agent_name) select sl_no,ag_date,login_time,logout_time,duration,agent_name from agent_loging_report;
