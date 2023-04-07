
Scenario Based questions:

Q. Will the reducer work or not if you use “Limit 1” in any HiveQL query?
```
Yes and No, if the query is a simple select query on any table for e.g . select * from table_name limit 1;  The mapper and reducer will not work but in case of complex query for e.g.  select * from table_name order by column_name limit 1;  the reducer will work.
```
Q. Suppose I have installed Apache Hive on top of my Hadoop cluster using default metastore configuration. Then, what will happen if we have multiple clients trying to access Hive at the same time? 
```
The default metastore configuration allows only one Hive session to be opened at a time for accessing the metastore. Therefore, if multiple clients try to access the metastore at the same time, they will get an error.
```

Q. Suppose, I create a table that contains details of all the transactions done by the customers: CREATE TABLE transaction_details (cust_id INT, amount FLOAT, month STRING, country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ ;
Now, after inserting 50,000 records in this table, I want to know the total revenue generated for each month. But, Hive is taking too much time in processing this query. How will you solve this problem and list the steps that I will be taking in order to do so?

```
We can solve this problem of query latency by partitioning the table according to each month. So, for each month we will be scanning only the partitioned data instead of whole data sets.

As we know, we can’t partition an existing non-partitioned table directly. So, we will be taking following steps to solve the very problem: 
i.Create a partitioned table, say partitioned_transaction:

CREATE TABLE partitioned_transaction (cust_id INT, amount FLOAT, country STRING) PARTITIONED BY (month STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ ; 

ii. Enable dynamic partitioning in Hive:

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

iii. Transfer the data from the non – partitioned table into the newly created partitioned table:

INSERT OVERWRITE TABLE partitioned_transaction PARTITION (month) SELECT cust_id, amount, country, month FROM transaction_details;


```

Q. How can you add a new partition for the month December in the above partitioned table?

```
ALTER TABLE partitioned_table ADD PARTITION (month = dec) LOCATION 'hdfs path to the partitoned table';
```

Q. I am inserting data into a table based on partitions dynamically. But, I received an error – FAILED ERROR IN SEMANTIC ANALYSIS: Dynamic partition strict mode requires at least one static partition column. How will you remove this error?
```

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

```

Q. Suppose, I have a CSV file – ‘sample.csv’ present in ‘/temp’ directory with the following entries:
id first_name last_name email gender ip_address
How will you consume this CSV file into the Hive warehouse using built-in SerDe?

```
Hive provides a specific SerDe for working with CSV files. We can use this SerDe for the sample.csv by issuing following commands:
CREATE EXTERNAL TABLE sample
(id int, first_name string, 
last_name string, email string,
gender string, ip_address string) 
ROW FORMAT SERDE ‘org.apache.hadoop.hive.serde2.OpenCSVSerde’ 
STORED AS TEXTFILE LOCATION ‘/temp’;
```

Q. Suppose, I have a lot of small CSV files present in the input directory in HDFS and I want to create a single Hive table corresponding to these files. The data in these files are in the format: {id, name, e-mail, country}. Now, as we know, Hadoop performance degrades when we use lots of small files.
So, how will you solve this problem where we want to create a single Hive table for lots of small files without degrading the performance of the system?

```
We can use the SequenceFile format which will group these small files together to form a single sequence file. 

i.Create a temporary table:

CREATE TABLE temp_table (id INT, name STRING, e-mail STRING, country STRING)
ROW FORMAT FIELDS DELIMITED TERMINATED BY ‘,’ STORED AS TEXTFILE;

ii.Load the data into temp_table:

LOAD DATA INPATH ‘/input’ INTO TABLE temp_table;

iii.Create a table that will store data in SequenceFile format:

CREATE TABLE sample_seqfile (id INT, name STRING, e-mail STRING, country STRING)
ROW FORMAT FIELDS DELIMITED TERMINATED BY ‘,’ STORED AS SEQUENCEFILE;

iv.Transfer the data from the temporary table into the sample_seqfile table:

INSERT OVERWRITE TABLE sample SELECT * FROM temp_table;

Hence, a single SequenceFile is generated which contains the data present in all of the input files and therefore, the problem of having lots of small files is finally eliminated.
```


Q. LOAD DATA LOCAL INPATH ‘Home/country/state/’
OVERWRITE INTO TABLE address;

The following statement failed to execute. What can be the cause?
```
The path is a directory not a file and data should be a file.
```

Is it possible to add 100 nodes when we already have 100 nodes in Hive? If yes, how?

```
Yes, we can add the nodes by following the below steps:

Step 1: Take a new system; create a new username and password
Step 2: Install SSH and with the master node setup SSH connections
Step 3: Add ssh public_rsa id key to the authorized keys file
Step 4: Add the new DataNode hostname, IP address, and other details in /etc/hosts slaves file:

192.168.1.102 slave3.in slave3
Step 5: Start the DataNode on a new node
Step 6: Login to the new node like suhadoop or:

ssh -X hadoop@192.168.1.103
Step 7: Start HDFS of the newly added slave node by using the following command:

./bin/hadoop-daemon.sh start data node
Step 8: Check the output of the jps command on the new node

```
Hive Practical questions:

Hive Join operations

Create a  table named CUSTOMERS(ID | NAME | AGE | ADDRESS   | SALARY)
Create a Second  table ORDER(OID | DATE | CUSTOMER_ID | AMOUNT
)

```
create table if not exists  customer ( id int ,name string ,age int, address string , salary int) row format delimited fields terminated by ',' stored as textfile;

create table if not exists  orders( order_id int ,order_date date ,customer_id int,  amount int) row format delimited fields terminated by ',' stored as textfile;

insert into customer values (1,"Tajbar",23,"Delhi",50000),(2,"Ankit",26,"Delhi",30000),(3,"Nitin",24,"Dehradun",40000),(4,"Neeraj",23,"Banglore",60000),(5,"Alok",24,"Haridwar",15000);


insert into orders values (1001,'2022-01-01',2,1299),(1002,'2023-04-03',4,999),(1003,'2023-03-23',1,3999),(1004,'2023-02-26',5,1029),(1005,'2023-04-06',5,1599) ;





Now perform different joins operations on top of these tables
(Inner JOIN, LEFT OUTER JOIN ,RIGHT OUTER JOIN ,FULL OUTER JOIN)

select * from customer inner join orders on customer.id=orders.customer_id ;

select * from customer left outer join orders on customer.id=orders.customer_id ;

select * from customer right outer join orders on customer.id=orders.customer_id ;

select * from customer full outer join orders on customer.id=orders.customer_id ;

```

BUILD A DATA PIPELINE WITH HIVE

Download a data from the given location - 
https://archive.ics.uci.edu/ml/machine-learning-databases/00360/

1. Create a hive table as per given schema in your dataset 

```
create table air_quality(Date_of_record string,Time_of_record string,CO_GT float,PT08_S1_CO int,NMHC_GT int,C6H6_GT float,PT08_S2_NMHC int,NOx_GT int ,PT08_S3_NOx int ,NO2_GT int,PT08_S4_NO2 int,PT08_S5_O3 int,T float,RH float,AH float) row format delimited fields terminated by ';' tblproperties("skip.header.line.count"="1");

```
2. try to place a data into table location

```
load data local inpath '/config/workspace/AirQualityUCI.csv' into table air_quality;

```

3. Perform a select operation . 

```
select * from air_quality ;
```

4. Fetch the result of the select operation in your local as a csv file . 

```
insert overwrite local directory '/config/workspace/sqlresult/'
row format delimited fields terminated by ','
select * from air_quality;

using cat command to create and append the csv file of sql result 

cat sqlresult/* > result_sql_query.csv
```

5. Perform group by operation . 

```
select date_of_record ,count(1)  from air_quality group by date_of_record ;
```

7. Perform filter operation at least 5 kinds of filter examples . 

```
select  * from air_quality where date_of_record >'01/01/2001' ;
 
```

8. show and example of regex operation

```
select regexp_extract(date_of_record,'(.+)/(.+)/(.+)' ,1) as date,regexp_extract(date_of_record,'(.+)/(.+)/(.+)' ,2) as month,regexp_extract(date_of_record,'(.+)/(.+)/(.+)' ,3) as year from air_quality_new;
```

9. alter table operation 

```
alter table air_quality rename to air_quality_new;
```

10 . drop table operation

```
drop table air_quality_new;
```

12 . order by operation . 

```
select * from air_quality_new order by co_gt desc;
```

13 . where clause operations you have to perform .

```
select * from air_quality_new where date_of_record >'01/01/2003' order by co_gt desc;
```
 
14 . sorting operation you have to perform . 

```
select * from air_quality_new order by co_gt desc;
```

15 . distinct operation you have to perform . 

```
select distinct(date_of_record) from air_quality_new;
```

16 . like an operation you have to perform . 

```
select * from air_quality_new  where co_gt like '2.%' ;
```

17 . union operation you have to perform . 

```
(select * from air_quality_new where co_gt <1 ) union (select * from air_quality_new where co_gt >2 ) 
```

18 . table view operation you have to perform . 

```
create view vw_air_quality as select * from air_quality_new where date_of_record>'01/03/2004';

select * from vw_air_quality;

```




