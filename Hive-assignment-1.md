1. Download vechile sales data -> https://github.com/shashank-mishra219/Hive-Class/blob/main/sales_order_data.csv

2. Store raw data into hdfs location

```
hdfs dfs -put /Config/workspace/sales_order_data.csv /data/
```

3. Create a internal hive table "sales_order_csv" which will store csv data sales_order_csv .. make sure to skip header row while creating table

```
create table if not exists sales_order_csv (ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH int,
ORDERLINENUMBER int,
SALES int,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string ,
MSRP int,
PRODUCTCODE string,
PHONE int,
CITY string,
STATE string,
POSTALCODE int,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string)
row format delimited 
fields terminated by ','
tblproperties("skip.header.line.count"="1");
```

4. Load data from hdfs path into "sales_order_csv" 

```
load data  inpath '/data/sales_order_data.csv' into table  sales_order_csv;
```

5. Create an internal hive table which will store data in ORC format "sales_order_orc"

```
create table if not exists sales_order_csv_orc(ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH int,
ORDERLINENUMBER int,
SALES int,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string ,
MSRP int,
PRODUCTCODE string,
PHONE int,
CITY string,
STATE string,
POSTALCODE int,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string)
row format delimited 
fields terminated by ','
stored  as ORC;

```
6. Load data from "sales_order_csv" into "sales_order_orc"

```
from sales_order_csv insert overwrite table sales_order_csv_orc select * ;
```


Perform below menioned queries on "sales_order_orc" table :

a. Calculatye total sales per year

```
select YEAR_ID,sum(sales) from sales_order_csv_orc group by YEAR_ID;
```
b. Find a product for which maximum orders were placed

```
select productcode,sum(quantityordered) as number_of_orders from sales_order_csv_orc group by productcode order by number_of_orders desc LIMIT 1;
```
c. Calculate the total sales for each quarter

```
select qtr_id as quarter, sum(sales) as total_sales_of_quarter from sales_order_csv_orc group by qtr_id;
```

d. In which quarter sales was minimum

```
select qtr_id as quarter_with_min_sales from sales_order_csv_orc group by qtr_id order by sum(sales) LIMIT 1;
```

e. In which country sales was maximum and in which country sales was minimum

```
select country as country_with_min_sales from sales_order_csv_orc group by  country order by sum(sales) LIMIT 1 ;
```

f. Calculate quartelry sales for each city

```
select city,qtr_id as quarter , sum(sales) as  quarterly sales from  sales_order_csv_orc group by city,qtr_id;
```

h. Find a month for each year in which maximum number of quantities were sold
```
select year,month,total_monthly_orders from (select year_id as year ,MONTH_ID as month ,sum(quantityordered) as total_monthly_orders,Dense_rank() OVER(partition by year_id order by sum(quantityordered) desc) as rank from sales_order_csv_orc group by year_id,month_id ) as new_table where rank=1 ;
```
