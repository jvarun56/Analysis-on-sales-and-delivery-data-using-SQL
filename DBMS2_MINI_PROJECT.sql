create database dbms2_mini_project;
use dbms2_mini_project;

#1. Join all the tables and create a new table called combined_table.(market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)

create table combined_table as
select MF.*,CD.Customer_Name,CD.Province,CD.Region,CD.Customer_Segment,
OD.Order_ID,OD.Order_Date,OD.Order_Priority,
PD.Product_Category,PD.Product_Sub_Category,
SD.Ship_Mode,SD.Ship_Date
FROM market_fact MF LEFT join cust_dimen CD using (Cust_id) 
LEFT join orders_dimen OD using(ord_id) 
LEFT join prod_dimen PD using (Prod_id)
LEFT join shipping_dimen SD using (Ship_id);
select * from combined_table limit 10000;   -- 8336 rows

# 2.	Find the top 3 customers who have the maximum number of orders

-- Considering only top3 counts ignoring the same counts
select Cust_Id,Customer_Name, count(order_ID) as orders_count from combined_table group by Cust_Id 
order by orders_count desc limit 3;  -- 3 rows

-- using dense rank to extract exact top 3
select Cust_Id,Customer_Name,count,top3
from (select Cust_Id,Customer_Name,count(order_id) as count, dense_rank() over(order by count(order_id) desc) as top3 from combined_table
group by cust_id)as temp where top3<=3; -- 9 rows

-- 3.	Create a new column DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date.
alter table combined_table
add column DaysTakenForDelivery int;
update combined_table set DaysTakenForDelivery = datediff(str_to_date(Ship_Date,'%d-%c-%Y'),str_to_date(Order_Date,'%d-%c-%Y'));

-- 4.	Find the customer whose order took the maximum time to get delivered.

select Cust_Id,Customer_Name,order_id,DaysTakenForDelivery from combined_table 
order by DaysTakenForDelivery desc limit 1;  -- 1 row (92 days)

-- 5.	Retrieve total sales made by each product from the data (use Windows function)

select distinct prod_id,product_sub_category,sum(sales) over (partition by prod_id) as 'Total Sales' 
from combined_table; -- 17 rows


-- 6.	Retrieve total profit made from each product from the data (use windows function)

select distinct prod_id,product_sub_category,sum(profit)  over (partition by prod_id) as 'Total Profit' 
from combined_table;  -- 17 rows

-- 7.	Count the total number of unique customers in January and how many of them came back every month over the 
-- entire year in 2011


# USING WINDOW FUNCTION FOR COUNT

select distinct year(str_to_date(order_date,'%d-%c-%Y')) as Year,Month(str_to_date(order_date,'%d-%c-%Y')) as 'month',
count(*) over(partition by Month(str_to_date(order_date,'%d-%c-%Y')) order by month (str_to_date(order_date,'%d-%c-%Y'))) as 'count' 
from combined_table
where cust_id in (select  distinct cust_id
from combined_table
where Month(str_to_date(order_date,'%d-%c-%Y')) =1 
and year(str_to_date(order_date,'%d-%c-%Y')) =2011) 
and year(str_to_date(order_date,'%d-%c-%Y')) =2011;

-- 12 ROWS


-- 8.	Retrieve month-by-month customer retention rate since the start of the business.(using views)
#1: Create a view where each user’s visits are logged by month, allowing for the possibility that these will have 
#occurred over multiple # years since whenever business started operations
# 2: Identify the time lapse between each visit. So, for each person and for each month, we see when the next visit is.
# 3: Calculate the time gaps between visits
# 4: categorise the customer with time gap 1 as retained, >1 as irregular and NULL as churned
# 5: calculate the retention month wise

# create view retention as
select order_year,order_month,count(cust_id) as count, count(if(retention='Regular',cust_ID,Null)) as retented_customers,
count(if(retention='Regular',cust_ID,Null))/ count(cust_id)*100 as retention_rate
from (select *,
case when time_gap_months=1 then 'Regular'
when time_gap_months>1 then 'irregular'
else 'churned'
end as retention
from (select cust_id,str_to_date(order_date,'%d-%c-%Y') as order_date,
Year(str_to_date(order_date,'%d-%c-%Y')) as order_year,
month(str_to_date(order_date,'%d-%c-%Y')) as order_month,
lead(str_to_date(order_date,'%d-%c-%Y'),1) over (partition by cust_id order by str_to_date(order_date,'%d-%c-%Y') asc) as next_order_date,
lead(year(str_to_date(order_date,'%d-%c-%Y')),1)over (partition by cust_id order by year(str_to_date(order_date,'%d-%c-%Y') )asc) as next_order_year,
lead(month(str_to_date(order_date,'%d-%c-%Y')),1)over (partition by cust_id) as next_order_month,
abs(timestampdiff(month,str_to_date(order_date,'%d-%c-%Y') ,
lead(str_to_date(order_date,'%d-%c-%Y'),1) over (partition by cust_id order by str_to_date(order_date,'%d-%c-%Y') asc))) as time_gap_months
from combined_table
order by cust_id) temp1) temp2 group by order_year,order_month order by order_year,order_month;


select *  from retention;  -- 48 ROWS

#madhu
select order_year,
	order_month,
    count(cust_id) as Count_Customers,
    count(if(retention='Regular',cust_id,Null)) As Regular_Customers,
       (count(if(retention='Regular',cust_id,Null))/count(cust_id))*100 as Retention_Percentage
       from(select *,
case when time_gap_months=1 then 'Regular'
when time_gap_months>1 then 'irregular'
else 'churned'
end as retention
from (select cust_id,str_to_date(order_date,'%d-%c-%Y') as order_date,
Year(str_to_date(order_date,'%d-%c-%Y')) as order_year,
month(str_to_date(order_date,'%d-%c-%Y')) as order_month,
lead(str_to_date(order_date,'%d-%c-%Y'),1) over (partition by cust_id order by str_to_date(order_date,'%d-%c-%Y') asc) as next_order_date,
lead(year(str_to_date(order_date,'%d-%c-%Y')),1)over (partition by cust_id order by year(str_to_date(order_date,'%d-%c-%Y') )asc) as next_order_year,
lead(month(str_to_date(order_date,'%d-%c-%Y')),1)over (partition by cust_id) as next_order_month,
abs(timestampdiff(month,str_to_date(order_date,'%d-%c-%Y') ,
lead(str_to_date(order_date,'%d-%c-%Y'),1) over (partition by cust_id order by str_to_date(order_date,'%d-%c-%Y') asc))) as time_gap_months
from combined_table
order by cust_id) temp1) as temp2
group by order_year,order_month
order by order_year,order_month;



















