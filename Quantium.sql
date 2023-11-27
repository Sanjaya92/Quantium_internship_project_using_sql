-- create a 'transaction_data' and 'purchase_behaviour' table. 

drop table if exists transaction_data ;
create table transaction_data(
date date,
store_num smallint,
lylty_card_num int,
	txn_id int,
prod_num smallint,
prod_name text ,
prod_qty smallint,
total_sale numeric(10,3)
)
;

drop table if exists purchase_behaviour ;
create table purchase_behaviour(
lylty_card_num int,
lifestage varchar(100),
	premium_customer varchar(25)
)
;
                        DATA CLEANING  & VALIDATION      
--check if there is null value in 'lylty_card_num' column from "transaction_data " table
select * 
from  transaction_data 
where lylty_card_num is null 
order by lylty_card_num asc ;
-- NONE 

 --Check any null value in 'purchase_behaviour' table 
    select * 
from purchase_behaviour 
where lylty_card_num is null or lifestage is null or premium_customer is null
 
 --check duplicates in 'lylty_card_num' column of 'purchase_behaviour' , so we can work on primary key and foreign key . 
select lylty_card_num , count(*)
from purchase_behaviour 
group by 1
having count(*) > 1


-- DECLARING PERIAMRY KEY IN 'purchase_behaviour' table for column 'lylty_card_num'
alter table purchase_behaviour
add constraint lylty_card_num_pk primary key (lylty_card_num)


-- ADD FOREIGN KEY ON TABLE 'transaction_data' in column 'lylty_card_num'
alter table transaction_data
add constraint lylty_card_num foreign key (lylty_card_num)
references purchase_behaviour (lylty_card_num) ;
  

-- We are doing retail analytics process,So first we need to check 'prod_qty' 
    select * from transaction_data  order by prod_qty desc

/** There is 'lylty_card_num' 226000 with only 2 transactions and 200 units of 'prod_qty' per transaction. it seems like 
that transactions is for commercial purpose but not retail customers, so we remove that both lylty_card_num and transactions**/
  delete from transaction_data
  where lylty_card_num = '226000';

-- '2018-12' has most amount of sale of 167913.400 and than sales start descresing, so we will analyze more in '2018-12' everyday.
select to_char(date, 'yyyy-mm-dd') as date_december, sum(total_sale)
from transaction_data
where to_char(date, 'yyyy-mm') = '2018-12'
group by 1
order by 1 asc
-- RESULT SHOWS THAT SALES ARE INCREASING BEACAUSE OF CHRISTMAS AND SALES IS ZERO IN CHRISTMAS DAY. SO WE KNOW DATE NO LONGER HAS OUTLIERS .
 
 -- we can make sure by below query also .
select date_series.date_day, td.date
from (select generate_series('2018-07-01'::date, '2019-06-30'::date, interval '1 day')::date as date_day) as date_series
left join transaction_data td
on date_series.date_day = td.date
where td.date is null
-- ABOVE QUERY SHOWS THAT '2018-12-25' ID ONLY DAY THAT COMPANY DO NOT HAVE ANY SALE.


-- total rows and transaction in 'transaction_data' table
select count(*) as transaction_table_rows from transaction_data  -- 264834 
select count(distinct txn_id) as num_of_txn_id from transaction_data	  --263125


-- THERE IS A DIFFERENCE IN ABOVE COUNT , SO WE GO FURTHER TO SEE TXN_ID
select sum(c.num) as total_rows_including_duplicates
from (
SELECT date, store_num , lylty_card_num , txn_id,count(*) as num
FROM transaction_data
group by 1,2,3,4 order by count(*) desc ) c
having count(*) > 1
/** SO ABOVE QUERIES SHOWS THAT THERE ARE MULTIPLE ROWS WHERE 'date', 'store_num', 'lylty_card_num', 'txn_id' ARE SAME.
 BUT IF WE SUM ALL ROWS WITH DUPLICATES IT SHOWS THAT ROWS ARE EQUAL TO transaction_data ROWS i.e 264834 **/
 
SELECT date, store_num , lylty_card_num , txn_id,prod_name, count(*) as num_duplicate
FROM transaction_data
group by 1,2,3,4 , 5
having count(*) > 1
order by count(*) desc

select * 
from transaction_data
where store_num = '107' and lylty_card_num = '107024' and txn_id = '108462'
-- THERE IS STILL ONE DUPLICATE REMAINING, I.E., ALL 5 COLUMNS ARE THE SAME. ABOVE QUERIES PROVIDE DETAILS ABOUT THAT TRANSACTION.

-- From "transaction_data" table column 'prod_name' we seperate 'pack_size' and fill it with corresponding values .
alter table transaction_data
add column pack_size_grams smallint ;
 
--Now we extract pack_size and create "new_values" as cte and update 'pack_size_grams' column in transaction_data table 
with new_values as 
( select *, cast((regexp_matches(prod_name ,'\d+'))[1] as int) as new_value
from transaction_data order by txn_id asc
)
update transaction_data as td
set pack_size_grams = nv.new_value
from new_values as nv
where td.lylty_card_num = nv.lylty_card_num and
     td.txn_id = nv.txn_id and
     td.date = nv.date and
	 td.store_num = nv.store_num and 
	 td.prod_name = nv.prod_name and
	 td.prod_qty = nv.prod_qty and
	 td.prod_num = nv.prod_num and
	 td.total_sale = nv.total_sale

-- now create brand_name and fill it . Brand name is first word in 'prod_name ' column
alter table transaction_data 
add column brand_name varchar(25) ;

update transaction_data
set brand_name = (regexp_split_to_array(prod_name, '\s+'))[1]

--Now we rename some 'brand_name' which seems different but they are same .

update transaction_data
set brand_name = case 
				  when brand_name = 'Smith' then 'Smiths'
				  when brand_name in ('Red', 'RRD') then 'Red_Rock_Deli'
                  when brand_name in ('Grain', 'GrnWves') then 'Grainwaves'
				  when brand_name = 'Dorito' then 'Doritos'
				  when brand_name = 'Infzns' then 'Infuzions'
				  when brand_name = 'Snbts' then 'Sunbites'
				  else brand_name
				  end ;


                                   ANALYSIS
								   
								   
-- Who spends the most on chips (total sales), describing customers by lifestage and how premium their general purchasing behaviour is
SELECT
  CASE WHEN row_number() over (partition by premium_customer order by lifestage) = 1
    THEN premium_customer
    ELSE ''
  END AS premium_customer_display,
  lifestage,
  SUM(total_sale) AS total_sales_amount
FROM
  transaction_data td
LEFT JOIN
  purchase_behaviour pb ON td.lylty_card_num = pb.lylty_card_num
GROUP BY
  premium_customer, lifestage
ORDER BY
  premium_customer, lifestage
  ;
--THUS, WE OBSERVE THAT OLDER FAMILIES IN THE 'BUDGET' CATEGORY, YOUNG SINGLES/COUPLES IN THE 'MAINSTREAM,' AND OLDER SINGLES/COUPLES ARE SPENDING THE HIGHEST AMOUNTS WITHIN THEIR RESPECTIVE PREMIUM CUSTOMER GROUPS

--- How many customers are in each segment
 select  CASE WHEN row_number() over (partition by premium_customer order by lifestage) = 1
    THEN premium_customer
    ELSE ''
  END AS premium_customer, 
 lifestage, count(distinct td.lylty_card_num) as total_numbers_of_customers
FROM
  transaction_data td
LEFT JOIN
  purchase_behaviour pb ON td.lylty_card_num = pb.lylty_card_num
GROUP BY
  premium_customer, lifestage
-- WE CAN SEE THAT YOUNG SINGLES/COUPLES AND OLDER SINGLES/COUPLES CUSTOMERS HAVE THE HIGHEST COUNTS IN NUMBERS.

-- How many chips are bought per customer by segment
   select  CASE WHEN row_number() over (partition by premium_customer order by lifestage) = 1
    THEN premium_customer
    ELSE ''
  END AS premium_customer, 
 lifestage, sum(prod_qty) as total_prod_qty
FROM
  transaction_data td
LEFT JOIN
  purchase_behaviour pb ON td.lylty_card_num = pb.lylty_card_num
GROUP BY
  premium_customer, lifestage  
-- IN THE 'NEW FAMILIES' LIFESTAGE CATEGORY, BUYING ACTIVITY IS OBSERVED TO BE THE LOWEST."

-- What's the average chip price by customer segment
   select  CASE WHEN row_number() over (partition by premium_customer order by lifestage) = 1
    THEN premium_customer
    ELSE ''
  END AS premium_customer, 
 lifestage, round(sum(total_sale)/ sum(prod_qty),2) as average_price_per_unit
 FROM
  transaction_data td
LEFT JOIN
  purchase_behaviour pb ON td.lylty_card_num = pb.lylty_card_num
GROUP BY
  premium_customer, lifestage  
 /** MAINSTREAM MIDAGE AND YOUNG SINGLES AND COUPLES ARE WILLING TO PAY MORE PER PACKET OF CHIPS. HOWEVER, IF WE LOOK AT 
 BUDGET AND PREMIUM CUSTOMERS, THEIR AVERAGE PRICE PER UNIT FALLS WITHIN A SIMILAR RANGE. **/
  
-- What's the average number of products by customer segments .
 
 with total as (
select  premium_customer, lifestage, sum(prod_qty) as total_qty
FROM
  transaction_data td
LEFT JOIN
  purchase_behaviour pb ON td.lylty_card_num = pb.lylty_card_num
GROUP BY 
    premium_customer, lifestage
),
customer_count as (
select premium_customer, lifestage , count(*) as num_of_customer
from purchase_behaviour
group by 1,2
)
select tt.premium_customer, tt.lifestage, round((total_qty::decimal / num_of_customer), 2) as avg_prod_qty
from total tt join customer_count  cc
on tt.premium_customer = cc.premium_customer and 
   tt.lifestage = cc.lifestage
   order by 1 , 3 desc
   ;
/** RESULT OF ABOVE QUERY CLEARLY SHOWS THAT OLDER AND YOUNG FAMILIES IN EACH PREMIUM CATEGORY ARE CONTRIBUTING MORE WHEN 
 IT COMES TO AVERAGE NUMBERS OF CHIPS PER CUSTOMER  **/ 

-- Customer Segments based on Pack Size .
  with pack_size as (
 select premium_customer, lifestage ,count(*) as numbers_of_customer, sum(pack_size_grams) as total_pack_grams
  FROM
  transaction_data td
LEFT JOIN
  purchase_behaviour pb ON td.lylty_card_num = pb.lylty_card_num
group by premium_customer, lifestage
)
select premium_customer, lifestage, 
 round((total_pack_grams::decimal /numbers_of_customer ), 2) as average_pack_size
 from pack_size
 order by 3 desc
-- MAINSTREAM YOUNG AND MIDAGE SINGLES AND COUPLES ARE PURCHASING THE LARGEST AVERAGE SIZE OF CHIPS AMONG THE CUSTOMER SEGMENTS  

-- Top brand based on customer segments with num_of_customer fro brands.
with customer_ranking as (
select premium_customer , lifestage, brand_name, count(*) as num_of_customer,
row_number() over(partition by premium_customer ,lifestage order by count(*) desc) as cust_rank
FROM
  transaction_data td
LEFT JOIN
  purchase_behaviour pb ON td.lylty_card_num = pb.lylty_card_num
group by premium_customer, lifestage, brand_name
)
select premium_customer, lifestage,brand_name,num_of_customer
from customer_ranking
where cust_rank = 1
;
-- KETTLE IS THE MOST DESIRED CHIP TYPE AMONG DIFFERENT CUSTOMERS, EXCEPT FOR BUDGET YOUNG SINGLES/COUPLES AND PREMIUM OLDER FAMILIES, WHERE THEY PREFER SMITHS.

-- How much contribution made on sale by each "premium_customer" group .
select premium_customer, sum(total_sale) as total_sale,
 round(100 * sum(total_sale) / (select sum(total_sale) from transaction_data td join purchase_behaviour pb
on td.lylty_card_num = pb.lylty_card_num), 2) as contribution_on_sale
from transaction_data td join purchase_behaviour pb
on td.lylty_card_num = pb.lylty_card_num
group by 1
order by 2 desc
-- "Mainstream" STAND ON FIRST PLACE WITH 38.81% , "Budget" IN SECOND 34.96% AND "Premium" AT LAST WITH 26.23% .

--"lylty_card_num" retention rate
with  previous as (
select count(distinct lylty_card_num) as transaction_last_year
from transaction_data
where extract(year from date) = '2018'
),
thisyear as (
select distinct count( distinct lylty_card_num ) as transaction_this_year
from transaction_data
where extract(year from date) = '2019' and
lylty_card_num in (
                        select distinct lylty_card_num
                       from transaction_data
                     where extract(year from date) = '2018'
))
 select 
    round((100.00 * thisyear.transaction_this_year / previous.transaction_last_year), 2) as retention_rate
	from previous, thisyear ;
-- SO, WE CAN CONCLUDE THAT 73.91 % "lytly_card_num" ARE STILL COMING IN 2019 FROM 2018 .	
