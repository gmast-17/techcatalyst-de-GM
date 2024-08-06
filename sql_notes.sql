-- window function
SELECT YEAR(O_ORDERDATE) AS YR,
        MONTH(O_ORDERDATE) AS MNTH,
        SUM(O_TOTALPRICE) AS MNTH_YR_SUBTOTAL,
        SUM(SUM(O_TOTALPRICE)) OVER(PARTITION BY YEAR(O_ORDERDATE), MONTH(O_ORDERDATE)) AS YEARLY_SUBTOTAL,  --encapsulate sum with another sum to trick it into                                                                                                    thinking its an aggreaget so we dont have to put it in the group by 
        SUM(SUM(O_TOTALPRICE)) OVER() AS GRAND_TOTAL,
        AVG(AVG(O_TOTALPRICE)) OVER(PARTITION BY YEAR(O_ORDERDATE) ORDER BY YEAR(O_ORDERDATE)) AS ANNUAL_AVG_SALES
FROM ORDERS/workspaces/techcatalyst-de-GM/course_notes
GROUP BY YR, MNTH
ORDER BY YR, MNTH;

-- file formats
CREATE OR REPLACE FILE FORMAT GMASTRORILLI_json_format
TYPE = 'JSON';

CREATE OR REPLACE FILE FORMAT GMASTRORILLI_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

CREATE OR REPLACE FILE FORMAT GMASTRORILLI_parquet_format
TYPE = 'PARQUET';

-- nested sql 
SELECT l_shipmode, AVG(l_shipdate - l_commitdate) AS AVG_DAYS
FROM lineitem
WHERE l_shipmode in (
                      SELECT DISTINCT l_shipmode
                      FROM lineitem
                      WHERE L_SHIPMODE NOT IN ('TRUCK','SHIP')
                      )
GROUP BY L_SHIPMODE;

-- simple case
SELECT O_ORDERKEY, 
       O_ORDERDATE, 
       O_ORDERSTATUS,
       CASE O_ORDERSTATUS
            WHEN 'P' THEN 'PARTIAL FUFILMENT'
            WHEN 'F' THEN 'FULLY DELIVERED'
            WHEN 'O' THEN 'ORDER OUT'
            ELSE 'NOT SURE'
        END AS STATUS_DESCRIPTION
FROM ORDERS;

-- search case expression
SELECT O_ORDERKEY, 
       O_ORDERDATE, 
       O_ORDERSTATUS,
       CASE 
            WHEN O_ORDERSTATUS ='P' THEN 'PARTIAL FUFILMENT'
            WHEN O_ORDERSTATUS ='F' THEN 'FULLY DELIVERED'
            WHEN O_ORDERSTATUS ='O' THEN 'ORDER OUT'
            ELSE 'NOT SURE'
        END AS STATUS_DESCRIPTION
FROM ORDERS;

-- case and nested sql
SELECT C.C_CUSTKEY,
        C.C_NAME,
        CASE
            WHEN C.C_CUSTKEY IN
                (
                SELECT C.C_CUSTKEY
                FROM CUSTOMER C
                JOIN ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY
                WHERE O.O_TOTALPRICE > 400000
                AND C.C_CUSTKEY BETWEEN 74000 AND 74020
                )THEN 'BIG SPENDER'
            ELSE 'REGULAR'
            END AS CUST_TYPE
FROM CUSTOMER C
WHERE C_CUSTKEY BETWEEN 74000 AND 74020;

-- cte
WITH SUPPLIER_OVERVIEW AS
    (SELECT S.S_NAME AS SUPPLIER_NAME,
            SUM(PS.PS_SUPPLYCOST) AS TOTAL_PART_VALUE
    FROM SUPPLIER S
    JOIN PARTSUPP PS ON S.S_SUPPKEY = PS.PS_SUPPKEY
    GROUP BY S.S_NAME
    ORDER BY SUM(PS.PS_SUPPLYCOST) DESC)
SELECT *
FROM SUPPLIER_OVERVIEW
LIMIT 5;


-- date time
SELECT CURRENT_DATE() as TODAY_DATE,
    YEAR(TODAY_DATE) AS YEAR,
    MONTH(TODAY_DATE) AS MONTH,
    QUARTER(TODAY_DATE) AS QUARTER,
    WEEK(TODAY_DATE) AS WEEK,
    DAYNAME(TODAY_DATE) AS NAME_OF_DAY;



-- create table
CREATE TRANSIENT TABLE TECHCATALYST_DE.GMASTRORILLI.AVG_SHIPPING_TIME_TRANS
(S_SUPPKEY NUMBER(38,0),
S_NAME VARCHAR(25),
AVG_SHIPPING_DAYS NUMBER(12,6));

-- insert into 
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.AVG_SHIPPING_TIME_TRANS
SELECT S.S_SUPPKEY,
        S.S_NAME,
        AVG(DATEDIFF(DAY, L.L_SHIPDATE, L.L_RECEIPTDATE)) AS AVG_SHIPPING_DAYS
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER AS S
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM L ON S.S_SUPPKEY = L.L_SUPPKEY
GROUP BY S.S_SUPPKEY, S.S_NAME;

-- create view
CREATE VIEW TECHCATALYST_DE.GMASTRORILLI.NATION_SALES_VIEW AS
SELECT N.N_NAME,
        YEAR(O.O_ORDERDATE) AS ORDER_YEAR,
        SUM(O.O_TOTALPRICE) AS TOTAL_SALES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS O
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C ON O.O_CUSTKEY = C.C_CUSTKEY
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION N ON C.C_NATIONKEY = N.N_NATIONKEY
GROUP BY N.N_NAME, ORDER_YEAR;













-- week 3 day 1
select l_partkey as part,
    l_suppkey as supplier_key,
    l_shipdate as shipment_date,
    l_extendedprice as final_price
from lineitem
limit 10;

select o_orderdate as "Order Date",
    o_orderstatus as "Order Status",
    o_totalprice as "Total Price",
    o_orderkey as "Order Key"
from orders
where o_orderstatus = 'F' 
order by o_totalprice DESC, o_orderdate ASC -- asc is desc
limit 5;


-- Activity 3.1.1

-- Exercise 1:
SELECT *
FROM customer;

-- Exercise 2:
SELECT c_name,
        c_phone
FROM customer;

-- Exercise 3:
SELECT c_name as "Customer Name",
        c_phone as "Customer Address"
FROM customer;

-- Exercise 4:
SELECT p_name,
    p_retailprice
FROM part
ORDER BY p_retailprice DESC;


-- Activity 3.1.2

-- Exercise 1:
SELECT *
FROM nation
WHERE n_name = 'UNITED STATES';

-- Exercise 2:
SELECT p_name,
    p_retailprice,
FROM part
WHERE p_type LIKE '%SMALL%' AND p_size = 5;

--Exercise 3:
SELECT o_orderdate,
    o_totalprice
FROM orders 
WHERE o_orderdate LIKE '1995%' --- or WHERE YEAR(O_orderdate) = 1995
ORDER BY o_totalprice DESC;


-- Exercise 4:
SELECT p_name,
    p_retailprice
FROM part
ORDER BY p_retailprice DESC
LIMIT 10;


-- Activity 3.1.3

-- Exercise 1:
SELECT p_name,
    p_retailprice
FROM part
WHERE p_retailprice BETWEEN 50.00 AND 100.00;

-- Exercise 2:
SELECT o_orderdate,
    o_totalprice
FROM orders
WHERE o_orderdate LIKE '1994%' AND o_orderdate NOT LIKE '1994-12%'; -- pr MONTH(Order date) <> 12, Year = 1994 



-- Activity 3.2.1

-- Exercise 1
SELECT YEAR(o_orderdate) as "Order Year",
    COUNT(o_orderkey)
FROM orders
GROUP BY "Order Year"
ORDER BY "Order Year";


-- Exercise 2 
SELECT l_shipmode,
    AVG(l_shipdate - l_commitdate) AS "Avg Days"
FROM lineitem
GROUP BY l_shipmode;


-- Exercise 3:
SELECT o_custkey,
    SUM(o_totalprice) AS "Total Sales",
    COUNT(*) AS "Number of Orders"
FROM orders
GROUP BY o_custkey;



-- week 3 day 2

-- Class Notes 7/1

--  must be DE, Compute_WH warehouse
-- choose DB: manually click sample data, tpch_sf1 or 
-- auto comment ctrl /
-- use schema snowflake_sample_data.tpch_sf1

select l_partkey as part,
    l_suppkey as supplier_key,
    l_shipdate as shipment_date,
    l_extendedprice as final_price
from lineitem
limit 10;

select o_orderdate as "Order Date",
    o_orderstatus as "Order Status",
    o_totalprice as "Total Price",
    o_orderkey as "Order Key"
from orders
where o_orderstatus = 'F' 
order by o_totalprice DESC, o_orderdate ASC -- asc is desc
limit 5;


-- Activity 3.1.1

-- Exercise 1:
SELECT *
FROM customer;

-- Exercise 2:
SELECT c_name,
        c_phone
FROM customer;

-- Exercise 3:
SELECT c_name as "Customer Name",
        c_phone as "Customer Address"
FROM customer;

-- Exercise 4:
SELECT p_name,
    p_retailprice
FROM part
ORDER BY p_retailprice DESC;


-- Activity 3.1.2

-- Exercise 1:
SELECT *
FROM nation
WHERE n_name = 'UNITED STATES';

-- Exercise 2:
SELECT p_name,
    p_retailprice,
FROM part
WHERE p_type LIKE '%SMALL%' AND p_size = 5;

--Exercise 3:
SELECT o_orderdate,
    o_totalprice
FROM orders 
WHERE o_orderdate LIKE '1995%' --- or WHERE YEAR(O_orderdate) = 1995
ORDER BY o_totalprice DESC;


-- Exercise 4:
SELECT p_name,
    p_retailprice
FROM part
ORDER BY p_retailprice DESC
LIMIT 10;


-- Activity 3.1.3

-- Exercise 1:
SELECT p_name,
    p_retailprice
FROM part
WHERE p_retailprice BETWEEN 50.00 AND 100.00;

-- Exercise 2:
SELECT o_orderdate,
    o_totalprice
FROM orders
WHERE o_orderdate LIKE '1994%' AND o_orderdate NOT LIKE '1994-12%'; -- pr MONTH(Order date) <> 12, Year = 1994 



-- Activity 3.2.1

-- Exercise 1
SELECT YEAR(o_orderdate) as "Order Year",
    COUNT(o_orderkey)
FROM orders
GROUP BY "Order Year"
ORDER BY "Order Year";


-- Exercise 2 
SELECT l_shipmode,
    AVG(l_shipdate - l_commitdate) AS "Avg Days"
FROM lineitem
GROUP BY l_shipmode;


-- Exercise 3:
SELECT o_custkey,
    SUM(o_totalprice) AS "Total Sales",
    COUNT(*) AS "Number of Orders"
FROM orders
GROUP BY o_custkey;


-- week 3 day 3
-- In Class Examples
SELECT
    O_ORDERSTATUS AS ORDER_STATUS,
    SUM(O_TOTALPRICE) AS TOTAL_PRICE,
    COUNT(*) AS RECORD_COUNT
FROM ORDERS
GROUP BY O_ORDERSTATUS
ORDER BY TOTAL_PRICE DESC
LIMIT 10;


SELECT O_ORDERKEY,
    SUM(O_TOTALPRICE) AS TOTAL_PRICE,
FROM ORDERS
WHERE O_ORDERSTATUS = 'F'
GROUP BY O_ORDERKEY
ORDER BY TOTAL_PRICE DESC
LIMIT 10;

SELECT  O_ORDERKEY,
        O_ORDERDATE, 
        O_ORDERSTATUS,
        O_TOTALPRICE
FROM ORDERS
WHERE O_ORDERSTATUS = 'F'
ORDER BY O_TOTALPRICE DESC
LIMIT 10;

 
SELECT O_ORDERKEY, 
       SUM(O_TOTALPRICE) as TOTAL_PRICE,
FROM ORDERS
WHERE O_ORDERSTATUS = 'F'
GROUP BY O_ORDERKEY
ORDER BY TOTAL_PRICE DESC
LIMIT 10;


SELECT 1+1;
SELECT UPPER('Gina Mastrorilli');


select year(o_orderdate),
    sum(o_totalprice)
from orders
group by year(o_orderdate) -- need a group by when doing a aggregation
order by year(o_orderdate) ASC;


select count(*) -- all records in orders table 
from orders;


-- case statements

-- simple case

-- ORDER STATUS: (P, F, O)
SELECT O_ORDERKEY, 
       O_ORDERDATE, 
       O_ORDERSTATUS,
       CASE O_ORDERSTATUS
            WHEN 'P' THEN 'PARTIAL FUFILMENT'
            WHEN 'F' THEN 'FULLY DELIVERED'
            WHEN 'O' THEN 'ORDER OUT'
            ELSE 'NOT SURE'
        END AS STATUS_DESCRIPTION
FROM ORDERS;


-- search case expression
SELECT O_ORDERKEY, 
       O_ORDERDATE, 
       O_ORDERSTATUS,
       CASE 
            WHEN O_ORDERSTATUS ='P' THEN 'PARTIAL FUFILMENT'
            WHEN O_ORDERSTATUS ='F' THEN 'FULLY DELIVERED'
            WHEN O_ORDERSTATUS ='O' THEN 'ORDER OUT'
            ELSE 'NOT SURE'
        END AS STATUS_DESCRIPTION
FROM ORDERS;


-- multiple search case expressions
select o_orderdate,
    case
        when o_totalprice < 1000 then 'low sales'
        when o_totalprice >= 1000 then 'high sales'
    end as sales_status,
    case
        when year(o_orderdate) <= 2018 then 'before economic crisis'
        when year(o_orderdate) > 2018 then 'after econ crisis'
    end as year_econ,
    case
        when year(o_orderdate) <= 2018 then o_totalprice/1000
        when year(o_orderdate) > 2018 then o_totalprice/100
    end as price_adjustment
from orders;


-- JOINING MULTIPLE TABLES

SELECT ORDERS.O_ORDERKEY, REGION.R_NAME
FROM ORDERS
JOIN CUSTOMER ON ORDERS.O_CUSTKEY = CUSTOMER.C_CUSTKEY
JOIN NATION ON CUSTOMER.C_NATIONKEY = NATION.N_NATIONKEY
JOIN REGION ON NATION.N_REGIONKEY = REGION.R_REGIONKEY
LIMIT 10;


SELECT O.O_ORDERKEY ORDER_KEY, R.R_NAME REGION_NAME
FROM ORDERS AS O
JOIN CUSTOMER AS C ON O.O_CUSTKEY = C.C_CUSTKEY
JOIN NATION AS N ON C.C_NATIONKEY = N.N_NATIONKEY
JOIN REGION AS R ON N.N_REGIONKEY = R.R_REGIONKEY
LIMIT 10;

SELECT R.R_NAME REGION_NAME, COUNT(O.O_ORDERKEY) ORDER_COUNT
FROM ORDERS AS O
JOIN CUSTOMER AS C ON O.O_CUSTKEY = C.C_CUSTKEY
JOIN NATION AS N ON C.C_NATIONKEY = N.N_NATIONKEY
JOIN REGION AS R ON N.N_REGIONKEY = R.R_REGIONKEY
GROUP BY R.R_NAME
ORDER BY ORDER_COUNT DESC;





-- Activity 3.2.1

-- Exercise 1
SELECT YEAR(o_orderdate) as "Order Year",
    COUNT(o_orderkey)
FROM orders
GROUP BY "Order Year"
ORDER BY "Order Year";


-- Exercise 2 
SELECT l_shipmode,
    AVG(l_shipdate - l_commitdate) AS "Avg Days"
FROM lineitem
GROUP BY l_shipmode;


SELECT L_SHIPMODE,AVG(DATEDIFF('DAYS', L_COMMITDATE,L_SHIPDATE)) AS "AVG DAYS"
FROM LINEITEM
GROUP BY L_SHIPMODE;

SELECT L_SHIPMODE, AVG_DAYS
FROM
(
SELECT l_shipmode, AVG(l_shipdate - l_commitdate) AS AVG_DAYS
FROM lineitem
GROUP BY l_shipmode
)
WHERE AVG_DAYS >1;



SELECT DISTINCT l_shipmode
FROM lineitem
WHERE L_SHIPMODE NOT IN ('TRUCK','SHIP');


SELECT l_shipmode, AVG(l_shipdate - l_commitdate) AS AVG_DAYS
FROM lineitem
WHERE l_shipmode in (
                      SELECT DISTINCT l_shipmode
                      FROM lineitem
                      WHERE L_SHIPMODE NOT IN ('TRUCK','SHIP')
        )
GROUP BY L_SHIPMODE;



-- Exercise 3:
SELECT O_CUSTKEY, 
       SUM(O_TOTALPRICE) as TOTAL_SALES,
       COUNT(O_ORDERKEY) as NUMBER_OF_ORDERS
FROM ORDERS 
GROUP BY O_CUSTKEY;


-- TOP 5 CUSTOMERS BY TOTAL SALES, MOR THAN 20 ORDERS 

SELECT O_CUSTKEY, 
       SUM(O_TOTALPRICE) as TOTAL_SALES,
       COUNT(O_ORDERKEY) as NUMBER_OF_ORDERS
FROM ORDERS 
GROUP BY O_CUSTKEY
HAVING NUMBER_OF_ORDERS >20
ORDER BY TOTAL_SALES desc
limit 5;


--- dont show sum of orders
SELECT O_CUSTKEY, 
       -- SUM(O_TOTALPRICE) as TOTAL_SALES,
       COUNT(O_ORDERKEY) as NUMBER_OF_ORDERS
FROM ORDERS 
GROUP BY O_CUSTKEY
HAVING NUMBER_OF_ORDERS >20
ORDER BY SUM(O_TOTALPRICE) desc
limit 5;



-- Activity 3.2.2

-- Exercise 8-1:
SELECT n_nationkey,
    n_name,
FROM NATION
WHERE n_regionkey <> 1 AND n_regionkey <> 2
LIMIT 10 ;

-- using in/not in

select n_nationkey, n_name
from nation
where n_regionkey not in (
    select r_regionkey
    from region
    where r_name in ('AMERICA', 'ASIA'));

select n_nationkey, n_name
from nation
where n_regionkey in (
    select r_regionkey
    from region
    where r_name not in ('AMERICA', 'ASIA'));
    
-- using exists/not exist

select n_nationkey, n_name
from nation as n 
where not exists 
    (
        select 1
        from region as r
        where r.r_regionkey = n.n_regionkey
        and r_name in ('AMERICA', 'ASIA')
    );


select n_nationkey, n_name
from nation as n 
where exists 
    (
        select 1
        from region as r
        where r.r_regionkey = n.n_regionkey
        and r_name not in ('AMERICA', 'ASIA')
    );    



-- Exercise 8-2:
select c_custkey, c_name 
from ctomer
where c_custkey in 
    (select o_custkey 
    from orders us
    where year(o_orderdate) = '1997' 
    group by o_custkey 
    having count(o_custkey) = 4);


select c_custkey, c_name
       from customer
       where c_custkey in
        (select o_custkey
         from orders
         where date_part(year, o_orderdate) = 1997
              group by o_custkey
              having count(*) = 4);

select c_custkey, c_name 
from customer c
where 4 = ( 
    select count(*)
         from orders o
         where date_part(year, o_orderdate) = 1997
           and o.o_custkey = c.c_custkey);
    

-- Exercise 8-3:
SELECT COUNT(*),
    MIN(S_ACCTBAL),
    MAX(S_ACCTBAL)
FROM SUPPLIER;

-- Exercise 8-4:
SELECT s_nationkey,
    COUNT(*),
    MIN(S_ACCTBAL),
    MAX(S_ACCTBAL)
FROM SUPPLIER
GROUP BY s_nationkey
order by s_nationkey;

-- Exercise 8-5: 
SELECT s_nationkey,
    COUNT(*),
    MIN(S_ACCTBAL),
    MAX(S_ACCTBAL)
FROM SUPPLIER
GROUP BY s_nationkey
HAVING COUNT(*) > 300;


-- Activity 3.2.3

SELECT PS_PARTKEY, 
       PS_SUPPKEY, 
       PS_AVAILQTY, -- need this comma since CASE creating a new column
        CASE 
            WHEN ps_availqty < 100 THEN 'order now'
            WHEN 101 <= ps_availqty and ps_availqty <=1000 THEN 'order soon'
            ELSE 'plenty in stock'
        END AS order_status
FROM PARTSUPP
WHERE PS_PARTKEY BETWEEN 148300 AND 148450;

SELECT PS_PARTKEY, 
       PS_SUPPKEY, 
       PS_AVAILQTY, -- need this comma since CASE creating a new column
        CASE 
            WHEN ps_availqty < 100 THEN 'order now'
            WHEN 101 < ps_availqty < 1000 THEN 'order soon'
            ELSE 'plenty in stock'
        END AS order_status
FROM PARTSUPP
WHERE PS_PARTKEY BETWEEN 148300 AND 148450;


-- Activity 3.2.4

-- 1:
SELECT C_NAME,COUNT(O_ORDERKEY) AS "ORDER COUNT"
FROM CUSTOMER 
JOIN ORDERS ON C_CUSTKEY = O_CUSTKEY
GROUP BY C_NAME
ORDER BY C_NAME;

-- 2:
SELECT C_NAME,
        COUNT(O_ORDERKEY) AS ORDER_COUNT,
        SUM(O_TOTALPRICE) AS TOTAL_PRICE
FROM CUSTOMER 
JOIN ORDERS ON C_CUSTKEY = O_CUSTKEY
GROUP BY C_NAME
HAVING SUM(O_TOTALPRICE) > 10000
ORDER BY TOTAL_PRICE DESC;

-- 3:
SELECT P_PARTKEY,
        P_NAME,
        COUNT(O_ORDERKEY) AS TOTAL_QUANTITY
FROM PART
JOIN LINEITEM ON P_PARTKEY = L_PARTKEY
JOIN ORDERS ON L_ORDERKEY = O_ORDERKEY
GROUP BY P_PARTKEY, p_name -- EVERYthin in select has to be in groupby, but everything in group by doesnt have to be in select , stuff without aggrgation needs 
ORDER BY COUNT(O_ORDERKEY) DESC
LIMIT 10;



-- Activity 3.2.5
SELECT
    P_BRAND,
    P_TYPE,
    P_SIZE,
    CASE 
        WHEN P_SIZE <= 20 THEN 'HIGH COST PART'
        WHEN P_SIZE > 20 THEN 'LOW COST PART'
    END AS NOTE 
FROM PART
WHERE P_BRAND IN ('Brand#11', 'Brand#42');



-- Activity 3.2.6
SELECT O_ORDERDATE, O_CUSTKEY,
         CASE 
           WHEN O_ORDERSTATUS ='P' THEN 'PARTIAL'
           WHEN O_ORDERSTATUS ='F' THEN 'FILLED'
           WHEN O_ORDERSTATUS ='O' THEN 'OPEN'
         END as STATUS
       FROM ORDERS
       WHERE O_ORDERKEY > 5999500;


-- Activity 3.2.7       

SELECT
    MAX(CASE WHEN R_NAME = 'AMERICA' THEN C END) AS AMERICA, -- check one by one going top to bottom, if region match then C count if not null and the max just 
    MAX(CASE WHEN R_NAME = 'AFRICA' THEN C END) AS AFRICA,
    MAX(CASE WHEN R_NAME = 'EUROPE' THEN C END) AS EUROPE, --  if region mathches then C count if not null and the max just grabs the values and not the nulls
    MAX(CASE WHEN R_NAME = 'MIDDLE EAST' THEN C END) AS MIDDLE_EAST,
    MAX(CASE WHEN R_NAME = 'ASIA' THEN C END) AS ASIA
FROM
    (SELECT R_NAME, COUNT(*) as C
           FROM NATION N
           INNER JOIN REGION R ON R.R_REGIONKEY = N.N_REGIONKEY
           INNER JOIN SUPPLIER S ON S.S_NATIONKEY = N.N_NATIONKEY
           GROUP BY R_NAME);


-- Activity 3.3.1

-- Exercise 1:
SELECT C_NAME, C_ACCTBAL
FROM CUSTOMER
WHERE C_MKTSEGMENT = 'MACHINERY' AND C_ACCTBAL > 9998;


-- Exercise 2:
SELECT C_NAME, C_ACCTBAL, C_MKTSEGMENT
FROM CUSTOMER
WHERE C_MKTSEGMENT IN ('MACHINERY','FURNITURE') AND C_ACCTBAL BETWEEN -1 AND 1;


-- Exercise 3:
SELECT C_NAME, C_ACCTBAL, C_MKTSEGMENT
FROM CUSTOMER
WHERE (C_MKTSEGMENT = 'MACHINERY' AND C_ACCTBAL = 20) OR (C_MKTSEGMENT = 'FURNITURE' AND C_ACCTBAL = 334);


-- Exercise 4:
SELECT COUNT(*),
    MIN(S_ACCTBAL),
    MAX(S_ACCTBAL)
FROM SUPPLIER;

-- Exercise 5:
SELECT s_nationkey,
    COUNT(*),
    MIN(S_ACCTBAL),
    MAX(S_ACCTBAL)
FROM SUPPLIER
GROUP BY s_nationkey
order by s_nationkey;

-- Exercise 6:
SELECT s_nationkey,
    COUNT(*),
    MIN(S_ACCTBAL),
    MAX(S_ACCTBAL)
FROM SUPPLIER
GROUP BY s_nationkey
HAVING COUNT(*) > 300;



-- Activity 3.3.2

-- Exercise 1
SELECT S_NAME, S_NATIONKEY, S_ACCTBAL
FROM SUPPLIER
ORDER BY S_ACCTBAL DESC
LIMIT 5 ;

SELECT S.S_NAME AS SUPPLIER_NAME
    , N.N_NAME AS NATION
    , S.S_ACCTBAL AS ACCOUNT_BALANCE
FROM SUPPLIER AS S
JOIN NATION AS N ON S.S_NATIONKEY = N.N_NATIONKEY
ORDER BY S.S_ACCTBAL desc
LIMIT 5;


-- Exercise 2:

SELECT YEAR(O_ORDERDATE), AVG(O_TOTALPRICE)
FROM ORDERS
GROUP BY YEAR(O_ORDERDATE)
ORDER BY YEAR(O_ORDERDATE);

SELECT DATE_PART(YEAR, O_ORDERDATE) AS ORDER_YEAR, -- SAME AS YEAR(O_ORDERDATE)
        ROUND(AVG(O_TOTALPRICE), 2) AS AVG_ORDER_VALUE -- ADDING ROUND 2 SINCE DOLLAR VALUE
FROM ORDERS
GROUP BY ORDER_YEAR
ORDER BY ORDER_YEAR ASC; -- MAKES YEARS GO IN ORDER, LOOKS PRETTY 


SELECT TO_DECIMAL('$15,000.00', '$99,999.00'); 



-- Exercise 3:

SELECT YEAR(O.O_ORDERDATE), SUM(L.L_EXTENDEDPRICE*(1-L.L_DISCOUNT)) AS TOTAL_REVENUE
FROM CUSTOMER AS C
JOIN ORDERS AS O ON C.C_CUSTKEY = O.O_CUSTKEY
JOIN LINEITEM AS L ON O.O_ORDERKEY = L.L_ORDERKEY
WHERE C.C_MKTSEGMENT = 'AUTOMOBILE'
GROUP BY YEAR(O.O_ORDERDATE)
ORDER BY YEAR(O.O_ORDERDATE);


SELECT YEAR(O.O_ORDERDATE) AS ORDER_YEAR,
        SUM(L.L_EXTENDEDPRICE*(1-L.L_DISCOUNT)) AS TOTAL_REVENUE
FROM ORDERS AS O
JOIN LINEITEM AS L ON O.O_ORDERKEY = L.L_ORDERKEY
JOIN CUSTOMER AS C ON O.O_CUSTKEY = C.C_CUSTKEY
WHERE UPPER(C.C_MKTSEGMENT) LIKE UPPER('auto%')
GROUP BY ORDER_YEAR
ORDER BY ORDER_YEAR;




-- Exercise 4:
SELECT N.N_NAME AS NATION, COUNT(S.S_SUPPKEY) AS SUPPLIER_COUNT
FROM SUPPLIER S
JOIN NATION N ON S.S_NATIONKEY = N.N_NATIONKEY
GROUP BY N_NAME
ORDER BY SUPPLIER_COUNT DESC
LIMIT 1;


-- Exercise 5:
SELECT MONTH(O_ORDERDATE) AS MONTH_PLACED, COUNT(O_ORDERKEY) AS ORDER_COUNT
FROM ORDERS GROUP BY MONTH_PLACED
ORDER BY ORDER_COUNT DESC
LIMIT 1;



-- Exercise 6: 
SELECT C.C_MKTSEGMENT, AVG(L.L_DISCOUNT) AS AVG_DISCOUNT
FROM CUSTOMER AS C
JOIN ORDERS AS O ON C.C_CUSTKEY = O.O_CUSTKEY
JOIN LINEITEM AS L ON O.O_ORDERKEY = L.L_ORDERKEY
GROUP BY C.C_MKTSEGMENT;



-- Exercise 7:
SELECT NATION.N_NAME AS NAME, MAX(SUPPLIER.S_ACCTBAL) AS BALANCE
FROM SUPPLIER 
JOIN NATION ON SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY
GROUP BY NAME
ORDER BY BALANCE
LIMIT 3;


-- Challenge (from peter)
SELECT C_CUSTKEY, C_NAME,
       CASE
         WHEN C_CUSTKEY IN
            (SELECT DISTINCT C_CUSTKEY
                FROM CUSTOMER JOIN ORDERS ON C_CUSTKEY = O_CUSTKEY
                WHERE O_TOTALPRICE > 400000) THEN 'Big Spender'
         ELSE 'Regular'
       END AS CUST_TYPE
FROM CUSTOMER c
WHERE C_CUSTKEY BETWEEN 74000 and 74020;

-- CHALLENGE IN CLASS 

SELECT C.C_CUSTKEY, C.C_NAME,
    CASE
        WHEN EXISTS
        (
        SELECT 1
        FROM ORDERS AS O
        WHERE O.O_CUSTKEY = C.C_CUSTKEY AND O.O_TOTALPRICE > 400000) 
        THEN 'BIG SPENDER'
        ELSE 'REGULAR'
        END AS CUST_TYPE
        
FROM CUSTOMER C
WHERE C_CUSTKEY BETWEEN 74000 AND 74020;


SELECT C.C_CUSTKEY,
        C.C_NAME,
        CASE
            WHEN C.C_CUSTKEY IN
                (
                SELECT C.C_CUSTKEY
                FROM CUSTOMER C
                JOIN ORDERS O ON C.C_CUSTKEY = O.O_CUSTKEY
                WHERE O.O_TOTALPRICE > 400000
                AND C.C_CUSTKEY BETWEEN 74000 AND 74020
                )THEN 'BIG SPENDER'
            ELSE 'REGULAR'
            END AS CUST_TYPE
FROM CUSTOMER C
WHERE C_CUSTKEY BETWEEN 74000 AND 74020;
        

-- week 3 day 5

-- Exercise 1
SELECT C_NAME,
        C_ADDRESS
FROM CUSTOMER
WHERE C_NATIONKEY = 3 
ORDER BY C_NAME ASC;


-- Exercise 2
SELECT S_NAME,
        S_ACCTBAL
FROM SUPPLIER
WHERE S_ACCTBAL > 5000
ORDER BY S_ACCTBAL DESC;


-- Exercise 3
SELECT P_NAME,
        CASE
            WHEN P_SIZE <= 20 THEN 'SMALL'
            WHEN P_SIZE > 20 THEN 'LARGE'
        END AS MARKET_SEGMENT    
FROM PART
ORDER BY P_NAME;

--Exercise 4
SELECT YEAR(O_ORDERDATE) AS YEAR_ORDERED,
        COUNT(O_ORDERKEY) AS ORDERS_MADE
FROM ORDERS
GROUP BY YEAR_ORDERED
ORDER BY YEAR_ORDERED;

-- Exercise 5
SELECT O_CUSTKEY,
        ROUND(AVG(O_TOTALPRICE),2)
FROM ORDERS
GROUP BY O_CUSTKEY;


-- Exercise 6
SELECT DISTINCT C.C_NAME, N.N_NAME
FROM CUSTOMER C
JOIN SUPPLIER S ON C.C_NATIONKEY = S.S_NATIONKEY
JOIN NATION N ON S.S_NATIONKEY = N.N_NATIONKEY
ORDER BY C.C_NAME
LIMIT 20;

-- Exercise 7
SELECT P.P_NAME,
        S.S_NAME
FROM PART P
JOIN PARTSUPP PS ON P.P_PARTKEY = PS.PS_PARTKEY
JOIN SUPPLIER S ON PS.PS_SUPPKEY = S.S_SUPPKEY
JOIN NATION N ON S.S_NATIONKEY = N.N_NATIONKEY
WHERE N.N_REGIONKEY = 3;


-- Exercise 8
SELECT C.C_NAME,
        O.O_TOTALPRICE
FROM ORDERS O
JOIN LINEITEM L ON O.O_ORDERKEY = L.L_ORDERKEY
JOIN CUSTOMER C ON O.O_CUSTKEY = C.C_CUSTKEY
WHERE L.L_PARTKEY = 5;


-- Exercise 9
WITH MONTHLY_SALES AS
    (SELECT DISTINCT MONTH(O_ORDERDATE) AS MONTH_ORDERED, 
                    YEAR(O_ORDERDATE) AS YEAR_ORDERED, 
                    SUM(O_TOTALPRICE) AS TOTAL_SALES
    FROM ORDERS
    GROUP BY MONTH(O_ORDERDATE), YEAR(O_ORDERDATE)
    ORDER BY YEAR(O_ORDERDATE),MONTH(O_ORDERDATE))
SELECT * 
FROM MONTHLY_SALES;


-- Exercise 10
-- TOTAL COST IS ACTUALLY SUM((L.L_EXTENDEDPRICE)*(1 - L.L_DISCOUNT)*(1 + L.L_TAX)) AS SUM_CHARGE SO NEED LINEITEM TABLE
WITH SUPPLIER_OVERVIEW AS
    (SELECT S.S_NAME AS SUPPLIER_NAME,
            SUM(PS.PS_SUPPLYCOST) AS TOTAL_PART_VALUE
    FROM SUPPLIER S
    JOIN PARTSUPP PS ON S.S_SUPPKEY = PS.PS_SUPPKEY
    GROUP BY S.S_NAME
    ORDER BY SUM(PS.PS_SUPPLYCOST) DESC)
SELECT *
FROM SUPPLIER_OVERVIEW
LIMIT 5;



-- Activity 3.4.4 Creating Tables/Views

-- Activity 1
CREATE TEMPORARY TABLE TECHCATALYST_DE.GMASTRORILLI.TOP_CUSTOMERS_TEMP
(C_CUSTKEY NUMBER(38,0),
C_NAME VARCHAR(25),
TOTAL_SALES NUMBER(12,2));

INSERT INTO TECHCATALYST_DE.GMASTRORILLI.TOP_CUSTOMERS_TEMP
SELECT C.C_CUSTKEY, 
        C.C_NAME,
        SUM(O.O_TOTALPRICE) AS TOTAL_SALES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER AS C
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS AS O ON C.C_CUSTKEY = O.O_CUSTKEY
GROUP BY C.C_CUSTKEY, C.C_NAME
ORDER BY TOTAL_SALES DESC
LIMIT 10;

-- Activity 2
CREATE TRANSIENT TABLE TECHCATALYST_DE.GMASTRORILLI.AVG_SHIPPING_TIME_TRANS
(S_SUPPKEY NUMBER(38,0),
S_NAME VARCHAR(25),
AVG_SHIPPING_DAYS NUMBER(12,6));

INSERT INTO TECHCATALYST_DE.GMASTRORILLI.AVG_SHIPPING_TIME_TRANS
SELECT S.S_SUPPKEY,
        S.S_NAME,
        AVG(DATEDIFF(DAY, L.L_SHIPDATE, L.L_RECEIPTDATE)) AS AVG_SHIPPING_DAYS
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER AS S
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM L ON S.S_SUPPKEY = L.L_SUPPKEY
GROUP BY S.S_SUPPKEY, S.S_NAME;

-- Activity 3
CREATE VIEW TECHCATALYST_DE.GMASTRORILLI.NATION_SALES_VIEW AS
SELECT N.N_NAME,
        YEAR(O.O_ORDERDATE) AS ORDER_YEAR,
        SUM(O.O_TOTALPRICE) AS TOTAL_SALES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS O
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C ON O.O_CUSTKEY = C.C_CUSTKEY
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION N ON C.C_NATIONKEY = N.N_NATIONKEY
GROUP BY N.N_NAME, ORDER_YEAR;

-- Activity 4
CREATE TABLE TECHCATALYST_DE.GMASTRORILLI.AVG_ORDER_PRICE
(C_MKTSEGMENT VARCHAR(10),
AVG_TOTAL_PRICE NUMBER(38,0));

INSERT INTO TECHCATALYST_DE.GMASTRORILLI.AVG_ORDER_PRICE
SELECT C.C_MKTSEGMENT,
        AVG(O.O_TOTALPRICE) AS AVG_TOTAL_PRICE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS O
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C ON O.O_CUSTKEY = C.C_CUSTKEY 
GROUP BY C.C_MKTSEGMENT;

-- OR to do without inserting/setting parameters 
CREATE OR REPLACE TABLE TECHCATALYST_DE.GMASTRORILLI.AVG_ORDER_PRICE AS
SELECT C.C_MKTSEGMENT,
        AVG(O.O_TOTALPRICE) AS AVG_TOTAL_PRICE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS O
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C ON O.O_CUSTKEY = C.C_CUSTKEY 
GROUP BY C.C_MKTSEGMENT;






--- SQL Mini Project 

create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.FACT_ACCIDENTS (
	ACCIDENT_ID NUMBER(38,0),
	POLICYHOLDER_ID NUMBER(38,0),
	VEHICLE_ID NUMBER(38,0),
	STATE_ID NUMBER(38,0),
	BODY_STYLE_ID NUMBER(38,0),
	ACCIDENT_TYPE_ID NUMBER(38,0),
	GENDER_MARITALSTATUS_ID NUMBER(38,0),
	VEHICLE_USECODE_ID VARCHAR(16777216),
	ACCIDENT_DATE DATE,
	VEHICLE_YEAR VARCHAR(16777216),
	POLICYHOLDER_BIRTHDATE DATE,
	ESTIMATED_COST FLOAT,
	ACTUAL_REPAIR_COST FLOAT,
	AT_FAULT BOOLEAN,
	IS_DUI BOOLEAN,
	COVERAGE_STATUS VARCHAR(16777216)
);

create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.DIM_ACCIDENT_TYPE (
	ACCIDENT_TYPE_ID NUMBER(38,0),
	ACCIDENT_TYPE VARCHAR(16777216)
);

create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.DIM_BODY_STYLE (
	BODY_STYLE_ID NUMBER(38,0),
	BODY_STYLE VARCHAR(16777216)
);

create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.DIM_GENDER_MARITAL (
	GENDER_MARITALSTATUS_ID NUMBER(38,0),
	GENDER_MARITAL_STATUS VARCHAR(16777216)
);

create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.DIM_POLICYHOLDER (
	POLICYHOLDER_ID NUMBER(38,0),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	ADDRESS VARCHAR(16777216)
);


create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.DIM_STATES (
	STATE_ID NUMBER(38,0),
	STATE VARCHAR(16777216)
);



create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.DIM_VEHICLE_USE (
	VEHICLE_USECODE_ID VARCHAR(16777216),
	VEHICLE_USE VARCHAR(16777216)
);


-- dim tables first

-- DIM_ACCIDENT_TYPE TABLE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_ACCIDENT_TYPE
(ACCIDENT_TYPE_ID, ACCIDENT_TYPE)
SELECT ACCIDENT_TYPE_CODE, ACCIDENT_TYPE
FROM TECHCATALYST_DE.PUBLIC.INS_ACCIDENT_TYPE;


-- DIM_BODY_STYLE TABLE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_BODY_STYLE
(BODY_STYLE_ID, BODY_STYLE)
SELECT BODY_STYLE_CODE, BODY_STYLE
FROM TECHCATALYST_DE.PUBLIC.INS_BODY_STYLE;


-- DIM_GENDER_MARITAL TABLE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_GENDER_MARITAL
(GENDER_MARITALSTATUS_ID,GENDER_MARITAL_STATUS)
SELECT GENDER_MARITAL_STATUS_CODE,GENDER_MARITAL_STATUS
FROM TECHCATALYST_DE.PUBLIC.INS_GENDER_MARITAL_STATUS;


-- DIM_POLICYHOLDER TABLE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_POLICYHOLDER
(POLICYHOLDER_ID, LAST_NAME, FIRST_NAME, ADDRESS)
SELECT POLICYHOLDER_ID, LAST_NAME, FIRST_NAME, ADDRESS
FROM TECHCATALYST_DE.PUBLIC.INS_POLICYHOLDER;

-- DIM_STATES TABLE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_STATES
(STATE_ID, STATE)
SELECT STATE_CODE, STATE
FROM TECHCATALYST_DE.PUBLIC.INS_STATES;


-- DIM_VEHICLE_USE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_VEHICLE_USE
(VEHICLE_USECODE_ID, VEHICLE_USE)
SELECT USE_CODE, VEHICLE_ID
FROM TECHCATALYST_DE.PUBLIC.INS_VEHICLE_USE;


-- FACT_ACCIDENTS
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.FACT_ACCIDENTS
(ACCIDENT_ID, POLICYHOLDER_ID, VEHICLE_ID, STATE_ID, BODY_STYLE_ID, ACCIDENT_TYPE_ID, GENDER_MARITALSTATUS_ID, VEHICLE_USECODE_ID, ACCIDENT_DATE, VEHICLE_YEAR, POLICYHOLDER_BIRTHDATE, ESTIMATED_COST, ACTUAL_REPAIR_COST, AT_FAULT, IS_DUI, COVERAGE_STATUS)
SELECT A.ACCIDENT_ID,
       P.POLICYHOLDER_ID,
       V.VEHICLE_ID,
       S.STATE_CODE, 
       V.BODY_STYLE_CODE, 
       A.ACCIDENT_TYPE, 
       G.GENDER_MARITAL_STATUS_CODE, 
       VU.USE_CODE, 
       A.ACCIDENT_DATE, 
       V.YEAR, 
       P.BIRTHDATE, 
       A.ESTIMATED_COST, 
       A.ACTUAL_REPAIR_COST, 
       A.AT_FAULT, 
       A.DUI, 
       I.COVERAGE_STATUS
FROM TECHCATALYST_DE.PUBLIC.INS_ACCIDENTS AS A
JOIN TECHCATALYST_DE.PUBLIC.INS_POLICYHOLDER AS P ON A.POLICYHOLDER_ID = P.POLICYHOLDER_ID
JOIN TECHCATALYST_DE.PUBLIC.INS_VEHICLES V ON A.VEHICLE_ID = V.VEHICLE_ID
JOIN TECHCATALYST_DE.PUBLIC.INS_STATES S ON P.STATE_CODE = S.STATE_CODE
JOIN TECHCATALYST_DE.PUBLIC.INS_GENDER_MARITAL_STATUS G ON P.GENDER_MARITAL_STATUS = G.GENDER_MARITAL_STATUS_CODE
JOIN TECHCATALYST_DE.PUBLIC.INS_VEHICLE_USE VU ON V.VEHICLE_ID = VU.VEHICLE_ID
JOIN TECHCATALYST_DE.PUBLIC.INS_INSURANCE_COVERAGE I ON P.POLICYHOLDER_ID = I.POLICYHOLDER_ID;



-- validate your tables once data is loaded
select *
FROM TECHCATALYST_DE.GMASTRORILLI.FACT_ACCIDENTS AS F
JOIN TECHCATALYST_DE.GMASTRORILLI.DIM_ACCIDENT_TYPE A ON F.ACCIDENT_TYPE_ID = A.ACCIDENT_TYPE_ID
JOIN TECHCATALYST_DE.GMASTRORILLI.DIM_BODY_STYLE B ON F.BODY_STYLE_ID = B.BODY_STYLE_ID
JOIN TECHCATALYST_DE.GMASTRORILLI.DIM_POLICYHOLDER P ON F.POLICYHOLDER_ID = P.POLICYHOLDER_ID
JOIN TECHCATALYST_DE.GMASTRORILLI.DIM_STATES S ON F.STATE_ID = S.STATE_ID
JOIN TECHCATALYST_DE.GMASTRORILLI.DIM_VEHICLE_USE V ON F.VEHICLE_USECODE_ID = V.VEHICLE_USECODE_ID
order by v.VEHICLE_USECODE_ID desc
LIMIT 100;



-- week 4 day 1
-- WINDOW FUNCTIONS

SELECT O_ORDERKEY, 
        O_ORDERSTATUS, 
        O_ORDERDATE,
        O_TOTALPRICE,
        SUM(O_TOTALPRICE)
FROM ORDERS
GROUP BY 1,2,3, 4;

SELECT SUM(O_TOTALPRICE)
FROM ORDERS;


SELECT O_ORDERKEY, 
        O_ORDERSTATUS, 
        O_ORDERDATE,
        O_TOTALPRICE,
        (SELECT SUM(O_TOTALPRICE)
            FROM ORDERS) AS GRAND_TOTAL,
        (O_TOTALPRICE / GRAND_TOTAL) *100
FROM ORDERS;


SELECT YEAR(O_ORDERDATE), 
        SUM(O_TOTALPRICE)
FROM ORDERS
GROUP BY YEAR(O_ORDERDATE);



SELECT O_ORDERKEY, 
        O_ORDERSTATUS, 
        O_ORDERDATE,
        O_TOTALPRICE,
        YEAR(O_ORDERDATE) AS YR,
        MONTH(O_ORDERDATE) AS MNTH,
        SUM(O_TOTALPRICE) OVER() AS GRAND_TOTAL,
        SUM(O_TOTALPRICE) OVER(PARTITION BY YEAR(O_ORDERDATE) ORDER BY YEAR(O_ORDERDATE)) AS ANNUAL_SUBTOTAL,
        SUM(O_TOTALPRICE) OVER(PARTITION BY YEAR(O_ORDERDATE), MONTH(O_ORDERDATE) ORDER BY YEAR(O_ORDERDATE)) AS YR_MNTH_SUBTOTAL,
        (O_TOTALPRICE/ YR_MNTH_SUBTOTAL) *100 AS MNTH_YR_PERCENT
FROM ORDERS;



SELECT YEAR(O_ORDERDATE) AS YR,
        MONTH(O_ORDERDATE) AS MNTH,
        SUM(O_TOTALPRICE) AS MNTH_YR_SUBTOTAL,
        SUM(SUM(O_TOTALPRICE)) OVER(PARTITION BY YEAR(O_ORDERDATE), MONTH(O_ORDERDATE)) AS YEARLY_SUBTOTAL,  --encapsulate sum with another sum to trick it into                                                                                                    thinking its an aggreaget so we dont have to put it in the group by 
        SUM(SUM(O_TOTALPRICE)) OVER() AS GRAND_TOTAL,
        AVG(AVG(O_TOTALPRICE)) OVER(PARTITION BY YEAR(O_ORDERDATE) ORDER BY YEAR(O_ORDERDATE)) AS ANNUAL_AVG_SALES
FROM ORDERS
GROUP BY YR, MNTH
ORDER BY YR, MNTH;


SELECT O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE,
        AVG(O_TOTALPRICE) OVER(PARTITION BY YEAR(O_ORDERDATE)) AS ANNUAL_AVG,
        AVG(O_TOTALPRICE) OVER() AS AVG_SALES
FROM ORDERS
ORDER BY O_ORDERDATE, O_TOTALPRICE DESC;


SELECT O_ORDERDATE,
        SUM(O_TOTALPRICE) AS DAILY_TOTAL,
        SUM(SUM(O_TOTALPRICE)) OVER(PARTITION BY YEAR(O_ORDERDATE), MONTH(O_ORDERDATE)) AS MONTHLY_TOTAL,
        SUM(COUNT(O_ORDERKEY)) OVER(PARTITION BY YEAR(O_ORDERDATE), MONTH(O_ORDERDATE)) AS MONTHLY_COUNT --cant do count of count need sum
FROM ORDERS
GROUP BY O_ORDERDATE
ORDER BY O_ORDERDATE;





WITH CTE AS (
SELECT O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE,
        SUM(O_TOTALPRICE) OVER() AS GRAND_TOTAL,
         SUM(O_TOTALPRICE) OVER(PARTITION BY YEAR(O_ORDERDATE)) AS YEARL_SUB,
         RANK() OVER(PARTITION BY YEAR(O_ORDERDATE) ORDER BY O_TOTALPRICE DESC) AS ANNUAL_SALES_RANK
FROM ORDERS
)
SELECT *
FROM CTE
WHERE ANNUAL_SALES_RANK <= 5;





SELECT O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE,
        YEAR(O_ORDERDATE) YR,
        SUM(O_TOTALPRICE) OVER() AS GRAND_TOTAL,
        SUM(O_TOTALPRICE) OVER(PARTITION BY YEAR(O_ORDERDATE)) AS YEARL_SUB,
        RANK() OVER(PARTITION BY YEAR(O_ORDERDATE) ORDER BY O_TOTALPRICE DESC) AS ANNUAL_SALES_RANK
FROM ORDERS
WHERE YR = 1998;






WITH CTE AS (
SELECT O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE,
        SUM(O_TOTALPRICE) OVER() AS GRAND_TOTAL,
         SUM(O_TOTALPRICE) OVER(PARTITION BY YEAR(O_ORDERDATE)) AS YEARL_SUB,
         RANK() OVER(PARTITION BY YEAR(O_ORDERDATE) ORDER BY O_TOTALPRICE) AS ANNUAL_SALES_RANK,
         CASE
            WHEN ANNUAL_SALES_RANK = 1 THEN 'REALLY BAD'
            WHEN ANNUAL_SALES_RANK = 2 THEN 'BAD'
            WHEN ANNUAL_SALES_RANK = 3 THEN 'NOT BAD'
        END AS RANK_NOTE
FROM ORDERS
)
SELECT *
FROM CTE
WHERE ANNUAL_SALES_RANK <= 3;



-- TOP 5 CUSTOMERS BY TOTAL SALES PER YEAR
WITH CTE2 AS(
SELECT C.C_NAME,
        YEAR(O.O_ORDERDATE) AS YR,
        SUM(O.O_TOTALPRICE) OVER(PARTITION BY C.C_CUSTKEY, YEAR(O.O_ORDERDATE)) AS TOTAL_SALES_BY_CUST,
        RANK() OVER(PARTITION BY YEAR(O.O_ORDERDATE) ORDER BY O.O_TOTALPRICE DESC) AS RNK
FROM ORDERS O 
JOIN CUSTOMER C ON O.O_CUSTKEY = C.C_CUSTKEY
)
SELECT *
FROM CTE2
WHERE RNK <= 5;


-- Activity 4.1 
-- 14-1 add grand_total_sales
select year(o_orderdate) as order_year,
        count(*) as num_orders, 
        sum(o_totalprice) as tot_sales,
        sum(sum(o_totalprice)) over() as grand_total_sales
from orders
group by order_year;


-- 14-2 records_per_year
select year(o_orderdate) as order_year,
        count(*) as num_orders, 
        sum(o_totalprice) as tot_sales,
        sum(sum(o_totalprice)) over(),
        sum(count(o_orderkey)) over(partition by year(o_orderdate)) as records_per_year
from orders
group by order_year;


-- 14-3 add avg_per_year
select year(o_orderdate) as order_year,
        count(*) as num_orders, 
        sum(o_totalprice) as tot_sales,
        sum(sum(o_totalprice)) over(),
        sum(count(o_orderkey)) over(partition by year(o_orderdate)) as records_per_year,
        avg(avg(o_totalprice)) over(partition by year(o_orderdate)) as avg_per_year
from orders
group by order_year;



-- 14-4 top 3 months for each year in terms of total sales (ASC)
with cte_top3 as
(
select year(o_orderdate) as order_year,
        month(o_orderdate) as order_month,
        count(*) as num_orders, 
        sum(o_totalprice) as tot_sales,
        rank() over(partition by year(o_orderdate) order by sum(o_totalprice)) as month_rank
from orders
group by order_year, order_month
order by order_year
)
select *
from cte_top3
where month_rank <= 3;




use schema techcatalyst_de.gmastrorilli;


describe table techcatalyst_de.gmastrorilli.AVG_ORDER_PRICE;

select *  from avg_order_price;


insert into avg_order_price (c_mktsegment, avg_total_price value
('Gina', 10),
('mast', 13);

create or replace TABLE techcatalyst_de.gmastrorilli.another_tbl(
	c_name VARCHAR(25) PRIMARY KEY, -- must be not null and unique 
    c_custkey Number(13.0) unique,
);

-- Activity 4.2
use schema techcatalyst_de.gmastrorilli;

-- Customers Table
create temporary table techcatalyst_de.gmastrorilli.Customers(
    customer_id INT Primary Key,
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    email VARCHAR(16777216)
);

INSERT INTO Customers (customer_id, first_name, last_name, email)
VALUES 
(1, 'John', 'Doe', 'john.doe@example.com'),
(2, 'Gina', 'Mastrorilli', 'gmast@example.com'),
(3, 'Emily', 'Smith', 'esmith@example.com'),
(4, 'Andris', 'Reba', 'areba@example.com'),
(5, 'Lola', 'Mark', 'lm@example.com');


-- Orders Table
create temporary table techcatalyst_de.gmastrorilli.Orders(
    order_id INT Primary Key,
    customer_id INT,
    order_date DATE, 
    total_amount NUMBER(13,2),
    CONSTRAINT FK_CUSTID foreign key (customer_id) references techcatalyst_de.gmastrorilli.Customers(customer_id)
);

INSERT INTO Orders (order_id, customer_id, order_date, total_amount)
VALUES 
(1, 1, '2023-01-01', 100.00),
(2, 2, '2023-02-01', 110.00),
(3, 3, '2022-02-11', 11.23),
(4, 3, '2022-01-12', 95.56),
(5, 4, '2023-07-01', 70.00);


-- Products Table 
create temporary table techcatalyst_de.gmastrorilli.Products(
    product_id INT Primary Key,
    product_name VARCHAR(16777216),
    price NUMBER(13,2)
);


INSERT INTO Products (product_id, product_name, price)
VALUES 
(1, 'Laptop', 999.99),
(2, 'TV', 100.01),
(3, 'Book', 12.30),
(4, 'Water', 3.00),
(5, 'Picture', 17.31);




-- OrderDetails Table  (relational table/ connector)
create temporary table techcatalyst_de.gmastrorilli.OrderDetails(
    order_id INT,
    product_id INT,
    quantity INT,
    Constraint FK_OrderID foreign key (order_id) references techcatalyst_de.gmastrorilli.Orders(order_id),
    Constraint FK_ProductID foreign key (product_id) references techcatalyst_de.gmastrorilli.Products(product_id)
);


INSERT INTO OrderDetails (order_id, product_id, quantity)
VALUES 
(1,1,1),
(2,3,1),
(3,4,1),
(4,1,2),
(5,2,1);


SELECT *
FROM ORDERS O
JOIN CUSTOMERS C ON O.CUSTOMER_ID = C.CUSTOMER_ID
JOIN ORDERDETAILS OD ON O.ORDER_ID = OD.ORDER_ID
JOIN PRODUCTS P ON OD.PRODUCT_ID = P.PRODUCT_ID;



SELECT O_ORDERKEY,SUM(O_TOTALPRICE) OVER(ORDER BY O_ORDERDATE)
FROM ORDERS;


-- week 4 day 2
-- Activity 4.1 

create or replace table fun_facts 
(
id INT PRIMARY KEY AUTOINCREMENT START 1 INCREMENT 1,
name string,
salary int,
other_id int unique 
);

insert into fun_facts
(name, salary, other_id)
values
('Tarek', 122.5, 123),
('Joe', 90.89, 123),
('Sara', 100, 123),
('Jack', 90.99, 150),
('Tarek', 122.5, 123),
('Joe', 90.89, 123),
('Sara', 100, 123),
('Jack', 90.99, 150);

select * from fun_facts;


-- auto increment is automatically assigning the id value. it will keep assigninging in sequential order
-- even if it is inserting duplicate data will give a unique id; makes ID non reliable 

-- snowflake does not enfore constraints they are used for information and providing metadata
-- snowflake is designed as a data warehouse and used for analytics so constraints arent as important 
-- only enforces not null 
-- constraints are used so that other tools can interact with snowflake

-- Distinct, group by, delete, Row Number window function (in the cte and then select * from cte where row_number = 1)

WITH cte AS (
SELECT id, 
    name, 
    salary, 
    other_id,
    ROW_NUMBER() OVER(PARTITION BY name, salary ORDER BY other_id) as real_row_num
FROM fun_facts
)
SELECT * FROM cte WHERE real_row_num = 1;

select distinct name, salary, other_id
from fun_facts;


select name, salary, other_id, count(id) as cnt_dups
from fun_facts
group by 1,2,3;

delete from fun_facts where id in
with cte as
(
select *,
    row_number() over(partition by name, salary, other_id order by ID) as row_num
from fun_facts
)
select *
from cte
where row_num = 1;




-- DATE TIME
SELECT CURRENT_DATE()AS TODAY_DATE,
    YEAR(TODAY_DATE) AS YR,
    DATE_PART('YEAR', TODAY_DATE) AS YR_PART,
    MONTH(TODAY_DATE) AS MNTH,
    DATE_PART('MONTH', TODAY_DATE) AS MNTH_PART,
    DAYNAME(TODAY_DATE) AS DAY_NAME,
    NEXT_DAY(TODAY_DATE, 'FR') AS NEXT_DATE,
    PREVIOUS_DAY(TODAY_DATE,'FR') AS PREV_DAY,
    DATEADD('DAY', 1, TODAY_DATE) AS ADD_1_DAY,
    DATEDIFF('DAY', TODAY_DATE, NEXT_DATE),
    DATEDIFF('DAY', TODAY_DATE, NEXT_DAY(TODAY_DATE, 'FR')) ;


SELECT
CAST('124' AS DECIMAL(5,1)),
TO_DECIMAL('124'),
'125'::DECIMAL(5,1),
TRY_TO_DECIMAL('121A'); -- WILL GIVE NULL IF NOT VALID WONT CRASH


SELECT
    TRY_CAST('202A-10-25' AS DATE),
    TRY_CAST('2020-10-25' AS DATETIME),
    TRY_TO_TIMESTAMP('2020-10-25'),
    TO_TIMESTAMP('2024-10-19 01:01:03.009');



-- Activity 4.4
--1
SELECT CURRENT_DATE() as TODAY_DATE,
    YEAR(TODAY_DATE) AS YEAR,
    MONTH(TODAY_DATE) AS MONTH,
    QUARTER(TODAY_DATE) AS QUARTER,
    WEEK(TODAY_DATE) AS WEEK,
    DAYNAME(TODAY_DATE) AS NAME_OF_DAY;
    
--2
SELECT CURRENT_DATE() as TODAY_DATE,
    NEXT_DAY(TODAY_DATE, 'WE') AS NEXT_WEDNESDAY,
    PREVIOUS_DAY(TODAY_DATE,'WE') AS PREV_WEDNESDAY;

--3
SELECT CURRENT_DATE() as TODAY_DATE,
    DATEADD(MONTH, 1, TODAY_DATE)AS NEXT_MONTH,
    DATEADD(QUARTER, 1, TODAY_DATE) AS NEXT_QUARTER,
    DATEADD(WEEK, -1, TODAY_DATE) AS PREVIOUS_WEEK,
    DATEADD(YEAR, -1, TODAY_DATE) AS PREVIOUS_YEAR;
    
--4
SELECT CURRENT_DATE() as TODAY_DATE,
    NEXT_DAY(TODAY_DATE, 'WE') AS NEXT_WEDNESDAY,
    DATEDIFF('DAY', TODAY_DATE, NEXT_WEDNESDAY) AS DIFF_IN_DAYS,
    DATEADD(QUARTER, 1, TODAY_DATE) AS NEXT_QUARTER,
    DATEDIFF('MONTH', TODAY_DATE, NEXT_QUARTER) AS DIFF_IN_MONTHS;

--5
SELECT
    CAST('320' AS DECIMAL(5,2)) AS DECIMAL_CAST,
    '320'::DECIMAL(5,2) AS DECIMAL_COLON,
    TO_DECIMAL('320', 5, 2) AS DECIMAL_TO,
    CAST('2024-01-01' AS DATE) AS DATE_CAST,
    '2024-01-01'::DATE AS DATE_COLON,
    CAST('2024-01-01' AS DATETIME) AS DATETIME_CAST,
    '2024-01-01'::DATETIME AS DATETIME_COLON,
    TO_TIMESTAMP('2024-01-01') AS DATETIME_TO;

--6
SELECT
    TRY_CAST('202A-10-25' as DATE) as try_date_cast,
    TRY_CAST('202A-10-25' as DATETIME) as try_datetime_cast,
    TRY_TO_DATE('202A-10-25') as try_date_to,
    TRY_TO_TIMESTAMP('202A-10-25') as try_datetime_to,
    TRY_TO_DECIMAL('12A', 5,2) as try_decimal_to;

--7
SELECT
    iff(5 = 0, 'Zero', iff(5 > 0, 'Positive', 'Negative')) as number_sign,
    IFF(4%2=1, 'ODD', 'EVEN') AS NUMBER_PARITY;

--8
SELECT
    IFF(NEXT_DAY(CURRENT_DATE(),'SA')- CURRENT_DATE() >1, 'WEEKDAY', 'WEEKEND') AS DAY_TYPE,
    datediff('day', current_date(), next_day(current_date(), 'SA')) as days_until_saturday;



-- week 4 day 4
selct *
from table(
    infer_schema(
        location =>'@GMASTRORILLI/class/yellow_tripdata.csv'.
        file_format => 'GMASTRORILLI_csv_format'
    )
);

CREATE OR REPLACE FILE FORMAT GMASTRORILLI_json_format
TYPE = 'JSON';

CREATE OR REPLACE FILE FORMAT GMASTRORILLI_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

CREATE OR REPLACE FILE FORMAT GMASTRORILLI_parquet_format
TYPE = 'PARQUET';

TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI;


CREATE OR REPLACE FILE FORMAT gina_json_strip_format
TYPE = 'JSON'
STRIP_OUTER_ARRAY = true;
 

SELECT *
FROM TABLE(
 INFER_SCHEMA(
   LOCATION=>'@TAGTWAN_AWS_STAGE/class/yellow_tripdata.parquet',
   FILE_FORMAT=>'tatwan_parquet_format' 
)
;


SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION=>'@TAGTWAN_AWS_STAGE/class/yellow_tripdata.csv',
    FILE_FORMAT=>'tatwan_csv_format'
  )
);


SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION=>'@TAGTWAN_AWS_STAGE/class/yellow_tripdata.json',
    FILE_FORMAT=>'tatwan_json_strip_format'
  )
);


-- Activity 4.6



CREATE OR REPLACE STAGE TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI
    STORAGE_INTEGRATION = s3_int
    URL='s3://techcatalyst-public/stocks';


LIST @TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI;


    
CREATE OR REPLACE FILE FORMAT GMASTRORILLI_json_format
TYPE = 'JSON';

CREATE OR REPLACE FILE FORMAT GMASTRORILLI_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

CREATE OR REPLACE FILE FORMAT GMASTRORILLI_parquet_format
TYPE = 'PARQUET';


-- csv
SELECT $1, $2, $3, $4, $5, $6, $7
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI/goog.csv
LIMIT 10;

SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION=>'@TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI/goog.csv',
    FILE_FORMAT=>'GMASTRORILLI_csv_format'
  )
);


-- parquet
SELECT *
FROM TABLE(
 INFER_SCHEMA(
   LOCATION=>'@TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI/aapl.parquet',
   FILE_FORMAT=>'GMASTRORILLI_parquet_format'
   )
);

SELECT $1:Date::NUMBER, 
        $1:Open::REAL
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI/aapl.parquet
(FILE_FORMAT=>'GMASTRORILLI_parquet_format')
match_by_column_name
LIMIT 10;



-- json
SELECT *
FROM TABLE(
 INFER_SCHEMA(
   LOCATION=>'@TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI/ge.json',
   FILE_FORMAT=>'gina_json_strip_format'
   )
);


SELECT $1:"Adj Close"::NUMBER(13, 10), 
        $1:Close::NUMBER(13, 10), 
        $1:Date::NUMBER(13, 0), 
        $1:High::NUMBER(13, 10), 
        $1:Low::NUMBER(13, 10), 
        $1:Open::NUMBER(13, 10), 
        $1:Volume::NUMBER(8, 0)
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI/ge.json
(FILE_FORMAT=>'gina_json_strip_format')
LIMIT 10;
 


-- create stock table
CREATE OR REPLACE TRANSIENT TABLE TECHCATALYST_DE.GMASTRORILLI.STOCKS (
    Date DATE,
    Close FLOAT(38),
    Adj_Close FLOAT(38),
    High FLOAT(38),
    Low FLOAT(38),
    Open FLOAT(38),
    Volume FLOAT(38),
    Stock_Name STRING);


-- Date	Open	High	Low	Close	Adj Close	Volume
-- csv insert
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.STOCKS
SELECT 
    $1::DATE AS Date,
    $5::FLOAT as Close,
    $6::FLOAT as Adj_Close,
    $3::FLOAT as High,
    $4::FLOAT as Low,
    $2::FLOAT as Open,
    $7::FLOAT as Volume,
    'Google' as Stock_Name
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI/goog.csv
(FILE_FORMAT=>'GMASTRORILLI_csv_format');


-- parquet insert

INSERT INTO TECHCATALYST_DE.GMASTRORILLI.STOCKS
SELECT 
     to_date(to_timestamp(g.$1:Date::NUMBER, 9)) as Date,
     g.$1:Close::FLOAT as Close,
     g.$1:"Adj Close"::FLOAT as Adj_Close,
     g.$1:High::FLOAT as High,
     g.$1:Low::FLOAT as Low,
     g.$1:Open::FLOAT as Open,
     g.$1:Volume::FLOAT as Volume,
    'Apple' as Stock_Name
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI/aapl.parquet
    (FILE_FORMAT => 'GMASTRORILLI_parquet_format') as g;


-- json insert 

INSERT INTO TECHCATALYST_DE.GMASTRORILLI.STOCKS
select
    to_date(to_timestamp($1:Date::STRING)) as Date,
    $1:Open::FLOAT as Open,
    $1:Close::FLOAT as Close,
    $1:"Adj Close"::FLOAT as Adj_Close,
    $1:High::FLOAT as High,
    $1:Low::FLOAT as Low,
    $1:Volume::FLOAT as Volume,
    'GE' as Stock_Name
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI/ge.json
(FILE_FORMAT => 'gina_json_strip_format');
 

-- daily percentage change
with pct_google as(
    select Date,
        Stock_name,
        LAG(Close) OVER(ORDER BY Date) AS PREV_CLOSE,
        ((Close - PREV_CLOSE)/ PREV_CLOSE)*100 AS PCT_CHANGE
    from TECHCATALYST_DE.GMASTRORILLI.STOCKS
    where Stock_Name = 'Google'
),
pct_apple as(
    select Date,
        Stock_name,
        LAG(Close) OVER(ORDER BY Date) AS PREV_CLOSE,
        ((Close - PREV_CLOSE)/ PREV_CLOSE)*100 AS PCT_CHANGE
    from TECHCATALYST_DE.GMASTRORILLI.STOCKS
    where Stock_Name = 'Apple'
),
pct_ge as(
    select Date,
        Stock_name,
        LAG(Close) OVER(ORDER BY Date) AS PREV_CLOSE,
        ((Close - PREV_CLOSE)/ PREV_CLOSE)*100 AS PCT_CHANGE
    from TECHCATALYST_DE.GMASTRORILLI.STOCKS
    where Stock_Name = 'GE'
),
combined as(
    select * 
    from pct_google
    union 
    select * 
    from pct_apple
    union
    select * 
    from pct_ge
)
select *
from combined;


-- moving average 
select
        Date,
        stock_name,
        AVG(CLOSE) OVER(PARTITION by stock_name ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MOVING_AVG_7,
    from techcatalyst_de.gmastrorilli.stocks;


with ma_google as(
    select
        Date,
        stock_name,
        AVG(CLOSE) OVER(ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MOVING_AVG_7,
    from techcatalyst_de.gmastrorilli.stocks
    where Stock_Name = 'Google'
),
ma_apple as(
    select
        Date,
        stock_name,
        AVG(CLOSE) OVER(ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MOVING_AVG_7,
    from techcatalyst_de.gmastrorilli.stocks
    where Stock_Name = 'Apple'
),
ma_ge as(
    select
        Date,
        stock_name,
        AVG(CLOSE) OVER(ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MOVING_AVG_7,
    from techcatalyst_de.gmastrorilli.stocks
    where Stock_Name = 'GE'
),
combined as(
    select * 
    from ma_google
    union 
    select * 
    from ma_apple
    union
    select * 
    from ma_ge
)
select *
from combined;




-- date with highest trade vol
with v_google as(
    select
        Date,
        stock_name,
        volume,
        row_number() over(order by volume desc) as rank,
    from techcatalyst_de.gmastrorilli.stocks
    where Stock_Name = 'Google'
),
v_apple as(
    select
        Date,
        stock_name,
        volume,
        row_number() over(order by volume desc) as rank,
    from techcatalyst_de.gmastrorilli.stocks
    where Stock_Name = 'Apple'
),
v_ge as(
    select
        Date,
        stock_name,
        volume,
        row_number() over(order by volume desc) as rank,
    from techcatalyst_de.gmastrorilli.stocks
    where Stock_Name = 'GE'
),
combined as(
    select * 
    from v_google
    union 
    select * 
    from v_apple
    union
    select * 
    from v_ge
)
select *
from combined
where rank = 1;



use schema techcatalyst_de.external_stage;
drop stage gmastrorilli;

drop file format TECHCATALYST_DE.EXTERNAL_STAGE.GMASTRORILLI_PARQUET_FORMAT;


-- week 4 day 5
-- Use the right Role and Warehouse
USE ROLE DE;
USE WAREHOUSE COMPUTE_WH;

-- Create the sales_data table
CREATE OR REPLACE TRANSIENT TABLE TECHCATALYST_DE.GMASTRORILLI.sales_data (
    Store STRING,
    Product STRING,
    Sales INT,
    Date DATE
);

-- Insert sample data into the sales_data table
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.sales_data (Store, Product, Sales, Date) VALUES 
('A', 'Apples', 100, '2024-07-01'),
('A', 'Oranges', 150, '2024-07-02'),
('B', 'Apples', 200, '2024-07-01'),
('B', 'Oranges', 120, '2024-07-02'),
('C', 'Apples', 90, '2024-07-01'),
('C', 'Oranges', 80, '2024-07-02'),
('A', 'Apples', 130, '2024-07-03'),
('B', 'Oranges', 110, '2024-07-03'),
('C', 'Apples', 95, '2024-07-03'),
('A', 'Oranges', 105, '2024-07-04'),
('B', 'Apples', 210, '2024-07-04'),
('C', 'Oranges', 70, '2024-07-04');




SELECT STORE,
    PRODUCT,
    SUM(SALES)
FROM SALES_DATA
GROUP BY PRODUCT, STORE;


SELECT STORE,
    PRODUCT,
    SALES,
    DATE,
    SALES - AVG(SALES) OVER(PARTITION BY PRODUCT) AS SALES_DIFFEENCE,
    CASE
        WHEN SALES > 100 THEN TRUE
        ELSE FALSE
    END AS HIGH_SALES
FROM SALES_DATA
GROUP BY STORE, PRODUCT, SALES, DATE;



SELECT STORE,
        PRODUCT,
        SALES,
        DATE,
        YEAR(DATE),
        MONTH(DATE),
        DAY(DATE),
        DAYNAME(DATE)
FROM SALES_DATA;




SELECT Store, Product, Sales, Date, AVG(Sales) OVER(PARTITION BY PRODUCT ORDER BY DATE ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MOVING_AVG_3_DAYS
FROM sales_data;



SELECT STORE,
        PRODUCT,
        SALES,
        DATE,
        LAG(SALES) OVER(ORDER BY Date)
FROM SALES_DATA;








-- 7
SELECT STORE,
        PRODUCT,
        SALES,
        DATE,
        CASE
            WHEN SALES <= 100 THEN 'LOW'
            WHEN SALES <= 150 THEN 'MEDIUM'
            WHEN SALES > 150 THEN 'HIGH' 
        END AS SALES_LEVEL
FROM SALES_DATA;



-- 8 
SELECT STORE,
        SUM(SALES),
        AVG(SALES),
        MAX(SALES),
        MIN(SALES)
FROM SALES_DATA
GROUP BY STORE;





use role de;
use warehouse compute_wh; 
use schema snowflake_sample_data.tpch_sf1; 
show tables; 

select o_orderkey, o_custkey, o_clerk, o_orderstatus, o_orderpriority, count(*)
from orders
group by o_orderkey, o_custkey, o_clerk, o_orderstatus, o_orderpriority; 

-- F 1-Urgent = 146143
select o_orderstatus, o_orderpriority, count(*)
from orders
group by  o_orderstatus, o_orderpriority
order by 1, 2;
 
-- F 1-URGENT = 146143
select o_orderkey, o_custkey, o_clerk, o_orderstatus, o_orderpriority, 
count(o_orderkey) over (partition by o_orderstatus, o_orderpriority) as cnt_by_status_priority 
from orders
order by o_orderstatus, o_orderpriority;



-- week 6 day 1
CREATE OR REPLACE STAGE TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE
        --STORAGE_INTEGRATION = s3_int
        URL='s3://gina-techcatalyst-lab/'
        CREDENTIALS = (AWS_KEY_ID= '', AWS_SECRET_KEY= '');


CREATE OR REPLACE FILE FORMAT gina_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;


create or replace transient table techcatalyst_de.gmastrorilli.test
(
name string,
favnumber number
);


create or replace pipe techcatalyst_de.external_stage.gina_pipe
auto_ingest = True
as 
copy into techcatalyst_de.gmastrorilli.test
from  @TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE
FILE_FORMAT = 'gina_csv_format';

alter pipe techcatalyst_de.external_stage.gina_pipe refresh;

select *
from techcatalyst_de.gmastrorilli.test;





-- sparkify mini project
USE ROLE DE;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA TECHCATALYST_DE.GMASTRORILLI;

-- creating stage & file format
CREATE OR REPLACE STAGE TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE
    STORAGE_INTEGRATION = s3_int
    URL='s3://techcatalyst-public';

CREATE OR REPLACE FILE FORMAT GMASTRORILLI_parquet_format
TYPE = 'PARQUET';



-- song table
SELECT *
FROM TABLE(
 INFER_SCHEMA(
   LOCATION=>'@TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE/dw_stage/gina/songs_table/',
   FILE_FORMAT=>'GMASTRORILLI_parquet_format'));

   
CREATE OR REPLACE TRANSIENT TABLE TECHCATALYST_DE.GMASTRORILLI.SONGS_DIM (
    SONG_ID STRING,
    TITLE STRING,
    YEAR NUMBER,
    ARTIST_ID STRING,
    DURATION FLOAT
);

INSERT INTO TECHCATALYST_DE.GMASTRORILLI.SONGS_DIM (SONG_ID, TITLE, YEAR, ARTIST_ID, DURATION)
SELECT
    $1:song_id::STRING AS SONG_ID,
    $1:title::STRING AS TITLE,
    REGEXP_SUBSTR(METADATA$FILENAME, 'year=(\\d+)', 1, 1, 'e')::NUMBER AS YEAR,
    REGEXP_SUBSTR(METADATA$FILENAME, 'artist_id=([^/]+)', 1, 1, 'e')::STRING AS ARTIST_ID,
    $1:duration::FLOAT AS DURATION
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE/dw_stage/gina/songs_table/ (FILE_FORMAT => 'GMASTRORILLI_parquet_format', PATTERN => '.*parquet.*');


SELECT *
FROM TECHCATALYST_DE.GMASTRORILLI.SONGS_DIM;



-- USERS table
SELECT *
FROM TABLE(
 INFER_SCHEMA(
   LOCATION=>'@TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE/dw_stage/gina/user_table/',
   FILE_FORMAT=>'GMASTRORILLI_parquet_format'));


CREATE TRANSIENT TABLE TECHCATALYST_DE.GMASTRORILLI.USERS_DIM (
    ID VARCHAR,
    FIRSTNAME VARCHAR,
    LASTNAME VARCHAR,
    GENDER VARCHAR,
    LEVEL VARCHAR
);

COPY INTO TECHCATALYST_DE.GMASTRORILLI.USERS_DIM
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE/dw_stage/gina/user_table/
PATTERN = '.*parquet.*'
FILE_FORMAT = 'GMASTRORILLI_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT *
FROM TECHCATALYST_DE.GMASTRORILLI.USERS_DIM;



-- ARTISTS table
SELECT *
FROM TABLE(
 INFER_SCHEMA(
   LOCATION=>'@TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE/dw_stage/gina/artists_table/',
   FILE_FORMAT=>'GMASTRORILLI_parquet_format'));


CREATE OR REPLACE TRANSIENT TABLE TECHCATALYST_DE.GMASTRORILLI.ARTISTS_DIM (
    ARTIST_ID VARCHAR,
    ARTIST_NAME VARCHAR,
    ARTIST_LOCATION VARCHAR,
    ARTIST_LATITUDE FLOAT,
    ARTIST_LONGITUDE FLOAT
);

COPY INTO TECHCATALYST_DE.GMASTRORILLI.ARTISTS_DIM
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE/dw_stage/gina/artists_table/
PATTERN = '.*parquet.*'
FILE_FORMAT = 'GMASTRORILLI_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT *
FROM TECHCATALYST_DE.GMASTRORILLI.ARTISTS_DIM;



-- TIME table
CREATE OR REPLACE TRANSIENT TABLE TECHCATALYST_DE.GMASTRORILLI.TIME_DIM (
    TS STRING,
    DATETIME DATETIME,
    START_TIME TIME,
    YEAR NUMBER,
    MONTH NUMBER,
    DAY_OF_MONTH NUMBER,
    WEEK_OF_YEAR NUMBER
);

INSERT INTO TECHCATALYST_DE.GMASTRORILLI.TIME_DIM (TS, DATETIME, START_TIME, YEAR, MONTH, DAY_OF_MONTH, WEEK_OF_YEAR)
SELECT
    $1:ts::STRING AS TS,
    $1:datetime::DATETIME AS DATETIME,
    $1:start_time::TIME AS START_TIME,
    REGEXP_SUBSTR(METADATA$FILENAME, 'year=(\\d+)', 1, 1, 'e')::NUMBER AS YEAR,
    REGEXP_SUBSTR(METADATA$FILENAME, 'month=([^/]+)', 1, 1, 'e')::NUMBER AS MONTH,
    $1:dayofmonth::NUMBER AS DAY_OF_MONTH,
    $1:weekofyear::NUMBER AS WEEK_OF_YEAR
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE/dw_stage/gina/time_table/ (FILE_FORMAT => 'GMASTRORILLI_parquet_format', PATTERN => '.*parquet.*');

SELECT *
FROM TECHCATALYST_DE.GMASTRORILLI.TIME_DIM;



-- SONGPLAYS table
SELECT *
FROM TABLE(
 INFER_SCHEMA(
   LOCATION=>'@TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE/dw_stage/gina/songplays_table/',
   FILE_FORMAT=>'GMASTRORILLI_parquet_format'));


CREATE OR REPLACE TRANSIENT TABLE TECHCATALYST_DE.GMASTRORILLI.SONGPLAYS_FACT (
    SONGPLAY_ID NUMBER,
    DATETIME_ID STRING,
    USER_ID VARCHAR,
    LEVEL STRING,
    SONG_ID STRING,
    ARTIST_ID STRING,
    SESSION_ID NUMBER,
    LOCATION STRING,
    USER_AGENT STRING
);

COPY INTO TECHCATALYST_DE.GMASTRORILLI.SONGPLAYS_FACT
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.GM_STAGE/dw_stage/gina/songplays_table/
PATTERN = '.*parquet.*'
FILE_FORMAT = 'GMASTRORILLI_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT *
FROM TECHCATALYST_DE.GMASTRORILLI.SONGPLAYS_FACT;


--- SQL Mini Project 

create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.FACT_ACCIDENTS (
	ACCIDENT_ID NUMBER(38,0),
	POLICYHOLDER_ID NUMBER(38,0),
	VEHICLE_ID NUMBER(38,0),
	STATE_ID NUMBER(38,0),
	BODY_STYLE_ID NUMBER(38,0),
	ACCIDENT_TYPE_ID NUMBER(38,0),
	GENDER_MARITALSTATUS_ID NUMBER(38,0),
	VEHICLE_USECODE_ID VARCHAR(16777216),
	ACCIDENT_DATE DATE,
	VEHICLE_YEAR VARCHAR(16777216),
	POLICYHOLDER_BIRTHDATE DATE,
	ESTIMATED_COST FLOAT,
	ACTUAL_REPAIR_COST FLOAT,
	AT_FAULT BOOLEAN,
	IS_DUI BOOLEAN,
	COVERAGE_STATUS VARCHAR(16777216)
);

create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.DIM_ACCIDENT_TYPE (
	ACCIDENT_TYPE_ID NUMBER(38,0),
	ACCIDENT_TYPE VARCHAR(16777216)
);

create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.DIM_BODY_STYLE (
	BODY_STYLE_ID NUMBER(38,0),
	BODY_STYLE VARCHAR(16777216)
);

create or replace TABLE TECHCATALYST_DE.GMASRORILLI.DIM_GENDER_MARITAL (
	GENDER_MARITALSTATUS_ID NUMBER(38,0),
	GENDER_MARITAL_STATUS VARCHAR(16777216)
);

create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.DIM_POLICYHOLDER (
	POLICYHOLDER_ID NUMBER(38,0),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	ADDRESS VARCHAR(16777216)
);


create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.DIM_STATES (
	STATE_ID NUMBER(38,0),
	STATE VARCHAR(16777216)
);



create or replace TABLE TECHCATALYST_DE.GMASTRORILLI.DIM_VEHICLE_USE (
	VEHICLE_USECODE_ID VARCHAR(16777216),
	VEHICLE_USE VARCHAR(16777216)
);


-- dim tables first

-- DIM_ACCIDENT_TYPE TABLE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_ACCIDENT_TYPE
(ACCIDENT_TYPE_ID, ACCIDENT_TYPE)
SELECT ACCIDENT_TYPE_CODE, ACCIDENT_TYPE
FROM TECHCATALYST_DE.PUBLIC.INS_ACCIDENT_TYPE;


-- DIM_BODY_STYLE TABLE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_BODY_STYLE
(BODY_STYLE_ID, BODY_STYLE)
SELECT BODY_STYLE_CODE, BODY_STYLE
FROM TECHCATALYST_DE.PUBLIC.INS_BODY_STYLE;


-- DIM_GENDER_MARITAL TABLE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_GENDER_MARITAL
(GENDER_MARITALSTATUS_ID,GENDER_MARITAL_STATUS)
SELECT GENDER_MARITAL_STATUS_CODE,GENDER_MARITAL_STATUS
FROM TECHCATALYST_DE.PUBLIC.INS_GENDER_MARITAL_STATUS;


-- DIM_POLICYHOLDER TABLE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_POLICYHOLDER
(POLICYHOLDER_ID, LAST_NAME, FIRST_NAME, ADDRESS)
SELECT POLICYHOLDER_ID, LAST_NAME, FIRST_NAME, ADDRESS
FROM TECHCATALYST_DE.PUBLIC.INS_POLICYHOLDER;

-- DIM_STATES TABLE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_STATES
(STATE_ID, STATE)
SELECT STATE_CODE, STATE
FROM TECHCATALYST_DE.PUBLIC.INS_STATES;


-- DIM_VEHICLE_USE
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.DIM_VEHICLE_USE
(VEHICLE_USECODE_ID, VEHICLE_USE)
SELECT USE_CODE, VEHICLE_ID
FROM TECHCATALYST_DE.PUBLIC.INS_VEHICLE_USE;


-- FACT_ACCIDENTS
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.FACT_ACCIDENTS
(ACCIDENT_ID, POLICYHOLDER_ID, VEHICLE_ID, STATE_ID, BODY_STYLE_ID, ACCIDENT_TYPE_ID, GENDER_MARITALSTATUS_ID, VEHICLE_USECODE_ID, ACCIDENT_DATE, VEHICLE_YEAR, POLICYHOLDER_BIRTHDATE, ESTIMATED_COST, ACTUAL_REPAIR_COST, AT_FAULT, IS_DUI, COVERAGE_STATUS)
SELECT A.ACCIDENT_ID,
       P.POLICYHOLDER_ID,
       V.VEHICLE_ID,
       S.STATE_CODE, 
       V.BODY_STYLE_CODE, 
       A.ACCIDENT_TYPE, 
       G.GENDER_MARITAL_STATUS_CODE, 
       VU.USE_CODE, 
       A.ACCIDENT_DATE, 
       V.YEAR, 
       P.BIRTHDATE, 
       A.ESTIMATED_COST, 
       A.ACTUAL_REPAIR_COST, 
       A.AT_FAULT, 
       A.DUI, 
       I.COVERAGE_STATUS
FROM TECHCATALYST_DE.PUBLIC.INS_ACCIDENTS AS A
JOIN TECHCATALYST_DE.PUBLIC.INS_POLICYHOLDER AS P ON A.POLICYHOLDER_ID = P.POLICYHOLDER_ID
JOIN TECHCATALYST_DE.PUBLIC.INS_VEHICLES V ON A.VEHICLE_ID = V.VEHICLE_ID
JOIN TECHCATALYST_DE.PUBLIC.INS_STATES S ON P.STATE_CODE = S.STATE_CODE
JOIN TECHCATALYST_DE.PUBLIC.INS_GENDER_MARITAL_STATUS G ON P.GENDER_MARITAL_STATUS = G.GENDER_MARITAL_STATUS_CODE
JOIN TECHCATALYST_DE.PUBLIC.INS_VEHICLE_USE VU ON V.VEHICLE_ID = VU.VEHICLE_ID
JOIN TECHCATALYST_DE.PUBLIC.INS_INSURANCE_COVERAGE I ON P.POLICYHOLDER_ID = I.POLICYHOLDER_ID;



-- validate your tables once data is loaded
select *
FROM TECHCATALYST_DE.GMASTRORILLI.FACT_ACCIDENTS AS F
JOIN TECHCATALYST_DE.GMASTRORILLI.DIM_ACCIDENT_TYPE A ON F.ACCIDENT_TYPE_ID = A.ACCIDENT_TYPE_ID
JOIN TECHCATALYST_DE.GMASTRORILLI.DIM_BODY_STYLE B ON F.BODY_STYLE_ID = B.BODY_STYLE_ID
JOIN TECHCATALYST_DE.GMASTRORILLI.DIM_POLICYHOLDER P ON F.POLICYHOLDER_ID = P.POLICYHOLDER_ID
JOIN TECHCATALYST_DE.GMASTRORILLI.DIM_STATES S ON F.STATE_ID = S.STATE_ID
JOIN TECHCATALYST_DE.GMASTRORILLI.DIM_VEHICLE_USE V ON F.VEHICLE_USECODE_ID = V.VEHICLE_USECODE_ID
order by v.VEHICLE_USECODE_ID desc
LIMIT 100;




-- Data Analysis

-- Identify the top cities with the highest number of claims using SQL/ CLAIMS  PER LOCATION
SELECT COUNT(F.ACCIDENT_ID) AS NUMBER_OF_CLAIMS,
        S.STATE AS STATE
FROM FACT_ACCIDENTS F
JOIN DIM_STATES S ON F.STATE_ID = S.STATE_ID
GROUP BY STATE
ORDER BY NUMBER_OF_CLAIMS DESC;

-- Number of claims,
SELECT COUNT(ACCIDENT_ID)
FROM FACT_ACCIDENTS;

--number of policyholders
SELECT COUNT(POLICYHOLDER_ID)
FROM DIM_POLICYHOLDER;


-- Total claim amount
SELECT SUM(ACTUAL_REPAIR_COST)
FROM FACT_ACCIDENTS;


--  total claim amount per accident type
SELECT sum(ACTUAL_REPAIR_COST),
        a.accident_type
FROM fact_accidents f 
join dim_accident_type a on f.accident_type_id = a.accident_type_id
group by a.accident_type
order by sum(ACTUAL_REPAIR_COST) desc;

from techcatalyst_de.gmastrorilli.fact_accidents;




-- AVG claim amount
SELECT AVG(ACTUAL_REPAIR_COST)
FROM FACT_ACCIDENTS;

-- Which body style of vehicle has the most claims?
select d.body_style, count(a.accident_id),
from fact_accidents a
join dim_body_style d on a.body_style_id = d.body_style_id
group by d.body_style
order by count(a.accident_id) desc;


--  Which year has the most claims?
select year(accident_date), count(accident_id)
from fact_accidents
group by year(accident_date)
order by count(accident_id) desc;


-- What are the Top 5 Year & Months?
select year(accident_date), month(accident_date), count(accident_id)
from fact_accidents
group by month(accident_date), year(accident_date)
order by count(accident_id) desc
limit 5;


-- How many policyholders have more than one claim in the dataset?
SELECT COUNT(ACCIDENT_ID),
        POLICYHOLDER_ID
FROM FACT_ACCIDENTS
GROUP BY POLICYHOLDER_ID
HAVING COUNT(ACCIDENT_ID) > 1;

-- What's the average estimated cost and actual repair cost per accident type?
SELECT a.accident_type,
AVG(ACTUAL_REPAIR_COST),
        avg(estimated_cost)
FROM fact_accidents f
join dim_accident_type a on f.accident_type_id = a.accident_type_id
group by a.accident_type;


-- Which state has the highest discrepancy between estimated and actual repair costs?
SELECT S.STATE,(F.ACTUAL_REPAIR_COST - F.ESTIMATED_COST)
FROM FACT_ACCIDENTS F
JOIN DIM_STATES S ON F.STATE_ID = S.STATE_ID
GROUP BY S.STATE, (F.ACTUAL_REPAIR_COST - F.ESTIMATED_COST)
ORDER BY (F.ACTUAL_REPAIR_COST - F.ESTIMATED_COST) DESC;





-- yellow taxi mini project
-- create Yellow_taxi table
CREATE OR REPLACE TRANSIENT TABLE TECHCATALYST_DE.GMASTRORILLI.YELLOW_TAXI (
    VENDORID NUMBER(38,0),
    TPEP_PICKUP_DATETIME TIMESTAMP,
    TPEP_DROPOFF_DATETIME TIMESTAMP,
    YEAR_SERVICE NUMBER,
    MONTH_SERVICE NUMBER,
    PASSENGER_COUNT NUMBER(38,0),
    TRIP_DISTANCE FLOAT,
    RATECODEID NUMBER(38,0),
    STORE_AND_FWD_FLAG VARCHAR(1),
    PULOCATIONID NUMBER(38,0),
    DOLOCATIONID NUMBER(38,0),
    PAYMENT_TYPE NUMBER(38,0),
    FARE_AMOUNT FLOAT,
    EXTRA FLOAT,
    MTA_TAX FLOAT,
    TIP_AMOUNT FLOAT,
    TOLLS_AMOUNT FLOAT,
    IMPROVEMENT_SURCHARGE FLOAT,
    TOTAL_AMOUNT FLOAT,
    CONGESTION_SURCHARGE FLOAT,
    AIRPORT_FEE FLOAT,
    MONTH_FILE VARCHAR(10) 
);

-- 01-2024
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.YELLOW_TAXI
SELECT VENDORID,
    TO_TIMESTAMP(TPEP_PICKUP_DATETIME, 6) AS TPEP_PICKUP_DATETIME,
    TO_TIMESTAMP(TPEP_DROPOFF_DATETIME, 6) AS TPEP_DROPOFF_DATETIME, 
    2024 AS YEAR_SERVICE,
    1 AS MONTH_SERVICE,
    PASSENGER_COUNT, 
    TRIP_DISTANCE, 
    RATECODEID, 
    STORE_AND_FWD_FLAG, 
    PULOCATIONID, 
    DOLOCATIONID, 
    PAYMENT_TYPE, 
    FARE_AMOUNT, 
    EXTRA, 
    MTA_TAX, 
    TIP_AMOUNT, 
    TOLLS_AMOUNT, 
    IMPROVEMENT_SURCHARGE, 
    TOTAL_AMOUNT, 
    CONGESTION_SURCHARGE, 
    AIRPORT_FEE,
    '2024-01'
FROM TECHCATALYST_DE.PUBLIC.YELLOW_TAXI_2024_01;

-- 02-2024
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.YELLOW_TAXI
SELECT VENDORID,
    TO_TIMESTAMP(TPEP_PICKUP_DATETIME, 6) AS TPEP_PICKUP_DATETIME,
    TO_TIMESTAMP(TPEP_DROPOFF_DATETIME, 6) AS TPEP_DROPOFF_DATETIME, 
    2024 AS YEAR_SERVICE,
    2 AS MONTH_SERVICE,
    PASSENGER_COUNT, 
    TRIP_DISTANCE, 
    RATECODEID, 
    STORE_AND_FWD_FLAG, 
    PULOCATIONID, 
    DOLOCATIONID, 
    PAYMENT_TYPE, 
    FARE_AMOUNT, 
    EXTRA, 
    MTA_TAX, 
    TIP_AMOUNT, 
    TOLLS_AMOUNT, 
    IMPROVEMENT_SURCHARGE, 
    TOTAL_AMOUNT, 
    CONGESTION_SURCHARGE, 
    AIRPORT_FEE,
    '2024-02'
FROM TECHCATALYST_DE.PUBLIC.YELLOW_TAXI_2024_02;


-- 03-2024
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.YELLOW_TAXI
SELECT VENDORID,
    TO_TIMESTAMP(TPEP_PICKUP_DATETIME, 6) AS TPEP_PICKUP_DATETIME,
    TO_TIMESTAMP(TPEP_DROPOFF_DATETIME, 6) AS TPEP_DROPOFF_DATETIME, 
    2024 AS YEAR_SERVICE,
    3 AS MONTH_SERVICE,
    PASSENGER_COUNT, 
    TRIP_DISTANCE, 
    RATECODEID, 
    STORE_AND_FWD_FLAG, 
    PULOCATIONID, 
    DOLOCATIONID, 
    PAYMENT_TYPE, 
    FARE_AMOUNT, 
    EXTRA, 
    MTA_TAX, 
    TIP_AMOUNT, 
    TOLLS_AMOUNT, 
    IMPROVEMENT_SURCHARGE, 
    TOTAL_AMOUNT, 
    CONGESTION_SURCHARGE, 
    AIRPORT_FEE,
    '2024-03'
FROM TECHCATALYST_DE.PUBLIC.YELLOW_TAXI_2024_03;

-- 04-2024
INSERT INTO TECHCATALYST_DE.GMASTRORILLI.YELLOW_TAXI
SELECT VENDORID,
    TO_TIMESTAMP(TPEP_PICKUP_DATETIME, 6) AS TPEP_PICKUP_DATETIME,
    TO_TIMESTAMP(TPEP_DROPOFF_DATETIME, 6) AS TPEP_DROPOFF_DATETIME, 
    2024 AS YEAR_SERVICE,
    4 AS MONTH_SERVICE,
    PASSENGER_COUNT, 
    TRIP_DISTANCE, 
    RATECODEID, 
    STORE_AND_FWD_FLAG, 
    PULOCATIONID, 
    DOLOCATIONID, 
    PAYMENT_TYPE, 
    FARE_AMOUNT, 
    EXTRA, 
    MTA_TAX, 
    TIP_AMOUNT, 
    TOLLS_AMOUNT, 
    IMPROVEMENT_SURCHARGE, 
    TOTAL_AMOUNT, 
    CONGESTION_SURCHARGE, 
    AIRPORT_FEE,
    '2024-04'
FROM TECHCATALYST_DE.PUBLIC.YELLOW_TAXI_2024_04;


-- CHECK
SELECT COUNT(*)
FROM YELLOW_TAXI;


-- TASK 2
SELECT MONTH_FILE,
        COUNT(TRIP_DISTANCE) AS TOTAL_TRIPS,
        SUM(FARE_AMOUNT) AS TOTAL_FARE_AMOUNT
FROM YELLOW_TAXI
GROUP BY MONTH_FILE
ORDER BY MONTH_FILE ASC;


SELECT VENDORID,
        COUNT(*)
FROM YELLOW_TAXI
GROUP BY VENDORID
ORDER BY VENDORID ASC;


SELECT PULOCATIONID,
        COUNT(*)
FROM YELLOW_TAXI
GROUP BY PULOCATIONID
ORDER BY PULOCATIONID;


SELECT PAYMENT_TYPE,
        COUNT(*)
FROM YELLOW_TAXI
GROUP BY PAYMENT_TYPE
ORDER BY PAYMENT_TYPE;


SELECT *
FROM YELLOW_TAXI
WHERE VENDORID = NULL;


SELECT TO_CHAR(TPEP_PICKUP_DATETIME, 'YYYY-MM') AS Period,
        COUNT(*) as "Total Records"
FROM YELLOW_TAXI
GROUP BY Period
ORDER BY Period ASC;


-- TASK 3

SELECT CASE
        WHEN PAYMENT_TYPE = 1 THEN 'Credit Card'
        WHEN PAYMENT_TYPE = 2 THEN 'Cash'
        WHEN PAYMENT_TYPE = 3 THEN 'No Charge'
        WHEN PAYMENT_TYPE = 4 THEN 'Dispute'
        WHEN PAYMENT_TYPE = 5 THEN 'Unknown'
        WHEN PAYMENT_TYPE = 6 THEN 'Voided Trip'
        ELSE 'No Data'
    END AS PAYMENT_TYPE,
        AVG(FARE_AMOUNT) AS AVERAGE_FARE_AMOUNT
FROM YELLOW_TAXI
WHERE MONTH_FILE = '2024-04'
GROUP BY PAYMENT_TYPE
ORDER BY PAYMENT_TYPE ASC;


SELECT
    MONTH_FILE, 
    (COUNT(CASE WHEN TIP_AMOUNT > 0 THEN 1 END) * 100.0 / COUNT(*)) AS "PERCENTAGE_WITH_TIPS"
FROM YELLOW_TAXI
GROUP BY MONTH_FILE
ORDER BY MONTH_FILE;


SELECT MONTH_FILE,
        DAY(TPEP_PICKUP_DATETIME) AS SERVICE_DAY_OF_MONTH,
        SUM(FARE_AMOUNT),
        sum(SUM(FARE_AMOUNT)) OVER(PARTITION BY MONTH_FILE order by SERVICE_DAY_OF_MONTH ) AS CUMULATIVE_TOTAL_FARE
FROM YELLOW_TAXI
GROUP BY SERVICE_DAY_OF_MONTH, MONTH_FILE
ORDER BY MONTH_FILE, SERVICE_DAY_OF_MONTH;




-- Task 4
CREATE VIEW TECHCATALYST_DE.GMASTRORILLI.V_YELLOW_TRIPDATA AS
SELECT 
    CASE
        WHEN VENDORID = 1 THEN 'Creative Mobile'
        WHEN VENDORID = 2 THEN 'VeriFone'
        ELSE 'No Data'
    END AS VENDOR_ID,
    TPEP_PICKUP_DATETIME,
    TPEP_DROPOFF_DATETIME,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    CASE
        WHEN RATECODEID = 1 THEN 'Standard Rate'
        WHEN RATECODEID = 2 THEN 'JFK'
        WHEN RATECODEID = 3 THEN 'Newark'
        WHEN RATECODEID = 4 THEN 'Nassau/Westchester'
        WHEN RATECODEID = 5 THEN 'Negotiated Rate'
        WHEN RATECODEID = 6 THEN 'Group Rate'
        WHEN RATECODEID = 99 THEN 'Special Rate'
        ELSE 'No Data'
    END AS RATE_TYPE,
    STORE_AND_FWD_FLAG,
    CASE
        WHEN PAYMENT_TYPE = 1 THEN 'Credit Card'
        WHEN PAYMENT_TYPE = 2 THEN 'Cash'
        WHEN PAYMENT_TYPE = 3 THEN 'No Charge'
        WHEN PAYMENT_TYPE = 4 THEN 'Dispute'
        WHEN PAYMENT_TYPE = 5 THEN 'Unknown'
        WHEN PAYMENT_TYPE = 6 THEN 'Voided Trip'
        ELSE 'No Data'
    END AS PAYMENT_TYPE,
    FARE_AMOUNT,
    EXTRA,
    MTA_TAX,
    TIP_AMOUNT,
    TOLLS_AMOUNT
FROM YELLOW_TAXI
limit 20;




CREATE VIEW TECHCATALYST_DE.GMASTRORILLI.V_FINANCE_YELLOW_TRIPDATA AS
SELECT 
    CASE
        WHEN VENDORID = 1 THEN 'Creative Mobile'
        WHEN VENDORID = 2 THEN 'VeriFone'
        ELSE 'No Data'
    END AS VENDOR_ID,
    TPEP_PICKUP_DATETIME,
    TPEP_DROPOFF_DATETIME,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    CASE
        WHEN RATECODEID = 1 THEN 'Standard Rate'
        WHEN RATECODEID = 2 THEN 'JFK'
        WHEN RATECODEID = 3 THEN 'Newark'
        WHEN RATECODEID = 4 THEN 'Nassau/Westchester'
        WHEN RATECODEID = 5 THEN 'Negotiated Rate'
        WHEN RATECODEID = 6 THEN 'Group Rate'
        WHEN RATECODEID = 99 THEN 'Special Rate'
        ELSE 'No Data'
    END AS RATE_TYPE,
    STORE_AND_FWD_FLAG,
    CASE
        WHEN PAYMENT_TYPE = 1 THEN 'Credit Card'
        WHEN PAYMENT_TYPE = 2 THEN 'Cash'
        WHEN PAYMENT_TYPE = 3 THEN 'No Charge'
        WHEN PAYMENT_TYPE = 4 THEN 'Dispute'
        WHEN PAYMENT_TYPE = 5 THEN 'Unknown'
        WHEN PAYMENT_TYPE = 6 THEN 'Voided Trip'
        ELSE 'No Data'
    END AS PAYMENT_TYPE,
    FARE_AMOUNT,
    EXTRA,
    MTA_TAX,
    TIP_AMOUNT,
    TOLLS_AMOUNT
FROM YELLOW_TAXI
WHERE VENDOR_ID = 'VeriFone' AND PAYMENT_TYPE = 1;





SELECT 
    EXTRACT(HOUR FROM TPEP_PICKUP_DATETIME) AS HOUR_OF_DAY,
    AVG(DATEDIFF('MINUTE', TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME)) AS AVG_TRIP_DURATION
FROM YELLOW_TAXI
GROUP BY HOUR_OF_DAY
ORDER BY HOUR_OF_DAY;










