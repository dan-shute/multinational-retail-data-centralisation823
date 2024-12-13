-- Milestone 4 Queries

-- Task 1
select country_code, count(*) as total_no_stores from dim_store_details
where store_type != 'Web Portal'
group by country_code
order by total_no_stores desc

-- Task 2
select locality, count(*) as total_no_stores 
from dim_store_details
group by locality
order by total_no_stores desc
limit 7

-- Task 3
select month, sum(total_price) as total_sales from
(select d.month, (o.product_quantity * p.product_price) as total_price
from orders_table as o
join dim_products p on o.product_code = p.product_code
join dim_date_times d on o.date_uuid = d.date_uuid) as t1
group by month
order by total_sales desc

-- Task 4
select 
count(*) as numbers_of_sales, 
sum(o.product_quantity) as product_quantity_count,
case when s.store_type != 'Web Portal' then 'Offline'
else 'Web'
end as "location"
from orders_table o
join dim_store_details s on o.store_code = s.store_code
group by location
order by numbers_of_sales

-- Task 5
select store_type, total_sales, round(total_sales * 100.00 / sum(total_sales) over(), 2) as "sales_made%"
from(select s.store_type, sum(o.product_quantity * p.product_price) as total_sales
from orders_table o
join dim_store_details s on o.store_code = s.store_code
join dim_products p on o.product_code = p.product_code
group by store_type
order by total_sales desc)

-- Task 6
select sum(o.product_quantity * p.product_price) as total_sales, d.year, d.month
from orders_table as o
join dim_products p on o.product_code = p.product_code
join dim_date_times d on o.date_uuid = d.date_uuid
group by month, year
order by total_sales desc

-- Task 7
select sum(staff_numbers) as total_staff_numbers, country_code
from dim_store_details
group by country_code
order by total_staff_numbers desc

-- Task 8
select sum(o.product_quantity * p.product_price) as total_sales, s.store_type,  s.country_code
from orders_table o
join dim_store_details s on o.store_code = s.store_code
join dim_products p on o.product_code = p.product_code
where s.country_code = 'DE'
group by store_type, s.country_code
order by total_sales

-- Task 9
select year, avg(difference) as average_time_taken
from (select date, timestamp, year, 
	(LEAD(date) over(partition by year order by date)) - date as difference
	from dim_date_times) t1
group by year
order by average_time_taken desc



