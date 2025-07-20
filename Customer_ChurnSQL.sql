-- Customer Churn Analysis
use northwind;

-- Problem Statement 1. Analyze order frequency for each customer & identify any pattern to figure out if the frequency purchase declines before churn
select * from orders;
select * from customers;
with order_frequency as	(select
		customer_id,
		order_date,
		Lag(order_date) over (partition by customer_id order by order_date) as previous_order_date,
		datediff(order_date, Lag(order_date) over (partition by customer_id order by order_date)) as time_gap
	from
		orders)
select
	customer_id,
    avg(time_gap) as average_time_gap
from 
	order_frequency
group by
	customer_id
order by 
	average_time_gap desc;
    
    
-- Problem Statement 2. Calculate metrics like average time between orders for customers segments that is frequent and non-frequent buyers.
 with order_frequency as	(select
		customer_id,
		order_date,
		Lag(order_date) over (partition by customer_id order by order_date) as previous_order_date,
		datediff(order_date, Lag(order_date) over (partition by customer_id order by order_date)) as time_gap
	from
		orders),
customer_segment as	(select
		customer_id,
		Case
			when avg(time_gap)<=30 then 'Frequent Buyer'
			when avg(time_gap)<=90 then 'Regular Buyer'
			else 'Infrequent Buyer'
		End as segment
	from 
		order_frequency
	group by
		customer_id)
	select
		segment,
        avg(time_gap) as average_time_gap
	from
		order_frequency o
	join customer_segment cs on o.customer_id= cs.customer_id
    group by segment; 
    
-- Problem Statement 3. Analyze average order value for churning and non-churning customers
With Churn_status as (select
		c.id,
		Case
			when o.customer_id is not null then 'Non-churning'
			else 'Churning'
		End as Churn_status
	from 
		customers c 
	left join orders o on c.id = o.customer_id),
Order_value as   (select
	o.customer_id,
    sum(od.quantity * od.unit_price) as order_value
   from 
	orders o
    join order_details od on o.id = od.order_id
    group by
		o.customer_id)
select
	cs.Churn_status,
    avg(ov.Order_value) as avg_order_value
from
	Churn_status cs join
    Order_value ov on cs.id = ov.customer_id
    group by
		cs.Churn_status;

-- Problem statement 4. Analyze distribution of order values

With order_value_distribution as (select
		c.id,
		sum(od.quantity * od.unit_price) as order_value
	from customers c
	left join orders o on c.id = o.customer_id
	left join order_details od on o.id = od.order_id
	group by c.id)
select
	Case
		when order_value >= 5000 then 'High order value'
        when order_value >= 1000 then 'Medium order value'
        Else 'Low order value'
	End as order_value_category,
    count(id) as customer_count
from order_value_distribution
group by order_value_category;

-- Problem statement 5. Identify changes in product category preferences for customers who churn

select
	Distinct c.id as customer_id,
    c.company as customer_name,
    p.category as churned_category,
    Case
		when o.id is null then 'Churned' 
        else 'Active'
	End as Churn_status
from customers c
left join orders o on c.id = o.customer_id
left join order_details od on o.id = od.order_id
left join products p on od.product_id = p.id
where 
	c.id in (select customer_id from orders where status_id = 3) 
And p.category not in (
	select distinct p.category
    from customers c
left join orders o on c.id = o.customer_id
left join order_details od on o.id = od.order_id
left join products p on od.product_id = p.id
where
	c.id not in (select customer_id from orders where status_id = 3)
)

-- Problem statement 6. Analyze most frequently purchased categories before and after churn

With Churned_customer as (select
		distinct customer_id
	from orders
	where status_id=3)
select
	p.category,
    count(Case when o.customer_id in (select customer_id from churned_customer) then o.id End) as Churned_count,
    count(Case when o.customer_id not in (select customer_id from churned_customer) then o.id End) as Active_count
from orders o
join order_details od on o.id = od.order_id
join products p on p.id = od.product_id
group by p.category
order by Churned_count desc;

-- Another method for solving this problem
With Churned_customer as (select
		distinct customer_id
	from orders
	where status_id=3),
churned_category_count as (select
		p.category,
		count(*) as churned_count
		from orders o
		join order_details od on o.id = od.order_id
		join products p on p.id = od.product_id
		where o.customer_id in (select customer_id from churned_customer)
		group by p.category),
		active_category_count as (select
			p.category,
			count(*) as active_count
			from orders o
			join order_details od on o.id = od.order_id
			join products p on p.id = od.product_id
			where o.customer_id not in (select customer_id from churned_customer)
			group by p.category)
		select 
			cc.category,
            cc.churned_count,
            ac.active_count
        from churned_category_count cc
        join active_category_count ac on cc.category = ac.category
        order by cc.churned_count desc, active_count desc;
        
-- Problem statement 7.Investigate churn rate by region to understand customer's purchase behaviour for a location

With churned_customer as (select distinct customer_id
	from orders
	where status_id in (
	select id from orders_status where status_name='Closed' or status_name='Shipped')),
customer_locations as (	select
			c.id,
			c.company,
			c.city,
			c.state_province,
			c.country_region,
			Case when cc.customer_id is not null then 'Churned' else 'Active' End as customer_status
		from customers c
		left join churned_customer cc on c.id = cc.customer_id)
select
	country_region,
	state_province,
    city,
    count(Case when customer_status='Churned' then 1 End) as churned_count,
    count(Case when customer_status='Active' then 1 End) as active_count,
    round((count(case when customer_status='Churned' then 1 End)*100.0)/count(*),2) as churn_rate
from customer_locations 
group by
	country_region,state_province,city
order by churn_rate desc;

-- Problem statement 8. Retrieve total number of orders,quantity,revenue for each region

select
	c.country_region,
    c.state_province,
    c.city,
    count(o.id) as total_orders,
    round(sum(od.quantity),2) as total_quantity,
    round(sum(od.quantity * od.unit_price),2) as total_revenue
from customers c
join orders o on c.id = o.customer_id
join order_details od on o.id = od.order_id
group by c.country_region,c.state_province,c.city
order by c.country_region,c.state_province,c.city;

-- Problem statement 9. Find out risk-score for each customer with respect to purchase frequency,purchase frequency decline,order value for some specific category

select
	c.id customer_id,
    c.company company_name,
    count(o.id) total_orders,
    sum(od.quantity * od.unit_price) total_spent,
    Case
		when count(o.id)>=7 and sum(od.quantity * od.unit_price)>=1000 then 'Low risk'
        when count(o.id) between 4 and 7 and sum(od.quantity * od.unit_price) between 500 and 999 then 'Medium risk'
        Else 'High risk'
	End risk_category
from customers c
left join orders o on c.id = o.customer_id
left join order_details od on o.id = od.order_id
group by c.id,c.company
order by total_orders desc,total_spent desc;

-- Problem statement 10. Find Customer Lifetime Value(CLTV)

select
	c.id,
    customer_id,
    c.company,
    count(Distinct o.id) as Total_orders_last_6_months
from customers c
left join orders o on c.id = o.customer_id
where
	o.order_date >= Date_sub((select max(order_date) from  orders),interval 6 month)
group by c.id,c.company
having
	count(Distinct o.id) < (select avg(order_count) from(
		select
			c.id,
			customer_id,
            count(Distinct o.id) as order_count
        from customers c
        join orders o on c.id = o.customer_id
        where
		o.order_date >= Date_sub((select max(order_date) from orders), interval 6 month)
        group by c.id
	) as order_counts)
-- to calculate CLTV
select
	c.id as customer_id,
    round(sum(od.quantity * od.unit_price * (1-od.discount))-
    sum(o.shipping_fee + o.taxes),2) as cltv
from
	customers c
join
	orders o on c.id = o.customer_id
join
	order_details od on o.id = od.order_id
group by c.id







