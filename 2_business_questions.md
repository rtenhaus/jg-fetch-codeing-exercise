All questions answered using the transformed models from [1b_transformation]([url](https://github.com/rtenhaus/jg-fetch-codeing-exercise/blob/main/1b_transformation.md)).
* BigQuery used for all answers

### Question 1
What are the top 5 brands by receipts scanned for most recent month?
```
with recent_receipts as (
	-- Get receipts from last complete month
	select * from `jg-fetch-coding-exercise`.analytics.receipts
	where date_trunc(date(scanned_ts_utc), month) = date_sub(date_trunc(current_date(), month), interval 1 month)
)

, receipt_items as (
	select * from `jg-fetch-coding-exercise`.analytics.receipt_items
)

, brands as (
	select * from `jg-fetch-coding-exercise`.analytics.brands
)

, joined as (

	select
		  brands.brand_name
	  , count(recent_receipts.receipt_id) as receipt_scanned_count
	from recent_receipts
	inner join receipt_items 
    on recent_receipts.receipt_id = receipt_items.receipt_id
	inner join brands
    on receipt_items.item_barcode = brands.barcode
	group by all

)

select * from joined
order by receipt_scanned_count desc
limit 5
```

### Question 2 
How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
```
with recent_receipts as (
	-- Get receipts from last 2 complete months
	select 
		* 
      , date_trunc(date(scanned_ts_utc), month) as scan_month
      -- helper bools for downstream
      , date_trunc(date(scanned_ts_utc), month) = date_sub(date_trunc(current_date(), month), interval 1 month) as is_recent_month
      , date_trunc(date(scanned_ts_utc), month) = date_sub(date_trunc(current_date(), month), interval 2 month) as is_previous_month
	from `jg-fetch-coding-exercise`.analytics.receipts
	where date_trunc(date(scanned_ts_utc), month) = date_sub(date_trunc(current_date(), month), interval 1 month)
	or date_trunc(date(scanned_ts_utc), month) = date_sub(date_trunc(current_date(), month), interval 2 month)
)

, receipt_items as (
	select * from `jg-fetch-coding-exercise`.analytics.receipt_items
)

, brands as (
	select * from `jg-fetch-coding-exercise`.analytics.brands
)

, joined as (

	select
	    recent_receipts.scan_month
	  , recent_receipts.is_recent_month
	  , recent_receipts.is_previous_month
	  ,	brands.brand_name
	  , count(recent_receipts.receipt_id) as scanned_receipt_count
	from recent_receipts
	inner join receipt_items 
    on recent_receipts.receipt_id = receipt_items.receipt_id
	inner join brands
    on receipt_items.item_barcode = brands.barcode
	group by all

)

, recent_brands_top_5 as (

	select
		brand_name
	  , rank() over(order by scanned_item_count desc) as brand_rank
	from joined
	where is_recent_month

	qualify brand_rank <= 5

)

, prev_month_brands_ranked as (

	select
		brand_name
	  , rank() over(order by scanned_item_count desc) as brand_rank
	from joined
	where is_previous_month

)

, final as (

	select
	  recent_brands_top_5.brand_rank
	, recent_brands_top_5.brand_name
	, case
			when recent_brands_top_5.brand_rank = prev_month_brands_ranked.brand_rank 
				then 'No Change'
			when recent_brands_top_5.brand_rank < prev_month_brands_ranked.brand_rank -- lower is an increase in this case, aka goes from 3 to 1
				then 'Improve by ' || cast(prev_month_brands_ranked.brand_rank - recent_brands_top_5.brand_rank as string)
			when recent_brands_top_5.brand_rank > prev_month_brands_ranked.brand_rank -- higher is a decrease in this case, aka dropping from 1 to 3
				then 'Drop by ' || cast(recent_brands_top_5.brand_rank- prev_month_brands_ranked.brand_rank as string)
			when prev_month_brands_ranked is null
				then 'Not ranked last month'
			else 'check calcs'
	  end as brand_rank_change

	from recent_brands_top_5
	left join prev_month_brands_ranked using(brand_name)

)
select * from final
order by brand_rank asc
```

### Question 3
When considering average spend from receipts with `rewardsReceiptStatus` of `Accepted` or `Rejected`, which is greater?
```
with receipts as (
	-- Only analysing Accepted and Rejected Receipts
	select * from `jg-fetch-coding-exercise`.analytics.receipts
	where rewards_receipt_status in('ACCEPTED', 'REJECTED')
	-- no 'ACCEPTED' in my version of the exercise, probably use 'FINISHED'
)

select
	rewards_receipt_status
  , avg(total_spent) as avg_spent
 from receipts
 group by all
 order by 2 desc
```
Alternately, if we don't trust the total from the receipt, we can calculate average spend from the receipt line items:
```
with receipts as (
	-- Only analysing Accepted and Rejected Receipts
	select * from `jg-fetch-coding-exercise`.analytics.receipts
	where rewards_receipt_status in('ACCEPTED', 'REJECTED')
	-- no 'ACCEPTED' in my version of the exercise, probably use 'FINISHED'
)

, receipt_items as (
	select * from `jg-fetch-coding-exercise`.analytics.receipt_items
)

, joined as (
	select
		  receipts.receipt_id
    , receipts.rewards_receipt_status
		, sum(receipt_items.quantity_purchased * receipt_items.item_price) as total_spent
	from receipts
	inner join receipt_items
		on receipts.receipt_id = receipt_items.receipt_id
	group by all
)

select
	rewards_receipt_status
  , avg(total_spent) as avg_spent
 from joined
 group by all
 order by 2 desc
```

### Question 4
When considering total number of items purchased from receipts with `rewardsReceiptStatus` of `Accepted` or `Rejected`, which is greater
**
```
with receipts as (
	-- Only analysing Accepted and Rejected Receipts
	select * from `jg-fetch-coding-exercise`.analytics.receipts
	where rewards_receipt_status in('ACCEPTED', 'REJECTED')
	-- no 'ACCEPTED' in my version of the exercise, probably use 'FINISHED'
)

select
	rewards_receipt_status
  , sum(purchased_item_count) as avg_spent
 from receipts
 group by all
 order by 2 desc
```
Alternately, if we don't trust the total from the receipt, we can calculate average spend from the receipt line items:
```
with receipts as (
	-- Only analysing Accepted and Rejected Receipts
	select * from `jg-fetch-coding-exercise`.analytics.receipts
	where rewards_receipt_status in('ACCEPTED', 'REJECTED')
	-- no 'ACCEPTED' in my version of the exercise, probably use 'FINISHED'
)

, receipt_items as (
	select * from `jg-fetch-coding-exercise`.analytics.receipt_items
)

select
	receipts.rewards_receipt_status
	-- since we don't have quantities purchased, count the unique items
  , sum(receipt_items.quantity_purchased) as item_purchased_counts
from receipts
inner join receipt_items 
	on receipts.receipt_id = receipt_items.receipt_id
group by all
order by 2 desc
```

### Question 5
Which brand has the most spend among users who were created within the past 6 months?
```
-- Which brand has the most spend among users who were created within the past 6 months?

with receipt_items as (
	select * from `jg-fetch-coding-exercise`.analytics.receipt_items
)

, receipts as (
	select * from `jg-fetch-coding-exercise`.analytics.receipts
)

, brands as (
	select * from `jg-fetch-coding-exercise`.analytics.brands
)

, recent_users as (
	-- Only get users created within the last 6 months
	select * from `jg-fetch-coding-exercise`.analytics.users
	where date(created_ts_utc) >= date_sub(current_date(), interval 6 month)
	-- assume we only want consumers
	and user_role = 'consumer'
)

, joined as (
	select
		-- receipts
		  receipts.receipt_id
		, receipts.user_id
		-- receipt items
		, receipt_items.receipt_item_sk
		, receipt_items.quantity_purchased
		, receipt_items.item_price
		-- brands
		, brands.brand_name

	from receipt_items
	inner join receipts
		on receipt_items.receipt_id = receipts.receipt_id
	inner join recent_users
		on receipts.user_id = recent_users.user_id
	inner join brands
		on receipt_items.item_barcode = brands.barcode -- could potentially use brand_code instead, both have issues
)

select 
		brand_name
	, sum(quantity_purchased * item_price) as total_spent
from joined
group by 1
order by 2 desc
limit 1
```

### Question 6
Which brand has the most transactions among users who were created within the past 6 months?
```
with receipt_items as (
	select * from `jg-fetch-coding-exercise`.analytics.receipt_items
)

, receipts as (
	select * from `jg-fetch-coding-exercise`.analytics.receipts
)

, brands as (
	select * from `jg-fetch-coding-exercise`.analytics.brands
)

, recent_users as (
	-- Only get users created within the last 6 months
	select * from `jg-fetch-coding-exercise`.analytics.users
	where date(created_ts_utc) >= date_sub(current_date(), interval 6 month)
	-- assume we only want consumers
	and user_role = 'consumer'
)

, joined as (
	select
		-- receipts
		  receipts.receipt_id
		, receipts.user_id
		-- receipt items
		, receipt_items.receipt_item_sk
		, receipt_items.quantity_purchased
		, receipt_items.item_price
		-- brands
		, brands.brand_name

	from receipt_items
	inner join receipts
		on receipt_items.receipt_id = receipts.receipt_id
	inner join recent_users
		on receipts.user_id = recent_users.user_id
	inner join brands
		on receipt_items.item_barcode = brands.barcode -- could potentially use brand_code instead, both have issues
)

select 
		brand_name
		-- assume a transsaction is at the receipt level, ie multiple purchases are considered 1 transaction 
	, count(distinct receipt_id) as transaction_count
from joined
group by 1
order by 2 desc
limit 1

```
