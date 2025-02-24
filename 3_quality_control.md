Investigation/QC is split between staging layers and modeled data
* Some items discovered here were incorporated into the staging model (de-duping for example)
* BigQuery used for all exploration
__________________________________
# Key Observations
* Duplicate users, but easily handled
* A small number of duplicate barcodes and brand_codes in the brands table.  Many test brands in this table.
* Receipt timestamps were not captued well, with > 80% missing
* Of 399 consumer recipts, 76 (20%) have a receipt line item without a barcode or brand_code, and 59 (15%) have a receipt line item with a missing/invalid item price or purchase quantity.

__________________________________
## Staging Layer

### Users
```
-- are there dupes?

select 
    count(*) as row_count
  , count(distinct user_id) as unique_user_count
from `jg-fetch-coding-exercise`.staging.stg_users
-- 495 rows
-- 212 unique user ids

-- who are they?
with potential_dupes as (
  select 
      user_id
    , count(*) as ct
  from `jg-fetch-coding-exercise`.staging.stg_users
  group by 1
  having ct > 1
)

select * from `jg-fetch-coding-exercise`.staging.stg_users
where user_id in (select user_id from potential_dupes)
order by user_id


-- are they true dupes or are there any unique elements
with unique_rows as (
  select
    distinct
        user_id
      , user_role
      , is_active
      , created_ts_utc
      , last_login_ts_utc
      , state
      , sign_up_source
  from `jg-fetch-coding-exercise`.staging.stg_users      
)

  select 
      user_id
    , count(*) as ct
  from unique_rows
  group by 1
  having ct > 1
  -- 0.  Great, true dupes, lets add dedupe to staging model


  -- any odd dates?
  select
      count(distinct user_id) as user_count
    , count(distinct if(created_ts_utc is not null, user_id, null)) as created_ts_populated_count
    , min(created_ts_utc) as earliest_created_ts
    , max(created_ts_utc) as most_recent_created_ts
    , count(distinct if(last_login_ts_utc is not null, user_id, null)) as last_login_ts_populated_count
    , min(last_login_ts_utc) as earliest_last_login_ts
    , max(last_login_ts_utc) as most_recent_last_login_ts
from `jg-fetch-coding-exercise`.staging.stg_users  
-- 212 users, all with created timestamp, reasonable values, 172 (80%) with last_login timestamp, reasonable values
```

### brands
```
-- are there dupes

select 
    count(*) as row_count
  , count(distinct brand_id) as unique_brand_count
from `jg-fetch-coding-exercise`.staging.stg_brands
-- 1167 rows
-- 1167 unique brand ids

-- how about barcodes
select
    barcode
  , count(*) as ct
from `jg-fetch-coding-exercise`.staging.stg_brands
group by 1
having ct > 1
order by 2 desc
-- 7 barcode dupes

-- who are they?
with potential_dupes as (
  select
      barcode
    , count(*) as ct
  from `jg-fetch-coding-exercise`.staging.stg_brands
  group by 1
  having ct > 1
  order by 2 desc
)
select * from `jg-fetch-coding-exercise`.staging.stg_brands
where barcode in (select barcode from potential_dupes)
order by barcode
-- looks fishy, check with eng/ops to update if possible

-- how about brand codes
select
    brand_code
  , count(*) as ct
from `jg-fetch-coding-exercise`.staging.stg_brands
group by 1
having ct > 1
order by 2 desc
-- 2 dupes and many blank/null items... might not be reliable

-- HUGGIES and GOODNITES
select * from `jg-fetch-coding-exercise`.staging.stg_brands
where brand_code in('HUGGIES','GOODNITES')
-- different brand_id, barcode, top_brand classification, flag for reporting

-- what about blanks nulls
select * from `jg-fetch-coding-exercise`.staging.stg_brands
where ifnull(brand_code, '') = ''
-- some valid brands, also potential test accounts

-- check out test accounts
select * from `jg-fetch-coding-exercise`.staging.stg_brands
where upper(brand_name) like '%TEST%'
-- 432, lot of test accounts, add flag to staging model, ask engineering to set this in prod for cleaner analysis

-- categories
select
    category_code
  , category
  , count(*) as ct
from `jg-fetch-coding-exercise`.staging.stg_brands
where not upper(brand_name) like '%TEST%'
group by 1,2
order by 1,2
-- category is mostly populated but category_code is not... consider making a dim table to accurately map category_code.
-- also 155 with no category information
```

### receipts
```
-- are there dupes?
select
    count(*) as row_count
  , count(distinct receipt_id) as unique_receipt_count
from `jg-fetch-coding-exercise`.staging.stg_receipts
-- 1117 rows, 1117 unique receipts, yay


-- rewards_receipt_status
select 
    rewards_receipt_status
  , count(*) as ct
from `jg-fetch-coding-exercise`.staging.stg_receipts
group by 1
order by 1
-- 100% coverage, no nulls

-- user_ids
select 
    count(*) as missing_user_ids
from `jg-fetch-coding-exercise`.staging.stg_receipts
where ifnull(user_id, '') = ''
-- 100% coverage, no missing user_ids


-- receipt totals
select
    count(distinct receipt_id) as receipt_count
  , count(distinct if(ifnull(purchased_item_count,0) > 0, receipt_id, null)) as valid_purchased_item_count
  , count(distinct if(ifnull(total_spent,0) > 0, receipt_id, null)) as valid_total_spent_count
from `jg-fetch-coding-exercise`.staging.stg_receipts
-- 1117 receipts, 618 (55%) with positive item count, 667 (60%) with postive total spend
-- maybe not reliable
-- check if this is due to test user accounts after staging


  -- any odd dates?
  select
      count(distinct receipt_id) as receipt_count
    , count(distinct if(created_ts_utc is not null, user_id, null)) as created_ts_populated_count
    , min(created_ts_utc) as earliest_created_ts
    , max(created_ts_utc) as most_recent_created_ts
    , count(distinct if(purchase_ts_utc is not null, user_id, null)) as last_login_ts_populated_count
    , min(purchase_ts_utc) as earliest_purchase_ts
    , max(purchase_ts_utc) as most_recent_purchase_ts
    , count(distinct if(scanned_ts_utc is not null, user_id, null)) as scanned_ts_populated_count
    , min(scanned_ts_utc) as earliest_scanned_ts
    , max(scanned_ts_utc) as most_recent_scanned_ts
    , count(distinct if(finished_ts_utc is not null, user_id, null)) as finished_ts_populated_count
    , min(finished_ts_utc) as earliest_finish_ts
    , max(finished_ts_utc) as most_recent_finish_ts        
from `jg-fetch-coding-exercise`.staging.stg_receipts  
-- 1117 receipts, 0% coverage of created timestamp, less than 20% coverage for other timestamps, check against test users
```

## transformed data models
```
-- receipts and their users
select
    ifnull(users.user_role, 'not in user table') as user_role
  , count(distinct receipts.receipt_id) as receipts
from `jg-fetch-coding-exercise`.analytics.receipts
left join `jg-fetch-coding-exercise`.analytics.users
  on receipts.user_id = users.user_id
group by 1
order by 2 desc
-- 147/1117 (13%) receipts have user_id not in the users table
-- 571/1117 (51%) receipts are from fetch-staff
-- 399/1117 (36%) consumer receipts


-- lets only look at consumer receipts (ignore fetch-staff) and their items going forward

-- validity checks for consumer receipt items
with receipts as (
  select * from `jg-fetch-coding-exercise`.analytics.receipts
)
, receipt_items as (
  select * from `jg-fetch-coding-exercise`.analytics.receipt_items
)
, consumers as (
  select * from `jg-fetch-coding-exercise`.analytics.users
  where user_role = 'consumer'
)
, line_item_stats as (
  select 
      receipts.receipt_id
    , receipt_items.receipt_item_sk as line_item
    , item_barcode is null as is_barcode_not_populated
    , item_brand_code is null as is_brand_code_not_populated
    , item_barcode is null and item_brand_code is null as is_barcode_and_brand_code_not_populated
    , ifnull(quantity_purchased,0) <= 0 as is_invalid_quantity_purchased
    , ifnull(item_price,0.0) <= 0.0 as is_invalid_item_price
  from receipts
  inner join consumers on receipts.user_id = consumers.user_id
  inner join receipt_items on receipts.receipt_id = receipt_items.receipt_id
  group by all
)

select
    count(distinct receipt_id) as consumer_receipt_count
  , count(distinct line_item) as avg_line_item_count
  , count(distinct if(is_barcode_not_populated, line_item, null)) as invalid_barcode_line_item_count
  , count(distinct if(is_barcode_not_populated, receipt_id, null)) as invalid_barcode_receipt_count
  , count(distinct if(is_brand_code_not_populated, line_item, null)) as invalid_brand_code_line_item_count
  , count(distinct if(is_brand_code_not_populated, receipt_id, null)) as invalid_brand_code_receipt_count
  , count(distinct if(is_barcode_not_populated and is_brand_code_not_populated, line_item, null)) as invalid_barcode_and_brand_code_line_item_count
  , count(distinct if(is_barcode_not_populated and is_brand_code_not_populated, receipt_id, null)) as invalid_barcode_and_brand_code_receipt_count
  , count(distinct if(is_invalid_quantity_purchased, line_item, null)) as invalid_quantity_purchased_line_item_count
  , count(distinct if(is_invalid_quantity_purchased, receipt_id, null)) as invalid_quantity_purchased_receipt_count  
  , count(distinct if(is_invalid_item_price, line_item, null)) as invalid_item_price_line_item_count
  , count(distinct if(is_invalid_item_price, receipt_id, null)) as invalid_item_price_receipt_count  
  , count(distinct if(is_invalid_quantity_purchased or is_invalid_item_price, line_item, null)) as invalid_quantity_or_price_line_item_count
  , count(distinct if(is_invalid_quantity_purchased or is_invalid_item_price, receipt_id, null)) as invalid_quantity_or_price_receipt_count  
  , count(distinct if(is_invalid_quantity_purchased and is_invalid_item_price, line_item, null)) as invalid_quantity_and_price_line_item_count
  , count(distinct if(is_invalid_quantity_purchased and is_invalid_item_price, receipt_id, null)) as invalid_quantity_and_price_receipt_count  
from line_item_stats
-- 399 valid consumer receipts
-- 4082 line items
-- 1384 line items have no valid barcode or brand_code, spread among 76 receipts (20% of receipts)
-- 87 line items have invalid/missing purchase quantities or item prices, spread among 59 receipts (15%)


-- barcodes
with barcodes as (
  select distinct barcode
  from `jg-fetch-coding-exercise`.analytics.brands
  where barcode is not null
)
, receipts as (
  select * from `jg-fetch-coding-exercise`.analytics.receipts
)
, receipt_items as (
  select * from `jg-fetch-coding-exercise`.analytics.receipt_items
)
, consumers as (
  select * from `jg-fetch-coding-exercise`.analytics.users
  where user_role = 'consumer'
)

select 
    count(distinct receipt_items.item_barcode) as receipt_item_barcode_count
  , count(distinct if(barcodes.barcode is not null, receipt_items.item_barcode, null)) as mapped_barcode_count
  , count(distinct if(barcodes.barcode is null, receipt_items.item_barcode, null)) as unmapped_barcode_count
from receipts
inner join consumers on receipts.user_id = consumers.user_id
inner join receipt_items on receipts.receipt_id = receipt_items.receipt_id
left join barcodes on receipt_items.item_barcode = barcodes.barcode
-- 421 unique barcodes
-- 11 mapped in brands table
-- 410 unmapped in brands table (97%)


-- brand_codes
with brand_codes as (
  select distinct brand_code
  from `jg-fetch-coding-exercise`.analytics.brands
  where brand_code is not null
)
, receipts as (
  select * from `jg-fetch-coding-exercise`.analytics.receipts
)
, receipt_items as (
  select * from `jg-fetch-coding-exercise`.analytics.receipt_items
)
, consumers as (
  select * from `jg-fetch-coding-exercise`.analytics.users
  where user_role = 'consumer'
)

select 
    count(distinct receipt_items.item_brand_code) as receipt_item_brand_code_count
  , count(distinct if(brand_codes.brand_code is not null, receipt_items.item_brand_code, null)) as mapped_brand_code_count
  , count(distinct if(brand_codes.brand_code is null, receipt_items.item_brand_code, null)) as unmapped_brand_code_count
from receipts
inner join consumers on receipts.user_id = consumers.user_id
inner join receipt_items on receipts.receipt_id = receipt_items.receipt_id
left join brand_codes on receipt_items.item_brand_code = brand_codes.brand_code
-- 167 unique brand_codes
-- 30 mapped in brands table
-- 137 unmapped in brands table (82%)
```
