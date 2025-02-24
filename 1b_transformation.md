This transformation script generates mart tables from the staging tables
* BigQuery was used
* Because I used staging models, these are pretty lightweight transformations, with simple de-duping and adding other useful bits
* In addition to `users`, `brands`, and `receipts` tables, a `receipt_items` table is extracted.
____________________________________________________________________________________________________________

**users**

```
create or replace table `jg-fetch-coding-exercise`.analytics.users
as

with stg_users as (
  select * from `jg-fetch-coding-exercise`.staging.stg_users
)

, deduped as (
  -- remove duplicate records
  select * from stg_users

  -- Most appear to be exact dupes, but lets sort by earliest created record just in case
  qualify row_number() over(partition by user_id order by created_ts_utc asc) = 1
)

select * from deduped
```


**brands**
```
create or replace table `jg-fetch-coding-exercise`.analytics.brands
as

with stg_brands as (
  select * from `jg-fetch-coding-exercise`.staging.stg_brands
)

, final as (
  select
      *
    , (upper(brand_code) like '%TEST%' or upper(brand_name) like '%TEST%') as is_test_brand
  from stg_brands
)

select * from final
```

**receipts**
```
create or replace table `jg-fetch-coding-exercise`.analytics.receipts
as

with stg_receipts as (
  select * from `jg-fetch-coding-exercise`.staging.stg_receipts
)

, final as (
  select
    -- extract nested array of receipt lines to its own model
    * except(receipt_line_items)
  from stg_receipts
)

select * from final
```

**receipt_items**
```
create or replace table `jg-fetch-coding-exercise`.analytics.receipt_items
as

with stg_receipts as (
  select * from `jg-fetch-coding-exercise`.staging.stg_receipts
)

, unnested as (
  select
      stg_receipts.receipt_id

      -- lets add a row number for surrogate key generation and debugging
      -- don't think we can count on item_number or unique barcodes (not sure if receipts automatically group multiple purchases together)
      -- nothing looks particularly sortable, so lets use item_number for some sense of idempotency
      , row_number() over(partition by stg_receipts.receipt_id order by line_item.item_number asc) as receipt_row_number
    
      -- item attributes
    , line_item.item_number
    , line_item.item_description
    , line_item.original_receipt_item_text
    , line_item.item_barcode
    , line_item.item_brand_code
    
      -- item amounts
    , line_item.quantity_purchased
    , line_item.item_price
    , line_item.target_price_usd
    , line_item.after_coupon_price_usd
    , line_item.discounted_item_price_usd
    , line_item.final_price_usd
    , line_item.original_final_price_usd

    -- misc
    , line_item.points_earned
    , line_item.points_payer_id
    , line_item.points_not_awarded_reason
    , line_item.partner_item_id
    , line_item.rewards_group
    , line_item.rewards_product_partner_id
    , line_item.competitor_rewards_group
    , line_item.is_competitive_product
    , line_item.is_prevent_target_gap_points


    -- user flags
    , line_item.has_user_flagged_price  
    , line_item.user_flagged_price           
    , line_item.has_user_flagged_quantity
    , line_item.user_flagged_quantity          
    , line_item.has_user_flagged_description
    , line_item.user_flagged_description          
    , line_item.has_user_flagged_barcode
    , line_item.user_flagged_barcode
    , line_item.has_user_flagged_new_item

    -- metabrite
    , line_item.metabrite_campaign_id
    , line_item.metabrite_original_barcode
    , line_item.metabrite_original_description
    , line_item.metabrite_original_item_price
    , line_item.metabrite_original_quantity_purchased

    -- -- fetch flags
    , line_item.needs_fetch_review
    , line_item.needs_fetch_review_reason
    , line_item.is_deleted
  from stg_receipts
  left join unnest(receipt_line_items) line_item
)

select 
  -- simple surrogate key
    md5( cast(receipt_id as string) || cast(receipt_row_number as string) ) as receipt_item_sk
  , * 
from unnested

```
