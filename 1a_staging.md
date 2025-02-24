This staging script extracts, casts, renames, and generally organizes the raw files into `stg_` tables.
* Raw json was loaded to BigQuery as a single json column
* Staging models extract, rename (snake_case), cast, and otherwise organize into a structured format
____________________________________________________________________________________________________________

**stg_users**

```
create or replace table `jg-fetch-coding-exercise`.staging.stg_users
as

with input as (
  select json_col from  `jg-fetch-coding-exercise`.raw.users
)

, extracted as (
  select
    -- user atrributes
      string(json_col.`_id`.`$oid`) as user_id
    , string(json_col.`role`) as user_role
    , coalesce(bool(json_col.`active`), false) as is_active

    -- user dates/timestamps
    , timestamp_millis(int64(json_col.`createdDate`.`$date`)) as created_ts_utc
    , timestamp_millis(int64(json_col.`lastLogin`.`$date`)) as last_login_ts_utc

    -- user misc
    , string(json_col.`state`) as state
    , string(json_col.`signUpSource`) as sign_up_source

    -- keep original
    , json_col as orig_input

  from input
)

select * from extracted
```


**stg_brands**
```
create or replace table `jg-fetch-coding-exercise`.staging.stg_brands
as

with input as (
  select json_col from  `jg-fetch-coding-exercise`.raw.brands
)

, extracted as (
  select
    -- brand attributes
      string(json_col.`_id`.`$oid`) as brand_id
    , string(json_col.`name`) as brand_name
    , string(json_col.`brandCode`) as brand_code
    , string(json_col.`barcode`) as barcode

    -- category
    , string(json_col.`category`) as category
    , string(json_col.`categoryCode`) as category_code

    -- misc
    , coalesce(bool(json_col.`topBrand`), false) as is_top_brand
    , string(json_col.`cpg`.`$id`.`$oid`) as cpg_id
    , string(json_col.`cpg`.`$ref`) as cpg_ref

    -- keep original
    , json_col as orig_input

  from input
)

select * from extracted
```

**stg_receipts**
```
create or replace table `jg-fetch-coding-exercise`.staging.stg_receipts
as

with input as (
  select json_col from `jg-fetch-coding-exercise`.raw.receipts
)

, base_extract as (
  select
    -- receipt attributes
      string(json_col.`_id`.`$oid`) as receipt_id
    , string(json_col.`rewardsReceiptStatus`) as rewards_receipt_status
    , string(json_col.`userId`) as user_id

    -- receipt amounts
    , int64(json_col.`purchasedItemCount`) as purchased_item_count
    , cast(coalesce(string(coalesce(json_col.`totalSpent`, to_json("0.0")))) as float64) as total_spent

    -- receipt dates/timestamps
    , timestamp_millis(int64(json_col.`createdDate`.`$date`)) as created_ts_utc
    , timestamp_millis(int64(json_col.`purchaseDate`.`$date`)) as purchase_ts_utc
    , timestamp_millis(int64(json_col.`dateScanned`.`$date`)) as scanned_ts_utc
    , timestamp_millis(int64(json_col.`finishedDate`.`$date`)) as finished_ts_utc
    , timestamp_millis(int64(json_col.`modifyDate`.`$date`)) as modified_ts_utc

    -- points
    , cast(coalesce(string(coalesce(json_col.`pointsEarned`, to_json("0.0")))) as float64) as points_earned
    , timestamp_millis(int64(json_col.`pointsAwardedDate`.`$date`)) as points_awarded_ts_utc

    -- bonus points
    , int64(json_col.`bonusPointsEarned`) as bonus_points_earned
    , string(json_col.`bonusPointsEarnedReason`) as bonus_points_earned_reason

    -- item list
    , json_query_array(json_col, '$.rewardsReceiptItemList') as rewards_receipt_item_list

    -- keep original
    , json_col as orig_input

  from input
  -- some empty rows
  where json_col.`_id`.`$oid` is not null
)

, receipt_items_extract as (
  select
      base_extract.receipt_id
    , array_agg(
        struct(
           -- item attributes
            string(line_item.itemNumber) as item_number
          , string(line_item.description) as item_description
          , string(line_item.originalReceiptItemText) as original_receipt_item_text
          , string(line_item.barcode) as item_barcode
          , string(line_item.brandCode) as item_brand_code
         
           -- item amounts
          , int64(line_item.quantityPurchased) as quantity_purchased
          , cast(string(line_item.itemPrice) as float64) as item_price
          , cast(string(line_item.targetPrice) as float64) as target_price_usd
          , cast(string(line_item.priceAfterCoupon) as float64) as after_coupon_price_usd
          , cast(string(line_item.discountedItemPrice) as float64) as discounted_item_price_usd
          , cast(string(line_item.finalPrice) as float64) as final_price_usd
          , cast(string(line_item.originalFinalPrice) as float64) as original_final_price_usd

          -- misc
          , cast(string(line_item.pointsEarned) as float64) as points_earned
          , string(line_item.pointsPayerId) as points_payer_id
          , string(line_item.pointsNotAwardedReason) as points_not_awarded_reason
          , string(line_item.partnerItemId) as partner_item_id
          , string(line_item.rewardsGroup) as rewards_group
          , string(line_item.rewardsProductPartnerId) as rewards_product_partner_id
          , string(line_item.competitorRewardsGroup) as competitor_rewards_group
          , coalesce(bool(line_item.competitiveProduct), false) as is_competitive_product
          , coalesce(bool(line_item.preventTargetGapPoints), false) as is_prevent_target_gap_points


          -- user flags
          , cast(string(line_item.userFlaggedPrice) as float64) is not null as has_user_flagged_price  
          , cast(string(line_item.userFlaggedPrice) as float64) as user_flagged_price           
          , int64(line_item.userFlaggedQuantity) is not null as has_user_flagged_quantity
          , int64(line_item.userFlaggedQuantity) as user_flagged_quantity          
          , string(line_item.userFlaggedDescription) is not null as has_user_flagged_description
          , string(line_item.userFlaggedDescription) as user_flagged_description          
          , string(line_item.userFlaggedBarcode) is null as has_user_flagged_barcode
          , string(line_item.userFlaggedBarcode) as user_flagged_barcode
          , coalesce(bool(line_item.userFlaggedNewItem),false) as has_user_flagged_new_item

          -- metabrite
          , string(line_item.metaBriteCampaignId) as metabrite_campaign_id
          , string(line_item.originalMetaBriteBarcode) as metabrite_original_barcode
          , string(line_item.originalMetaBriteDescription) as metabrite_original_description
          , cast(string(line_item.originalMetaBriteItemPrice) as float64) as metabrite_original_item_price
          , int64(line_item.originalMetaBriteQuantityPurchased) as metabrite_original_quantity_purchased

          -- fetch flags
          , coalesce(bool(line_item.needsFetchReview), false) as needs_fetch_review
          , string(line_item.needsFetchReviewReason) as needs_fetch_review_reason
          , coalesce(bool(line_item.deleted), false) as is_deleted

        )
    ) as receipt_line_items
  
  from base_extract
  left join unnest(rewards_receipt_item_list) as line_item
  group by 1
)

, joined as (
  select
      base_extract.* except(rewards_receipt_item_list, orig_input)
    , receipt_items_extract.receipt_line_items
    
    -- include original json
    , base_extract.orig_input

  from base_extract
  inner join receipt_items_extract using(receipt_id)
)

select * from joined
```
