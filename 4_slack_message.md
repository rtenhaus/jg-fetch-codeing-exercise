Hi @stakeholder, I was able to review the data set you shared earlier.  In its present state, I unfortunately don't think it is a suitable basis for analysis.
* While the provided receipt file has 1.1K receipt records, only 36% are associated with Fetch customers (51% fetch-staff and 13% with no matching user)
* Of that limited set of consumer receipts, 20% have at least one receipt line item with barcode or brand_code issues and 15% have at least one receipt line item with invalid/missing item prices or purchase quantities. This will make classification less reliable and widen your confidence interval for aggregated item-level metrics.
* Finally, receipt dates/timestamps are missing for a large percent of the receipts, limiting longitudinal slices.
* There are a few other lesser data quality issues on top of the major issues above (happy to share if needed).

I'd be happy to walk you through the data if needed, or you can check out the `receipts`, `receipt_items`, `users`, and `brands` tables in the `analytics` warehouse.

In the meantime, I will reach out to engineering/ops with a few of the issues to see if there are any quick/reasonable changes to improve the data quality.  Some of my initial thoughts are:
* I want to double-check our scan acceptance criteria. If we are accepting incorrect and/or impartial receipts it isn't good for Fetch nor our customers.
* I will bring the missing user IDs and creation timestamps to the product engineering team; those are generated on our backend so they really shouldn't be missing.
* Lastly, need to talk to ops about our product/brand database.  There were both duplicate and missing bar/brand codes, so want to scope out how often and how easy it is for them to update.  I could probably a quick report, sorted by potential impact, that they could use in their database update workflow. 

Let's discuss this more at our upcoming sync.

Thx,  
Joe
