# Brooklyn Data Additional Information

Included are brooklyn_test and brooklyn_rolling to show verification process and possibility of extending
possible analysis further.

__Contents:__
- [Part II - Explanation - brooklyn_test](#part-ii---explanation---brooklyn_test)



## Part II - Explanation - brooklyn_test
- **Purpose**: brooklyn_test is added to make sure that category revenue and/or category product name 
               is validated because of missing order_id from product category, causing the nulls when
               joined by the order_purchase_date, which may cause counts of products or categories to
               be misaligned. 
- **Questions**: For verification, there may be a need to understand more about the upstream process 
                 for tables products and category to ensure that these are updated appropriately, and fully. 
- **Additionals**: There are definitely additional information for further analysis.  These can include 
                   finding time series data between purchased and delivered and comparing to the delivery
                   time, and also supporting demographic analysis because of availability of customer
                   data using zipcode and region using state and city data.
