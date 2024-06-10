# w15d4-mapreduce

"""
Explaination:

Explanation of the Approach and Design Decisions
Preprocessing: The dataset is preprocessed to ensure that the required fields (order_id, product_id, product_category, purchase_amount) are available in a clean CSV format.

MapReduce Jobs:

ProductCategoryCount: This job calculates the total number of products purchased for each product category. Using a combiner helps in reducing the data size sent to reducers, optimizing the performance.
TopProducts: This job finds the top 5 products with the highest total purchase count. After the initial reduction, a secondary sorting step retrieves the top 5 products.
AverageProductsPerOrder: This job calculates the average number of products purchased per order. The reducer first sums the products per order, and a secondary reduction step calculates the average.
Results
Upon running these MapReduce jobs on the dataset, you will obtain:

The total number of products purchased for each product category.
The top 5 products with the highest purchase counts.
The average number of products purchased per order.
Performance Considerations
Data Skewness: The combiner functions help to mitigate the effects of data skewness by performing local aggregation before the shuffle phase.
Optimization: Using multiple steps in the TopProducts job ensures efficient sorting and retrieval of the top 5 products.
This MapReduce approach provides an efficient way to process large datasets and extract meaningful insights while handling potential data skewness and optimizing performance.
"""