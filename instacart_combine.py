import pandas as pd

# Load the datasets
order_products = pd.read_csv('order_products__prior.csv')
products = pd.read_csv('products.csv')

# Merge datasets to get product category information
merged_data = pd.merge(order_products, products, on='product_id')

# Extract the required fields and rename them
preprocessed_data = merged_data[['order_id', 'product_id', 'product_name', 'add_to_cart_order']]
preprocessed_data.columns = ['order_id', 'product_id', 'product_category', 'purchase_amount']

# Save the preprocessed data to a new CSV file
preprocessed_data.to_csv('preprocessed_instacart_data.csv', index=False)

print("Preprocessing complete. The preprocessed data is saved as 'preprocessed_instacart_data.csv'.")