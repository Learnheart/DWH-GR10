from google.cloud import bigquery
from sklearn.cluster import KMeans
import pandas as pd

def cluster_model(project_id, dataset_id, table_id, num_clusters):
    client = bigquery.Client(project=project_id)
    
    # Query the table
    query = f"""
        SELECT city_id, frequency, total_price
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE frequency IS NOT NULL AND total_price IS NOT NULL
    """
    results = client.query(query).result()
    
    # Load data into a Pandas DataFrame
    data = pd.DataFrame([dict(row) for row in results])
    
    if data.empty:
        raise ValueError("The query returned no data. Ensure the table contains valid rows.")
    
    # Extract features for clustering
    features = data[['frequency', 'total_price']].values
    
    # Fit the K-Means model
    kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)  # Explicit `n_init` for scikit-learn compatibility
    data['result_cluster'] = kmeans.fit_predict(features)
    
    # Prepare the updated table with clusters
    update_data = data[['city_id', 'result_cluster']]
    
    # Create a temporary table to hold the updated data
    temp_table_id = f"{dataset_id}.temp_clusters"
    client.delete_table(temp_table_id, not_found_ok=True)  # Ensure no conflicts
    client.load_table_from_dataframe(update_data, temp_table_id).result()
    
    # Merge the clusters back into the main table
    merge_query = f"""
        MERGE `{project_id}.{dataset_id}.{table_id}` AS target
        USING `{project_id}.{temp_table_id}` AS source
        ON target.city_id = source.city_id
        WHEN MATCHED THEN
            UPDATE SET target.result_cluster = source.result_cluster
    """
    client.query(merge_query).result()
    
    # Clean up the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)
    
    print("Clustering complete. Column 'result_cluster' updated successfully.")