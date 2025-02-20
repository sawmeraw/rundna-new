import pandas as pd

def read_file(file_path: str)-> pd.DataFrame:
    df = pd.read_csv(file_path, low_memory=False)
    return df

def update_inventory(matrixify_old_df: pd.DataFrame, 
                    matrixify_new_df: pd.DataFrame, 
                    filter_file: pd.DataFrame) -> tuple[pd.DataFrame, set]:
    updated_df = matrixify_new_df.copy()
    
    grouped = matrixify_old_df.groupby('ID')
    
    filter_skus = set(filter_file['Variant SKU'].astype(str))
    
    updated_count = 0
    skipped_count = 0
    
    # Set to store updated SKUs
    updated_skus = set()
    
    for product_id, group in grouped:
        try:
            first_variant_sku = "'" + str(group['Variant SKU'].iloc[0])
            
            if first_variant_sku in filter_skus:
                product_skus = group['Variant SKU'].astype(str)
                
                for sku in product_skus:
                    try:
                        old_mask = matrixify_old_df['Variant SKU'] == sku
                        new_mask = updated_df['Variant SKU'] == sku
                        
                        if old_mask.any() and new_mask.any():
                            old_qty = matrixify_old_df.loc[old_mask, 'Variant Inventory Qty'].iloc[0]
                            updated_df.loc[new_mask, 'Variant Inventory Qty'] = old_qty
                            # Add updated SKU to our set
                            updated_skus.add(sku)
                            print(f'Updated id {product_id} with sku {sku}')
                        else:
                            print(f"Skipping SKU {sku} - not found in both old and new files")
                            skipped_count += 1
                            continue
                            
                    except Exception as e:
                        print(f"Error processing SKU {sku}: {str(e)}")
                        skipped_count += 1
                        continue
                        
                updated_count += 1
                
        except Exception as e:
            print(f"Error processing product ID {product_id}: {str(e)}")
            continue
    
    print(f"Updated inventory for {updated_count} products")
    return updated_df, updated_skus

if __name__ == "__main__":
    MATRIXIFY_OLD = 'inputData/matrixify.csv'
    MATRIXIFY_NEW = 'inputData/matrixify_new.csv'
    FILTER_FILE = 'inputData/test_upload.csv'
    
    matrixify_old_df = read_file(MATRIXIFY_OLD)
    matrixify_new_df = read_file(MATRIXIFY_NEW)
    filter_file = read_file(FILTER_FILE)
    
    result_df, updated_skus = update_inventory(matrixify_old_df, matrixify_new_df, filter_file)
    
    updated_products_df = result_df[result_df['Variant SKU'].isin(updated_skus)]
    
    output_path = 'output/updated_matrixify.csv'
    updated_only_path = 'output/updated_products_only.csv'
    
    result_df.to_csv(output_path, index=False)
    updated_products_df.to_csv(updated_only_path, index=False)
    
    print(f"Full updated inventory saved to: {output_path}")
    print(f"Only updated products saved to: {updated_only_path}")
    print('Exiting....')