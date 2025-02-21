import pandas as pd

def read_file(file_path: str)-> pd.DataFrame:
    df = pd.read_csv(file_path, low_memory=False)
    return df

def update_inventory(matrixify_new: pd.DataFrame, filter_df: pd.DataFrame, rex_df: pd.DataFrame)-> tuple[pd.DataFrame, set]:
    updated_df = matrixify_new.copy()
    
    filter_barcodes = set(filter_df["Variant SKU"].astype("str"))
    grouped = matrixify_new.groupby("ID")
    
    updated_count = 0
    skipped_count = 0
    
    # Set to track updated SKUs
    updated_skus = set()
    
    for product_id, group in grouped:
        try:
            first_variant = "'" + str(group["Variant SKU"].iloc[0])
            
            if first_variant in filter_barcodes:
                product_skus = group["Variant SKU"].astype(str)
                
                for sku in product_skus:
                    try:
                        # Create masks for matrixify_new and rex_df
                        new_mask = updated_df["Variant SKU"] == sku
                        rex_mask = rex_df["Supplier_product_id"] == sku
                        
                        if new_mask.any() and rex_mask.any():
                            rex_qty = rex_df.loc[rex_mask, "qtyAvailable"].iloc[0]
                            
                            updated_df.loc[new_mask, "Variant Inventory Qty"] = rex_qty

                            updated_skus.add(sku)
                            print(f"Updated product ID {product_id} SKU {sku} with quantity {rex_qty}")
                        else:
                            print(f"Skipping SKU {sku} - not found in both matrixify and rex files")
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
    print(f"Skipped {skipped_count} SKUs due to missing data or errors")
    return updated_df, updated_skus
    


if __name__ == "__main__":
    MATRIXIFY_NEW = 'inputData/matrixify_new.csv'
    FILTER_FILE = 'inputData/test_upload.csv'
    REX_FILE = 'inputData/rex_inventory.csv'
    
    matrixify_new_df = read_file(MATRIXIFY_NEW)
    filter_df = read_file(FILTER_FILE)
    rex_df = read_file(REX_FILE)
    
    result_df, updated_skus = update_inventory(matrixify_new_df, filter_df, rex_df)
    
    # Create a new dataframe with only the updated products
    updated_products_df = result_df[result_df['Variant SKU'].isin(updated_skus)]
    
    # Save only the updated products
    output_path = 'output/updated_products_rex_inventory.csv'
    updated_products_df.to_csv(output_path, index=False)
    
    print(f"Updated products saved to: {output_path}")
    print('Exiting....')
    