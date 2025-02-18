import pandas as pd

def read_file(file_name: str)-> pd.DataFrame :
    df = pd.read_csv(file_name, low_memory=False)
    return df


def process_data(rex_df: pd.DataFrame, sportitude_df: pd.DataFrame, stock_df: pd.DataFrame)-> pd.DataFrame:
    
    ignore_product_name = ['quantum', 'sale', 'champion', 'zoggs', 'afl', 'patrick', "port adelaide", "pafc", "basketball", "jersey", "jsy", "jordan", "crocs", "futura"]
    ignore_product_type = ['ftwr', "shoes", "balls", "gloves", "football", "soccer"]  
    
    barcodes_with_stock = set(stock_df[stock_df['Qty Avail'] > 0]['SKU'].astype(str))
    barcodes_in_website = set(sportitude_df["SKU Code"].astype(str))
    
    product_groups = rex_df.groupby("ManufacturerSKU")
    
    valid_product_codes = set()
    
    for product_code, group in product_groups:
        barcodes = set(group["SupplierSKU"].astype(str))
        
        has_stock = any(barcode in barcodes_with_stock for barcode in barcodes)
        
        not_in_website = all(barcode not in barcodes_in_website for barcode in barcodes)
        if has_stock and not_in_website:
            valid_product_codes.add(product_code)

    
    result_df = rex_df[rex_df["ManufacturerSKU"].isin(valid_product_codes)]
    for keyword in ignore_product_name:
        result_df = result_df[~result_df['ShortDescription'].str.lower().str.contains(keyword.lower(), na=False)]
    
    for keyword in ignore_product_type:
        result_df = result_df[~result_df['ProductType'].str.lower().str.contains(keyword.lower(), na=False)]
        
    return result_df


if __name__ == "__main__":
    
    STOCK_FILE = "inputData/stock_data.csv"
    SPORTIDUE_FILE = "inputData/sportitude.csv"
    REX_FILE = "inputData/rex_all_data.csv"
    stock_df = read_file(STOCK_FILE)
    sportitude_df = read_file(SPORTIDUE_FILE)
    rex_df = read_file(REX_FILE)
    
    result_df = process_data(rex_df=rex_df, sportitude_df=sportitude_df, stock_df=stock_df)
    
    result_df.to_csv('result.csv', index=False)
    print(f'Processing complete, result file includes {len(result_df)} rows.') 
    print('Exiting...')