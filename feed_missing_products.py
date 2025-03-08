import pandas as pd
import requests
import os
import time
from urllib.parse import urlparse
from typing import List, Dict, Set

def read_file(file_name: str)-> pd.DataFrame :
    df = pd.read_csv(file_name, low_memory=False)
    return df

def process_sku(sku:str) -> str:
    return sku.replace(".0", "")

def process_data(rex_df: pd.DataFrame, feed_df: pd.DataFrame)-> pd.DataFrame: 
    
    missing_barcodes = set(feed_df["gtin"].astype(str).apply(process_sku))
    
    product_groups = rex_df.groupby("ManufacturerSKU")

    valid_product_codes = set()
    
    for product_code, group in product_groups:
        barcodes = set(group["SupplierSKU"].astype(str))
        
        is_missing = any(sku in missing_barcodes for sku in barcodes)
        
        if is_missing:
            valid_product_codes.add(product_code)

    result_df = rex_df[rex_df["ManufacturerSKU"].isin(valid_product_codes)]
    
    return result_df

def download_images(feed_df: pd.DataFrame, matrixify_df: pd.DataFrame, rex_df: pd.DataFrame):

    gtin_col = 'gtin'
    rex_color_column = "Colour"

    base_dir = "product_images"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    
    print(feed_df[gtin_col].head(5))
    feed_df = feed_df.dropna(subset=[gtin_col])
    
    feed_df[gtin_col] = feed_df[gtin_col].astype(str).apply(lambda sku: sku.replace(".0", ""))
    print(feed_df[gtin_col].head(5))
    matrixify_df['Variant Barcode'] = matrixify_df['Variant Barcode'].fillna('').astype(str).str.strip()
    rex_df['SupplierSKU'] = rex_df['SupplierSKU'].fillna('').astype(str).str.strip()
    
    barcode_to_products = {}
    for _, row in matrixify_df.iterrows():
        barcode = row['Variant Barcode']
        if barcode and barcode.lower() != 'nan':
            if barcode not in barcode_to_products:
                barcode_to_products[barcode] = []
            barcode_to_products[barcode].append(row['ID'])
    
    product_to_images = {}
    for product_id, group in matrixify_df.groupby('ID'):
        image_urls = []
        for _, row in group.iterrows():
            if isinstance(row.get('Image Src'), str) and row['Image Src'].strip():
                image_urls.append(row['Image Src'].strip())
        
        if image_urls:
            product_to_images[product_id] = list(dict.fromkeys(image_urls))
    

    total_gtins = len(feed_df)
    processed_gtins = 0
    downloaded_images = 0
    products_with_images = 0
    
    print(f"Processing {total_gtins} GTINs from feed file")
    
    for idx, row in feed_df.iterrows():
        gtin = row[gtin_col]
        
        if not gtin or gtin.lower() == 'nan':
            continue
        
        processed_gtins += 1
        
        if processed_gtins % 100 == 0:
            print(f"Progress: Processed {processed_gtins}/{total_gtins} GTINs")
        
        product_ids = barcode_to_products.get(gtin, [])
        
        if not product_ids:
            continue
        
        matches = rex_df[rex_df['SupplierSKU'] == gtin]
        
        if matches.empty:
            print(f"No manufacturer SKU found for GTIN {gtin}")
            continue
        
        match = matches.iloc[0]
        manufacturer_sku = match['ManufacturerSKU']
        

        color = "unknown"
        if pd.notna(match[rex_color_column]):
            color = str(match[rex_color_column]).strip()
            color = "".join(c if c.isalnum() or c in [' ', '-', '_'] else '_' for c in color)
            color = color.replace(' ', '_')
        
        folder_name = str(manufacturer_sku)
        product_dir = os.path.join(base_dir, folder_name)
        if not os.path.exists(product_dir):
            os.makedirs(product_dir)
        
        for product_id in product_ids:
            image_urls = product_to_images.get(product_id, [])
            
            if not image_urls:
                continue
            
            products_with_images += 1
            print(f"\nProcessing GTIN {gtin}, manufacturer SKU: {manufacturer_sku}, product ID: {product_id}")
            print(f"Found {len(image_urls)} image URLs")
            
            for i, image_url in enumerate(image_urls):
                img_filename = os.path.basename(urlparse(image_url).path)
                file_ext = os.path.splitext(img_filename)[1]
                if not file_ext:
                    file_ext = ".jpg"
                
                new_filename = f"{manufacturer_sku}-{color}-{i+1}{file_ext}"
                save_path = os.path.join(product_dir, new_filename)
                
                if os.path.exists(save_path):
                    print(f"Image already exists: {save_path}")
                    continue
                
                print(f"Downloading image {i+1}/{len(image_urls)}: {image_url}")
                if download_image(image_url, save_path):
                    print(f"Saved to {save_path}")
                    downloaded_images += 1
                
                time.sleep(0.5)
    
    print(f"\nProcessed {processed_gtins}/{total_gtins} GTINs")
    print(f"Found {products_with_images} products with images")
    print(f"Downloaded {downloaded_images} images")
    print("Image download complete")

def download_image(image_url: str, save_path: str) -> bool:
    
    if not image_url or not image_url.startswith(('http://', 'https://')):
        print(f"Invalid image URL: {image_url}")
        return False
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(image_url, headers=headers, stream=True, timeout=10)
        
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            return True
        else:
            print(f"Failed to download image, status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error downloading image: {e}")
        return False



if __name__ == "__main__":
    
    FEED_FILE = "inputData/feed.csv"
    REX_FILE = "inputData/rex.csv"
    MATRIXIFY_FILE = "inputData/matrixify.csv"
    feed_df = read_file(FEED_FILE)
    rex_df = read_file(REX_FILE)
    matrixify_df = read_file(MATRIXIFY_FILE)
    
    download_images(feed_df=feed_df, rex_df=rex_df, matrixify_df=matrixify_df)
    print('Exiting...')