import pandas as pd

def read_file(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    return df

def preprocess_text(text: str):
    return " ".join(str(text).split()).lower()
 


if __name__ == "__main__":

    web_df = read_file('./missing products.csv')
    rex_df = read_file('./rex.csv')

    rex_df['Custom1'] = rex_df['Custom1'].apply(preprocess_text)
    web_df['Product'] = web_df['Product'].apply(preprocess_text)

    web_df['Product_Stripped'] = web_df['Product'].apply(lambda x : x.split('-')[0].strip())

    result_rows = []

    for _, rex_row in rex_df.iterrows():
        custom1_value = rex_row['Custom1']

        for _, web_row in web_df.iterrows():
            product_stripped = web_row['Product_Stripped']
            
            if all(word in custom1_value for word in product_stripped.split()):

                if any(keyword in custom1_value for keyword in ["mens", "womens", "unisex"]):
                    result_rows.append(rex_row)
                    break  
    
    result_df = pd.DataFrame(result_rows, columns=rex_df.columns)

    result_df.to_csv('result_file.csv', index=False)

    print('Exiting...')