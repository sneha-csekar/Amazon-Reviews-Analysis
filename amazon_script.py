# -*- coding: utf-8 -*-
"""
Created on Mon Jan  7 21:38:32 2022

@author: sneha
"""

# Importing libraries
import pandas as pd
import json
import gzip
from datetime import datetime
import os


def parse(path):
  g = gzip.open(path, 'r')
  for l in g:
    yield json.loads(l)
    
    
def getDF(path):
  i = 0
  df = {}
  for d in parse(path):
    df[i] = d
    i += 1
  return pd.DataFrame.from_dict(df, orient='index')


def cleanDFReviews(df):    
    for i,row in df.iterrows():
        # print(i)
        df.loc[i,'unixReviewTime_dt']  = datetime.fromtimestamp(row['unixReviewTime']).date()
        df.loc[i,'reviewTime_dt'] = datetime.strptime(row['reviewTime'].replace(',',''),"%m %d %Y").date()
    return df

        
def cleanDFMeta(df):
    for i,row in df.iterrows():
        if len(row['price'])>0:
            if row['price'][0]=="$":
                df.loc[i,'price_val'] = row['price'].strip("$")
            else:
                df.loc[i,'price_val'] = float("nan")
        else:
            df.loc[i,'price_val'] = float("nan")
            
        if type(row['rank'])==str:
            df.loc[i,'rank_val'] = int(row['rank'].split(" ")[0].replace(',',''))
        else:
            df.loc[i,'rank_val'] = float("nan")

        try:
            datetime.strptime(row['date'],"%B %d, %Y")
            df.loc[i,'date_dt'] = datetime.strptime(row['date'],"%B %d, %Y").date()
        except:
            df.loc[i,'date_dt'] = ''
    df.drop(columns = 'similar_item', inplace=True)
    
    index_list = []
    # Removing rows having cells of length greater than 32767 (max allowed in csv)
    for i,row in df.iterrows():
        row_list = row.to_list()
        # print(row_list)
        for j in range(0,len(row_list)):
            if len(str(row[j]))>32767:
                index_list.append(i)
                break

    df.drop(index_list,axis=0, inplace=True)
    df.reset_index(drop = True, inplace=True)           
    return df
    
            

def main():
    path = os.getcwd()
    
    metadata_path = 'zip files\\meta_All_Beauty_json.gz'
    reviews_path = 'zip files\\Reviews_All_Beauty_json.gz'
    
    metadata_gz_path = os.path.join(path,metadata_path)
    reviews_gz_path = os.path.join(path,reviews_path)
    
    df_metadata = getDF(metadata_gz_path)
    df_metadata_cln = cleanDFMeta(df_metadata)
    df_metadata_cln.to_csv(r'E:\Projects\Amazon reviews\UCSD\Data files\All beauty\Cleaned\All_beauty_metadata.csv')

    df_reviews = getDF(reviews_gz_path)
    df_reviews_cln = cleanDFReviews(df_reviews)
    df_reviews_cln.to_csv(r'E:\Projects\Amazon reviews\UCSD\Data files\All beauty\Cleaned\All_beauty_reviews.csv')

    
if __name__ == "__main__":
    main()   
    
