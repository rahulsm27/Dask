import dask.dataframe as dd


#ddf = dd.read_csv("twitter_parsed_dataset.csv")
#print(ddf.head()) 

# forces to compute
# gives error # dask dataframe expects very column to have a single dtype

ddf = dd.read_csv("twitter_parsed_dataset.csv",
                  dtype={
                      "id" : str,
                      "index" : str ,
                      "oh_label" : float
                  }
            
                  
                  )
print(ddf.head()) 
print(f"{ddf.npartitions}")
ddf = ddf.repartition(npartitions=10)
# storing as parquet
ddf.to_parquet("twitter_parsed_datset.parquet ")