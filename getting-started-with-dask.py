import numpy as np
import pandas as pd
import dask.dataframe as dd


def main()-> None : 
    data_dict = { "a" : np.arange(5000),
                 "b" : np.random.randn(5000),
                 "c" : np.random.choice(["a","b","c"],5000)
    }
    df = pd.DataFrame(data_dict)
   # print(df.head())

    ddf = dd.from_pandas(df,npartitions=10) # 10 pandas dataframe
  #  print(ddf.compute()) # only if we specify compute it will print
    print(ddf.partitions[0].compute())

    sum_df = ddf.groupby("c").sum()
    print(sum_df.compute())

    sum_df.visualize()

if __name__ == "__main__":
    main()
