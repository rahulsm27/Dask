import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask

from dask.distributed import Client

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

    sum_df.visualize() # creates mydask.png file to visualize


    # using dask delayed

    @dask.delayed
    def increment(x:int) -> int:
        return x +1
    
    @dask.delayed
    def add(x,y):
        return x+y
    
    a = increment(1) # it will not compute
    b = increment(2) # it will not compute
    c = add(a,b) # it will not compute

    c.visualize()
    c = c.compute() # computation will happen here
    print(c)

    # DISTRIBUTED SCHEDULER



if __name__ == "__main__":
    client = Client()

    main()
