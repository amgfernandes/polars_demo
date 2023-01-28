#%%
import polars as pl
import seaborn as sns

# to enrich the examples in this quickstart with dates
from datetime import datetime, timedelta 
# to generate data for the examples
import numpy as np 
# %%
df = pl.read_csv("https://j.mp/iriscsv")
print(df.filter(pl.col("sepal_length") > 5)
      .groupby("species", maintain_order=True)
      .agg(pl.all().sum())
)
#%%
df = (pl.read_parquet('https://raw.githubusercontent.com/RobinL/iris_parquet/main/gridwatch/gridwatch_2023-01-08.parquet')
    .lazy()
)
# %%
print(df.collect().shape)
df.collect().head()
# %%lazy load
df = pl.read_csv("https://j.mp/iriscsv").lazy()
print(df.filter(pl.col("sepal_length") > 5)
      .groupby("species", maintain_order=True)
      .agg(pl.all().sum())
)
# %%
df.collect().head(10)
# %% read parquet
df.collect().write_csv('output.csv')
# %%
df.collect().write_parquet('output.parquet')
# %%
pd_df=df.collect().to_pandas()
sns.displot(data=pd_df)
# %%

df= pl.from_pandas(pd_df).lazy()
print(df.fetch(5))
# %%
df.select(
      pl.col(['species']),
      ).collect()
# %%
df.collect().groupby(["species"], maintain_order=True).mean()
# %%
df2=df.collect()
df2.groupby("species", maintain_order=True).agg([
    pl.col("sepal_length").count().alias("count"),
    pl.col("sepal_length").sum().alias("sum")
])
# %%
df = pl.DataFrame({"a": np.arange(0, 10), 
                   "b": np.random.rand(10), 
                   "c": [datetime(2022, 12, 1) + timedelta(days=idx) for idx in range(10)],
                   "d": [1, 2.0, np.NaN, np.NaN, 0, -5, -42, None, 12,30]
                  })
print(df)
df2 = pl.DataFrame({
                    "x": np.arange(0, 12), 
                    "y": ['A', 'A', 'A', 'B', 'B', 'C', 'X', 'X', 'Z','Z','Z','Z'],
})
print(df2)
# %%
df3=df.join(df2, left_on="a", right_on="x", how="inner")
df3
# %%
pl.concat([df,df2], how="horizontal")
# %%
df3.select(pl.col("*")).filter(pl.col(['y']).str.contains("A|B"))
# %%
