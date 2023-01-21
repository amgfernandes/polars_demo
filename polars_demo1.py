#%%
import polars as pl
import seaborn as sns
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
