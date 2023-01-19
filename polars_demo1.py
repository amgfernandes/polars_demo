#%%
import polars as pl
# %%
df = pl.read_csv("https://j.mp/iriscsv")
print(df.filter(pl.col("sepal_length") > 5)
      .groupby("species", maintain_order=True)
      .agg(pl.all().sum())
)

# %%lazy load


df = pl.read_csv("https://j.mp/iriscsv").lazy()
print(df.filter(pl.col("sepal_length") > 5)
      .groupby("species", maintain_order=True)
      .agg(pl.all().sum())
)
# %%
df.collect()
# %%

df = (pl.read_parquet('https://raw.githubusercontent.com/RobinL/iris_parquet/main/gridwatch/gridwatch_2023-01-08.parquet')
    .lazy()
)
# %%
df= df.collect()
print(df.shape)
# %%
