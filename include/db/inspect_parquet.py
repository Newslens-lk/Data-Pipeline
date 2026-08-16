import pandas as pd

df = pd.read_parquet("/home/sychpra/dev/projects/DSE-Project/newslens_pipeline/include/db/dataset.parquet")
print("Shape:", df.shape)
print("\nColumns:", df.columns.tolist())
print("\nDtypes:")
print(df.dtypes)

print("\nFirst row:")
row = df.iloc[0]
for col in df.columns:
    if col == "embedding":
        val = row[col]
        print(f"  embedding: type={type(val).__name__}, len={len(val)}, first3={val[:3]}")
    elif col in ("body_text", "text"):
        print(f"  {col}: {str(row[col])[:100]}...")
    else:
        print(f"  {col}: {row[col]}")

print("\nUnique publishers:", df["publisher"].unique().tolist())
print("Unique bias labels:", df["bias_label"].unique().tolist())
print("Cluster labels (unique count):", df["cluster_id"].nunique())
