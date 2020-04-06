# Weight of Evidence & Information Value

The implementation of Weight of Evidence (WOE) encoding and Information Value (IV). Lots of implementation out there, yet this repo offers one using PySpark for data processing.

## What you need

PySpark >= 2.4.0

## Quickstart

Please check the <a href="https://github.com/albertusk95/weight-of-evidence-spark/blob/master/woe_iv/main.py">main</a> module for the example.

### A) Fit the WOE

```python
df = <spark_dataframe>
cols_to_woe = <list_of_categorical_columns_to_encode>
label_col = <label_column>
good_label = <good_label>

woe = WOE_IV(df, cols_to_woe, label_col, good_label)
woe.fit()
```

### B) Transform

```python
woe_df = woe.transform(df)
```
