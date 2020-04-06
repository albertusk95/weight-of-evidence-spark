# Weight of Evidence & Information Value

The implementation of Weight of Evidence (WOE) encoding and Information Value (IV). Lots of implementation out there, yet this repo offers one using PySpark for data processing.

## What you need

<ul>
  <li>Python >= 3.7.0</li>
  <li>PySpark >= 2.4.0</li>
</ul>

## Install

<ul>
  <li>Clone this repo</li>
  <li>Go to the root directory of the local repo</li>
  <li>Run <i>python setup.py install</i></li>
</ul>

## Quickstart

Please check the <a href="https://github.com/albertusk95/weight-of-evidence-spark/blob/master/woe_iv/main.py">main</a> module for the example.

### A) Prepare the data

```python
df = <spark_dataframe>
cols_to_woe = <list_of_categorical_columns_to_encode>
label_col = <label_column>
good_label = <good_label>
```

### B) WOE Encoding

```python
woe = WOE_IV(df, cols_to_woe, label_col, good_label)
woe.fit()

encoded_df = woe.transform(df)
```

### C) Information Value

```python
ivs = woe.compute_iv()
```
