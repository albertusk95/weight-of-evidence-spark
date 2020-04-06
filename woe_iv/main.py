from pyspark.sql import SparkSession

from woe_iv.woe import WOE_IV


if __name__ == '__main__':
    spark = SparkSession.builder.appName('woe-encoding').getOrCreate()

    df = spark.createDataFrame([('AX', 'BX', 0), ('AX', 'BY', 0), ('AY', 'BY', 1), ('AY', 'BX', 1), ('AX', 'BX', 1),
                                ('AY', 'BY', 0), ('AX', 'BX', 1), ('AY', 'BY', 1), ('AY', 'BX', 0), ('AY', 'BY', 0)],
                               ['col_a', 'col_b', 'label'])

    cols_to_woe = ['col_a', 'col_b']
    woe = WOE_IV(df, cols_to_woe, 'label', 1)
    woe.fit()

    print(woe.fit_data)

    woe_df = woe.transform(df)
    woe_df.show()
