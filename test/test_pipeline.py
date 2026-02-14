from pyspark.sql.functions import col

def test_salary_spread(df):

    assert df.filter(col("Salary_Spread") < 0).count() == 0
