from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

TRANSACTION_SCHEMA = StructType([
    StructField('transaction_id',    StringType(),  True),
    StructField('card_id',           StringType(),  True),
    StructField('timestamp',         StringType(),  True),
    StructField('amount',            DoubleType(),  True),
    StructField('merchant',          StringType(),  True),
    StructField('merchant_category', StringType(),  True),
    StructField('location_city',     StringType(),  True),
    StructField('location_country',  StringType(),  True),
    StructField('latitude',          DoubleType(),  True),
    StructField('longitude',         DoubleType(),  True),
    StructField('device',            StringType(),  True),
    StructField('is_online',         BooleanType(), True),
    StructField('card_present',      BooleanType(), True)
])
