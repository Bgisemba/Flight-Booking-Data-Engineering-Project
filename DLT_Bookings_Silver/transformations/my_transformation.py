from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp
##########################################################################################
#bookings data
@dp.table(
    name="stage_bookings"
)
def stage_bookings():

    df=spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data")
    return df



@dp.view(
    name="trans_bookings"
)
def trans_bookings():

    df=spark.readStream.table("stage_bookings")
    df=df.withColumn("amount",col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate",current_timestamp())\
        .withColumn("booking_date",to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df

rules={
    "rule1":"booking_id IS NOT NULL",
    "rule2":"passenger_id IS NOT NULL",
}
 

@dp.table(
    name="silver_bookings"
)
@dp.expect_all(rules)
def silver_bookings():

    df=spark.readStream.table("trans_bookings")
    return df

#######################################################################################
#flights data
@dp.view(
    name="trans_flights"
)
def trans_flights():

    df=spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data")
    df=df.drop("_rescued_data")\
        .withColumn("modifiedDate",current_timestamp())
        
    return df



dp.create_streaming_table("silver_flights")

dp.create_auto_cdc_flow(
  target = "silver_flights",
  source = "trans_flights",
  keys = ["flight_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1
)
###############################################################################
#passengers data
@dp.view(
    name="trans_passengers"
)
def trans_passengers():

    df=spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/passengers/data")
    df=df.drop("_rescued_data")\
        .withColumn("modifiedDate",current_timestamp())

    return df

dp.create_streaming_table("silver_passengers")

dp.create_auto_cdc_flow(
  target = "silver_passengers",
  source = "trans_passengers",
  keys = ["passenger_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1
)

##################################################################################
#airport data

@dp.view(
    name="trans_airports"
)
def trans_airports():

    df=spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data")
    df=df.drop("_rescued_data")\
        .withColumn("modifiedDate",current_timestamp())
    return df

dp.create_streaming_table("silver_airports")

dp.create_auto_cdc_flow(
  target = "silver_airports",
  source = "trans_airports",
  keys = ["airport_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1
)



#silver busines view
@dp.table (
    name="silver_business"
)
def silver_business():
    df=dp.readStream("silver_bookings")\
        .join(dp.readStream("silver_flights"),on="flight_id")\
        .join(dp.readStream("silver_passengers"),on="passenger_id")\
        .join(dp.readStream("silver_airports"),on="airport_id")\
        .drop("modifiedDate")
    return df
        
