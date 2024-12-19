# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

class BronzeLayerIngestion:
    def __init__(self):
        self.catalog = 'dev'
        self.db = 'swatch_raw'
        self.device_registration_table = 'device_registration'
        self.gym_attendance_table = 'gym_attendance'
        self.bpm_table = 'bpm'
        self.workout_table = 'workout_session'
        self.user_profile_table = 'user_profile'
        self.bootstrap_server = 'pkc-619z3.us-east1.gcp.confluent.cloud:9092'
        self.security_protocol = 'SASL_SSL'
        self.jaas_module = 'org.apache.kafka.common.security.plain.PlainLoginModule'
        self.api_key = 'PWCWCEVJQBUXSSNB'
        self.api_secret = 'kvR0VmvqnZFW2+8VktiegzT2kI4+JIy6aPybyMj6jVXuwhp5c34kbctYyUucGmnM'
        self.bpm_topic = 'bpm2'
        self.user_profile_topic = 'user_profile'
        self.workout_topic = 'workout_session'


# COMMAND ----------

# Ingest the devices into bronze layer (raw)
class DeviceIngestion(BronzeLayerIngestion):
    def __init__(self):
        super().__init__()
    
    def get_device_schema(self):
        schema = StructType([
                StructField('user_id', StringType(),True),
                StructField('device_id', LongType(),True),
                StructField('mac_address', StringType(),True),
                StructField('registration_timestamp', LongType(),True)
                            ])
        return schema
    
    def get_raw_data(self):
        raw_df = (spark
                  .readStream
                  .format('csv')
                  .option("delimiter", ",")
                  .schema(self.get_device_schema())
                  .option('header',True)
                  .load('/Volumes/dev/swatch_raw/device_registration_landing_zone/'))
        return raw_df
    
    def persist_devices(self, raw_df):
        devices_streaming_query = (raw_df
                                   .writeStream
                                   .format("delta")
                                   .queryName("bronze_layer_device_registration")
                                   .option("maxFilesPerTrigger",10)
                                   .option('checkpointLocation','/Volumes/dev/swatch_raw/device_registration_checkpoint')
                                   .outputMode("append")
                                   .trigger(processingTime='10 seconds')
                                   .toTable(f"{self.catalog}.{self.db}.{self.device_registration_table}")
                                   )
        return devices_streaming_query
    
    def start_stream(self):
        devices_streaming_query = self.persist_devices(self.get_raw_data())
        return devices_streaming_query


# COMMAND ----------

devices = DeviceIngestion()
devices_streaming_query=devices.start_stream()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.swatch_raw.device_registration;

# COMMAND ----------

devices_streaming_query.stop()

# COMMAND ----------

class GymSessionIngestion(BronzeLayerIngestion):
    def __init__(self):
        super().__init__()

    def get_schema(self):
        schema = StructType([
            StructField("mac_address", StringType(), True),
            StructField("gym_id", StringType(), True),
            StructField("login_time", LongType(), True),
            StructField("logout_time", LongType(), True)
        ])
        return schema
    
    def read_raw_data(self):
        raw_df = (spark
                  .readStream
                  .format("csv")
                  .option("header", True)
                  .option("delimiter", ",")
                  .schema(self.get_schema())
                  .load("/Volumes/dev/swatch_raw/gym_attendance_landing_zone")
                  )
        return raw_df
    
    def persist_data(self,raw_df):
        gym_streaming_query = (raw_df
                               .writeStream
                               .format("delta")
                               .queryName("bronze_layer_gym_session")
                               .option("checkpointLocation","/Volumes/dev/swatch_raw/gym_attendance_checkpoint")
                               .outputMode("append")
                               .option("maxFilePerTrigger",10)
                               .trigger(processingTime='5 seconds')
                                .toTable(f"{self.catalog}.{self.db}.{self.gym_attendance_table}")
                               )
        return gym_streaming_query
    
    def start_stream(self):
        gym_streaming_query = self.persist_data(self.read_raw_data())
        return gym_streaming_query

# COMMAND ----------

gym_attendance = GymSessionIngestion()
gym_streaming_query=gym_attendance.start_stream()

# COMMAND ----------

gym_streaming_query.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.swatch_raw.gym_attendance;

# COMMAND ----------


# Ingest the raw data from the kafka
# Apply the Dedup to make sure we only save the kafka record once in the table 
# Kakfa provide the atleast once gurantee.

class BPMIngestion(BronzeLayerIngestion):
    def __init__(self):
        super().__init__()

    def get_schema(self):
        schema = StructType([
            StructField("device_id", LongType(), True),
            StructField("heartrate", DoubleType(), True),
            StructField("time", LongType(), True)
        ])
        return schema
    
    def load_raw_data(self):
        raw_df = (spark
                  .readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers",self.bootstrap_server)
                  .option("kafka.security.protocol",self.security_protocol)
                  .option("kafka.sasl.mechanism", "PLAIN")
                  .option("kafka.sasl.jaas.config", f"{self.jaas_module} required username='{self.api_key}' password='{self.api_secret}';")
                  .option("maxOffsetsPerTrigger",100)
                  .option("subscribe",self.bpm_topic)
                  .load()
                  )
        return raw_df
    
    def format_raw_data(self,raw_df):
        formatted_df = (raw_df
                     .select(from_json(col("value").cast("string"), self.get_schema()).alias("value"))
                     .selectExpr("value.*")
                     )
        #remove_dup = formatted_df.dropDuplicates(["device_id","time"])
        return formatted_df
    
    def upsert(self,df,batch_id):
        df.createOrReplaceTempView("raw_bpm")
        merge_statement = f"""
        MERGE into {self.catalog}.{self.db}.{self.bpm_table} t
        USING raw_bpm s
        on (s.device_id = t.device_id and t.time = s.time)
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        df._jdf.sparkSession().sql(merge_statement)

    
    def persist_data(self,formatted_df):
        bpm_streaming_query = (formatted_df
                               .writeStream
                               .queryName("bronze_layer_bpm_ingestion")
                               .format("delta")
                               .option("checkpointLocation","/Volumes/dev/swatch_raw/bpm_checkpoint")
                               .outputMode("update")
                               .foreachBatch(self.upsert)
                               .start()
                               )
        return bpm_streaming_query
    
    def start_bpm_stream(self):
        return self.persist_data(self.format_raw_data(self.load_raw_data()))
    

# COMMAND ----------

bpm = BPMIngestion()
bpm_streaming_query = bpm.start_bpm_stream()

# COMMAND ----------

bpm_streaming_query.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.swatch_raw.bpm;
# MAGIC

# COMMAND ----------

class UserProfileIngestion(BronzeLayerIngestion):

    def __init__(self):
        super().__init__()

    def get_schema(self):
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("update_type", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("dob", StringType(), True),
            StructField("sex", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("address", StructType([
                 StructField("street_address", StringType(), True),
                 StructField("city", StringType(), True),
                 StructField("state", StringType(), True),
                 StructField("zip", StringType(), True)
            ]), True)
        ])
        return schema
    
    def load_raw_data(self):
        raw_df = (spark
                  .readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers",self.bootstrap_server)
                  .option("kafka.security.protocol",self.security_protocol)
                  .option("kafka.sasl.mechanism", "PLAIN")
                  .option("kafka.sasl.jaas.config", f"{self.jaas_module} required username='{self.api_key}' password='{self.api_secret}';")
                  .option("maxOffsetsPerTrigger",100)
                  .option("subscribe",self.user_profile_topic)
                  .load()
                  )
        return raw_df
    
    def format_raw_data(self,raw_df):
        formatted_df = (raw_df
                     .select(from_json(col("value").cast("string"), self.get_schema()).alias("value"))
                     .selectExpr("value.*")
                     )
        return formatted_df
    
    # Remove the duplicates from the stream
    def upsert(self,df,batch_id):
        df.createOrReplaceTempView("raw_user_profile")
        merge_statement = f"""
        MERGE into {self.catalog}.{self.db}.{self.user_profile_table} t
        USING raw_user_profile s
        on (s.user_id = t.user_id and t.timestamp = s.timestamp and t.update_type=s.update_type)
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        df._jdf.sparkSession().sql(merge_statement)

    
    def persist_data(self,formatted_df):
        user_profile_streaming_query = (formatted_df
                               .writeStream
                               .queryName("bronze_layer_user_profile_ingestion")
                               .format("delta")
                               .option("checkpointLocation","/Volumes/dev/swatch_raw/user_profile_checkpoint")
                               .outputMode("update")
                               .trigger(processingTime='5 seconds')
                               .foreachBatch(self.upsert)
                               .start()
                               )
        return user_profile_streaming_query
    
    def start_user_profile_stream(self):
        return self.persist_data(self.format_raw_data(self.load_raw_data()))
    
    


# COMMAND ----------

user_profile = UserProfileIngestion()
user_profile_streaming_query = user_profile.start_user_profile_stream()

# COMMAND ----------

user_profile_streaming_query.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.swatch_raw.user_profile;

# COMMAND ----------

class WorkoutIngestion(BronzeLayerIngestion):

    def __init__(self):
        super().__init__()

    def get_schema(self):
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("workout_id", IntegerType(), True),
            StructField("timestamp", LongType(), True),
            StructField("action", StringType(), True),
            StructField("session_id", IntegerType(), True)
        ])
        return schema
    
    def load_raw_data(self):
        raw_df = (spark
                  .readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers",self.bootstrap_server)
                  .option("kafka.security.protocol",self.security_protocol)
                  .option("kafka.sasl.mechanism", "PLAIN")
                  .option("kafka.sasl.jaas.config", f"{self.jaas_module} required username='{self.api_key}' password='{self.api_secret}';")
                  .option("maxOffsetsPerTrigger",100)
                  .option("subscribe",self.workout_topic)
                  .load()
                  )
        return raw_df
    
    def format_raw_data(self,raw_df):
        formatted_df = (raw_df
                     .select(from_json(col("value").cast("string"), self.get_schema()).alias("value"))
                     .selectExpr("value.*")
                     )
        return formatted_df
    
    def upsert(self,df,batch_id):
        df.createOrReplaceTempView("raw_workout")
        merge_statement = f"""
        MERGE into {self.catalog}.{self.db}.{self.workout_table} t
        USING raw_workout s
        on (s.user_id = t.user_id and t.timestamp = s.timestamp and t.workout_id=s.workout_id)
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        df._jdf.sparkSession().sql(merge_statement)

    
    def persist_data(self,formatted_df):
        workout_streaming_query = (formatted_df
                                        .writeStream
                                        .queryName("bronze_layer_workout_ingestion")
                                        .format("delta")
                                        .option("checkpointLocation","/Volumes/dev/swatch_raw/workout_session_checkpoint")
                                        .outputMode("update")
                                        .trigger(processingTime='5 seconds')
                                        .foreachBatch(self.upsert)
                                        .start()
                                  )
        return workout_streaming_query
    
    def start_workout_stream(self):
        return self.persist_data(self.format_raw_data(self.load_raw_data()))
    
    


# COMMAND ----------

workout = WorkoutIngestion()
workout_streaming_query = workout.start_workout_stream()

# COMMAND ----------

workout_streaming_query.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.swatch_raw.workout_session;
