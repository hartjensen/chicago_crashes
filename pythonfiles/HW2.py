#!/usr/bin/env python
# coding: utf-8

# In[19]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
spark


# In[20]:


df = spark.read.csv("/home/analytics4220/Hannah/cc_clean.csv", 
                    header=True, sep=',')
df


# In[21]:


df.show(5)


# In[22]:


df.printSchema()


# In[23]:


crash_df = df.select(
    "CRASH_DATE",
    "WEATHER_CONDITION",
    "LIGHTING_CONDITION",
    "ROADWAY_SURFACE_COND",
    "CRASH_HOUR",
    "CRASH_DAY"
)

crash_df.show(5)


# In[24]:


from pyspark.sql.functions import count

crashes_by_day = crash_df.groupBy("CRASH_DAY").agg(count("*").alias("total_crashes")).orderBy("CRASH_DAY")

crashes_by_day.show()


# In[25]:


from pyspark.sql.functions import count, col

crashes_by_day = crash_df.groupBy("CRASH_DAY").agg(count("*").alias("total_crashes")).orderBy(col("total_crashes").desc())

crashes_by_day.show()


# In[26]:


crashes_by_weather = crash_df.groupBy("WEATHER_CONDITION").agg(count("*").alias("total_crashes")).orderBy(col("total_crashes").desc())

crashes_by_weather.show()


# In[15]:


crashes_by_day_time = (
    crash_df
    .groupBy("CRASH_DAY", "CRASH_HOUR")
    .agg(count("*").alias("total_crashes"))
    .orderBy("CRASH_DAY", "CRASH_HOUR")
)

crashes_by_day_time.show(50)


# In[27]:


from pyspark.sql.functions import when

crashes_by_day = crashes_by_day.withColumn(
    "day_order",
    when(crashes_by_day.CRASH_DAY == "Sunday", 1)
    .when(crashes_by_day.CRASH_DAY == "Monday", 2)
    .when(crashes_by_day.CRASH_DAY == "Tuesday", 3)
    .when(crashes_by_day.CRASH_DAY == "Wednesday", 4)
    .when(crashes_by_day.CRASH_DAY == "Thursday", 5)
    .when(crashes_by_day.CRASH_DAY == "Friday", 6)
    .when(crashes_by_day.CRASH_DAY == "Saturday", 7)
)

crashes_by_day = crashes_by_day.orderBy("day_order")


# In[28]:


import matplotlib.pyplot as plt

pdf_day = crashes_by_day.toPandas()

plt.figure()
plt.bar(pdf_day["CRASH_DAY"], pdf_day["total_crashes"])

plt.xlabel("Day of Week")
plt.ylabel("Number of Crashes")
plt.title("Crashes by Day of Week")

plt.show()


# In[32]:


spark.stop()


# In[ ]:


s

