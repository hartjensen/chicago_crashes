#!/usr/bin/env python
# coding: utf-8

# In[39]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
spark


# In[61]:


df = spark.read.csv("/home/analytics4220/Hannah/cc_clean.csv", 
                    header=True, sep=',')
df


# In[62]:


df.printSchema()


# In[63]:


df.select('ROADWAY_SURFACE_COND').show()


# In[64]:


from pyspark.sql.functions import to_date, col

df = df.withColumn(
    "CRASH_DATE_CLEAN",
    to_date(col("CRASH_DATE"), "M/d/yyyy")
)


# In[65]:


df.filter(
    col("CRASH_DATE_CLEAN").between("2025-01-01", "2025-12-31")
).show()


# In[66]:


df = df.withColumn('CRASH_HOUR', 
                   df['CRASH_HOUR'].cast('int'))
df


# In[12]:


df.groupBy('WEATHER_CONDITION').count().orderBy('count').show()


# In[76]:


crashes = df.groupBy('CRASH_DAY').count().orderBy('count')
crashes


# In[82]:


crashes.toPandas().plot.bar(x='CRASH_DAY');


# In[23]:


from pyspark.sql.functions import desc
df.groupBy('CRASH_DATE').count().orderBy(desc('count')).show()


# In[83]:


spark.stop()


# In[ ]:




