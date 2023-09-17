# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

import zipfile
import pandas as pd

# COMMAND ----------

sp=SparkSession.builder.appName("Parking_tickets_EDA").getOrCreate()

# COMMAND ----------

pdf = sp.read.csv('/FileStore/tables/parking_tickets_2017_reduced.csv',sep=',',header='true',inferSchema='true')
#/FileStore/tables/parking_tickets_2017_reduced.csv
#/FileStore/tables/parking_tickets_2017_reduced.csv
pdf.display()

# COMMAND ----------

len(pdf.columns)

# COMMAND ----------

pdf.count()

# COMMAND ----------

pdf.printSchema()

# COMMAND ----------

data = pdf.dropDuplicates()

# COMMAND ----------

data.describe("Summons Number").show()

# COMMAND ----------

data.describe("Plate ID").show()

# COMMAND ----------

data.describe("Registration State").show()

# COMMAND ----------

data.describe("Violation Code").show()

# COMMAND ----------

data.describe("Vehicle Body Type").show()

# COMMAND ----------

data=data.fillna('0')

# COMMAND ----------

data.createOrReplaceTempView("datav")

# COMMAND ----------

nullrc=sp.sql('select count(*) from datav where `Summons Number` is null or `Plate ID` is null or `Registration State` is null or `Issue Date` is null or `Violation Code` is null or `Vehicle Body Type` is null or `Vehicle Make` is null or `Violation Precinct` is null or `Issuer Precinct` is null or `Violation Time` is null')


# COMMAND ----------

#
# %sql select * from datav limit 1000

# COMMAND ----------

nullrc.show()

# COMMAND ----------

# DBTITLE 1,Viz/ Exploratory data analysis
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

df=sp.sql("select `Summons Number` as `summons_number`, `Plate ID` as `plate_id`, `Registration State` as `registration_state`, `Issue Date` as `issue_date`, `Violation Code` as `violation_code`,`Vehicle Body Type` as `vehicle_body_type`,`Vehicle Make` as `vehicle_make`, `Violation Precinct` as `violation_precinct`,`Issuer Precinct` as `issuer_precinct`, `Violation Time` as `violation_time` FROM datav")


# COMMAND ----------

df.display(2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView('nycdata')

# COMMAND ----------

# DBTITLE 1,ticket frequency by year and month
ticket=sp.sql('select year(issue_date) as year, month(issue_date) as month, count(*) total_tickets from nycdata group by 1,2 order by 1,2 desc')
#ticket.show()


# COMMAND ----------

ticket_m= ticket.toPandas()
plt.clf
ticket_m.plot(x='month',y='total_tickets',kind='bar')
plt.title('voilations per month')
plt.xlabel('month')
plt.ylabel('voilations')
plt.show()

# COMMAND ----------

# DBTITLE 1,Voilation by registration state
#registration_state

reg_data=sp.sql('select registration_state,count(*) as total_tickets from nycdata group by 1  order by total_tickets desc')


# COMMAND ----------

reg_d=reg_data.toPandas()
plt.clf
plt.figure(figsize=(100,200))
reg_d.head(10).plot(x='registration_state',y='total_tickets',kind='bar')
plt.title('voilations per registration_state')
plt.xlabel('state')
plt.ylabel('voilations')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC examine the data 
# MAGIC

# COMMAND ----------

# DBTITLE 1,1. How often does each violation code occur?
# MAGIC %sql select violation_code, count(*) as cnt from nycdata group by 1

# COMMAND ----------

# DBTITLE 1,2. How often does each 'vehicle body type' get a parking ticket? How about the 'vehicle make'?
# MAGIC %sql select vehicle_body_type, count(*) cnt from nycdata group by 1

# COMMAND ----------

# DBTITLE 1,violation_precinct with highest tickets
# MAGIC %sql select violation_precinct, count(*) cnt from nycdata group by 1 order by 2 desc limit 6

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 4. Find the violation code frequencies for three precincts that have issued the most number of tickets. Do these precinct zones have an exceptionally high frequency of certain violation codes? Are these codes common across precincts?

# COMMAND ----------

# MAGIC %sql select violation_code, count(*) as cnt from nycdata where violation_precinct=19
# MAGIC group by 1
# MAGIC --in (19,14,1,18,114)

# COMMAND ----------

# MAGIC %sql select violation_code, count(*) as precinct_14 from nycdata where violation_precinct=14
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql select violation_code, count(*) as precinct_1 from nycdata where violation_precinct=1
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql select violation_code, count(*) as precinct_18 from nycdata where violation_precinct=18
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql select violation_code, count(*) as precinct_114 from nycdata where violation_precinct=114
# MAGIC group by 1

# COMMAND ----------

sp.sql('select * from nycdata limit 100').show()

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, udf, date_format

# COMMAND ----------

df.select("violation_time", col("violation_time").rlike("^[01][0-9]{3}[AP]$").alias("criteria")).where(col("criteria")=="false").show()
df.select("violation_time", col("violation_time").rlike("^[01][0-9]{3}[AP]$").alias("criteria")).where(col("criteria")=="false").count()

# COMMAND ----------

nycdf=df.withColumn('violation_time', regexp_replace('violation_time', '^.*[^0-9AP].*$', '0000A')).withColumn('violation_time', regexp_replace('violation_time', '^.*[^AP]$', '0000A')).withColumn('violation_time', regexp_replace('violation_time', '^[^01].*$', '0000A')).withColumn('violation_time', regexp_replace('violation_time', '^[0-9AP]{1,4}$', '0000A'))

# COMMAND ----------

nycdf.show(5)

# COMMAND ----------

print("Initially 0000A count: ",df.select("violation_time").where(col("violation_time")=="0000A").count())
print("Finally 0000A count: ", nycdf.select("violation_time").where(col("violation_time")=="0000A").count())

# COMMAND ----------

nycdf.select("violation_time", col("violation_time").rlike("^[01][0-9]{3}[AP]$").alias("criteria")).where(col("criteria")=="false").show()

# COMMAND ----------

from pyspark.sql.types import StringType

# COMMAND ----------

maketime_udf_str_1 = udf(lambda x: x[0:2]+':'+x[2:4]+':00' if x[-1]=="A" or int(x[0:2]) in range(13,25) else str(int(x[0:2])+12)+':'+x[2:4]+':00', StringType())

maketime_udf_str_2 = udf(lambda x: '00'+x[2:] if x[0:2]=="24" else x, StringType())
nycdf= nycdf.withColumn('violation_time_new', maketime_udf_str_1('violation_time')).withColumn('violation_time_new', maketime_udf_str_2('violation_time_new')).withColumn('violation_time_new_formatted', date_format('violation_time_new', 'H:m:s'))

nycdf.printSchema()

# COMMAND ----------

nycdf.createOrReplaceTempView("time_slots")

# COMMAND ----------

# MAGIC %sql select violation_code, violation_time_new_formatted, count(*) count from time_slots group by violation_code, violation_time_new_formatted

# COMMAND ----------

result = spark.sql(
    'SELECT `violation_code`, count(*) as `number of tickets for the time bin for violation code`,  CASE WHEN HOUR(`violation_time_new_formatted`) IN ("0","1","2","3") THEN "1" WHEN HOUR(`violation_time_new_formatted`) IN ("4","5","6","7") THEN "2"  WHEN HOUR(`violation_time_new_formatted`) IN ("8","9","10","11") THEN "3"  WHEN HOUR(`violation_time_new_formatted`) IN ("12","13","14","15") THEN "4"  WHEN HOUR(`violation_time_new_formatted`) IN ("16","17","18","19") THEN "5"  WHEN HOUR(`violation_time_new_formatted`) IN ("20","21","22","23") THEN "6" END AS `violation_time_bin` FROM time_slots GROUP BY `violation_time_bin`,`violation_code`')
result.show(15)

# COMMAND ----------

result.createOrReplaceTempView("bins_table")

# COMMAND ----------

bin_6 = spark.sql("select violation_code from bins_table where violation_time_bin == 6 group by violation_code order by count(*) desc")
bin_6.show(3)

# COMMAND ----------

bin_5 = spark.sql("select violation_code from bins_table where violation_time_bin == 5 group by violation_code order by count(*) desc")
bin_5.show(3)

# COMMAND ----------

bin_4 = spark.sql("select violation_code from bins_table where violation_time_bin == 4 group by violation_code order by count(*) desc")
bin_4.show(3)

# COMMAND ----------

# MAGIC %sql select violation_time_bin from bins_table group by violation_time_bin order by count(*) desc 

# COMMAND ----------

# MAGIC %md
# MAGIC find sesonality of data

# COMMAND ----------

byseason = spark.sql("select Violation_Code , Issuer_Precinct, case when MONTH(TO_DATE(Issue_Date, 'yyyy-mm-dd')) between 03 and 05 then 'spring' when MONTH(TO_DATE(Issue_Date, 'yyyy-mm-dd')) between 06 and 08 then 'summer' when MONTH(TO_DATE(Issue_Date, 'yyyy-mm-dd')) between 09 and 11 then 'autumn' when MONTH(TO_DATE(Issue_Date, 'yyyy-mm-dd')) in (1,2,12) then 'winter' else 'unknown' end  as season from nycdata")
byseason.show()

# COMMAND ----------

byseason.createOrReplaceTempView("seasonality_table")

# COMMAND ----------

# MAGIC %sql select season, count(*) as tickets from seasonality_table group by 1

# COMMAND ----------

# DBTITLE 1,Find the three most common violations for each of these seasons.
# MAGIC %sql select Violation_Code, count(*) as ticket from seasonality_table where season='autumn' group by 1 
# MAGIC

# COMMAND ----------

# MAGIC %sql select Violation_Code, count(*) as ticket from seasonality_table where season='summer' group by 1 

# COMMAND ----------

# MAGIC %sql select Violation_Code, count(*) as ticket from seasonality_table where season='winter' group by 1 

# COMMAND ----------

# MAGIC %sql select Violation_Code, count(*) as ticket from seasonality_table where season='spring' group by 1 

# COMMAND ----------

# MAGIC %sql select Violation_Code, count(*) cnt from seasonality_table group by 1

# COMMAND ----------

sp.stop()

# COMMAND ----------


