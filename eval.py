
# check matched
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)


# cb_adf


n = 4
# pred file
path = "/Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_{0}/pred_train".format(n)
#
pred_train = spark.read.text(path)
#pred_train.show(1,False)
#
split_col = split(pred_train['value'], ' ')
pred_train = pred_train.withColumn('action_index', split_col.getItem(0))
pred_train = pred_train.withColumn('PassengerId', split_col.getItem(1))
pred_train = pred_train.filter(col("PassengerId").isNotNull()).withColumn("PassengerId", col("PassengerId").cast("integer"))
pred_train = pred_train.withColumn("action_index",col("action_index").cast("integer"))#

# # true label df
total_N = train.alias("t").filter(col("t.action_chosen")=="1").count()
df_train = train.alias("t").filter(col("t.action_chosen")=="1")
df_train = df_train.withColumn("action",col("action").cast("integer")).withColumn("add_one",lit(-1).cast("integer")) \
.withColumn("num_action",lit(3).cast("integer"))
# df_train = df_train.withColumn("seg_combined_offerid_index_2", sum([col("seg_combined_offerid_index"),col("add_one")]))
df_train = df_train.withColumn("action_index_2", expr("action + add_one")) \
.withColumn("Survival_cost",when(col("Survived")==0,lit(1)).otherwise(lit(-1)))
#df_train.select("Survived","Survival_cost").show(10,False)

# join pred df and true label df
df_match = pred_train.alias("a") \
.join(df_train.alias("b"),
[col("a.PassengerId") == col("b.PassengerId"),col("a.action_index") == col("b.action_index_2"),col("b.action_chosen") == "1"]) \
.select("b.PassengerId",
"a.action_index",
col("b.Survival_cost").cast("float"),
col("b.num_action").cast("integer"))
# df_match.select("negative_truncated_measure_revenue_cs").describe().show(100,False)
df_match = df_match.withColumn("week_prob", 1/col("num_action")) \
.withColumn("new_col",col("Survival_cost")*col("num_action"))
# df_match.select("negative_truncated_measure_revenue_cs","new_col").describe().show(100,False)
df_match = df_match.withColumn("n",lit(n)).withColumn("pred_type",lit("train"))
# df_match_avg = df_match.groupby("start_date").agg(count("new_col").alias("total_user"),
# avg("new_col").alias("avg_loss_ips"),
# avg("negative_truncated_measure_revenue_cs").alias("avg_loss_0")) \
# .sort("start_date")
df_match_avg = df_match.groupby("n","pred_type").agg(count("new_col").alias("total_user"),
avg("new_col").alias("avg_loss_ips"),
avg("Survival_cost").alias("avg_loss_0"))
df_match_avg = df_match_avg.withColumn("total_N",lit(total_N).cast("integer"))
total_N = train.count()
df_match_avg = df_match_avg.withColumn("avg_loss_only_matched",col("avg_loss_ips")*col("total_user")/col("total_N").cast("double")).withColumn("total_user",col("total_user").cast("integer"))
df_match_avg = df_match_avg.withColumn("match_rate",col("total_user")/col("total_N"))
df_match_avg.show(10,False)
# n = 1
print("n : " + str(n) + " train done")

path = "/Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_{0}/pred_train_as_test".format(n)
#
pred_train = spark.read.text(path)
#pred_train.show(1,False)
#
split_col = split(pred_train['value'], ' ')
pred_train = pred_train.withColumn('action_index', split_col.getItem(0))
pred_train = pred_train.withColumn('PassengerId', split_col.getItem(1))
pred_train = pred_train.filter(col("PassengerId").isNotNull()).withColumn("PassengerId", col("PassengerId").cast("integer"))
pred_train = pred_train.withColumn("action_index",col("action_index").cast("integer"))#

# # true label df
total_N = train.alias("t").filter(col("t.action_chosen")=="1").count()
df_train = train.alias("t").filter(col("t.action_chosen")=="1")
df_train = df_train.withColumn("action",col("action").cast("integer")).withColumn("add_one",lit(-1).cast("integer")) \
.withColumn("num_action",lit(3).cast("integer"))
# df_train = df_train.withColumn("seg_combined_offerid_index_2", sum([col("seg_combined_offerid_index"),col("add_one")]))
df_train = df_train.withColumn("action_index_2", expr("action + add_one")) \
.withColumn("Survival_cost",when(col("Survived")==0,lit(1)).otherwise(lit(-1)))
#df_train.select("Survived","Survival_cost").show(10,False)

# join pred df and true label df
df_match = pred_train.alias("a") \
.join(df_train.alias("b"),
[col("a.PassengerId") == col("b.PassengerId"),col("a.action_index") == col("b.action_index_2"),col("b.action_chosen") == "1"]) \
.select("b.PassengerId",
"a.action_index",
col("b.Survival_cost").cast("float"),
col("b.num_action").cast("integer"))
# df_match.select("negative_truncated_measure_revenue_cs").describe().show(100,False)
df_match = df_match.withColumn("n",lit(n)).withColumn("week_prob", 1/col("num_action")) \
.withColumn("new_col",col("Survival_cost")*col("num_action"))
# df_match.select("negative_truncated_measure_revenue_cs","new_col").describe().show(100,False)
df_match = df_match.withColumn("pred_type",lit("train_as_test"))
# df_match_avg = df_match.groupby("start_date").agg(count("new_col").alias("total_user"),
# avg("new_col").alias("avg_loss_ips"),
# avg("negative_truncated_measure_revenue_cs").alias("avg_loss_0")) \
# .sort("start_date")
df_match_avg = df_match.groupby("n","pred_type").agg(count("new_col").alias("total_user"),
avg("new_col").alias("avg_loss_ips"),
avg("Survival_cost").alias("avg_loss_0"))
df_match_avg = df_match_avg.withColumn("total_N",lit(total_N).cast("integer"))
total_N = train.count()
df_match_avg = df_match_avg.withColumn("avg_loss_only_matched",col("avg_loss_ips")*col("total_user")/col("total_N").cast("double")).withColumn("total_user",col("total_user").cast("integer"))
df_match_avg = df_match_avg.withColumn("match_rate",col("total_user")/col("total_N"))
df_match_avg.show(10,False)
# n = 1
print("n : " + str(n) + " train as test done")

# # test_test
# pred file
path = "/Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_{0}/pred_test".format(n)
#
pred_test = spark.read.text(path)
#pred_test.show(1,False)
#
split_col = split(pred_test['value'], ' ')
pred_test = pred_test.withColumn('action_index', split_col.getItem(0))
pred_test = pred_test.withColumn('PassengerId', split_col.getItem(1))
pred_test = pred_test.filter(col("PassengerId").isNotNull()).withColumn("PassengerId", col("PassengerId").cast("integer"))
pred_test = pred_test.withColumn("action_index",col("action_index").cast("integer"))#

# # true label df
total_N = test.alias("t").filter(col("t.action_chosen")=="1").count()
df_test = test.alias("t").filter(col("t.action_chosen")=="1")
df_test = df_test.withColumn("action",col("action").cast("integer")).withColumn("add_one",lit(-1).cast("integer")) \
.withColumn("num_action",lit(3).cast("integer"))
# df_test = df_test.withColumn("seg_combined_offerid_index_2", sum([col("seg_combined_offerid_index"),col("add_one")]))
df_test = df_test.withColumn("action_index_2", expr("action + add_one")) \
.withColumn("Survival_cost",when(col("Survived")==0,lit(1)).otherwise(lit(-1)))
#df_test.select("Survived","Survival_cost").show(10,False)

# join pred df and true label df
df_match = pred_test.alias("a") \
.join(df_test.alias("b"),
[col("a.PassengerId") == col("b.PassengerId"),col("a.action_index") == col("b.action_index_2"),col("b.action_chosen") == "1"]) \
.select("b.PassengerId",
"a.action_index",
col("b.Survival_cost").cast("float"),
col("b.num_action").cast("integer"))
# df_match.select("negative_truncated_measure_revenue_cs").describe().show(100,False)
df_match = df_match.withColumn("week_prob", 1/col("num_action")) \
.withColumn("new_col",col("Survival_cost")*col("num_action"))
# df_match.select("negative_truncated_measure_revenue_cs","new_col").describe().show(100,False)
df_match = df_match.withColumn("n",lit(n)).withColumn("pred_type",lit("test"))
# df_match_avg = df_match.groupby("start_date").agg(count("new_col").alias("total_user"),
# avg("new_col").alias("avg_loss_ips"),
# avg("negative_truncated_measure_revenue_cs").alias("avg_loss_0")) \
# .sort("start_date")
df_match_avg = df_match.groupby("n","pred_type").agg(count("new_col").alias("total_user"),
avg("new_col").alias("avg_loss_ips"),
avg("Survival_cost").alias("avg_loss_0"))
df_match_avg = df_match_avg.withColumn("total_N",lit(total_N).cast("integer"))
total_N = test.count()
# df_match_avg = df_match_avg.withColumn("avg_loss_only_matched",col("avg_loss_ips")*col("total_user")/col("total_N").cast("double"))
df_match_avg = df_match_avg.withColumn("avg_loss_only_matched",col("avg_loss_ips")*col("total_user")/col("total_N").cast("double")).withColumn("total_user",col("total_user").cast("integer"))
df_match_avg = df_match_avg.withColumn("match_rate",col("total_user")/col("total_N"))
df_match_avg.show(10,False)
# n = 1
print("n : " + str(n) + " test as test done")


# pred_train
# pred_train_as_test
# pred_test

# +---+---------+----------+-------------------+--------------------+-------+---------------------+------------------+
# |n  |pred_type|total_user|avg_loss_ips       |avg_loss_0          |total_N|avg_loss_only_matched|match_rate        |
# +---+---------+----------+-------------------+--------------------+-------+---------------------+------------------+
# |1  |train    |198       |-1.0606060606060606|-0.35353535353535354|891    |-0.2356902356902357  |0.2222222222222222|
# +---+---------+----------+-------------------+--------------------+-------+---------------------+------------------+
# +---+-------------+----------+-------------------+-------------------+-------+---------------------+------------------+
# |n  |pred_type    |total_user|avg_loss_ips       |avg_loss_0         |total_N|avg_loss_only_matched|match_rate        |
# +---+-------------+----------+-------------------+-------------------+-------+---------------------+------------------+
# |1  |train_as_test|127       |-1.7244094488188977|-0.5748031496062992|891    |-0.24579124579124578 |0.1425364758698092|
# +---+-------------+----------+-------------------+-------------------+-------+---------------------+------------------+
# +---+---------+----------+------------------+------------------+-------+---------------------+-------------------+
# |n  |pred_type|total_user|avg_loss_ips      |avg_loss_0        |total_N|avg_loss_only_matched|match_rate         |
# +---+---------+----------+------------------+------------------+-------+---------------------+-------------------+
# |1  |test     |143       |0.5664335664335665|0.1888111888111888|418    |0.1937799043062201   |0.34210526315789475|
# +---+---------+----------+------------------+------------------+-------+---------------------+-------------------+

# +---+---------+----------+-------------------+-------------------+-------+---------------------+------------------+
# |n  |pred_type|total_user|avg_loss_ips       |avg_loss_0         |total_N|avg_loss_only_matched|match_rate        |
# +---+---------+----------+-------------------+-------------------+-------+---------------------+------------------+
# |2  |train    |247       |-0.7894736842105263|-0.2631578947368421|891    |-0.21885521885521886 |0.2772166105499439|
# +---+---------+----------+-------------------+-------------------+-------+---------------------+------------------+
# +---+-------------+----------+-------------------+-------------------+-------+---------------------+-------------------+
# |n  |pred_type    |total_user|avg_loss_ips       |avg_loss_0         |total_N|avg_loss_only_matched|match_rate         |
# +---+-------------+----------+-------------------+-------------------+-------+---------------------+-------------------+
# |2  |train_as_test|188       |-2.4574468085106385|-0.8191489361702128|891    |-0.5185185185185186  |0.21099887766554434|
# +---+-------------+----------+-------------------+-------------------+-------+---------------------+-------------------+
# +---+---------+----------+------------------+-------------------+-------+---------------------+------------------+
# |n  |pred_type|total_user|avg_loss_ips      |avg_loss_0         |total_N|avg_loss_only_matched|match_rate        |
# +---+---------+----------+------------------+-------------------+-------+---------------------+------------------+
# |2  |test     |137       |0.6788321167883211|0.22627737226277372|418    |0.22248803827751196  |0.3277511961722488|
# +---+---------+----------+------------------+-------------------+-------+---------------------+------------------+

+---+---------+----------+-------------------+-------------------+-------+---------------------+-------------------+
|n  |pred_type|total_user|avg_loss_ips       |avg_loss_0         |total_N|avg_loss_only_matched|match_rate         |
+---+---------+----------+-------------------+-------------------+-------+---------------------+-------------------+
|3  |train    |214       |-1.5700934579439252|-0.5233644859813084|891    |-0.3771043771043771  |0.24017957351290684|
+---+---------+----------+-------------------+-------------------+-------+---------------------+-------------------+
+---+-------------+----------+-------------------+-------------------+-------+---------------------+------------------+
|n  |pred_type    |total_user|avg_loss_ips       |avg_loss_0         |total_N|avg_loss_only_matched|match_rate        |
+---+-------------+----------+-------------------+-------------------+-------+---------------------+------------------+
|3  |train_as_test|209       |-2.9425837320574164|-0.9808612440191388|891    |-0.6902356902356902  |0.2345679012345679|
+---+-------------+----------+-------------------+-------------------+-------+---------------------+------------------+
+---+---------+----------+------------+----------+-------+---------------------+------------------+
|n  |pred_type|total_user|avg_loss_ips|avg_loss_0|total_N|avg_loss_only_matched|match_rate        |
+---+---------+----------+------------+----------+-------+---------------------+------------------+
|3  |test     |144       |0.375       |0.125     |418    |0.1291866028708134   |0.3444976076555024|
+---+---------+----------+------------+----------+-------+---------------------+------------------+
 
# +---+---------+----------+------------------+-------------------+-------+---------------------+-----------------+
# |n  |pred_type|total_user|avg_loss_ips      |avg_loss_0         |total_N|avg_loss_only_matched|match_rate       |
# +---+---------+----------+------------------+-------------------+-------+---------------------+-----------------+
# |4  |train    |218       |-1.761467889908257|-0.5871559633027523|891    |-0.43097643097643096 |0.244668911335578|
# +---+---------+----------+------------------+-------------------+-------+---------------------+-----------------+
# +---+-------------+----------+------------------+-------------------+-------+---------------------+-------------------+
# |n  |pred_type    |total_user|avg_loss_ips      |avg_loss_0         |total_N|avg_loss_only_matched|match_rate         |
# +---+-------------+----------+------------------+-------------------+-------+---------------------+-------------------+
# |4  |train_as_test|280       |-2.807142857142857|-0.9357142857142857|891    |-0.8821548821548821  |0.31425364758698093|
# +---+-------------+----------+------------------+-------------------+-------+---------------------+-------------------+
# +---+---------+----------+------------------+-------------------+-------+---------------------+------------------+
# |n  |pred_type|total_user|avg_loss_ips      |avg_loss_0         |total_N|avg_loss_only_matched|match_rate        |
# +---+---------+----------+------------------+-------------------+-------+---------------------+------------------+
# |4  |test     |159       |0.7358490566037735|0.24528301886792453|418    |0.27990430622009566  |0.3803827751196172|
# +---+---------+----------+------------------+-------------------+-------+---------------------+------------------+
