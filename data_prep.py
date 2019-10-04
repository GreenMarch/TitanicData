from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import concat_ws

path = "/Users/lcui/study_toy_examples/titanic/titanic/train.csv"
train = spark.read.csv(path,header=True,sep=",")
train.groupby("Survived").count().show()
train = train.fillna('0', ["Survived","SibSp","Parch"])

# filter and remove Pclass == 3
train.groupby("PClass","Survived").count().sort("PClass","Survived").show(100)

train.groupby("PClass","Survived").count().sort("PClass","Survived").show(100)

# +------+--------+-----+
# |PClass|Survived|count|
# +------+--------+-----+
# |     1|       0|   80|
# |     1|       1|  136|
# |     2|       0|   97|
# |     2|       1|   87|
# |     3|       0|  372|
# |     3|       1|  119|
# +------+--------+-----+
#train = train.filter(col("Pclass")!="3")
# train.groupby("PClass","Survived").count().sort("PClass","Survived").show(100)
# train.groupby("PClass","Ticket","Survived").count().sort("PClass","Ticket","Survived").show(100)
# train.groupby("PClass","Ticket").count().sort("PClass","Ticket").show(100)
# train.groupby("PClass","Cabin").count().sort("PClass","Cabin").show(1000)




train_male = train.filter(col("Sex") == "male").fillna('24', "Age")
train_female = train.filter(col("Sex") == "female").fillna('23', "Age")
train = train_male.union(train_female)
train = train.fillna('unknown', ["Pclass","Ticket","Cabin","Embarked"])

#train = train.fillna('unknown', ["Cabin"])#.show()

train = train.select(col("PassengerId").cast("integer"),
"Survived",
"Pclass",
"Sex",
col("Age").cast("float"),
col("SibSp").cast("integer"),
col("Parch").cast("integer"),
"Ticket",
col("Fare").cast("float"),
"Cabin",
"Embarked")

df_Fare = train.select("Pclass",col("Fare").cast("float")) \
.groupby("Pclass") \
.agg(mean("Fare").alias("Fare_avg"),min("Fare"),max("Fare"))


# +------+------------------+---------+---------+
# |Pclass|         avg(Fare)|min(Fare)|max(Fare)|
# +------+------------------+---------+---------+
# |     3|13.675550210257411|      0.0|    69.55|
# |     1| 84.15468752825701|      0.0| 512.3292|
# |     2| 20.66218318109927|      0.0|     73.5|
# +------+------------------+---------+---------+


 # |-- PassengerId: integer (nullable = true) ID
 # |-- Survived: string (nullable = false) Y
 # |-- Pclass: string (nullable = false) action
 # |-- Sex: string (nullable = true) shared
 # |-- Age: float (nullable = true) shared
 # |-- SibSp: integer (nullable = true) shared
 # |-- Parch: integer (nullable = true) shared
 # |-- Ticket: string (nullable = false) action not good too many levels
 # |-- Fare: float (nullable = true) action
 # |-- Cabin: string (nullable = false) action not good too many levels
 # |-- Embarked: string (nullable = false) action
train_shared = train.select("PassengerId","Sex","Age","SibSp","Parch")
train_action = train.select("PassengerId","Pclass","Ticket","Fare","Cabin","Embarked")

cat_features_shared = [a for a in ["Sex"]]
num_features_shared = [a for a in ["Age","SibSp","Parch"]]

cat_features_action = [a for a in ["Ticket","Cabin","Embarked"]]
num_features_action = [a for a in ["Fare"]]
#
train_header_parsing = train_shared
train_body_parsing = train_action

for name in cat_features_shared:
    train_header_parsing = train_header_parsing \
        .withColumn("vw_s_{0}".format(name),
            when(col(name).isNotNull(),concat(lit("{0}_".format(name)), regexp_replace(col(name), ' ', ''))).otherwise(lit("")))

for name in num_features_shared:
    train_header_parsing = train_header_parsing \
        .withColumn("vw_s_{0}".format(name),
            when(col(name).isNotNull(),concat(lit("{0}:".format(name)), regexp_replace(col(name), ' ', ''))).otherwise(lit("")))

for name in cat_features_action:
    train_body_parsing = train_body_parsing \
        .withColumn("vw_a_{0}".format(name),
            when(col(name).isNotNull(),concat(lit("{0}_".format(name)), regexp_replace(col(name), ' ', ''))).otherwise(lit("")))

for name in num_features_action:
    train_body_parsing = train_body_parsing \
        .withColumn("vw_a_{0}".format(name),
            when(col(name).isNotNull(),concat(lit("{0}:".format(name)), regexp_replace(col(name), ' ', ''))).otherwise(lit("")))

# train_header_parsing = train_header_parsing.withColumn("vw_s_string",concat(col("vw_s_Sex"),lit(" "),col("vw_s_Age")))
# train_header_parsing.show()

train_body_parsing.show()

real_features_shared = [a for a in train_header_parsing.columns if a.startswith("vw_s_")]
real_features_shared

real_features_action = [a for a in train_body_parsing.columns if a.startswith("vw_a_")]
real_features_action

action_prob_dist = train.groupby("Pclass").count()
total_N = train.count()
action_prob_dist = action_prob_dist.withColumn("total_N",lit(total_N).cast("integer")) \
.withColumn("count",col("count").cast("integer"))
action_prob_dist = action_prob_dist.withColumn("prob",col("count")/col("total_N"))
action_prob_dist.show()

# train = train.alias("a").join(action_prob_dist.alias("b"),
# [col("a.Pclass") == col("b.Pclass"),
# col("a.Embarked") == col("b.Embarked")]) \
# .select("a.*", "b.prob")
train = train.alias("t").join(action_prob_dist.alias("p"),
[col("t.Pclass") == col("p.Pclass")]) \
.select("t.*", "p.prob") \
.withColumn("action_list",lit("1 2 3"))

train = train.alias("t").join(train_header_parsing.alias("s"),[col("t.PassengerId") == col("s.PassengerId")]) \
.withColumn("vw_s_string",
concat(lit("shared "), col("t.PassengerId"), lit("|s "), col("vw_s_Sex"), lit(" "),
col("vw_s_Age"), lit(" "),
col("vw_s_SibSp"), lit(" "),
col("vw_s_Parch"), lit(" $"))) \
.select("t.PassengerId","t.Survived","t.Pclass","t.prob","t.action_list","vw_s_string")


# +------+------------------+---------+---------+
# |Pclass|         avg(Fare)|min(Fare)|max(Fare)|
# +------+------------------+---------+---------+
# |     3|13.675550210257411|      0.0|    69.55|
# |     1| 84.15468752825701|      0.0| 512.3292|
# |     2| 20.66218318109927|      0.0|     73.5|
# +------+------------------+---------+---------+

# explode action list step
train = train.alias("t").withColumn("action",explode(split(train["action_list"], " "))) \
.withColumn("action_chosen", when(col("Pclass") == col("action"), lit(1)).otherwise(lit(0))) \
.join(df_Fare.alias("F"),col("action") == col("F.Pclass")) \
.select("action","action_chosen",col("t.*"),col("F.Fare_avg"))

train = train.alias("t") \
.join(train_body_parsing.alias("a"),
[col("t.PassengerId") == col("a.PassengerId"),
col("t.action") == col("a.Pclass")],
how="left") \
.withColumn("vw_a_string",
concat(lit("|a "), col("vw_a_Ticket"), lit(" "),
col("vw_a_Cabin"), lit(" "),
col("vw_a_Embarked"), lit(" "),
col("vw_a_Fare"), lit(" $"))) \
.select(col("t.*"),"vw_a_string") \
.withColumn("action_cost_prob",
        when(col("t.action_chosen")==1,
            concat(col("t.Pclass"),lit(":"),col("t.Survived"),lit(":"),col("prob"))).otherwise("")) \
.withColumn("Fare_avg_new",when(col("action_chosen")==0,col("Fare_avg")).otherwise(None))

train = train.alias("t") \
.withColumn("vw_a_string_chosen",
concat(col("action_cost_prob"), lit(" "), col("vw_a_string"))) \
.withColumn("vw_a_string_other",
concat(lit("|a Fare:"), col("Fare_avg_new"), lit(" $")))

train = train.withColumn("vw_a_string_merge",when(col("t.action_chosen")==1,col("vw_a_string_chosen")).otherwise(col("vw_a_string_other")))
train = train.withColumn("vw_a_string_merge_new", concat(col("vw_a_string_merge"),lit("|i "),col("action"),lit(" $")))

train.select("PassengerId","action","vw_s_string","vw_a_string_merge_new").show(10,False)

grouped_train = train.groupby("PassengerId","vw_s_string").agg(collect_list("vw_a_string_merge_new").alias("vw_a_string_merge_new"))

grouped_train = grouped_train \
.withColumn("vw_a_string_multi_row",
concat_ws("\n", "vw_a_string_merge_new")) \
.select("PassengerId","vw_s_string","vw_a_string_multi_row")

grouped_train = grouped_train.withColumn("vw_string_final",concat(col("vw_s_string"),lit("\n"),col("vw_a_string_multi_row"),lit("\n$"))) \
.sort("PassengerId").select("vw_string_final").alias("value")
#grouped_train2.show(2,False)

#grouped_train2.sort("PassengerId").select("PassengerId","vw_string_final").show(2,False)

out_path = "/Users/lcui/study_toy_examples/titanic/titanic/parsed_train_filter_Pclass3"

grouped_train.coalesce(1).write.csv(out_path, quote="",mode="overwrite")


# test
path = "/Users/lcui/study_toy_examples/titanic/titanic/test.csv"
test = spark.read.csv(path,header=True,sep=",")

path = "/Users/lcui/study_toy_examples/titanic/titanic/gender_submission.csv"
y = spark.read.csv(path,header=True,sep=",")
test = test.alias("t").join(y,"PassengerId").drop("y.PassengerId")
test.printSchema()

test.groupby("Survived").count().show()
test = test.fillna('0', ["Survived","SibSp","Parch"])

test_male = test.filter(col("Sex") == "male").fillna('24', "Age")
test_female = test.filter(col("Sex") == "female").fillna('23', "Age")
test = test_male.union(test_female)
test = test.fillna('unknown', ["Pclass","Ticket","Cabin","Embarked"])

#test = test.fillna('unknown', ["Cabin"])#.show()

test = test.select(col("PassengerId").cast("integer"),
"Survived",
"Pclass",
"Sex",
col("Age").cast("float"),
col("SibSp").cast("integer"),
col("Parch").cast("integer"),
"Ticket",
col("Fare").cast("float"),
"Cabin",
"Embarked")

df_Fare = test.select("Pclass",col("Fare").cast("float")) \
.groupby("Pclass") \
.agg(mean("Fare").alias("Fare_avg"),min("Fare"),max("Fare"))


# +------+------------------+---------+---------+
# |Pclass|         avg(Fare)|min(Fare)|max(Fare)|
# +------+------------------+---------+---------+
# |     3|13.675550210257411|      0.0|    69.55|
# |     1| 84.15468752825701|      0.0| 512.3292|
# |     2| 20.66218318109927|      0.0|     73.5|
# +------+------------------+---------+---------+


 # |-- PassengerId: integer (nullable = true) ID
 # |-- Survived: string (nullable = false) Y
 # |-- Pclass: string (nullable = false) action
 # |-- Sex: string (nullable = true) shared
 # |-- Age: float (nullable = true) shared
 # |-- SibSp: integer (nullable = true) shared
 # |-- Parch: integer (nullable = true) shared
 # |-- Ticket: string (nullable = false) action not good too many levels
 # |-- Fare: float (nullable = true) action
 # |-- Cabin: string (nullable = false) action not good too many levels
 # |-- Embarked: string (nullable = false) action
test_shared = test.select("PassengerId","Sex","Age","SibSp","Parch")
test_action = test.select("PassengerId","Pclass","Ticket","Fare","Cabin","Embarked")

cat_features_shared = [a for a in ["Sex"]]
num_features_shared = [a for a in ["Age","SibSp","Parch"]]

cat_features_action = [a for a in ["Ticket","Cabin","Embarked"]]
num_features_action = [a for a in ["Fare"]]
#
test_header_parsing = test_shared
test_body_parsing = test_action

for name in cat_features_shared:
    test_header_parsing = test_header_parsing \
        .withColumn("vw_s_{0}".format(name),
            when(col(name).isNotNull(),concat(lit("{0}_".format(name)), regexp_replace(col(name), ' ', ''))).otherwise(lit("")))

for name in num_features_shared:
    test_header_parsing = test_header_parsing \
        .withColumn("vw_s_{0}".format(name),
            when(col(name).isNotNull(),concat(lit("{0}:".format(name)), regexp_replace(col(name), ' ', ''))).otherwise(lit("")))

for name in cat_features_action:
    test_body_parsing = test_body_parsing \
        .withColumn("vw_a_{0}".format(name),
            when(col(name).isNotNull(),concat(lit("{0}_".format(name)), regexp_replace(col(name), ' ', ''))).otherwise(lit("")))

for name in num_features_action:
    test_body_parsing = test_body_parsing \
        .withColumn("vw_a_{0}".format(name),
            when(col(name).isNotNull(),concat(lit("{0}:".format(name)), regexp_replace(col(name), ' ', ''))).otherwise(lit("")))

# test_header_parsing = test_header_parsing.withColumn("vw_s_string",concat(col("vw_s_Sex"),lit(" "),col("vw_s_Age")))
# test_header_parsing.show()

test_body_parsing.show()

real_features_shared = [a for a in test_header_parsing.columns if a.startswith("vw_s_")]
real_features_shared

real_features_action = [a for a in test_body_parsing.columns if a.startswith("vw_a_")]
real_features_action

action_prob_dist = test.groupby("Pclass").count()
total_N = test.count()
action_prob_dist = action_prob_dist.withColumn("total_N",lit(total_N).cast("integer")) \
.withColumn("count",col("count").cast("integer"))
action_prob_dist = action_prob_dist.withColumn("prob",col("count")/col("total_N"))
action_prob_dist.show()

# test = test.alias("a").join(action_prob_dist.alias("b"),
# [col("a.Pclass") == col("b.Pclass"),
# col("a.Embarked") == col("b.Embarked")]) \
# .select("a.*", "b.prob")
test = test.alias("t").join(action_prob_dist.alias("p"),
[col("t.Pclass") == col("p.Pclass")]) \
.select("t.*", "p.prob") \
.withColumn("action_list",lit("1 2 3"))

test = test.alias("t").join(test_header_parsing.alias("s"),[col("t.PassengerId") == col("s.PassengerId")]) \
.withColumn("vw_s_string",
concat(lit("shared "), col("t.PassengerId"), lit("|s "), col("vw_s_Sex"), lit(" "),
col("vw_s_Age"), lit(" "),
col("vw_s_SibSp"), lit(" "),
col("vw_s_Parch"), lit(" $"))) \
.select("t.PassengerId","t.Survived","t.Pclass","t.prob","t.action_list","vw_s_string")


# +------+------------------+---------+---------+
# |Pclass|         avg(Fare)|min(Fare)|max(Fare)|
# +------+------------------+---------+---------+
# |     3|13.675550210257411|      0.0|    69.55|
# |     1| 84.15468752825701|      0.0| 512.3292|
# |     2| 20.66218318109927|      0.0|     73.5|
# +------+------------------+---------+---------+

# explode action list step
test = test.alias("t").withColumn("action",explode(split(test["action_list"], " "))) \
.withColumn("action_chosen", when(col("Pclass") == col("action"), lit(1)).otherwise(lit(0))) \
.join(df_Fare.alias("F"),col("action") == col("F.Pclass")) \
.select("action","action_chosen",col("t.*"),col("F.Fare_avg"))

test = test.alias("t") \
.join(test_body_parsing.alias("a"),
[col("t.PassengerId") == col("a.PassengerId"),
col("t.action") == col("a.Pclass")],
how="left") \
.withColumn("vw_a_string",
concat(lit("|a "), col("vw_a_Ticket"), lit(" "),
col("vw_a_Cabin"), lit(" "),
col("vw_a_Embarked"), lit(" "),
col("vw_a_Fare"), lit(" $"))) \
.select(col("t.*"),"vw_a_string") \
.withColumn("action_cost_prob",
        when(col("t.action_chosen")==1,
            concat(col("t.Pclass"),lit(":"),col("t.Survived"),lit(":"),col("prob"))).otherwise("")) \
.withColumn("Fare_avg_new",when(col("action_chosen")==0,col("Fare_avg")).otherwise(None))

test = test.alias("t") \
.withColumn("vw_a_string_chosen",
concat(col("action_cost_prob"), lit(" "), col("vw_a_string"))) \
.withColumn("vw_a_string_other",
concat(lit("|a Fare:"), col("Fare_avg_new"), lit(" $")))

test = test.withColumn("vw_a_string_merge",when(col("t.action_chosen")==1,col("vw_a_string_chosen")).otherwise(col("vw_a_string_other")))
test = test.withColumn("vw_a_string_merge_new", concat(col("vw_a_string_merge"),lit("|i "),col("action"),lit(" $")))

test.select("PassengerId","action","vw_s_string","vw_a_string_merge_new").show(10,False)

grouped_test = test.groupby("PassengerId","vw_s_string").agg(collect_list("vw_a_string_merge_new").alias("vw_a_string_merge_new"))

grouped_test = grouped_test \
.withColumn("vw_a_string_multi_row",
concat_ws("\n", "vw_a_string_merge_new")) \
.select("PassengerId","vw_s_string","vw_a_string_multi_row")

grouped_test = grouped_test.withColumn("vw_string_final",concat(col("vw_s_string"),lit("\n"),col("vw_a_string_multi_row"),lit("\n$"))) \
.sort("PassengerId").select("vw_string_final").alias("value")
#grouped_test2.show(2,False)

#grouped_test2.sort("PassengerId").select("PassengerId","vw_string_final").show(2,False)

out_path = "/Users/lcui/study_toy_examples/titanic/titanic/parsed_test"

grouped_test.coalesce(1).write.csv(out_path, quote="",mode="overwrite")

