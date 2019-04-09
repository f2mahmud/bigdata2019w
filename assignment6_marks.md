# CS 451/651 Data-Intensive Distributed Computing (Winter 2019)
## Assignment 6 Marking

**Student details**
Number:20551791
WATIAM:f2mahmud
github:f2mahmud

**Test 1** Getting your code to compile mark: 4.0/4

**Test 2** TrainSpamClassifier mark: 15.0/15

**Test 3** ApplySpamClassifier mark: 5.0/5

**Test 4** ApplyEnsembleSpamClassifier mark: 3.0/6

**Test 5** TrainSpamClassifier shuffle mark: 5.0/5

**Test 6** Q1 mark: 3.0/3

**Test 7** Q2 mark: 1.0/3

**Test 8** Q3 mark: 1.0/3

**Test 9** Q4 mark: 1.0/3

**Test 10** Q5 mark: 0.0/3

**Test 11** Being able to successfully run all your code on the Datasci cluster mark: 5.0/10

**late penalty :** None

**Feedback :** 
Error running ApplyEnsembleSpamClassifier:
'''
Exception in thread "main" org.apache.spark.SparkException: Task not serializable
        at org.apache.spark.util.ClosureCleaner$.ensureSerializable(ClosureCleaner.scala:403)
        at org.apache.spark.util.ClosureCleaner$.org$apache$spark$util$ClosureCleaner$$clean(ClosureCleaner.scala:393)
        at org.apache.spark.util.ClosureCleaner$.clean(ClosureCleaner.scala:162)
'''

Results of `Run the shuffle trainer 10 times on the britney dataset` should be different.


**Final grade**
mark: 43.0/60.0

summary: 20551791,f2mahmud,f2mahmud,43.0/60.0
