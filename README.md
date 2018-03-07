# SPARK-Scala
TP spark

1- connecte to service spark 2:
$ sudo su spark

2- lancer spark shell:
$ spark-shell

3-create SQLContext.
scala> val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

4-Read the .csv file Document from hdfs to spark:
 val df = spark.read.format("csv").option("header", "true").option("delimiter",";").option("inferSchema","true").load("/user/hadoop/Boubaker/resultats_electoraux.csv")
 
5-Show the Data: If you want to see the data in the DataFrame, then use the following command.
scala> df.show()

6-Use printSchema Method:If you want to see the Structure (Schema) of the DataFrame, then use the following command.
scala> df1.printSchema()
root
 |-- Rentrée: integer (nullable = true)
 |-- Année universitaire: string (nullable = true)
 |-- Établissement: string (nullable = true)
 |-- Type établissement: string (nullable = true)
 |-- Région: string (nullable = true)
 |-- Académie: string (nullable = true)
 |-- Sexe: string (nullable = true)
 |-- Classe âge: string (nullable = true)
 |-- Categorie de personnels: string (nullable = true)
 |-- Grandes disciplines: string (nullable = true)
 |-- Groupes CNU: string (nullable = true)
 |-- Sections CNU: string (nullable = true)
 |-- Quotite travail: integer (nullable = true)
 |-- ID académie: string (nullable = true)
 |-- ID région: string (nullable = true)
 |-- Identifiant établissement: string (nullable = true)
 |-- Code categorie personnels: string (nullable = true)
 |-- Code groupe CNU: integer (nullable = true)
 |-- Code grande discipline: string (nullable = true)
 |-- effectif: integer (nullable = true)
 |-- code_section_cnu: integer (nullable = true)
 |-- geolocalisation: string (nullable = true)
 
 scala> df1.select("Année universitaire","Categorie de personnels", "effectif","Établissement").show()
+-------------------+-----------------------+--------+--------------------+
|Année universitaire|Categorie de personnels|effectif|       Établissement|
+-------------------+-----------------------+--------+--------------------+
|          2016-2017|   Professeur et ass...|       1|Université Paris-...|
|          2016-2017|   Professeur et ass...|       1|Université Paris-...|
|          2016-2017|   Professeur et ass...|       1|Université Paris-...|
|          2016-2017|   Professeur et ass...|       1|Université Paris-...|
|          2016-2017|   Professeur et ass...|       1|Université Paris-...|
|          2016-2017|   Professeur et ass...|       1|Université Paris-...|
|          2016-2017|   Professeur et ass...|       1|Université Paris-...|
|          2016-2017|   Enseignants du 2n...|       1|École nationale s...|
|          2016-2017|   Maître de confére...|       2|École nationale s...|
|          2016-2017|   Professeur et ass...|       1|École nationale s...|
|          2016-2017|   Enseignants du 2n...|       2|École nationale s...|
|          2016-2017|   Maître de confére...|       4|École nationale s...|
|          2016-2017|   Enseignants du 2n...|       1|École nationale s...|
|          2016-2017|   Maître de confére...|       3|École nationale s...|
|          2016-2017|   Maître de confére...|       1|École nationale s...|
|          2016-2017|   Enseignants du 2n...|       5|Université de Cer...|
|          2016-2017|   Maître de confére...|       3|Université de Cer...|
|          2016-2017|   Maître de confére...|       1|Université de Cer...|
|          2016-2017|   Enseignants du 2n...|       9|Université de Cer...|
|          2016-2017|   Maître de confére...|       4|Université de Cer...|
+-------------------+-----------------------+--------+--------------------+
only showing top 20 rows

7- Transformations on data frame:

------>Filter:
scala> val df1Filtered = df1.filter(df1("Sections CNU") === "Mathématiques")
------>Select:
scala> df1Filtered.select("Année universitaire","Categorie de personnels", "Sections CNU","effectif","Établissement").show()
+-------------------+-----------------------+-------------+--------+--------------------+
|Année universitaire|Categorie de personnels| Sections CNU|effectif|       Établissement|
+-------------------+-----------------------+-------------+--------+--------------------+
|          2016-2017|   Enseignants du 2n...|Mathématiques|       2|École nationale s...|
|          2016-2017|   Enseignants du 2n...|Mathématiques|       9|Université de Cer...|
|          2016-2017|   Maître de confére...|Mathématiques|       7|Université de Cer...|
|          2016-2017|   Enseignants du 2n...|Mathématiques|       4|Université de Cor...|
|          2010-2011|   Maître de confére...|Mathématiques|       2|Université d'Évry...|
|          2010-2011|   Professeur et ass...|Mathématiques|       3|Université Nice -...|
|          2014-2015|   Professeur et ass...|Mathématiques|       6|Université Paris-Sud|
|          2016-2017|   Enseignants du 2n...|Mathématiques|       1|Université des An...|
|          2016-2017|   Enseignants du 2n...|Mathématiques|       3|Université de La ...|
|          2016-2017|   Maître de confére...|Mathématiques|       2|Université de La ...|
|          2010-2011|   Maître de confére...|Mathématiques|       1|Université Paris ...|
|          2010-2011|   Maître de confére...|Mathématiques|       3|Université Paris ...|
|          2010-2011|   Maître de confére...|Mathématiques|       4|Université Paris ...|
|          2010-2011|   Professeur et ass...|Mathématiques|       3|Université Paris ...|
|          2010-2011|   Maître de confére...|Mathématiques|      19|Aix-Marseille uni...|
|          2010-2011|   Maître de confére...|Mathématiques|       7|Université de Bou...|
|          2011-2012|   Professeur et ass...|Mathématiques|       8|Université de Bor...|
|          2014-2015|   Maître de confére...|Mathématiques|       2|Université Paris ...|
|          2014-2015|   Maître de confére...|Mathématiques|       8|Université Paris ...|
|          2016-2017|   Maître de confére...|Mathématiques|       3|Université de la ...|
+-------------------+-----------------------+-------------+--------+--------------------+
only showing top 20 rows

---->Filter/ Fonction: Count.
scala> df1.filter(df1("Rentrée") === "2016").count()
res17: Long = 25560

-----> groupBy
scala> df1.groupBy("Categorie de personnels").count().show()
+-----------------------+-----+
|Categorie de personnels|count|
+-----------------------+-----+
|   Professeur et ass...|62143|
|   Enseignants du 2n...|22659|
|   Maître de confére...|84341|
+-----------------------+-----+
----> SORT:
scala> df1.select("Categorie de personnels", "Categorie de personnels").sort("Categorie de personnels").show(10)
+-----------------------+-----------------------+
|Categorie de personnels|Categorie de personnels|
+-----------------------+-----------------------+
|   Enseignants du 2n...|   Enseignants du 2n...|
|   Enseignants du 2n...|   Enseignants du 2n...|
|   Enseignants du 2n...|   Enseignants du 2n...|
|   Enseignants du 2n...|   Enseignants du 2n...|
|   Enseignants du 2n...|   Enseignants du 2n...|
|   Enseignants du 2n...|   Enseignants du 2n...|
|   Enseignants du 2n...|   Enseignants du 2n...|
|   Enseignants du 2n...|   Enseignants du 2n...|
|   Enseignants du 2n...|   Enseignants du 2n...|
|   Enseignants du 2n...|   Enseignants du 2n...|
+-----------------------+-----------------------+
only showing top 10 rows

---> aggrégation:
scala> df1.agg(sum("effectif"),min("effectif"),max("effectif"), avg("effectif")
     | ).show()
+-------------+-------------+-------------+----------------+
|sum(effectif)|min(effectif)|max(effectif)|   avg(effectif)|
+-------------+-------------+-------------+----------------+
|       488975|            1|           78|2.89089705160722|
+-------------+-------------+-------------+----------------+

9- Jointure entre deux dataFrames:
val joinedDF = df1.join(df2, df1("Établissement") === df2("Établissement"), "inner").show()

