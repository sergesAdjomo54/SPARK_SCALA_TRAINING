# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/songs/data-001/

# COMMAND ----------

sc.textFile("databricks-datasets/songs/data-001/header.txt").collect()

# COMMAND ----------

#utilisons textFile pour charger l'un des fichiers de données
dataRDD = sc.textFile("/databricks-datasets/songs/data-001/part-000*")
#utilisons take pour afficher les 3 premières lignes des données.
dataRDD.take(3)

# COMMAND ----------

#construisons une fonction qui prend une ligne de texte et renvoie un tableau de champs analysés
#pour detecter les types de champs de manière dynamique
#Split the header by its separator
header = sc.textFile("/databricks-datasets/songs/data-001/header.txt").map(lambda line: line.split(":")).collect()

#Create the Python function
def parseLine(line):
  tokens = zip(line.split("\t"), header)
  parsed_tokens = []
  for token in tokens:
    token_type = token[1][1]
    if token_type == 'double':
      parsed_tokens.append(float(token[0]))
    elif token_type == 'int':
      parsed_tokens.append(-1 if '-' in token[0] else int(token[0])) # Taking care of fields with --
    else:
      parsed_tokens.append(token[0])
  return parsed_tokens


# COMMAND ----------

display(header)
type(header)

# COMMAND ----------

#A5
# ici on recupère le type de donnée par ligne
from pyspark.sql.types import *

def strToType(str):
  if str == 'int':
    return IntegerType()
  elif str == 'double':
    return DoubleType()
  else:
    return StringType()

schema = StructType([StructField(t[0], strToType(t[1]), True) for t in header])

# COMMAND ----------

#A6
# On cree le dataFrame combiné avec le schema de donnée précedent

df = sqlContext.createDataFrame(dataRDD.map(parseLine), schema)

# COMMAND ----------

#A7
# On cree un table tempon pour pouvoir effectués des requetes facilement dessus

#df.registerTempTable("songsTable")

df.createOrReplaceTempView("songsTable")

# COMMAND ----------

#A8
# sauve la Table dans le cache vu que nous allons l'appeler plusieurs fois
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
#%sql cache table songsTable
#OK

# COMMAND ----------

#A9

df1 = spark.sql("select * from songsTable limit 15")
display(df1)

# COMMAND ----------

####################################################
# PARTIE B: EXPLORATION ET VISUALISATION DE DONNEES#
####################################################

# COMMAND ----------

#B1 
# On expose le shema de donnée avec la fonction printSchema()

table("songsTable").printSchema()

# COMMAND ----------

#B2
# On compte le nombre de lignes de la table

totalRow = table("songsTable").count()
nbRow = df1.count()
print(nbRow)
print(totalRow)

# COMMAND ----------

pip install ggplot

# COMMAND ----------

pip install --upgrade pip

# COMMAND ----------

pip install ggplot

# COMMAND ----------

#B3
# on affiche les données en temps réel

from ggplot import *
baseQuery = sqlContext.sql("select avg(duration) as duration, year from songsTable group by year")
df_filtered = baseQuery.filter(baseQuery.year > 0).filter(baseQuery.year < 2010).toPandas()
plot = ggplot(df_filtered, aes('year', 'duration')) + geom_point() + geom_line(color='blue')
display(plot)


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


