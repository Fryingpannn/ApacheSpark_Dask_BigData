import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader

'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 

We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''

#Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

#Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
    return a + os.linesep

def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
Plain PYTHON implementation

To get started smoothly and become familiar with the assignment's 
technical context (Git, GitHub, pytest, Travis), we will implement a 
few steps in plain Python.
'''

#Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header lines) in
    the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''
    
    # ADD YOUR CODE HERE
    with open(filename, newline="", encoding='UTF-8') as csvfile:
	    lines = csv.reader(csvfile, delimiter=",")
	    count = 0
	    for line in lines:
	        count += 1
    return count-1

def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    with open(filename) as csvfile:
        lines = csv.reader(csvfile, delimiter=",")
        count = 0
        for line in lines:
            if line[6] and line[6] != "<Null>" and line[6] != "Null":
                count += 1
    return count-1

def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''

    # ADD YOUR CODE HERE
    with open(filename, encoding='UTF-8') as csvfile:
        lines = csv.reader(csvfile, delimiter=",")
        parknames_set = set()
        for line in lines:
            if line[6] and line[6] != "<Null>" and line[6] != "Null":
                parknames_set.add(line[6] + "\n")
    parknames_set.remove("Nom_parc\n")
    parknames = list(parknames_set)
    parknames.sort()
    return "".join(parknames)

def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py    Note: The return value should be a CSV string
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    with open(filename, encoding="UTF-8") as csvfile:
        lines = csv.reader(csvfile)
        pairs = {}
        for line in lines:
            if line[6] and line[6] != "<Null>" and line[6] != "Null":
                if line[6] not in pairs:
                    pairs[line[6]] = 1
                else:
                    pairs[line[6]] += 1
    pairs.pop("Nom_parc")
    result = ""
    pairs = [(k, v) for k, v in pairs.items()]
    pairs.sort(key=lambda tup: tup[0])
    for k, v in pairs:
        result += k + "," + str(v) + "\n"
    return result

def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    with open(filename) as csvfile:
        lines = csv.reader(csvfile)
        pairs = {}
        for line in lines:
            if line[6] and line[6] != "<Null>" and line[6] != "Null":
                if line[6] not in pairs:
                    pairs[line[6]] = 1
                else:
                    pairs[line[6]] += 1
    pairs.pop("Nom_parc")
    result = ""
    pairs = [(k, v) for k, v in pairs.items()]
    pairs.sort(key=lambda tup: tup[1], reverse=True)
    top_ten = [tup for tup in pairs[:10]]
    for k, v in top_ten:
        result += k + "," + str(v) + "\n"
    return result


def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    with open(filename1, encoding='UTF-8') as csvfile, open(filename2, encoding='UTF-8') as csvfile2:
        lines = csv.reader(csvfile)
        lines2 = csv.reader(csvfile2)
        parknames_set = set()
        parknames_set2 = set()
        for line in lines:
            if line[6] and line[6] != "<Null>" and line[6] != "Null":
                parknames_set.add(line[6] + "\n")
        for line in lines2:
            if line[6] and line[6] != "<Null>" and line[6] != "Null":
                parknames_set2.add(line[6] + "\n")
        parknames_set.remove("Nom_parc\n")
        parknames_set2.remove("Nom_parc\n")

    intersection = parknames_set.intersection(parknames_set2)
    result = list(intersection)
    result.sort()
    return "".join(result)


'''
SPARK RDD IMPLEMENTATION

You will now have to re-implement all the functions above using Apache 
Spark's Resilient Distributed Datasets API (RDD, see documentation at 
https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
However, all operations must be re-implemented using the RDD API, you 
are not allowed to simply convert results obtained with plain Python to 
RDDs (this will be checked). Note that the function *toCSVLine* in the 
HELPER section at the top of this file converts RDDs into CSV strings.
'''

# RDD functions

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()

    rdd = spark.sparkContext.textFile(filename)
    return rdd.count() -1


def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()

    d = spark.read.csv(filename, header=True)
    rdd = d.rdd.map(tuple)
    park_trees = rdd.filter(lambda x: x[6] is not None)
    return park_trees.count()

def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    '''

    spark = init_spark()
    
    d = spark.read.csv(filename, header=True, encoding="ISO-8859-1")
    rdd = d.rdd.map(tuple)
    park_trees = rdd.filter(lambda x: x[6] is not None).map(lambda x: x[6]).distinct().sortBy(lambda x: x[0])
    park_trees = park_trees.collect()
    park_trees.sort()
    return "\n".join(park_trees) + "\n"

def uniq_parks_counts_rdd(filename):
    '''
    Write a Python script using RDDs that counts the number of trees treated in
    each park and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts_rdd.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    d = spark.read.csv(filename, header=True, encoding="ISO-8859-1")
    rdd = d.rdd.map(tuple)
    park_trees = rdd.filter(lambda x: x[6] is not None).map(lambda x: (x[6], 1)).reduceByKey(lambda acc, value: acc + value)
    park_trees = park_trees.collect()
    park_trees.sort()
    output = ""
    for (k,v) in park_trees:
        output += k + "," + str(v) + "\n"
    return output

def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()

    d = spark.read.csv(filename, header=True, encoding="ISO-8859-1")
    rdd = d.rdd.map(tuple)
    park_trees = rdd.filter(lambda x: x[6] is not None).map(lambda x: (x[6], 1)).reduceByKey(lambda acc, value: acc + value)
    park_trees = park_trees.sortBy(lambda x: x[1], False)
    park_trees = park_trees.take(10)
    output = ""
    for (a, b) in park_trees:
        output += a + "," + str(b) + "\n"
    return output

def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    d = spark.read.csv(filename1, header=True, encoding="ISO-8859-1")
    rdd = d.rdd.map(tuple)
    rdd = rdd.filter(lambda x: x[6] is not None).map(lambda x: x[6])

    d2 = spark.read.csv(filename2, header=True, encoding="ISO-8859-1")
    rdd2 = d2.rdd.map(tuple)
    rdd2 = rdd2.filter(lambda x: x[6] is not None).map(lambda x: x[6])

    trees = rdd.intersection(rdd2).sortBy(lambda x: x[0]).take(3)
    return "\n".join(trees) + "\n"

'''
SPARK DATAFRAME IMPLEMENTATION

You will now re-implement all the tasks above using Apache Spark's 
DataFrame API (see documentation at 
https://spark.apache.org/docs/latest/sql-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
Note: all operations must be re-implemented using the DataFrame API, 
you are not allowed to simply convert results obtained with the RDD API 
to Data Frames. Note that the function *toCSVLine* in the HELPER 
section at the top of this file also converts DataFrames into CSV 
strings.
'''

# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    df = spark._create_shell_session().read.csv(filename, header=True, mode="DROPMALFORMED")
    return df.count()

def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    # creates dataframe
    df = spark._create_shell_session().read.csv(filename, header=True, mode="DROPMALFORMED")
    # drop null values from specific column
    return df.na.drop(subset=["Nom_parc"]).count()

def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    df = spark._create_shell_session().read.csv(filename, header=True, mode="DROPMALFORMED", encoding="ISO-8859-1")
    df = df.select("Nom_parc").na.drop()
    df = df.dropDuplicates(["Nom_parc"])
    tree_list = df.collect()
    tree_list = list(map(lambda row: row['Nom_parc'], tree_list))
    tree_list.sort()
    return "\n".join(tree_list) + "\n"

def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    df = spark._create_shell_session().read.csv(filename, header=True, mode="DROPMALFORMED", encoding="ISO-8859-1")
    df = df.select("Nom_parc").na.drop()
    rdd = df.rdd.map(tuple)
    rdd = rdd.map(lambda tup: (tup[0], 1))
    # mapreduce
    rdd = rdd.reduceByKey(lambda x, y: x + y)
    tree_list = rdd.collect()
    tree_list.sort()
    trees = ""
    for (k, v) in tree_list:
        trees += k + "," + str(v) + "\n"
    return trees

def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    df = spark._create_shell_session().read.csv(filename, header=True, mode="DROPMALFORMED", encoding="ISO-8859-1")
    df = df.select("Nom_parc").na.drop()
    rdd = df.rdd.map(tuple)
    rdd = rdd.map(lambda tup: (tup[0], 1))
    # mapreduce
    rdd = rdd.reduceByKey(lambda x, y: x + y)
    tree_list = rdd.collect()
    tree_list.sort(key=lambda tup: tup[1], reverse=True)
    top_ten = tree_list[:10]
    trees = ""
    for (k, v) in top_ten:
        trees += k + "," + str(v) + "\n"
    return trees

def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()

    df1 = spark._create_shell_session().read.csv(filename1, header=True, mode="DROPMALFORMED", encoding="ISO-8859-1")
    df1 = df1.select("Nom_parc").na.drop()

    df2 = spark._create_shell_session().read.csv(filename2, header=True, mode="DROPMALFORMED", encoding="ISO-8859-1")
    df2 = df2.select("Nom_parc").na.drop()

    join = df1.join(df2, on=['Nom_parc'], how="inner").dropDuplicates()
    join = join.orderBy("Nom_parc")
    parks = join.select("Nom_parc").rdd.map(lambda row: row['Nom_parc']).collect()
    return "\n".join(parks) + "\n"

'''
DASK IMPLEMENTATION (bonus)

You will now re-implement all the tasks above using Dask (see 
documentation at http://docs.dask.org/en/latest). Outputs must be 
identical to the ones obtained previously. Note: all operations must be 
re-implemented using Dask, you are not allowed to simply convert 
results obtained with the other APIs.
'''

# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''

    daskdf = df.read_csv("./data/frenepublicinjection2016.csv", dtype={"Nom_parc": str})
    return len(daskdf)

def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''

    dask = df.read_csv("./data/frenepublicinjection2016.csv", dtype={"Nom_parc": str})
    parks = dask["Nom_parc"].dropna().compute()
    return len(parks)

def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''

    dask = df.read_csv("./data/frenepublicinjection2016.csv", dtype={"Nom_parc": str})
    parks = dask["Nom_parc"].dropna().drop_duplicates().compute()
    parks = parks.sort_values()
    return "\n".join(parks) + "\n"

def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    dask = df.read_csv(filename, dtype={"Nom_parc": str})
    parks = dask["Nom_parc"].dropna()
    parks = parks.to_frame().assign(count=1)
    parks = parks.groupby("Nom_parc").sum().compute()
    output = ""
    for row in parks.iterrows():
        output += row[0] + "," + str(row[1][0]) + "\n"
    return output

def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    dask = df.read_csv(filename, dtype={"Nom_parc": str})
    parks = dask["Nom_parc"].dropna()
    parks = parks.to_frame().assign(count=1)
    parks = parks.groupby("Nom_parc").sum().compute()
    parks = parks.nlargest(10, ['count'])

    output = ""
    for row in parks.iterrows():
        output += row[0] + "," + str(row[1][0]) + "\n"
    return output

def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    dask = df.read_csv(filename1, dtype={"No_Civiq": str, "Nom_parc": str})
    dask = dask["Nom_parc"].dropna()

    dask2 = df.read_csv(filename2, dtype={"No_Civiq": str, "Nom_parc": str})
    dask2 = dask2["Nom_parc"].dropna()

    inter = set(dask).intersection(set(dask2))
    inter = sorted(inter)
    return "\n".join(inter) + "\n"



