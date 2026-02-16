from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Transforming Names") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

name_set = ["alice smith ", "  juan cruz", " eva", "  Jack jack", "Solenia Cruz", "Mama mia ", " rj " , "Charles Xavier  ", " Philip doeman", "argus", " Ada smith", "  jane doeman", " "]

name_init = sc.parallelize(name_set)

# 1st Transformation: remvoe leading and trailing whitespaces
clean_name = name_init.map(lambda name: name.strip())

# 2nd Transformation: filter out names with leass than 4 characters or empty names
filter_name = clean_name.filter(lambda name: len(name) > 0 and len(name) > 4) 

# 3rd Transformation: capitalize the first letter of each name
capitalized_name = filter_name.map(lambda name: name.title())

# 4th Transformation: splits name into first and last name if applicable
split_name = capitalized_name.flatMap(lambda name: name.split(" "))

# 5th Transformation: removes duplicate names
unique_name = split_name.distinct()

# 6th Transformation: groups names by their first letter
group_name = unique_name.groupBy(lambda name: name[0]).mapValues(list)

# 7th Transformation: sorts names within each group in aphabetical order
sort_name = group_name.mapValues(lambda names: sorted(names))



final_results = sort_name.collect()


print("\nProcessed Names:\n")
for key, value in sorted(final_results):
    print(f"{key}: {value}")

spark.stop()