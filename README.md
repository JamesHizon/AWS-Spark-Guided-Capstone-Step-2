# AWS Spark Guided Capstone Step 2

### Description
After constructing the data structure diagram, I can start the build process. For a data system, the first step is to ingest the data sources. My Spring Capital data sources come from stock exchange daily submissions files in a semi-structured text format. This means that the records follow a certain formatting convention like JSON, but don't obey a tabular structure formatted for a relational database. The data ingestion process parses the semi-structured data out to be loaded into Spark.

### Learning Objectives:
1. Learn to parse CSV and JSON files.
2. Create a Spark DataFrame w/ defined schema.
3. Persist the Spark DataFrame into file system using partitioning.

### Prerequisites:
1. Python: basics, string manipulation, control flow, exception handling, JSON parsing
2. PySpark: RDD from text file, custom DataFrames, write with partitions, Parquet

### Azure jars
Originally, there are ```hadoop-azure.jar``` and ```azure-storage.jar``` files to work with. Since I ran into small issue and wanted more experience with AWS, I simply used EMR Cluster with EMR Notebook to run Spark via AWS Cloud Solutions. I also used AWS S3 Bucket to store the source files and create parquet files while applying partitioning.

