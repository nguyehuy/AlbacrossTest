# IP Range Elimination

This implementation eliminates the overlapping IP ranges using Spark and Scala. Given a set of IP ranges, it returns a set of mutually exclusive IP ranges. The input IP ranges are represented as start and end IP addresses.
The implementation for the test case generator is also included.

### Input
The input can be written either in a text file or in Postgre database.
1. #### Csv File
This input can be stored as a csv file in the this directory `input-data` of the repo. The Ip ranges can be splitted and stored in the multiple csv files, but these files **MUST** be located in the same sub-directory in directory `input-data`. The sample input csv file has the path `input-data/test.csv`:
~~~~
197.203.0.0, 197.206.9.255
197.204.0.0,197.204.0.24
201.233.7.160, 201.233.7.168
201.233.7.164, 201.233.7.168
201.233.7.167, 201.233.7.167
203.133.0.0, 203.133.255.255
~~~~
where each line represents a single IP range in the format start-end.

2. #### Postgre
The Postgre connection is configured in the file `src/main/resources/metadata/db.conf`. When docker file is running, a database name `test` will be generated and this code uses this database to store table input and output. The input table name is provided by user and **MUST** has the structure with two columns `start`, `end` and each row represents a single IP range in the format start-end. Sample input Postgre table:

|          start|            end| 
|---------------|---------------|
|197.203.0.0|197.206.9.255|
|197.204.0.0|197.204.0.024|
|201.233.7.160|201.233.7.168|
|201.233.7.164|201.233.7.168|
|201.233.7.167|201.233.7.167|
|203.133.0.0|203.133.255.255|

### Output
Depending the type of input, the implementation will generate the output accordingly :
1. #### Csv File
The code returns the output and save it in some csv files. When the code is running, the user will be asked to provide the name of a directory that will contains these csv files. After running, this directory will be created inside directory `output-data` of the repo. User can navigate to this directory to see the output files. They have the same format as the input file. Sample output file has the path `output-data/output/output.csv`:
~~~~
197.203.0.0, 197.203.255.255
197.204.0.25, 197.206.9.255
201.233.7.160, 201.233.7.163
203.133.0.0, 203.133.255.255
~~~~

2. #### Postgre
The output will be stored in a table in the same database as the input table. It has the name provided by user and has the similar structure as the input table. Sample output Postgre table:

|     start|         end|
|-------------|---------------|
|  197.203.0.0|197.203.255.255|
| 197.204.0.25|  197.206.9.255|
|201.233.7.160|  201.233.7.163|
|  203.133.0.0|203.133.255.255|

## How to run code

We created a simpler version of a spark cluster in docker-compose, the main goal of this cluster is to provide you with a local environment to test the distributed nature of your spark apps without making any deploy to a production cluster.

In the docker-compose.yaml, for both spark master and worker I configured the following environment variables:

|Environment|Description|
|---------------|---------------|
|SPARK_MASTER|Spark master url|
|SPARK_WORKER_CORES|Number of cpu cores allocated for the worker|
|SPARK_WORKER_MEMORY| Amount of ram allocated for the worker|
|SPARK_DRIVER_MEMORY| Amount of ram allocated for the driver programs|
|SPARK_EXECUTOR_MEMORY| Amount of ram allocated for the executor programs|
|SPARK_WORKLOAD|The spark workload to run(can be any of master, worker, submit)|

The `docker-compose.yaml` also contains the configurations of postgresql, which will be listening in the port 5432. To run the job :
### Step 1
Clone the repository to your local machine.
### Step 2
Run the command :
```
docker build -t cluster-apache-spark:3.1.1 .
```
### Step 3
Run the command :
```
docker compose up 
```
### Step 4
You can validate your cluster just access the spark UI on each worker & master URL:

* Spark master: http://localhost:9090
* Spark worker 1: http://localhost:9091
* Spark worker 2: http://localhost:9092

You also can validate if the Postgre container has been established.

### Step 5 (Only perform when you want to process with your own input)
* If you want to process input from local file: Store your input in a csv file inside directory `input-data` of the repo. In case you want to store it in multiple files, locate these files in a sub-directory inside directory `input-data` of the repo.
* If you want to process input from Postgre server : Store your input in a table in `test` database. This table **MUST** have the schema described in section **Input**.

### Step 6
To submit the app connect to one of the workers or the master, execute the command the bash container of master or worker node:
```
./spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 1G --executor-memory 1G remove-intersection-ip.jar -g <param_1> -t <param_2> -i <param_3> -o <param_4>
```
where
1. `param_1` takes the values :
   `process-with-gen-test` : Generate test input and process the this generated input.
* `process-with-existing-test` : Process the existing input.
2. `param_2` takes the values :
* `local-file` : Run with input from local file and return output in local file
* `postgre` : Run with input from postgre server  and return output in postgre server
3. `param_3` indicate the storing service for input:
* If `param_2` is `local-file`, `param_3` will be the path to the directory or file that stores input
* If `param_2` is `postgre`, `param_3` will be table name that stores input Ip ranges in Postgre server
4. `param_4` indicate the storing service for output:
* If `param_2` is `local-file`, `param_3` will be the path to the directory that stores output files
* If `param_2` is `postgre`, `param_3` will be table name that stores output Ip ranges in Postgre server

### Note
If you want to generate the input, the `param_3` will be considered as the name of directory. The generated input will be stored in some csv files in this directory.

### Example
1. If you want to generate the test input and process it and return the output in directory, in the step 6, run the command in container of master or worker node:
```
./spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 1G --executor-memory 1G remove-intersection-ip.jar -g process-with-gen-test -t local-file -i input -o output
```
The generated input test will be store in the directory `input-data/input/` and the output of this input will be stored in directory `outputput-data/output/`

2. If you want to process your own input in local file, in the step 5 :
* Create a csv file or a directory that stores your input in directory `input-data` of the repo. In this example, I store my input in a single file name `test.csv`
* Run the command in container of master or worker node
```
./spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 1G --executor-memory 1G remove-intersection-ip.jar -g process-with-existing-test -t local-file -i test.csv -o output
```
The output  of this input will be stored in some files in directory `outputput-data/output/`

3. If you want to generate the test input and process it on Postgre server, in the step 6, run the command in container of master or worker node:
```
./spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 1G --executor-memory 1G remove-intersection-ip.jar -g process-with-gen-test -t postgre -i input_table -o output_table
```
The generated input test will be stored in the table `input_table` and the output of this input will be stored in table `input_table` of the `test` database.

4. If you want to process your own input in Postgre, in the step 5 :
* Create a table that stores your input in the `test` database (in this example, I name the table as `input_table`)
* Run the command in container of master or worker node
```
./spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 1G --executor-memory 1G remove-intersection-ip.jar -g process-with-existing-test -t postgre -i input_table -o output_table
```
The output  of this input will be stored in table `output_table` in the same database as table `input_table`.

### Note
If you want to run the code using Postgre storing service, after the step 2, you need to create a database with the name is the same as the cofiguration given in the file `src/main/resources/metadata/db.conf`

## Implementation
The solution for this task will be much easier if the input this small which we can load all the Ip ranges to memory an process it. However, if this is not the case, when the input is very big, we need to obtain a solution such that it can process paralelly on partitioned data. We transfer this task to a pure SQL task so that we can adapt SparkSQL to handle big data.
1. Load the input IP ranges into a Spark DataFrame with two columns `start` and `end` represents the start and end Ip. Note that each part of the Ip is formatted in three digit so that we can order the Ip as the string:

|          start|            end| 
|---------------|---------------|
|197.203.000.000|197.206.009.255|
|197.204.000.000|197.204.000.024|
|201.233.007.160|201.233.007.168|
|201.233.007.164|201.233.007.168|
|201.233.007.167|201.233.007.167|
|203.133.000.000|203.133.255.255|

2. Create a new Spark DataFrame to store the Ip along with a "flag" varible indicates this Ip is start or end Ip. The start Ip has flag 1, end Ip has flag -1. Then sort the DataFrame by the Ip address:

|          start|           flag|
|---------------|---------------|
|197.203.000.000|   1           |
|197.204.000.000|   1           |
|197.204.000.024|  -1           |
|197.206.009.255|  -1           |
|201.233.007.160|   1           |
|201.233.007.164|   1           |
|201.233.007.167|  -1           |
|201.233.007.167|   1           |
|201.233.007.168|  -1           |
|201.233.007.168|  -1           |
|203.133.000.000|   1           |
|203.133.255.255|  - 1          |

3. Append a new column called `cumulative-flag` to perform the cumulative sum of the `flag` column:

|          start|flag|cumulative-flag|
|---------------|----|----------------|
|197.203.000.000|   1|               1|
|197.204.000.000|   1|               2|
|197.204.000.024|  -1|               1|
|197.206.009.255|  -1|               0|
|201.233.007.160|   1|               1|
|201.233.007.164|   1|               2|
|201.233.007.167|   1|               2|
|201.233.007.167|  -1|               2|
|201.233.007.168|  -1|               0|
|201.233.007.168|  -1|               0|
|203.133.000.000|   1|               1|
|203.133.255.255|  -1|               0|

4. Create a new column called `end` which is the next value of the column `start` in the DataFrame:

|          start|flag|cumulative-flag|            end|
|---------------|----|----------------|---------------|
|197.203.000.000|   1|               1|197.204.000.000|
|197.204.000.000|   1|               2|197.204.000.024|
|197.204.000.024|  -1|               1|197.206.009.255|
|197.206.009.255|  -1|               0|201.233.007.160|
|201.233.007.160|   1|               1|201.233.007.164|
|201.233.007.164|   1|               2|201.233.007.167|
|201.233.007.167|   1|               2|201.233.007.167|
|201.233.007.167|  -1|               2|201.233.007.168|
|201.233.007.168|  -1|               0|201.233.007.168|
|201.233.007.168|  -1|               0|203.133.000.000|
|203.133.000.000|   1|               1|203.133.255.255|
|203.133.255.255|  -1|               0|           null|

5. Each row the `cumulative-flag` is 1, we have the value `start` of the DataFrame being the start Ip of a non-overlapping Ip range. The value in column `end` is the end Ip of this range. We filter the DataFrame to keep the rows have `cumulative-flag` 1, the non-overlapping Ip ranges will be represented by the columns `start` and `end` in the result:

|          start|cumulative-flag|            end|
|---------------|--------------------|---------------|
|197.203.000.000|               1|197.204.000.000|
|197.204.000.024|               1|197.206.009.255|
|201.233.007.160|               1|201.233.007.164|
|203.133.000.000|               1|203.133.255.255|

However, the non-overlapping Ip ranges we obtained here is not the result that we wanted. For example, in the first row of the DataFrame we have the range (`197.203.000.000` , `197.204.000.000`), but the Ip `197.204.000.000` should be excluded, so that the ideal result must be (`197.203.000.000` , `197.203.255.255`). We need to perform extra step to handle this case. From the DataFrame in the step 4, we add two more columns `cumulative-flag-next` , `cumulative-flag-prev` stored the next and previous values of columns `cumulative-flag` in the DataFrame:


|          start|flag|cumulative-flag|            end|cumulative-flag-next|cumulative-flag-prev|
|---------------|----|----------------|---------------|---------------------|---------------------|
|197.203.000.000|   1|               1|197.204.000.000|                    2|                 null|
|197.204.000.000|   1|               2|197.204.000.024|                    1|                    1|
|197.204.000.024|  -1|               1|197.206.009.255|                    0|                    2|
|197.206.009.255|  -1|               0|201.233.007.160|                    1|                    1|
|201.233.007.160|   1|               1|201.233.007.164|                    2|                    0|
|201.233.007.164|   1|               2|201.233.007.167|                    2|                    1|
|201.233.007.167|   1|               2|201.233.007.167|                    2|                    2|
|201.233.007.167|  -1|               2|201.233.007.168|                    0|                    2|
|201.233.007.168|  -1|               0|201.233.007.168|                    0|                    2|
|201.233.007.168|  -1|               0|203.133.000.000|                    1|                    0|
|203.133.000.000|   1|               1|203.133.255.255|                    0|                    0|
|203.133.255.255|  -1|               0|           null|                 null|                    1|

6. Now we can filter the DataFrame to only keep the rows have `cumulative-flag` 1 :

|          start|flag|cumulative-flag|            end|cumulative-flag-next|cumulative-flag-prev|
|---------------|----|----------------|---------------|---------------------|---------------------|
|197.203.000.000|   1|               1|197.204.000.000|                    2|                 null|
|197.204.000.024|  -1|               1|197.206.009.255|                    0|                    2|
|201.233.007.160|   1|               1|201.233.007.164|                    2|                    0|
|203.133.000.000|   1|               1|203.133.255.255|                    0|                    0|

7. We exclude the case mentioned in step 5 by checking the values in the columns `cumulative-flag-next` and `cumulative-flag-prev`:
* For `start` column : If the `cumulative-flag-prev` not 0, we add 1 to this value. Otherwise, we keep it unchanged.
* For `end` column : If the `cumulative-flag-net` not 0, we subtract 1 from this value. Otherwise, we keep it unchanged.

After perform this step, we have the new columns `start` and `end` represents the non-overlapping Ip ranges :

|     start|         end|
|-------------|---------------|
|  197.203.000.000|197.203.255.255|
| 197.204.000.025|  197.206.009.255|
|201.233.007.160|  201.233.007.163|
|  203.133.000.000|203.133.255.255|

8. Finnal, we fillter the DataFrame to only the rows where `end` >= `start` and format the Ip addresses:

|     start|         end|
|-------------|---------------|
|  197.203.0.0|197.203.255.255|
| 197.204.0.25|  197.206.9.255|
|201.233.7.160|  201.233.7.163|
|  203.133.0.0|203.133.255.255|

This is the final output of this implementation.

## Test Generator
We provide the test generator by running the command with parameter `process-with-existing-test` mentioned in section **How to run code**.The generator will automatically generate `n` random Ip ranges where `n` is a random number betweem 1 and 100.