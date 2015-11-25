There is a file with 100,000 lines. Each line has a record "ID;amount". There are 1000 unique id in a file. 
The Program sums amounts for each ID and writes aggregation result in a single file.


So, for running the program you have to run sbt command:
```
run input=examples/bigfile.txt  output=examples/output.txt blockSize=1024 workers=4
```
* input - input file
* output - output file
* blockSize - a buffer size for reading file. The default value is 1024*1024 bytes
* workers - a number of threads for reading file. The default value is 4

The results for the file examples/bigfile.txt:
--------------------------
|blockSize|workers|Time  |
--------------------------
|1024     |1      |430 ms|
--------------------------
|1024     |4      |127 ms|
--------------------------
|1024     |7      |164 ms|
--------------------------
|1024     |9      |128 ms|
--------------------------
