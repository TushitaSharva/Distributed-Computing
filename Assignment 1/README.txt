PROGRAMMING ASSIGNMENT 1 : IMPLEMENTING VECTOR CLOCKS

-------------------------------------------------------
This directory consists of the following files:
 - VC-CS21BTECH11022.cpp
 - SK-CS21BTECH11022.cpp
 - report.pdf
 - README.txt
 - inp-params.txt
 - testcases.txt
 - bash.sh

-----------------------------------------------------------------
'inp-params.txt' is a sample input file. The file contains
the following information, as mentioned in the problem statement. 
The first line contains four numbers (seperated by space)
 - n: number of processes
 - lambda: exponential distribution parameter for sleep time between events
 - alpha: ratio of no. of internal events to msg send events
 - m: number of messages to be sent by a processes
The next 'n' lines contain the adjacency list representation
of the graph topology.


NOTE BEFORE RUNNING:
There must be inp-params.txt.
IT SHOULD HAVE ADJACENCY LIST IN THE INCREASING ORDER OF PROCESS ID'S ONLY.
THEY SHOULD BE IN ONE-BASED INDEXING ONLY.
I had provided some sample test cases in the zip file, which can be modified accordingly.
-----------------------------------------------------------------
This directory contains a bash script "bash.sh". To run this
file,
Change the permissions of the executable 
 - chmod u+x bash.sh
Run the script
 - ./bash.sh
It will execute both the scripts, VC-CS21BTECH11022.cpp and also SK-CS21BTECH11022.cpp.
It will output the TIMESTAMPS into output-VC.log and output-SK.log
and MESSAGE SPACE utilised by each process to space-VC.log anf space-SK.log
files respectively. 
The bash iteself will remove the TIMESTAMP FILES, since they are appended by my program.
However, SPACE FILES will not be removed since observing space utilized in different scenarios was desirable for my observations in my report.

-----------------------------------------------------------------
However, to compile manually, these are the steps:

VC ALGO:
mpic++ VC-CS21BTECH11022.cpp -g -o a.exe
mpirun --mca orte_base_help_aggregate 0 --oversubscribe -n <number of processes> ./a.exe

SK ALGO
mpic++ SK-CS21BTECH11022.cpp -g -o b.exe
mpirun --mca orte_base_help_aggregate 0 --oversubscribe -n <number of processes> ./b.exe

After execution please delete the output files.
-----------------------------------------------------------------