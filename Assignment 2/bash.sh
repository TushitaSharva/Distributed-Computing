#!/bin/bash

input_file="inp-params.txt"

# Read the first number from the first line of the input file
first_number=$(head -n 1 "$input_file" | awk '{print $1}')
rm -f proc*
mpic++  RC-CS21BTECH11022.cpp -g -o a.exe
mpic++  MK-CS21BTECH11022.cpp -g -o b.exe
mpirun --mca orte_base_help_aggregate 0 --oversubscribe -n $first_number ./a.exe
mpirun --mca orte_base_help_aggregate 0 --oversubscribe -n $first_number ./b.exe