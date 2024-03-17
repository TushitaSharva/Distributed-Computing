#!/bin/bash

input_file="inp-params.txt"

# Read the first number from the first line of the input file
first_number=$(head -n 1 "$input_file" | awk '{print $1}')

# Compile the C++ code with mpic++
rm -f output*
mpic++ VC-CS21BTECH11022.cpp -g -o a.exe
mpic++ SK-CS21BTECH11022.cpp -g -o b.exe

# Run with x threads and append output to space-VC.log and space-SK.log
mpirun --mca orte_base_help_aggregate 0 --oversubscribe -n $first_number ./a.exe >> space-VC.log
mpirun --mca orte_base_help_aggregate 0 --oversubscribe -n $first_number ./b.exe >> space-SK.log