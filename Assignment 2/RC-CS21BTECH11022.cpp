/*
    Janga Tushita Sharva
    CS21BTECH11022
    Aim: To implement and analyze Roucairol and Carvalho's Algorithm fro MUTEX
    Algorithm:
    All processes work induvidually for outCSTime (variable, exponential) seconds
    Then they would want to enter mutual exclusion



    - reqSet: a set which contains those it needs to send request and recieve reply from
            (initially it should be set of all pids lesser than the rank)
    - defSet : a set containing processes whose messages are deferred
    - repSet : a set containing processes to whom reply was sent

    REPLY:
    - Add the requesting process ID to the repSet
    - send reply message

    REQUEST:
    Send requests to all elements in the reqSet

    RECEIVE:
    // Keep listening for messages. There can be two types of messages: REPLY, REQUEST
    - If recieved a REQ message:
        execute REPLY if:
            - The process has no unfullfilled request and it is not executing CS
            - The unfullfilled request has larger timestamp than that of received request
        Else add to defSet
    - If recieved a REPLY message:
            - Remove from the reqSet this process
            - When reqSet is null, put reqSet = repSet, repSet = null
            - Execute critical section
            - After done with critical section, send REPLY to defSet processes. make defSet null.
*/

#include <iostream>
#include <set>
#include <random>
#include <string.h>
#include <unistd.h>
#include <string>
#include <stdlib.h>
#include <cstdlib>
#include <ctime>
#include <sstream>
#include <mpi.h>
#include <time.h>
#include <chrono>
#include <fstream>
#include <atomic>
#include <mutex>
#include <thread>
#define D 1
#define REQ 1
#define REP 2
#define DONE 3
using namespace std;

class my_data
{
public:
    int pid;
    int lamport_clock;
    int size;
    int total_requests;
    int requests_sent;
    int alpha;
    int beta;
};

ifstream inputfile;
set<int> reqSet;
set<int> defSet;
set<int> repSet;
int done;
bool inCS = false;
int request_time = -1; // Stores the time of requesting
bool grantWithMe = false;

/* Helper Function: Generates a random number from an exponential distribution with a mean of 'exp_time'. */
double Timer(float exp_time)
{
    default_random_engine generate;
    exponential_distribution<double> distr(1.0 / exp_time);
    return distr(generate);
}

/* Function to simulate critical section */
void criticalSection(my_data *data)
{
    grantWithMe = true;
    inCS = true;
    sleep(Timer(data->beta));
    inCS = false;
    data->requests_sent++;

    for (auto i : defSet)
    {
        MPI_Send(&data->lamport_clock, 1, MPI_INT, i, REP, MPI_COMM_WORLD);
        defSet.erase(i);
    }

    if (data->requests_sent == data->total_requests)
    {
        done++;
        for (int i = 0; i < data->size; i++)
        {
            if (i != data->pid)
            {
                MPI_Send(&data->lamport_clock, 1, MPI_INT, i, DONE, MPI_COMM_WORLD);
            }
        }
    }

    return;
}

void performer_func(my_data *data)
{
    while (data->requests_sent < data->total_requests)
    {
        request_time = -1;
        data->lamport_clock += 1;
        sleep(Timer(data->alpha));
        request_time = data->lamport_clock;

        if (!grantWithMe)
        {
            data->lamport_clock += 1;

            for (auto i : repSet)
            {
                reqSet.insert(i);
            }

            repSet.clear();

            for (auto i : reqSet)
            {
                MPI_Send(&data->lamport_clock, 1, MPI_INT, i, REQ, MPI_COMM_WORLD);
            }
        }

        else if (grantWithMe)
        {
            criticalSection(data);
        }
    }

    return;
}

void reciever_func(my_data *data)
{
    while (true)
    {
        int recv_msg = 0;
        MPI_Status status;
        MPI_Recv(&recv_msg, 1, MPI_INT, MPI::ANY_SOURCE, MPI::ANY_TAG, MPI_COMM_WORLD, &status);
        data->lamport_clock = max(data->lamport_clock, recv_msg);
        data->lamport_clock += 1;

        int sender = status.MPI_SOURCE;

        if (status.MPI_TAG == REQ)
        {
            if(inCS == true) // If I am currently executing critical section, I will put the incoming request in defSet
            {
                defSet.insert(sender);
            }

            else if(request_time != -1 && recv_msg > request_time) // I am not in CS, I am requesting, but the msg I recvd has greater time stamp than me, I will put it in defSet
            {
                defSet.insert(sender);
            }

            else if(request_time != -1 && recv_msg <= request_time) // I am requesting, but the msg I recvd has smaller timestamp than me, I will reply
            {
                MPI_Send(&data->lamport_clock, 1, MPI_INT, sender, REP, MPI_COMM_WORLD);
            }

            else if(request_time == -1) // I am not even requesting, I will reply
            {
                MPI_Send(&data->lamport_clock, 1, MPI_INT, sender, REP, MPI_COMM_WORLD);
            }

            else
            {
                std::cout << "Request time " << request_time << "\n";
                std::cout << "Message time " << recv_msg << "\n";
                std::cout << "My req time " << request_time << "\n";
                std::cout << "IN CS? " << inCS << "\n";
                std::cout << "Error Here\n";
            }
        }

        else if (status.MPI_TAG == REP)
        {
            reqSet.erase(status.MPI_SOURCE); // When I recieve a reply, I will remove from the reqSet, impyling my request has been catered with their reply

            if (reqSet.empty() == true) // If everyone I requested got a reply, ready to enter CS, but before that, I will modify the list I need to request before entering the CS next time.
            {
                for (auto i : repSet)
                {
                    reqSet.insert(i);
                    repSet.erase(i);
                }

                criticalSection(data);
            }
        }

        else if (status.MPI_TAG == DONE)
        {
            done++;
        }

        if (done == data->size)
        {
            break;
        }
    }

    return;
}

int main(int argc, char *argv[])
{
    /* Reading paramters starts */

    int n, k, alpha, beta; // Input parameters
    inputfile.open("inp-params.txt");

    inputfile >> n >> k >> alpha >> beta;
    inputfile.close();

    /* Reading paramters ends */

    /* MPI INITIALISATION*/
    int pid, size;
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    /* Initializing reqSet with processes of lower ranks */
    for (int i = 0; i < pid; i++)
    {
        reqSet.insert(i);
    }

    /*Initialising parameters to be sent over threads */
    my_data *data = new my_data;
    data->pid = pid;
    data->size = size;
    data->total_requests = k;
    data->requests_sent = 0;
    data->lamport_clock = 0;
    data->alpha = alpha;
    data->beta = beta;

    std::thread listener;
    std::thread performer;

    listener = std::thread(&reciever_func, data);
    performer = std::thread(&performer_func, data);

    listener.join();
    performer.join();

    delete data;
    MPI_Finalize();
    return 0;
}