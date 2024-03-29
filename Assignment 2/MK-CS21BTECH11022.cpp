/*
    Janga Tushita Sharva
    CS21BTECH11022
    Aim: To implement and analyze Maekawa's Algorithm for MUTEX
    Algorithm:
    All processes work induvidually for outCSTime (variable, exponential) seconds
    Then they would want to enter mutual exclusion

    - reqSet: a set which contains those it needs to send request and recieve reply from (it will be determined based on grid)
    - failSet: a set which contains the quorum members who sent fail message
    - grantSet: a set which contains the quorum members who sent grant message
    - yeildSet: a set which contains the quorum members whom process sent yeild message

Sends:
    REQUEST:
    Send requests to all elements in the reqSet

    INQUIRE:
    On recieving lower time stamp request, it will send inquire message to the process(es) it sent GRANT

    YEILD:
    If I have any elements in my failed set or yeild set and fail+yeild+grant = all quorum members, yeild
    Else, wait for the last quorum member's message

    GRANT:
    If a process id is top in it's priority queue, it will send grant message to that process

    FAIL:
    If a request is made, and it's time stamp is more that the root of the priority queue, then send a fail message

    RELEASE:
    Sends release message to all members in the quorum, once done executing the CS


RECIEVES:
    REQUEST:
    Send grant if top of the timestamp is top of the priority queue
    If not, SEND a FAIL message

    GRANT:
    Put in grant set, and enter CS if grant set has all members in the quorum

    FAIL:
    Put is fail set

    YEILD:
    Grant permission to the lower timestamp process

    RELEASE:
    Send grant to the root of priority queue

    INQUIRE:
    Send YEILD if condition for yeild is satisfied
    Else, just keep aside, and the process will eventually send release message to the quorum members

    RECIEVE:
    Can recieve
        - INQUIRE
        - YEILD
        - GRANT
        - RELEASE
        - REQUEST
        - FAIL
*/

#include <iostream>
#include <set>
#include <utility>
#include <queue>
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
#include <math.h>
#include <fstream>
#include <atomic>
#include <mutex>
#include <pthread.h>
#define REQUEST 1
#define REPLY 2
#define RELEASE 3
#define FAIL 4
#define INQUIRE 5
#define YEILD 6
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
bool inCS = 0;
set<int> quorum;
int done = 0;

// Helper Function: Generates a random number from an exponential distribution with a mean of 'exp_time'.
double Timer(float exp_time)
{
    default_random_engine generate;
    exponential_distribution<double> distr(1.0 / exp_time);
    return distr(generate);
}

class Compare
{
public:
    bool operator()(pair<int, int> &p1, pair<int, int> &p2)
    {
        return p1.second > p2.second;
    }
};

void quorum_recv(my_data *data)
{
    std::priority_queue<pair<int, int>, vector<pair<int, int>>, Compare> pq;
    while (true)
    {
        int recv_msg = 0;
        MPI_Status status;
        MPI_Recv(&recv_msg, 1, MPI_INT, MPI::ANY_SOURCE, MPI::ANY_TAG, MPI_COMM_WORLD, &status);
        data->lamport_clock = max(data->lamport_clock, recv_msg);
        data->lamport_clock += 1;

        int tag = status.MPI_TAG;
        int senderId = status.MPI_SOURCE;
        int last_time_stamp = 0;
        int last_sent_proc = 0;

        if (tag == REQUEST)
        {
            if (pq.empty() == true)
            {
                pq.push({senderId, recv_msg});
                MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, REPLY, MPI_COMM_WORLD);
                last_time_stamp = recv_msg;
                last_sent_proc = senderId;
            }

            else if (recv_msg > last_time_stamp)
            {
                pq.push({senderId, recv_msg});
                MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, FAIL, MPI_COMM_WORLD);
            }

            else if (recv_msg < last_time_stamp)
            {
                // Sending inquire
                MPI_Send(&data->lamport_clock, 1, MPI_INT, last_sent_proc, INQUIRE, MPI_COMM_WORLD);

                int recv_msg_2 = 0;
                MPI_Status status;
                MPI_Recv(&recv_msg_2, 1, MPI_INT, last_sent_proc, MPI::ANY_TAG, MPI_COMM_WORLD, &status);

                if (status.MPI_TAG == YEILD)
                {
                    pq.push({senderId, recv_msg});
                }

                else if(status.MPI_TAG == RELEASE)
                {
                    pq.pop();
                }

                MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, REPLY, MPI_COMM_WORLD);
            }
        }

        else if(tag == RELEASE)
        {
            int dest = pq.top().first;
            MPI_Send(&data->lamport_clock, 1, MPI_INT, dest, REPLY, MPI_COMM_WORLD);            
        }

        else
        {
            std::cout << "Error: Recieved tag: " << tag << "\n";
        }
    }
}

void criticalSection(my_data *data)
{
    inCS = data->pid;
    sleep(Timer(data->beta));
}

void process_send(my_data *data)
{
    while(data->requests_sent < data->total_requests)
    {
        sleep(Timer(data->alpha));

        for(auto i : quorum)
        {
            MPI_Send(&data->lamport_clock, 1, MPI_INT, i, REQUEST, MPI_COMM_WORLD);
        }

        criticalSection(data);
    }
}

void process_recv(my_data *data)
{
    while(true)
    {
        int recv_msg = 0;
        MPI_Status status;
        MPI_Recv(&recv_msg, 1, MPI_INT, MPI::ANY_SOURCE, MPI::ANY_TAG, MPI_COMM_WORLD, &status);
        data->lamport_clock = max(data->lamport_clock, recv_msg);
        data->lamport_clock += 1;

        int tag = status.MPI_TAG;
        int senderId = status.MPI_SOURCE;

        if(tag == REQUEST)
        {
            if(inCS != data->pid)
        }
    }
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

    int root = pow(n, 0.5);
    int my_row = pid / root;
    int my_col = pid % root;

    /* Elements same row as me, changing columns from 0 to root n */
    for (int i = 0; i < root; i++)
    {
        quorum.insert(root * my_row + i);
        quorum.insert(my_col + root * i);
    }

    my_data *data = new my_data;
    data->alpha = alpha;
    data->beta = beta;
    data->lamport_clock = 0;
    data->pid = pid;
    data->requests_sent = 0;
    data->total_requests = k;
    data->size = size;

    MPI_Finalize();
    delete (data);
    return 0;
}