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


    Still should handle:
    Fail set clear? done
    If haven't recieved all messages yet and recieved inquire
*/

#include <iostream>
#include <set>
#include <utility>
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
#include <thread>
#include <condition_variable>
#include <mutex>
#define REQUEST 1
#define REPLY 2
#define RELEASE 3
#define FAIL 4
#define INQUIRE 5
#define YEILD 6
#define DONE 0
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
atomic<bool> inCS(false);
set<int> quorum;
set<int> grantSet;
set<int> failSet;
set<int> yeildSet;
condition_variable cv;
mutex mtx;
bool ready = false;
atomic<bool> done = false;
int done_recv = 0;

// Helper Function: Generates a random number from an exponential distribution with a mean of 'exp_time'.
double Timer(float exp_time)
{
    default_random_engine generate;
    exponential_distribution<double> distr(1.0 / exp_time);
    return distr(generate);
}

/* Helper function to insert an element in the queue */
vector<pair<int, int>> insert_queue(vector<pair<int, int>> &queue, int senderId, int time_of_request)
{
    std::cout << "Queue before queue: ";
    for (auto i : queue)
    {
        std::cout << "(" << i.first << "," << i.second << ")";
    }
    std::cout << "\n";

    // in pair: {senderId, timeOfRequest}
    auto it = queue.begin();
    while (it->second < time_of_request)
    {
        it++;
    }

    if (it->first < senderId)
    {
        it++;
    }

    queue.insert(it, {senderId, time_of_request});

    std::cout << "Queue after queue: ";
    for (auto i : queue)
    {
        std::cout << "(" << i.first << "," << i.second << ")";
    }
    std::cout << "\n";

    return queue;
}

/* Helper function to remove an element in the queue */
vector<pair<int, int>> remove_queue(vector<pair<int, int>> &queue, int senderId)
{
    std::cout << "Queue before remove: ";
    for (auto i : queue)
    {
        std::cout << "(" << i.first << "," << i.second << ")";
    }
    std::cout << "\n";

    auto it = queue.begin();

    while (it != queue.end())
    {
        if (it->first == senderId)
        {
            break;
        }
    }

    queue.erase(it);

    std::cout << "Queue after remove: ";
    for (auto i : queue)
    {
        std::cout << "(" << i.first << "," << i.second << ")";
    }
    std::cout << "\n";

    return queue;
}

void criticalSection(my_data *data)
{
    std::cout << data->pid << " ";
    std::cout << "Entered Critical Section\n";
    data->requests_sent += 1;
    inCS = true;
    sleep(Timer(data->beta));
    inCS = false;
    ready = false;
    std::cout << " ";
    std::cout << "Left Critical Section, sending release to quorum members\n";

    data->lamport_clock += 1;
    for(auto i : quorum)
    {
        MPI_Send(&data->lamport_clock, 1, MPI_INT, i, RELEASE, MPI_COMM_WORLD);
    }

    data->lamport_clock += 1;
    if (data->requests_sent == data->total_requests)
    {
        done = true;
        std::cout << data->pid << " ";
        std::cout << "I am done with all CS, sending done messages\n";
        for (auto i : quorum)
        {
            MPI_Send(&data->lamport_clock, 1, MPI_INT, i, DONE, MPI_COMM_WORLD);
        }
    }

    return;
}

void process_send(my_data *data)
{
    while (data->requests_sent < data->total_requests)
    {
        std::cout << data->pid << " ";
        std::cout << "Not requesting state\n";
        data->lamport_clock += 1;
        sleep(Timer(data->alpha));

        std::cout << data->pid << " ";
        std::cout << "Sending requests to my quorum members\n";

        data->lamport_clock += 1;
        for (auto i : quorum)
        {
            MPI_Send(&data->lamport_clock, 1, MPI_INT, i, REQUEST, MPI_COMM_WORLD);
        }

        {
            unique_lock<mutex> lock(mtx);
            cv.wait(lock, []
                    { return ready; });
            std::cout << data->pid << " ";
            std::cout << "Got notfied that I can enter the critical section\n";
            criticalSection(data);
        }

        for (auto i : quorum)
        {
            grantSet.insert(i);
        }
    }
}

void process_recv(my_data *data)
{
    int last_sent_pid = 0;
    int last_time_stamp = 0;
    vector<pair<int, int>> queue;

    while (true)
    {
        int recv_msg = 0;
        MPI_Status status;
        MPI_Recv(&recv_msg, 1, MPI_INT, MPI::ANY_SOURCE, MPI::ANY_TAG, MPI_COMM_WORLD, &status);
        data->lamport_clock = max(data->lamport_clock, recv_msg);
        data->lamport_clock += 1;

        int tag = status.MPI_TAG;
        int senderId = status.MPI_SOURCE;

        if (tag == REPLY)
        {
            std::cout << data->pid << " ";
            std::cout << "Recieved REPLY from " << senderId << "\n";
            grantSet.erase(senderId);
            if (failSet.find(senderId) != failSet.end())
            {
                failSet.erase(senderId);
            }

            if (grantSet.empty() == true)
            {
                std::cout << data->pid << " ";
                std::cout << "Notifying the performer thread to enter CS and reset grant set\n";
                {
                    lock_guard<mutex> lock(mtx);
                    ready = true;
                }
                cv.notify_all();
            }
        }

        else if (tag == FAIL)
        {
            std::cout << data->pid << " ";
            std::cout << "Received FAIL from " << senderId << "\n";
            grantSet.insert(senderId);
            failSet.insert(senderId);
        }

        else if (tag == INQUIRE)
        {
            std::cout << data->pid << " ";
            if (failSet.empty() == false || yeildSet.empty() == false)
            {
                data->lamport_clock += 1;
                std::cout << "Sending YEILD because I have a fail\n";
                MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, YEILD, MPI_COMM_WORLD);
                grantSet.insert(senderId);
            }

            else if (grantSet.empty() == false)
            {
                data->lamport_clock += 1;
                std::cout << "Sending YEILD becuase I am not in CS\n";
                MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, YEILD, MPI_COMM_WORLD);
                grantSet.insert(senderId);
            }

            else if (inCS == true)
            {
                std::cout << "Continuing my work because I am in CS\n";
                continue;
            }

            else
            {
                std::cout << "Error Processing the process reply to INQUIRE\n";
            }
        }

        else if (tag == REQUEST)
        {
            std::cout << data->pid << " ";
            std::cout << "I recieved request from " << senderId << "\n";
            int request_time = recv_msg;

            if (last_time_stamp < request_time || (last_time_stamp == request_time && last_sent_pid < senderId))
            {
                data->lamport_clock += 1;
                std::cout << data->pid << " ";
                std::cout << "I am sending fail to " << senderId << "\n";
                MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, FAIL, MPI_COMM_WORLD);
                queue = insert_queue(queue, senderId, request_time);
            }

            else if (last_time_stamp > request_time || (last_time_stamp == request_time && last_sent_pid > senderId))
            {
                data->lamport_clock += 1;
                std::cout << data->pid << " ";
                std::cout << "I am sending Inquire to to " << last_sent_pid << "\n";

                MPI_Send(&data->lamport_clock, 1, MPI_INT, last_sent_pid, INQUIRE, MPI_COMM_WORLD);
                int sub_recv = 0;
                MPI_Status substatus;
                MPI_Recv(&sub_recv, 1, MPI_INT, last_sent_pid, MPI::ANY_TAG, MPI_COMM_WORLD, &substatus);
                data->lamport_clock = max(data->lamport_clock, sub_recv);
                data->lamport_clock += 1;

                int subtag = substatus.MPI_TAG;

                if (subtag == RELEASE)
                {
                    std::cout << data->pid << " ";
                    std::cout << "I recieved release from " << last_sent_pid << " sending reply to " << senderId << "\n";
                    queue = remove_queue(queue, last_sent_pid);
                    queue = insert_queue(queue, senderId, request_time);
                    data->lamport_clock += 1;
                    MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, REPLY, MPI_COMM_WORLD);
                }

                else if (subtag == YEILD)
                {
                    std::cout << data->pid << " ";
                    std::cout << "I recieved yeild from " << last_sent_pid << " sending reply to " << senderId << "\n";
                    queue = insert_queue(queue, senderId, request_time);
                    data->lamport_clock += 1;
                    MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, REPLY, MPI_COMM_WORLD);
                }

                else
                {
                    std::cout << "Error while recieving response to INQUIRE\n";
                }
            }

            else
            {
                std::cout << "Error while processing REQUEST\n";
            }
        }

        else if (tag == RELEASE)
        {

            queue = remove_queue(queue, senderId);
            int new_dest = queue[0].first;

            std::cout << data->pid << " ";
            std::cout << "I recieved release from " << senderId << " sending reply to " << new_dest << "\n";

            data->lamport_clock += 1;            
            MPI_Send(&data->lamport_clock, 1, MPI_INT, new_dest, REPLY, MPI_COMM_WORLD);
        }

        else if (tag == DONE)
        {
            std::cout << data->pid << " ";
            std::cout << "I recieved done from " << senderId << "\n";
            done_recv++;
            if (done_recv == quorum.size())
            {
                std::cout << data->pid << " ";
                std::cout << "Recieved all dones\n";
                break;
            }
        }

        else
        {
            std::cout << "Error, process recieved " << tag << " tag\n";
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

    std::cout << data->pid << " ";
    for (auto i : quorum)
    {
        std::cout << i << " ";
    }
    std::cout << "\n";

    std::thread performer;
    std::thread listener;

    listener = std::thread(&process_recv, data);
    performer = std::thread(&process_send, data);

    listener.join();
    performer.join();

    MPI_Finalize();
    delete (data);
    return 0;
}