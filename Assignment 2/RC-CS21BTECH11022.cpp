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
#include <mutex>
#include <time.h>
#include <chrono>
#include <fstream>
#include <atomic>
#include <mutex>
#include <thread>
#define REQ 1
#define REP 2
#define DONE 3
using namespace std;

class my_data
{
public:
    int pid;
    atomic<int> lamport_clock;
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
atomic<bool> inCS(false);
int done;
mutex file_lock;
std::atomic<int> request_time(-1); // Stores the time of requesting
std::atomic<int> grantWithMe(0);
atomic<int> message_complexity{0};

/* Helper Function: Generates a random number from an exponential distribution with a mean of 'exp_time'. */
double Timer(float exp_time)
{
    default_random_engine generate;
    exponential_distribution<double> distr(1.0 / exp_time);
    return distr(generate);
}

void print(string str)
{
    file_lock.lock();
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    time_t current = time(0);
    struct tm *timeinfo = localtime(&current);
    char buffer[80];
    strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);
    std::string timeString(buffer);

    string filename = "proc_RC" + to_string(pid) + ".log";
    ofstream outfile(filename, ios::app);

    outfile << "[" << timeString << "]"
            << " Process " << pid << " " << str << "\n";
    outfile.close();
    file_lock.unlock();
}

/* Function to simulate critical section */
void criticalSection(my_data *data)
{
    print("Entered CS function");
    data->requests_sent++;
    sleep(Timer(data->beta));
    inCS = false;

    if (defSet.empty() == false)
    {
        print("Sending replies to defset");
        grantWithMe = -1;
        data->lamport_clock.fetch_add(1);
        for (auto i : defSet)
        {
            MPI_Send(&data->lamport_clock, 1, MPI_INT, i, REP, MPI_COMM_WORLD);
            repSet.insert(i);
        }

        defSet.clear();
    }

    if (data->requests_sent == data->total_requests)
    {
        print("Sending done to all");
        data->lamport_clock.fetch_add(1);
        for (int i = 0; i < data->size; i++)
        {
            MPI_Send(&data->lamport_clock, 1, MPI_INT, i, DONE, MPI_COMM_WORLD);
        }

        request_time = -1;
    }
    return;
}

void performer_func(my_data *data)
{
    while (data->requests_sent < data->total_requests)
    {
        print("Started internal computation");
        request_time = -1;
        data->lamport_clock.fetch_add(1);
        sleep(Timer(data->alpha));
        print("Finished internal computation");

        if (grantWithMe == data->pid)
        {
            inCS = true;
            criticalSection(data);
        }

        else if (grantWithMe != data->pid)
        {
            data->lamport_clock.fetch_add(1);
            request_time = data->lamport_clock.load();

            for (auto i : repSet)
            {
                reqSet.insert(i);
            }

            repSet.clear();
            string strr = "Sent requests to ";
            data->lamport_clock.fetch_add(1);
            for (auto i : reqSet)
            {
                MPI_Send(&data->lamport_clock, 1, MPI_INT, i, REQ, MPI_COMM_WORLD);
                strr += to_string(i) + " ";
            }
            strr += "at ";
            strr += to_string(data->lamport_clock);
            print(strr);

            {
                while (inCS == false)
                {
                    continue;
                }
                print("Recieved all replies, going to critical section");
                criticalSection(data);
            }
        }

        else
        {
            std::cout << "Error here\n";
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
        data->lamport_clock.store(max(data->lamport_clock.load(), recv_msg));
        data->lamport_clock.fetch_add(1);

        int sender = status.MPI_SOURCE;

        if (status.MPI_TAG == REQ)
        {
            print("Recieved REQUEST from " + to_string(sender));

            if (inCS == true) // If I am currently executing critical section, I will put the incoming request in defSet
            {
                print("Kept " + to_string(sender) + " " + to_string(recv_msg) + " " + to_string(request_time) + " in defSet (1)");
                defSet.insert(sender);
            }

            else if (request_time != -1 && recv_msg > request_time) // I am not in CS, I am requesting, but the msg I recvd has greater time stamp than me, I will put it in defSet
            {
                print("Kept " + to_string(sender) + " " + to_string(recv_msg) + " " + to_string(request_time) + " in defSet (2)");
                defSet.insert(sender);
            }

            else if (request_time != -1 && recv_msg < request_time) // I am requesting, but the msg I recvd has smaller timestamp than me, I will reply
            {
                print("Sending REPLY to " + to_string(sender) + " " + to_string(recv_msg) + " " + to_string(request_time) + " (1)");
                data->lamport_clock.fetch_add(1);
                MPI_Send(&data->lamport_clock, 1, MPI_INT, sender, REP, MPI_COMM_WORLD);
                repSet.insert(sender);
                if (reqSet.find(sender) == reqSet.end())
                {
                    print("Sending REQUEST to " + to_string(sender) + " " + to_string(recv_msg) + " " + to_string(request_time) + " (1)");
                    repSet.erase(sender);
                    data->lamport_clock.fetch_add(1);
                    MPI_Send(&data->lamport_clock, 1, MPI_INT, sender, REQ, MPI_COMM_WORLD);
                    reqSet.insert(sender);
                }
                grantWithMe = -1;
            }

            else if (request_time != -1 && recv_msg == request_time)
            {
                if (sender > data->pid)
                {
                    print("Keeping " + to_string(sender) + " " + to_string(recv_msg) + " " + to_string(request_time) + " in defSet (3)");
                    defSet.insert(sender);
                }

                else
                {
                    print("Sending REPLY to " + to_string(sender) + " " + to_string(recv_msg) + " " + to_string(request_time) + " (2)");
                    data->lamport_clock.fetch_add(1);
                    MPI_Send(&data->lamport_clock, 1, MPI_INT, sender, REP, MPI_COMM_WORLD);
                    repSet.insert(sender);
                    if (reqSet.find(sender) == reqSet.end())
                    {
                        print("Sending REPLY to " + to_string(sender) + " " + to_string(recv_msg) + " " + to_string(request_time) + " (2)");
                        repSet.erase(sender);
                        data->lamport_clock.fetch_add(1);
                        MPI_Send(&data->lamport_clock, 1, MPI_INT, sender, REQ, MPI_COMM_WORLD);
                        reqSet.insert(sender);
                    }
                    grantWithMe = -1;
                }
            }

            else if (request_time == -1) // I am not even requesting, I will reply
            {
                print("Sending REPLY to " + to_string(sender) + " " + to_string(recv_msg) + " " + to_string(request_time) + " (3)");
                data->lamport_clock.fetch_add(1);
                MPI_Send(&data->lamport_clock, 1, MPI_INT, sender, REP, MPI_COMM_WORLD);
                repSet.insert(sender);
                grantWithMe = -1;
            }

            else
            {
                std::cout << data->pid << " ";
                std::cout << "Request time " << request_time << "\n";
                std::cout << data->pid << " ";
                std::cout << "Message time " << recv_msg << "\n";
                std::cout << data->pid << " ";
                std::cout << "My req time " << request_time << "\n";
                std::cout << data->pid << " ";
                std::cout << "IN CS? " << inCS << "\n";
                std::cout << data->pid << " ";
                std::cout << "Error Here\n";
            }
        }

        else if (status.MPI_TAG == REP)
        {
            print("Recieved REPLY from " + to_string(sender));
            reqSet.erase(status.MPI_SOURCE); // When I recieve a reply, I will remove from the reqSet, impyling my request has been catered with their reply

            if (reqSet.empty() == true) // If everyone I requested got a reply, ready to enter CS, but before that, I will modify the list I need to request before entering the CS next time.
            {
                grantWithMe = data->pid;

                inCS = true;
            }
        }

        else if (status.MPI_TAG == DONE)
        {
            print("Recieved DONE from " + to_string(sender));
            done++;

            if (done == data->size)
            {
                print("All Done");
                break;
            }
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

    MPI_Finalize();
    delete data;

    ofstream message_complexity_file("messages_RC.txt", ios::app);
    message_complexity_file << pid << ": " << message_complexity << " ";
    message_complexity_file.close();

    return 0;
}