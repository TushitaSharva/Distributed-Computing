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

    Process perform:
    - Does it's execution --> in neutral state
    - Sends REQUEST for CS, waits for reply (notified by the receiver thread)
    - When all replies arrive, enter CS. After finish, send RELEASE to all quorums, reset grantSet

    Process receive:
    FAIL:
    - Put this in fail set, make sure that it waits for the sender's grant
    INQUIRE:
    - If there is a fail: Send yeild, make sure that this waits for sender's grant again
    - If not recieved grants from all: Send Yeild, make sure this waits fro sender's grant again
    - If in CS: continue; (Will automatically send release upon finishing CS)
    REPLY:
    - Modify grantSet that the sender sent it's grant
    - If replies from all quorum members recieved, notify performer to enter CS
    REQUEST:
    - Sender's request time is smaller than time root of pq / lesser pid, equal time: Send INQUIRE.
        Wait for INQUIRE's reply.
        If reply is RELEASE:
        - remove the proc which sent release
        - add the proc which sent the REQUEST
        - send reply to the requesting proc
        If reply is YEILD:
        - add the proc which sent request
        - no need to remove the proc which sent yeild
        - send reply to the requesting proc
    - Sender's request time is larger/ same time + larger pid: Send FAIL, add in pq
    - If pq is empty : Send reply, add in pq
    YEILD:
    - send reply to the root of pq
    RELEASE:
    - remove the previous proc from the priority queue (pop)
    - send REPLY to top of pq
    DONE:
    - done++
    - if done == quorum.size(), break from the while loop of reciever, will return

    CS:
    - each time enter, request++;
    if total requests reached, send done to all of it's quorum members.

*/

#include <iostream>
#include <set>
#include <queue>
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
    atomic<int> lamport_clock;
    int size;
    int total_requests;
    int requests_sent;
    int alpha;
    int beta;
};

/*
    Here x.first is senderID, x.second is time_of_request
    if a has lesser timestamp than b, a should be found before b
    For equal timestamps, if a has lesser pid than b, a should be placed before b
*/
class Compare
{
public:
    bool operator()(pair<int, int> a, pair<int, int> b)
    {
        if (a.second > b.second)
        {
            return true;
        }

        else if (a.second < b.second)
        {
            return false;
        }

        else if (a.second == b.second)
        {
            if (a.first > b.first)
            {
                return true;
            }

            else if (a.first < b.first)
            {
                return false;
            }

            else
            {
                std::cout << "Error in comparator\n";
            }
        }

        else
        {
            std::cout << "Error in comparator\n";
        }

        return false;
    }
};

ifstream inputfile;
atomic<bool> inCS(false);
set<int> quorum;
set<int> grantSet;
set<int> failSet;
set<int> yeildSet;
mutex mtx;
mutex file_lock;
int done_recv = 0;

/* Helper function to print */
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

    string filename = "proc_MK" + to_string(pid) + ".log";
    ofstream outfile(filename, ios::app);

    outfile << "[" << timeString << "]"
            << " Process " << pid << " " << str << "\n";
    outfile.close();
    file_lock.unlock();
}


/* Helper function to print queue */
string printq(priority_queue<pair<int, int>, vector<pair<int, int>>, Compare> pq)
{
    string str = "";
    while(!pq.empty())
    {
        str += "(" + to_string(pq.top().first) + ", " + to_string(pq.top().second) +")";
        pq.pop();
    }

    return str;
}


/* Helper Function: Generates a random number from an exponential distribution with a mean of 'exp_time'. */
double Timer(float exp_time)
{
    default_random_engine generate;
    exponential_distribution<double> distr(1.0 / exp_time);
    return distr(generate);
}

/* Critical Section Function */
void criticalSection(my_data *data)
{
    print("Entered CS function");
    data->requests_sent += 1;
    inCS.store(true);
    sleep(Timer(data->beta));
    inCS.store(false);

    data->lamport_clock.fetch_add(1);
    for (auto i : quorum)
    {
        print("sending RELEASE message to " + to_string(i));
        MPI_Send(&data->lamport_clock, 1, MPI_INT, i, RELEASE, MPI_COMM_WORLD);
    }

    data->lamport_clock.fetch_add(1);
    if (data->requests_sent == data->total_requests)
    {
        print("Sending done messages");
        for (int i = 0; i < data->size; i++)
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
        print("Started internal computations");
        data->lamport_clock.fetch_add(1);
        sleep(Timer(data->alpha));
        print("Finished internal computations, sending requests");
        string strr = "Sending request messages to ";
        data->lamport_clock.fetch_add(1);
        for (auto i : quorum)
        {
            strr += to_string(i) + " ";
            MPI_Send(&data->lamport_clock, 1, MPI_INT, i, REQUEST, MPI_COMM_WORLD);
        }
        print(strr);

        while (inCS == false)
        {
            continue;
        }

        criticalSection(data);

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
    priority_queue<pair<int, int>, vector<pair<int, int>>, Compare> pq;

    while (true)
    {
        int recv_msg = 0;
        MPI_Status status;
        MPI_Recv(&recv_msg, 1, MPI_INT, MPI::ANY_SOURCE, MPI::ANY_TAG, MPI_COMM_WORLD, &status);
        data->lamport_clock = max(data->lamport_clock.load(), recv_msg);
        data->lamport_clock.fetch_add(1);

        int tag = status.MPI_TAG;
        int senderId = status.MPI_SOURCE;

        if (tag == REPLY)
        {
            print("Recieved REPLY message from " + to_string(senderId));
            grantSet.erase(senderId);
            if (failSet.find(senderId) != failSet.end())
            {
                failSet.erase(senderId);
            }

            if (grantSet.empty() == true)
            {
                inCS = true;
            }
        }

        else if (tag == FAIL)
        {
            print("Recieved FAIL message from " + to_string(senderId));
            grantSet.insert(senderId);
            failSet.insert(senderId);
        }

        else if (tag == INQUIRE)
        {
            print("Recieved INQUIRE message from " + to_string(senderId));
            if (inCS == false)
            {
                data->lamport_clock.fetch_add(1);
                print("Sending YEILD message to " + to_string(senderId));
                MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, YEILD, MPI_COMM_WORLD);
                grantSet.insert(senderId);
            }

            else if (inCS == true)
            {
                continue;
            }

            else
            {
                std::cout << "Error Processing the process reply to INQUIRE\n";
            }
        }

        else if (tag == REQUEST)
        {
            print("Recieved REQUEST message from " + to_string(senderId));
            int request_time = recv_msg;

            if (pq.empty() == true)
            {
                data->lamport_clock.fetch_add(1);
                MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, REPLY, MPI_COMM_WORLD);
                last_sent_pid = senderId;
                last_time_stamp = request_time;
                pq.push({senderId, request_time});
                print("Sent REPLY message to " + to_string(senderId) + " " + printq(pq));
            }

            else if (last_time_stamp < request_time || (last_time_stamp == request_time && last_sent_pid < senderId))
            {
                data->lamport_clock.fetch_add(1);
                MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, FAIL, MPI_COMM_WORLD);
                pq.push({senderId, request_time});
                print("Sent FAIL message to " + to_string(senderId) + " " + printq(pq));
            }

            else if (last_time_stamp > request_time || (last_time_stamp == request_time && last_sent_pid > senderId))
            {
                data->lamport_clock.fetch_add(1);
                MPI_Send(&data->lamport_clock, 1, MPI_INT, last_sent_pid, INQUIRE, MPI_COMM_WORLD);
                int sub_recv = 0;
                MPI_Status substatus;
                print("Sending INQUIRE to " + to_string(senderId));
            }

            else
            {
                std::cout << "Error while processing REQUEST recieved tag " << request_time << " " << last_time_stamp << " " << last_sent_pid << " " << senderId <<"\n";
            }
        }

        else if (tag == RELEASE)
        {
            print("Recieved RELEASE message from " + to_string(senderId));
            pq.pop();

            if (!pq.empty())
            {
                int new_dest = pq.top().first;
                data->lamport_clock.fetch_add(1);
                print("Sending REPLY message to " + to_string(new_dest) + " " + printq(pq));
                MPI_Send(&data->lamport_clock, 1, MPI_INT, new_dest, REPLY, MPI_COMM_WORLD);

                last_sent_pid = senderId;
                last_time_stamp = recv_msg;
            }
        }


        else if (tag == YEILD)
        {
            data->lamport_clock.fetch_add(1);
            print("Sending REPLY message to " + to_string(senderId) + " " + printq(pq));
            MPI_Send(&data->lamport_clock, 1, MPI_INT, senderId, REPLY, MPI_COMM_WORLD);

            last_sent_pid = senderId;
            last_time_stamp = recv_msg;
            pq.push({senderId, recv_msg});
        }

        else if (tag == DONE)
        {
            print("Recieved DONE message from " + to_string(senderId));
            done_recv++;
            if (done_recv == data->size)
            {
                print("All Done");
                break;
            }
        }
        
        else
        {
            std::cout << "Error, process recieved " << tag << " tag from " << senderId;
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

    quorum.erase(pid);

    my_data *data = new my_data;
    data->alpha = alpha;
    data->beta = beta;
    data->lamport_clock = 0;
    data->pid = pid;
    data->requests_sent = 0;
    data->total_requests = k;
    data->size = size;

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