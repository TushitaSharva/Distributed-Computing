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
#include <condition_variable>
#define DONE 1
#define REQUEST 2
#define REPLY 3
#define RELEASE 4
#define FAIL 5
#define INQUIRE 6
#define YEILD 7

using namespace std;

/* Global parameters */
int n, k, a, b;
int times_entered;
int message_complexity;
int done_recv;
atomic<int> lamport_clock;

set<int> quorum;
set<int> recvSet;
set<int> failSet;

std::condition_variable cv;
mutex lck;
bool inCS;
mutex file_lock;

/* Helper Function: Generates a random number from an exponential distribution with a mean of 'exp_time'. */
double Timer(float exp_time)
{
    default_random_engine generate;
    exponential_distribution<double> distr(1.0 / exp_time);
    return distr(generate);
}

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

/* Critical section function */
void critical_section()
{
    print("entered critical section");
    sleep(Timer(b));
    print("exit from critical section");
    inCS = false;
    times_entered += 1;

    lamport_clock.fetch_add(1);

    int timestamp = lamport_clock.load();
    for (auto i : quorum)
    {
        MPI_Send(&timestamp, 1, MPI_INT, i, RELEASE, MPI_COMM_WORLD);
    }
    print("sending RELEASE to quorum members");

    return;
}

/* Performer function: Sends, waits, and enters critical section on loop */
void performer_func()
{
    while (times_entered < k)
    {
        // Performs internal computations
        print("performing internal computations");
        sleep(Timer(b));
        print("finished doing internal computations, sending requests");

        for (auto i : quorum)
        {
            recvSet.insert(i);
            int timestamp = lamport_clock.load();

            // Sending requests
            MPI_Send(&timestamp, 1, MPI_INT, i, REQUEST, MPI_COMM_WORLD);
        }

        {
            auto ul = std::unique_lock<std::mutex>(lck);
            cv.wait(ul, []()
                    { return inCS; });
        }
        critical_section();
    }

    for (int i = 0; i < n; i++)
    {
        MPI_Send(&lamport_clock, 1, MPI_INT, i, DONE, MPI_COMM_WORLD);
    }

    return;
}

/* Reciever function: Recives messages, both quorum side and process side, and communicates with performer when needed */
void reciever_func()
{
    priority_queue<pair<int, int>, vector<pair<int, int>>, greater<pair<int, int>>> pq;
    vector<pair<int, int>> buffer;

    while (done_recv < n)
    {
        int pid;
        MPI_Comm_rank(MPI_COMM_WORLD, &pid);

        int recv_msg = 0;
        MPI_Status status;
        MPI_Recv(&recv_msg, 1, MPI_INT, MPI::ANY_SOURCE, MPI::ANY_TAG, MPI_COMM_WORLD, &status);
        message_complexity += 1;

        /* Clock update after recieve */
        lamport_clock.store(max(lamport_clock.load(), recv_msg) + 1);

        /* Sender Information */
        int sender_clock = recv_msg;
        int sender_id = status.MPI_SOURCE;
        int tag = status.MPI_TAG;

        /* Four types as recieved by the actual process */
        if (tag == DONE)
        {
            print("recieved DONE from " + to_string(sender_id));
            done_recv++;
        }

        else if (tag == FAIL)
        {
            print("recieved FAIL from " + to_string(sender_id));
            failSet.insert(sender_id); // If recieved fail, add to failSet. No need to add to grant set cause it won't be removed
        }

        else if (tag == REPLY)
        {
            print("recieved REPLY from " + to_string(sender_id));

            if (failSet.find(sender_id) != failSet.end())
            {
                failSet.erase(sender_id);
            }

            if (recvSet.find(sender_id) != recvSet.end())
            {
                recvSet.erase(sender_id);
            }

            if (recvSet.empty() && failSet.empty())
            {
                {
                    auto ul = std::unique_lock<std::mutex>(lck);
                    inCS = true;
                }

                cv.notify_one();
            }
        }

        else if (tag == INQUIRE)
        {
            print("recieved INQUIRE from " + to_string(sender_id));

            if (!failSet.empty() || !recvSet.empty())
            {
                lamport_clock.fetch_add(1);
                int timestamp = lamport_clock.load();

                if(recvSet.find(sender_id) == recvSet.end())
                {
                    recvSet.insert(sender_id);
                }

                print("sending YEILD to " + to_string(sender_id));
                MPI_Send(&timestamp, 1, MPI_INT, sender_id, YEILD, MPI_COMM_WORLD);
            }
        }

        /* Three types as recieved by the quorum side of the process */
        else if (tag == REQUEST)
        {
            print("recieved REQUEST from " + to_string(sender_id));

            if (pq.empty())
            {
                print("sending REPLY to " + to_string(sender_id));
                lamport_clock.fetch_add(1);
                MPI_Send(&lamport_clock, 1, MPI_INT, sender_id, REPLY, MPI_COMM_WORLD);
                pq.push(std::make_pair(sender_id, sender_clock));
            }

            else
            {
                int last_send_pid = pq.top().first;
                int last_send_time = pq.top().second;

                if (last_send_time < sender_clock || (last_send_time == sender_clock && last_send_pid < sender_id))
                {
                    print("sending FAIL to " + to_string(sender_id));
                    lamport_clock.fetch_add(1);
                    MPI_Send(&lamport_clock, 1, MPI_INT, sender_id, FAIL, MPI_COMM_WORLD);
                    pq.push(std::make_pair(sender_id, sender_clock));
                }

                else if (last_send_time > sender_clock || (last_send_time == sender_clock && last_send_pid > sender_id))
                {
                    print("sending INQUIRE to " + to_string(last_send_pid));
                    lamport_clock.fetch_add(1);
                    MPI_Send(&lamport_clock, 1, MPI_INT, last_send_pid, INQUIRE, MPI_COMM_WORLD);
                    buffer.push_back(std::make_pair(sender_id, sender_clock));
                }
            }
        }

        else if (tag == YEILD)
        {
            print("recieved YEILD from " + to_string(sender_id));

            if (!buffer.empty())
            {
                for (int i = 0; i < buffer.size(); i++)
                {
                    pq.push(std::make_pair(buffer[i].first, buffer[i].second));
                }

                int new_dest = pq.top().first;
                lamport_clock.fetch_add(1);
                int timestamp = lamport_clock.load();

                for (auto i : buffer)
                {
                    if (i.first != new_dest)
                    {
                        print("sending FAIL to " + to_string(i.first));
                        MPI_Send(&timestamp, 1, MPI_INT, i.first, FAIL, MPI_COMM_WORLD);
                    }
                }

                buffer.clear();
            }

            if (!pq.empty())
            {
                int new_dest = pq.top().first;
                lamport_clock.fetch_add(1);
                int timestamp = lamport_clock.load();

                print("sending REPLY to " + to_string(new_dest));
                MPI_Send(&timestamp, 1, MPI_INT, new_dest, REPLY, MPI_COMM_WORLD);
            }
        }

        else if (tag == RELEASE)
        {
            print("recieved RELEASE from " + to_string(sender_id));
            pq.pop();

            if (!buffer.empty())
            {
                for (int i = 0; i < buffer.size(); i++)
                {
                    pq.push(std::make_pair(buffer[i].first, buffer[i].second));
                }

                int new_dest = pq.top().first;
                lamport_clock.fetch_add(1);
                int timestamp = lamport_clock.load();

                for (auto i : buffer)
                {
                    if (i.first != new_dest)
                    {
                        print("sending FAIL to " + to_string(i.first));
                        MPI_Send(&timestamp, 1, MPI_INT, i.first, FAIL, MPI_COMM_WORLD);
                    }
                }

                buffer.clear();
            }

            if (!pq.empty())
            {
                int new_dest = pq.top().first;
                lamport_clock.fetch_add(1);
                int timestamp = lamport_clock.load();

                print("sending REPLY to " + to_string(new_dest));
                MPI_Send(&timestamp, 1, MPI_INT, new_dest, REPLY, MPI_COMM_WORLD);
            }
        }
    }

    return;
}

int main(int argc, char *argv[])
{
    ifstream inputfile("inp-params.txt");
    inputfile >> n >> k >> a >> b;
    times_entered = 0;
    message_complexity = 0;
    done_recv = 0;

    std::thread performer;
    std::thread listener;

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

    listener = thread(reciever_func);
    performer = thread(performer_func);

    listener.join();
    performer.join();

    MPI_Finalize();

    ofstream message_complexity_file("messages_MK.csv", ios::app);
    message_complexity_file << pid << "," << message_complexity << "\n";
    message_complexity_file.close();

    return 0;
}