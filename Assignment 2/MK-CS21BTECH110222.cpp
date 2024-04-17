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

#define RECVSET 8
#define FAILSET 9
#define PQ 10
#define BUFFER 11

#define ADD 12
#define ERASE 13
#define POP 14
#define CLEAR 15

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
priority_queue<pair<int, int>, vector<pair<int, int>>, greater<pair<int, int>>> pq;
vector<pair<int, int>> buffer;

std::condition_variable cv;
mutex lck;
bool inCS;
mutex file_lock;
mutex set_lock;

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

void analyze_sets()
{
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    ofstream outfile("analyze_" + to_string(pid) + ".log", ios::app);
    outfile << "recvSet: (";
    for (auto i : recvSet)
    {
        outfile << i << " ";
    }
    outfile << ") ";

    outfile << "failSet: (";
    for (auto i : failSet)
    {
        outfile << i << " ";
    }
    outfile << ") ";

    outfile << "pq: ";
    priority_queue<pair<int, int>, vector<pair<int, int>>, greater<pair<int, int>>> temp = pq;
    while (!temp.empty())
    {
        outfile << "(" << temp.top().first << "," << temp.top().second << ") ";
        temp.pop();
    }

    outfile << "\n";

    outfile.close();
    return;
}

void set_handler(int setName, int operation, int element)
{
    set_lock.lock();
    if (setName == RECVSET)
    {
        if (operation == ADD)
        {
            recvSet.insert(element);
        }
        else if (operation == ERASE)
        {
            recvSet.erase(element);
        }
    }

    else if (setName == FAILSET)
    {
        if (operation == ADD)
        {
            failSet.insert(element);
        }

        else if (operation == ERASE)
        {
            failSet.erase(element);
        }
    }

    else if (setName == PQ)
    {
        if (operation == ADD)
        {
            pq.push(std::make_pair(element, lamport_clock.load()));
        }
        else if (operation == POP)
        {
            pq.pop();
        }
    }

    else if (setName == BUFFER)
    {
        if (operation == ADD)
        {
            buffer.push_back(std::make_pair(element, lamport_clock.load()));
        }

        if (operation == CLEAR)
        {
            buffer.clear();
        }
    }

    set_lock.unlock();
    return;
}

bool is_set_empty(int setName)
{
    set_lock.lock();
    bool ans = false;
    if (setName == RECVSET)
    {
        ans = recvSet.empty();
    }

    else if (setName == FAILSET)
    {
        ans = failSet.empty();
    }

    else if(setName == PQ)
    {
        ans = pq.empty();
    }

    else if(setName == BUFFER)
    {
        ans = buffer.empty();
    }

    set_lock.unlock();
    return ans;
}

bool set_contains(int setName, int element)
{
    bool ans = false;
    set_lock.lock();
    if (setName == RECVSET)
    {
        ans = (recvSet.find(element) != recvSet.end());
    }

    else if (setName == FAILSET)
    {
        ans = (failSet.find(element) != failSet.end());
    }

    set_lock.unlock();
    return ans;
}

/* Critical section function */
void critical_section()
{
    print("entered critical section");
    this_thread::sleep_for(std::chrono::milliseconds(int(Timer(a))));
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
    analyze_sets();

    return;
}

/* Performer function: Sends, waits, and enters critical section on loop */
void performer_func()
{
    while (true)
    {
        if (times_entered == k)
        {
            break;
        }

        // Performs internal computations
        lamport_clock.fetch_add(1);
        print("performing internal computations");
        this_thread::sleep_for(std::chrono::milliseconds(int(Timer(a))));
        print("finished doing internal computations");


        // Sending requests
        lamport_clock.fetch_add(1);
        string str = "Sending REQUEST to quorum members:";
        for (auto i : quorum)
        {
            str += " " + to_string(i);
            set_handler(RECVSET, ADD, i);
            int timestamp = lamport_clock.load();
            MPI_Send(&timestamp, 1, MPI_INT, i, REQUEST, MPI_COMM_WORLD);
        }
        print(str);

        analyze_sets();

        {
            auto ul = std::unique_lock<std::mutex>(lck);
            cv.wait(ul, []()
                    { return inCS; });
        }
        critical_section();
    }

    lamport_clock.fetch_add(1);
    for (int i = 0; i < n; i++)
    {
        MPI_Send(&lamport_clock, 1, MPI_INT, i, DONE, MPI_COMM_WORLD);
    }

    analyze_sets();
    return;
}

/* Reciever function: Recives messages, both quorum side and process side, and communicates with performer when needed */
void reciever_func()
{
    while (true)
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
            analyze_sets();

            if (done_recv == n)
            {
                analyze_sets();
                break;
            }
        }

        else if (tag == FAIL)
        {
            print("recieved FAIL from " + to_string(sender_id));
            set_handler(FAILSET, ADD, sender_id);
            analyze_sets();
        }

        else if (tag == REPLY)
        {
            print("recieved REPLY from " + to_string(sender_id));

            if (set_contains(FAILSET, sender_id))
            {
                set_handler(FAILSET, ERASE, sender_id);
            }

            if (set_contains(RECVSET, sender_id))
            {
                set_handler(RECVSET, ERASE, sender_id);
            }

            if (is_set_empty(RECVSET) && is_set_empty(FAILSET))
            {
                {
                    auto ul = std::unique_lock<std::mutex>(lck);
                    inCS = true;
                }

                cv.notify_one();
            }

            analyze_sets();
        }

        else if (tag == INQUIRE)
        {
            print("recieved INQUIRE from " + to_string(sender_id));

            if (!is_set_empty(FAILSET) || !is_set_empty(RECVSET))
            {
                lamport_clock.fetch_add(1);
                int timestamp = lamport_clock.load();

                if (!set_contains(RECVSET, sender_id)) // If recv set is not waiting for this sender's approval, it will now wait for it as it is sending INQUIRE   
                {
                    set_handler(RECVSET, ADD, sender_id);
                }

                print("sending YEILD to " + to_string(sender_id));
                MPI_Send(&timestamp, 1, MPI_INT, sender_id, YEILD, MPI_COMM_WORLD);
            }

            analyze_sets();
        }

        /* Three types as recieved by the quorum side of the process */
        else if (tag == REQUEST)
        {
            print("recieved REQUEST from " + to_string(sender_id));
            if (is_set_empty(PQ) == true)
            {
                print("sending REPLY to " + to_string(sender_id));
                lamport_clock.fetch_add(1);
                MPI_Send(&lamport_clock, 1, MPI_INT, sender_id, REPLY, MPI_COMM_WORLD);
                set_handler(PQ, ADD, sender_id);
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
                    set_handler(PQ, ADD, sender_id);
                }

                else if (last_send_time > sender_clock || (last_send_time == sender_clock && last_send_pid > sender_id))
                {
                    print("sending INQUIRE to " + to_string(last_send_pid));
                    lamport_clock.fetch_add(1);
                    MPI_Send(&lamport_clock, 1, MPI_INT, last_send_pid, INQUIRE, MPI_COMM_WORLD);
                    set_handler(BUFFER, ADD, sender_id);
                }

                else
                {
                    std::cout << "Error Here\n";
                }
            }

            analyze_sets();
        }

        else if (tag == YEILD)
        {
            print("recieved YEILD from " + to_string(sender_id));

            if (!is_set_empty(BUFFER))
            {
                for (int i = 0; i < buffer.size(); i++)
                {
                    set_handler(PQ, ADD, buffer[i].first);
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

                set_handler(BUFFER, CLEAR, 0);
            }

            if (!is_set_empty(PQ))
            {
                int new_dest = pq.top().first;
                lamport_clock.fetch_add(1);
                int timestamp = lamport_clock.load();

                print("sending REPLY to " + to_string(new_dest));
                MPI_Send(&timestamp, 1, MPI_INT, new_dest, REPLY, MPI_COMM_WORLD);
            }

            analyze_sets();
        }

        else if (tag == RELEASE)
        {
            print("recieved RELEASE from " + to_string(sender_id));
            set_handler(PQ, POP, 0);

            if (!is_set_empty(BUFFER))
            {
                for (int i = 0; i < buffer.size(); i++)
                {
                    set_handler(PQ, ADD, buffer[i].first);
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

                // buffer.clear();
                set_handler(BUFFER, CLEAR, 0);
            }

            if (!is_set_empty(PQ))
            {
                int new_dest = pq.top().first;
                lamport_clock.fetch_add(1);
                int timestamp = lamport_clock.load();

                print("sending REPLY to " + to_string(new_dest));
                MPI_Send(&timestamp, 1, MPI_INT, new_dest, REPLY, MPI_COMM_WORLD);
            }

            analyze_sets();
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