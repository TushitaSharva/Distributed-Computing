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

std::condition_variable cv;
mutex lck;
bool inCS;
mutex file_lock;
mutex set_lock;
mutex time_lock;

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
        outfile << "(" << temp.top().second << "," << temp.top().first << ") ";
        temp.pop();
    }

    outfile << "\n";

    outfile.close();
    return;
}

void set_handler(int setName, int operation, int element_a, int element_b)
{
    set_lock.lock();
    if (setName == RECVSET)
    {
        if (operation == ADD)
        {
            recvSet.insert(element_a);
        }
        else if (operation == ERASE)
        {
            recvSet.erase(element_a);
        }
    }

    else if (setName == FAILSET)
    {
        if (operation == ADD)
        {
            failSet.insert(element_a);
        }

        else if (operation == ERASE)
        {
            failSet.erase(element_a);
        }
    }

    else if (setName == PQ)
    {
        if (operation == ADD)
        {
            pq.push(std::make_pair(element_a, element_b));
        }
        else if (operation == POP)
        {
            pq.pop();
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

    else if (setName == PQ)
    {
        ans = pq.empty();
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
    sleep(Timer(b));
    print("exit from critical section");
    inCS = false;
    times_entered += 1;

    time_lock.lock();
    lamport_clock.fetch_add(1);
    int x = lamport_clock.load();
    for (auto i : quorum)
    {
        MPI_Send(&x, 1, MPI_INT, i, RELEASE, MPI_COMM_WORLD);
    }

    time_lock.unlock();

    print("sending RELEASE to quorum members");
    // analyze_sets();

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
        time_lock.lock();
        lamport_clock.fetch_add(1);
        time_lock.unlock();
        print("performing internal computations");
        sleep(Timer(a));
        print("finished doing internal computations");

        // Sending requests
        time_lock.lock();
        lamport_clock.fetch_add(1);
        int x = lamport_clock.load();
        string str = "Sending REQUEST to quorum members:";

        for (auto i : quorum)
        {
            str += " " + to_string(i);
            set_handler(RECVSET, ADD, i, 0);
            MPI_Send(&x, 1, MPI_INT, i, REQUEST, MPI_COMM_WORLD);
        }

        time_lock.unlock();
        print(str);

        {
            auto ul = std::unique_lock<std::mutex>(lck);
            cv.wait(ul, []()
                    { return inCS; });
        }
        critical_section();
        // analyze_sets();
    }

    lamport_clock.fetch_add(1);
    for (int i = 0; i < n; i++)
    {
        MPI_Send(&lamport_clock, 1, MPI_INT, i, DONE, MPI_COMM_WORLD);
    }

    return;
}

/* Reciever function: Recives messages, both quorum side and process side, and communicates with performer when needed */
void reciever_func()
{
    int last_send_pid = -1;
    int last_send_time = -1;
    int inquires_sent = 0;

    while (true)
    {
        int pid;
        MPI_Comm_rank(MPI_COMM_WORLD, &pid);

        int recv_msg = 0;
        MPI_Status status;
        MPI_Recv(&recv_msg, 1, MPI_INT, MPI::ANY_SOURCE, MPI::ANY_TAG, MPI_COMM_WORLD, &status);
        message_complexity += 1;

        /* Clock update after recieve */
        time_lock.lock();
        lamport_clock.store(max(lamport_clock.load(), recv_msg) + 1);

        /* Sender Information */
        int sender_clock = recv_msg;
        int sender_id = status.MPI_SOURCE;
        time_lock.unlock();
        int tag = status.MPI_TAG;

        /* Four types as recieved by the actual process */
        if (tag == DONE)
        {
            print("recieved DONE from " + to_string(sender_id));
            done_recv++;

            if (done_recv == n)
            {
                break;
            }
        }

        else if (tag == FAIL)
        {
            print("recieved FAIL from " + to_string(sender_id));
            set_handler(FAILSET, ADD, sender_id, 0);
        }

        else if (tag == REPLY)
        {
            print("recieved REPLY from " + to_string(sender_id));

            if (set_contains(FAILSET, sender_id))
            {
                set_handler(FAILSET, ERASE, sender_id, 0);
            }

            if (set_contains(RECVSET, sender_id))
            {
                set_handler(RECVSET, ERASE, sender_id, 0);
            }

            if (is_set_empty(RECVSET) && is_set_empty(FAILSET))
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

            if (!is_set_empty(FAILSET) || !is_set_empty(RECVSET))
            {
                lamport_clock.fetch_add(1);
                int timestamp = lamport_clock.load();

                // If recv set is not waiting for this sender's approval, it will now wait for it as it is sending INQUIRE
                if (!set_contains(RECVSET, sender_id))
                {
                    set_handler(RECVSET, ADD, sender_id, 0);
                }

                print("sending YEILD to " + to_string(sender_id));
                MPI_Send(&timestamp, 1, MPI_INT, sender_id, YEILD, MPI_COMM_WORLD);
            }
        }

        /* Three types as recieved by the quorum side of the process */
        else if (tag == REQUEST)
        {
            print("recieved REQUEST from " + to_string(sender_id));

            if (is_set_empty(PQ) == true)
            {
                print("sending REPLY to " + to_string(sender_id));
                time_lock.lock();
                lamport_clock.fetch_add(1);
                MPI_Send(&lamport_clock, 1, MPI_INT, sender_id, REPLY, MPI_COMM_WORLD);
                time_lock.unlock();

                set_handler(PQ, ADD, sender_clock, sender_id);

                last_send_pid = sender_id;
                last_send_time = sender_clock;
            }
            else
            {
                if (make_pair(last_send_time, last_send_pid) < make_pair(sender_clock, sender_id))
                {
                    print("sending FAIL(1) to " + to_string(sender_id));
                    time_lock.lock();
                    lamport_clock.fetch_add(1);
                    MPI_Send(&lamport_clock, 1, MPI_INT, sender_id, FAIL, MPI_COMM_WORLD);
                    time_lock.unlock();

                    set_handler(PQ, ADD, sender_clock, sender_id);
                }

                else
                {
                    if (inquires_sent == 0)
                    {
                        print("sending INQUIRE to " + to_string(last_send_pid));
                        time_lock.lock();
                        lamport_clock.fetch_add(1);
                        MPI_Send(&lamport_clock, 1, MPI_INT, last_send_pid, INQUIRE, MPI_COMM_WORLD);
                        time_lock.unlock();
                        set_handler(PQ, POP, 0, 0);
                        inquires_sent++;
                    }

                    set_handler(PQ, ADD, sender_clock, sender_id);
                }
            }
        }

        else if (tag == YEILD)
        {
            print("recieved YEILD from " + to_string(sender_id));
            inquires_sent--; // Here we can be very sure that this is reponse to the INQUIRE. So, we can decrement the inquires_sent

            // Previously, an inquire must had been sent by this process to which YEILD was recieved. So (ONLY) last sender was popped.
            // Now that we recieved yeild, we have to push that back. last_sent wouldn't change

            set_handler(PQ, ADD, last_send_time, last_send_pid);

            // And now, we can send the reply to the top of the queue
            last_send_pid = pq.top().second;
            last_send_time = pq.top().first;
            print("sending REPLY to " + to_string(last_send_pid));

            time_lock.lock();
            lamport_clock.fetch_add(1);
            MPI_Send(&lamport_clock, 1, MPI_INT, last_send_pid, REPLY, MPI_COMM_WORLD);
            time_lock.unlock();
        }

        else if (tag == RELEASE)
        {
            print("recieved RELEASE from " + to_string(sender_id));

            /*
                If this RELEASE is a reponse to a previous INQUIRE, no need to pop,
                but if there were no inquires sent before, then we should pop it.
            */

            if (inquires_sent == 0) // Means this is NOT a response to INQUIRE, so should pop
            {
                set_handler(PQ, POP, 0, 0);
            }

            else // Since this is response to an INQUIRE, we can reduce the count of INQURES
            {
                inquires_sent--;
            }

            last_send_pid = pq.top().second;
            last_send_time = pq.top().first;

            if (is_set_empty(PQ) == false)
            {
                // Send REPLY to the new top of the queue
                time_lock.lock();
                lamport_clock.fetch_add(1);
                MPI_Send(&lamport_clock, 1, MPI_INT, last_send_pid, REPLY, MPI_COMM_WORLD);
                time_lock.unlock();
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
    message_complexity_file << message_complexity << "\n";
    message_complexity_file.close();

    return 0;
}