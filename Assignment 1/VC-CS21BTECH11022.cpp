#include <bits/stdc++.h>
#include <string.h>
#include <string>
#include <stdlib.h>
#include <cstdlib>
#include <ctime>
#include <sstream>
#include "mpi.h"
#include <time.h>
#include <chrono>
#include <fstream>
#include <atomic>
#include <mutex>
#include <random>
#include <pthread.h>
using namespace std;

ifstream inputfile;
mutex clock_lock;
mutex file_lock;

class my_data
{
public:
    int pid;
    int *my_clock;
    int size;
    int num_neighbours;
    int my_counter;
    int messages_left;
    int internal_left;
    int *my_neighbours;
    int exp_time;
    int space;
};

// Helper Function: Generates a random number from an exponential distribution with a mean of 'exp_time'.
double Timer(float exp_time)
{
    default_random_engine generate;
    exponential_distribution<double> distr(1.0 / exp_time);
    return distr(generate);
}

// Helper Function: Generates a random number among it's number of neighbours, to select one randomly
int Random_Neighbour(int neighbours)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distr(1, neighbours - 1);
    return distr(gen);
}

/*
    Helper function: Given we have the ratio, and the numner of messages to be sent,
    this calculates exact number of messages to be send and internal events to be perfomed
    Note that this function might adjust number of messages to be sent, inorder to get the exact ratio.
    For example, 1.5 ratio, and 1 send message results in 3:2 istad of 1:2.
*/
void get_numbers(float alpha, int m, int &messages_left, int &internal_left)
{
    int integral = floor(alpha);
    float decimal = alpha - integral;
    const int precision = 100000;

    int gcd = __gcd(int(decimal * precision), precision);

    int denom_frac = precision / gcd;
    int num_frac = round(decimal * precision) / gcd;

    internal_left = m * ((denom_frac * integral) + num_frac);
    messages_left = m * (denom_frac);
}

/* Function that sends and performs internal events */
void *performer_func(void *args)
{
    my_data *data = (my_data *)args;
    ofstream outputfile;
    string filename = "output-VC.log";

    while (true)
    {
        if (data->messages_left > 0) // Still should send
        {
            int instance[data->size];
            clock_lock.lock();
            data->my_clock[data->pid] += 1;
            for (int i = 0; i < data->size; i++)
            {
                instance[i] = data->my_clock[i];
            }
            clock_lock.unlock();

            int rand = Random_Neighbour(data->num_neighbours);
            int dest = data->my_neighbours[rand];

            data->messages_left--;
            data->my_counter++;
            MPI_Send(data->my_clock, data->size, MPI_INT, dest, data->my_counter, MPI_COMM_WORLD);
            data->space += data->size;

            /* Output file handling */
            time_t current = time(0);
            struct tm *timeinfo = localtime(&current);
            char buffer[80];
            strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);
            std::string timeString(buffer);

            file_lock.lock();
            outputfile.open(filename, ios::app);
            outputfile << "Process " << data->pid << " sends message m" << data->pid << data->my_counter << " to process " << dest << " at " << timeString << ", vc:";
            outputfile << "[" << instance[0];

            for (int i = 1; i < data->size; i++)
            {
                outputfile << " " << instance[i];
            }
            outputfile << "]\n";
            outputfile.close();
            file_lock.unlock();
        }

        if (data->messages_left == 0) // All sent, sending marker messages
        {
            for (int i = 0; i < data->size; i++)
            {
                if (i != data->pid)
                {
                    MPI_Send(data->my_clock, data->size, MPI_INT, i, 0, MPI_COMM_WORLD);
                }
            }
            data->messages_left--;
        }

        if (data->internal_left > 0) // Internal events
        {
            int instance[data->size];
            clock_lock.lock();
            data->my_clock[data->pid] += 1;
            copy(data->my_clock, data->my_clock + data->size, instance);
            clock_lock.unlock();
            time_t current = time(0);
            sleep(Timer(data->exp_time));
            data->internal_left--;
            data->my_counter++;

            /* Output file handling */
            file_lock.lock();
            outputfile.open(filename, ios::app);

            struct tm *timeinfo = localtime(&current);
            char buffer[80];
            strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);
            std::string timeString(buffer);

            outputfile << "Process " << data->pid << " executes an internal event e" << data->pid << data->my_counter << " at " << timeString << ", vc:";
            outputfile << "[" << instance[0];

            for (int i = 1; i < data->size; i++)
            {
                outputfile << " " << instance[i];
            }
            outputfile << "]\n";
            outputfile.close();
            file_lock.unlock();
        }

        if (data->internal_left == 0 && data->messages_left == -1)
        {
            break;
        }
    }

    return (void *)NULL;
}

void *reciever_func(void *args)
{
    my_data *data = (my_data *)args;
    ofstream outputfile;
    string filename = "output-VC.log";
    int finished = 0;

    while (finished < data->size - 1)
    {
        int n = data->size;
        int *recv = new int[n];

        MPI_Status status1;
        MPI_Recv(recv, n, MPI_INT, MPI::ANY_SOURCE, MPI::ANY_TAG, MPI_COMM_WORLD, &status1);

        int sender = status1.MPI_SOURCE;
        int tag = status1.MPI_TAG;

        /* Time for output file handling */
        time_t current = time(0);
        struct tm *timeinfo = localtime(&current);
        char buffer[80];
        strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);
        std::string timeString(buffer);

        if (tag > 0)
        {
            int instance[data->size];
            clock_lock.lock();
            for (int i = 0; i < data->size; i++)
            {
                data->my_clock[i] = max(data->my_clock[i], recv[i]);
                instance[i] = data->my_clock[i]; // Capturing this so that recording time is printed, not the printing time
            }
            data->my_clock[data->pid] += 1;
            clock_lock.unlock();

            /* Output file handling */
            file_lock.lock();
            outputfile.open(filename, ios::app);
            outputfile << "Process " << data->pid << " recieved message m" << sender << tag << " from process " << sender << " at " << timeString << ", vc:";

            outputfile << "[" << instance[0];

            for (int i = 1; i < data->size; i++)
            {
                outputfile << " " << instance[i];
            }
            outputfile << "]\n";
            outputfile.close();
            file_lock.unlock();
        }

        else if (tag == 0)
        {
            finished++;
        }

        delete[] recv;
    }
    // cout << data->pid << " exiting receive fun\n";
    return (void *)NULL;
}

int main(int argc, char *argv[])
{
    int num_procs = 0, exp_time = 0, messages = 0; // n, lambda, m values as in doc
    float ratio = 0.0;                             // alpha value as in doc

    int num_neighbours = 0; // Gives number of neighbours of this process
    int messages_left = 0;  // Gives number of messages left for this process to send
    int internal_left = 0;  // Gives number of internal events for this process to perform

    /* Input file handling starts */
    inputfile.open("inp-params.txt");
    inputfile >> num_procs >> exp_time >> ratio >> messages;
    inputfile.ignore();
    /* Input file handline ends */

    int pid = 0, size = 0;
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    int this_proc_counter = 0;

    /* Snippet for getting neighbours */
    vector<int> my_neighbours_vec;
    int number;
    string str;
    for (int i = 0; i < pid; i++)
    {
        std::getline(inputfile, str);
    }
    getline(inputfile, str);
    stringstream iss(str);

    while (iss >> number)
    {
        num_neighbours++;
        my_neighbours_vec.push_back(number - 1);
    }
    inputfile.close();
    /* Snippet ends */

    /* Gets the number of internal events and send events to perform */
    get_numbers(ratio, messages, messages_left, internal_left);

    my_data *data = new my_data;
    data->pid = pid;
    data->my_clock = new int[size];
    memset(data->my_clock, 0, sizeof(data->my_clock[0]) * size);

    data->my_counter = this_proc_counter;
    data->messages_left = messages_left;
    data->internal_left = internal_left;
    data->num_neighbours = num_neighbours;
    data->exp_time = exp_time;
    data->my_neighbours = (int *)malloc(sizeof(int) * num_neighbours);
    data->size = size;
    data->space = 0;

    for (int i = 0; i < num_neighbours; i++)
    {
        data->my_neighbours[i] = my_neighbours_vec[i];
    }

    pthread_t listener;
    pthread_t performer;

    pthread_create(&listener, NULL, &reciever_func, data);
    pthread_create(&performer, NULL, &performer_func, data);

    pthread_join(listener, NULL);
    pthread_join(performer, NULL);

    std::cout << data->space << " ";

    MPI_Barrier(MPI_COMM_WORLD);
    free(data->my_neighbours);
    delete[] data->my_clock;
    delete data;
    MPI_Finalize();
    return 0;
}