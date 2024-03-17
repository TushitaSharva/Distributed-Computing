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
    int *last_sent;
    int *last_update;
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

/* Function for sending and performing internal events */
void *performer_func(void *args)
{
    my_data *data = (my_data *)args;
    ofstream outputfile;
    string filename = "output-SK.log";

    while (true)
    {
        if (data->messages_left > 0)
        {
            /* Capturing the time of sending */
            int instance[data->size];
            clock_lock.lock();
            data->my_clock[data->pid] += 1;
            data->last_update[data->pid] = data->my_clock[data->pid];

            for(int i = 0; i < data->size; i++)
            {
                instance[i] = data->my_clock[i];
            }
            clock_lock.unlock();

            int rand = Random_Neighbour(data->num_neighbours);
            int dest = data->my_neighbours[rand];
            // std::cout << dest << "skdest\n";

            data->messages_left--;
            data->my_counter++;

            /* Arranging the tuples to send */
            int num_tuples = 0;
            vector<int> indexes;

            for (int i = 0; i < data->size; i++)
            {
                if (data->last_sent[dest] < data->last_update[i])
                {
                    indexes.push_back(i);
                }
            }

            int *send_msg = (int *)malloc(sizeof(int) * (2 * indexes.size() + 1));
            send_msg[2 * indexes.size()] = -1;
            int j = 0;

            for (int i = 0; i < indexes.size(); i++)
            {
                send_msg[j] = indexes[i];
                send_msg[j + 1] = instance[indexes[i]];
                j += 2;
            }

            MPI_Send(send_msg, 2 * indexes.size() + 1, MPI_INT, dest, data->my_counter, MPI_COMM_WORLD);
            data->last_sent[dest] = instance[data->pid];
            data->space += 2 * indexes.size() + 1;

            /* Output file handling */
            time_t current = time(0);
            struct tm *timeinfo = localtime(&current);
            char buffer[80];
            strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);
            std::string timeString(buffer);

            outputfile.open(filename, ios::app);
            outputfile << "Process " << data->pid << " sends message m" << data->pid << data->my_counter << " to process " << dest << " at " << timeString << ", vc:";
            outputfile << "[" << instance[0];

            for (int i = 1; i < data->size; i++)
            {
                outputfile << " " << instance[i];
            }
            outputfile << "] ";

            outputfile << "Sending tuples: [";
            for (int i = 0; i < indexes.size() * 2; i += 2)
            {
                outputfile << "[" << send_msg[i] << ", " << send_msg[i + 1] << "]";
            }
            outputfile << "]\n";
            outputfile.close();
            free(send_msg);
        }

        if (data->messages_left == 0)
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

        if (data->internal_left > 0)
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
        }

        if (data->internal_left == 0 && data->messages_left == -1)
        {
            break;
        }
    }

    return (void *)NULL;
}

/* Function for recieving */
void *reciever_func(void *args)
{
    my_data *data = (my_data *)args;
    ofstream outputfile;
    string filename = "output-SK.log";
    int finished = 0;

    while (finished < data->size - 1)
    {
        int *recv = (int *)malloc(sizeof(int) * (2 * data->size + 1));

        MPI_Status status1;
        MPI_Recv(recv, 2 * data->size + 1, MPI_INT, MPI::ANY_SOURCE, MPI::ANY_TAG, MPI_COMM_WORLD, &status1);
        int sender = status1.MPI_SOURCE;
        int tag = status1.MPI_TAG;
        /* Time for output file handling */
        time_t current = time(0);
        struct tm *timeinfo = localtime(&current);
        char buffer[80];
        strftime(buffer, sizeof(buffer), "%H:%M:%S", timeinfo);
        std::string timeString(buffer);

        if (tag == 0)
        {
            finished++;
        }

        else
        {
            int recv_size = 0;

            int i = 0;
            while (recv[i] != -1)
            {
                i++;
            }

            recv_size = i >> 1;

            int instance[data->size];

            clock_lock.lock();
            data->my_clock[data->pid] += 1;
            data->last_update[data->pid] = data->my_clock[data->pid];

            for (int i = 0; i < recv_size; i += 2)
            {
                int index = recv[i];
                int value = recv[i + 1];

                if (data->my_clock[index] < value)
                {
                    data->my_clock[index] = value;
                    data->last_update[index] = data->my_clock[data->pid];
                }
            }

            copy(data->my_clock, data->my_clock + data->size, instance);
            clock_lock.unlock();

            /* Output file handling */
            outputfile.open(filename, ios::app);
            outputfile << "Process " << data->pid << " recieved message m" << sender << tag << " from process " << sender << " at " << timeString << ", vc:";

            outputfile << "[" << instance[0];

            for (int i = 1; i < data->size; i++)
            {
                outputfile << " " << instance[i];
            }
            outputfile << "]\n";
            outputfile.close();
        }

        free(recv);
    }

    return (void *)NULL;
}

int main(int argc, char *argv[])
{
    int num_procs, exp_time, messages; // n, lambda, m values as in doc
    float ratio;                       // alpha value as in doc

    int num_neighbours; // Gives number of neighbours of this process
    int messages_left;  // Gives number of messages left for this process to send
    int internal_left;  // Gives number of internal events for this process to perform

    /* Input file handling starts */
    inputfile.open("inp-params.txt");
    inputfile >> num_procs >> exp_time >> ratio >> messages;
    // std::cout << num_procs << exp_time << ratio << messages << "\n";
    inputfile.ignore();
    /* Input file handline ends */

    int pid, size;
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    int this_proc_counter = 0;

    pthread_t listener;
    pthread_t performer;

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

    data->last_sent = new int[size];
    memset(data->last_sent, 0, sizeof(data->last_sent[0]) * size);

    data->last_update = new int[size];
    memset(data->last_update, 0, sizeof(data->last_update[0]) * size);

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

    pthread_create(&listener, NULL, &reciever_func, data);
    pthread_create(&performer, NULL, &performer_func, data);

    pthread_join(listener, NULL);
    pthread_join(performer, NULL);

    std::cout << data->space << " ";

    free(data->my_neighbours);
    delete[] data->my_clock;
    delete[] data->last_sent;
    delete[] data->last_update;
    delete data;

    MPI_Finalize();
    return 0;
}
