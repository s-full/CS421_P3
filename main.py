from heapq import heappush, heappop

from prettytable import PrettyTable


class Process:
    def __init__(self, pid, burst_time, arrival_time):
        self.pid = pid
        self.burst_time = burst_time
        self.arrival_time = arrival_time
        self.remaining_time = self.burst_time

        self.start_time = None
        self.exit_time = None

    def __lt__(self, other):
        return int(self.remaining_time) < int(other.remaining_time)

    def __gt__(self, other):
        return int(self.remaining_time) > int(other.remaining_time)


def print_stats(completed):
    sum_of_tt = 0
    sum_of_wt = 0
    completed.sort(key=lambda process: process.pid)
    my_table = PrettyTable(["Process ID", "Turnaround Time", "Waiting Time"])

    for job in completed:
        turnaround_time = int(job.exit_time) - int(job.arrival_time)
        waiting_time = turnaround_time - int(job.burst_time)
        my_table.add_row([job.pid, turnaround_time, waiting_time])
        sum_of_tt = sum_of_tt + turnaround_time
        sum_of_wt = sum_of_wt + waiting_time

    my_table.add_row(["Average", sum_of_tt / len(completed), sum_of_wt / len(completed)])
    print(my_table)


def run_FCFS(queue):
    print("First Come First Serve Scheduling")
    print()
    completed = []
    waiting = []
    time = 0
    current_job = None

    while True:

        # if no jobs left and last job complete, exit
        if len(queue) == 0 and len(waiting) == 0 and current_job.remaining_time == 0:
            current_job.exit_time = time
            completed.append(current_job)
            print("%d %s  Process terminated" % (time, current_job.pid))
            print("Complete")
            print()
            break
        # add jobs that are arriving
        while len(queue) != 0 and int(queue[0].arrival_time) <= time:
            waiting.append(queue.pop(0))

        # check if nothing is running or if current_job is done
        if current_job is None or current_job.remaining_time == 0:

            # if job finished, move to completed queue
            if current_job is not None:
                # set exit time
                current_job.exit_time = time
                # move to completed list
                completed.append(current_job)
                print("%d %s  Process terminated" % (time, current_job.pid))

            # set curr job to most recent in waiting queue, if waiting queue has jobs waiting
            if len(waiting) != 0:
                current_job = waiting.pop(0)
                current_job.start_time = time

        # Do the job and increment time
        current_job.remaining_time = int(current_job.remaining_time) - 1
        time = time + 1

    print_stats(completed)

    return


def load_jobs(queue):
    with open('Prog3inputfileS23.txt', 'r') as f:
        while True:
            line = f.readline()
            if not line:
                break
            contents = line.split()
            p = Process(contents[0], contents[1], contents[2])
            queue.append(p)
    return queue


def run_RR(queue, max_quantum):
    print("Round Robin Scheduling")
    print()
    completed = []
    waiting = []
    time = 0
    current_job = None

    while True:

        # if no jobs left and last job complete, exit
        if len(queue) == 0 and len(waiting) == 0 and (current_job.remaining_time == 0 or current_job is None):
            current_job.exit_time = time
            completed.append(current_job)
            print("%d %s  Process terminated" % (time, current_job.pid))
            print("Complete")
            print()
            break

        # add jobs that are arriving
        while len(queue) != 0 and int(queue[0].arrival_time) <= time:
            waiting.append(queue.pop(0))

        # check if nothing is running or if current_job is done
        if current_job is None or current_job.remaining_time == 0:
            if current_job is not None:
                # set current job quantum to 0
                quantum_count = 0
                # set exit time of current job
                current_job.exit_time = time
                completed.append(current_job)
                print("%d %s  Process terminated" % (time, current_job.pid))

            # set curr job to most recent in waiting queue, if waiting queue has jobs waiting
            if len(waiting) != 0:
                # set current job quantum to 0
                quantum_count = 0
                current_job = waiting.pop(0)
                current_job.start_time = time

        # check if current job's quantum has expired
        if max_quantum == quantum_count:
            print("%d %s  Quantum expired - %d ms remaining" % (time, current_job.pid, int(current_job.remaining_time)))
            quantum_count = 0
            # move current job to waiting queue
            waiting.append(current_job)

            # move next job in waiting queue to be current job
            if len(waiting) != 0:
                current_job = waiting.pop(0)
            else:
                current_job = None

        # do work, increment current job's quantum,
        quantum_count = quantum_count + 1
        current_job.remaining_time = int(current_job.remaining_time) - 1
        time = time + 1

    print_stats(completed)

    return


def run_SRTF(queue):
    print("Shortest Remaining Time First Scheduling")
    print()
    completed = []
    waiting = []
    time = 0
    current_job = None

    while True:

        # if no jobs left and last job complete, exit
        if len(queue) == 0 and len(waiting) == 0 and (current_job.remaining_time == 0 or current_job is None):
            current_job.exit_time = time
            completed.append(current_job)
            print("%d %s  Process terminated" % (time, current_job.pid))
            print("Complete")
            print()
            break

        # add jobs that are arriving
        while len(queue) != 0 and int(queue[0].arrival_time) <= time:
            heappush(waiting, (int(queue[0].remaining_time), queue.pop(0)))

        if current_job is None or current_job.remaining_time == 0:
            if current_job is not None:
                current_job.exit_time = time
                completed.append(current_job)
                print("%d %s  Process terminated" % (time, current_job.pid))

            if len(waiting) != 0:
                current_job = heappop(waiting)[1]
                current_job.start_time = time

        if len(waiting) != 0:
            # peek heap tuple
            heaptop = waiting[0]
            if int(heaptop[0]) < int(current_job.remaining_time):
                print("%d %s  Process preempted - %d ms remaining" % (
                    time, current_job.pid, int(current_job.remaining_time)))
                heappush(waiting, (int(current_job.remaining_time), current_job))
                current_job = heappop(waiting)[1]

        current_job.remaining_time = int(current_job.remaining_time) - 1
        time = time + 1

    print_stats(completed)
    return


if __name__ == '__main__':
    queue = []
    load_jobs(queue)
    run_FCFS(queue)

    load_jobs(queue)
    quantum = 3
    run_RR(queue, quantum)

    load_jobs(queue)
    run_SRTF(queue)
