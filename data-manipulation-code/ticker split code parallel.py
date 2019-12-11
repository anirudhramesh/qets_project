import pandas as pd
import os
from multiprocessing import Process, Queue
from time import sleep


column_names = ['DATE', 'TIME_M', 'EX', 'BID', 'BIDSIZ', 'ASK', 'ASKSIZ', 'SYM_ROOT', 'SYM_SUFFIX']


# print_queue = Queue()
# def print_queue_func():
#     while True:
#         print(print_queue.get())
# Process(target=print_queue_func, daemon=True).start()


def iterate_chunk(data_chunks, file_process_queue, val):
    count = 0
    for chunk in data_chunks:
        chunk.columns = column_names
        count += 1
        for ticker in chunk.SYM_ROOT.unique():
            print(val, count, ticker)
            file_process_queue.put((chunk[chunk.SYM_ROOT == ticker], ticker+'.csv'))


def file_write_queue_processor(file_path, file_process_queue):
    print('writer initiated')
    tickers = []
    processes = []

    while True:
        write_data = file_process_queue.get()
        # print('Sum of active queue lengths:', sum([q[1].qsize() for q in processes]), file_process_queue.qsize())
        if isinstance(write_data, str):
            # print('main one ' + write_data)
            break
        if write_data[1] in tickers:
            processes[tickers.index(write_data[1])][1].put(write_data[0])
        else:
            some_queue = Queue(maxsize=2)
            processes.append((Process(target=file_writer, args=(os.path.join(file_path, write_data[1]), some_queue)),
                              some_queue))
            some_queue.put(write_data[0])
            # processes[-1][0].start()
            tickers.append(write_data[1])

    for proc in processes:
        proc[1].put('Fin.')
        proc[0].join()


def file_writer(file_name, data_queue):
    while True:
        val = data_queue.get()
        if isinstance(val, str):
            # print(val)
            break
        # print(file_name)
        if os.path.exists(file_name):
            header = False
        else:
            header = True
        with open(file_name, 'a') as f:
            val.to_csv(f, header=header, index=False)
            f.close()


def file_reader(main_file, file_process_queue):
    print('reader initiated')
    chunksize = 10**6
    per_process_chunk = 10**3
    skiprows = 1
    total_rows = 4636032216
    # total_rows = 100000000

    processes = []

    val = 0
    while skiprows < total_rows:
        # print(val)
        val += 1
        data_chunk = pd.read_csv(main_file, skiprows=skiprows, header=None, chunksize=chunksize,
                                 nrows=chunksize * per_process_chunk)
        processes.append(Process(target=iterate_chunk, args=(data_chunk, file_process_queue, val)))
        processes[-1].start()
        skiprows += chunksize * per_process_chunk

    print('Active read processes: ', len(processes))

    for proc in processes:
        proc.join()

    file_process_queue.put('Fin.')


def main():
    main_file = 'taq_aug_2019_quotes_500_tickers.csv'
    # main_file = 'sample_100M_rows_taq_aug_2019_quotes_500_tickers.csv'
    output_file_path = 'taq_aug_2019_quotes_parallel'

    file_process_queue = Queue(maxsize=5)
    file_write_process = Process(target=file_write_queue_processor, args=(output_file_path, file_process_queue))
    file_write_process.start()

    file_reader(main_file, file_process_queue)

    file_write_process.join()


if __name__ == '__main__':
    main()
