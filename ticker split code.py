import pandas as pd
import os
from multiprocessing import Process, Queue
from time import sleep


column_names = ['DATE', 'TIME_M', 'EX', 'BID', 'BIDSIZ', 'ASK', 'ASKSIZ', 'SYM_ROOT', 'SYM_SUFFIX']


def iterate_chunk(data_chunks, file_process_queue):
    count = 0
    for chunk in data_chunks:
        chunk.columns = column_names
        count += 1
        for ticker in chunk.SYM_ROOT.unique():
            file_process_queue.put((chunk[chunk.SYM_ROOT == ticker], ticker+'.csv'))
        # while len(file_process_queue) > 5:
        # 	pass


def file_write_queue_processor(file_path, file_process_queue):
    tickers = []
    processes = []
    while True:
        write_data = file_process_queue.get()
        if isinstance(write_data, str):
            break
        if write_data[1] in tickers:
            processes[tickers.index(write_data[1])][1].put(write_data[0])
        else:
            some_queue = Queue(maxsize=5)
            processes.append((Process(target=file_writer, args=(os.path.join(file_path, write_data[1]), some_queue)),
                              some_queue))
            some_queue.put(write_data[0])
            processes[-1][0].start()
            tickers.append(write_data[1])
        # while sum([len(proc[1]) for proc in processes]) > 5:
        # 	pass

    for proc in processes:
        proc[1].put('Fin.')
        proc[0].join()


def file_writer(file_name, data_queue):
    while True:
        val = data_queue.get()
        if isinstance(val, str):
            break
        if os.path.exists(file_name):
            header = False
        else:
            header = True
        with open(file_name, 'a') as f:
            val.to_csv(f, header=header, index=False)
            f.close()


def file_reader(main_file, file_process_queue):
    chunksize = 10**6
    per_process_chunk = 10**3
    skiprows = 1
    total_rows = 4636032216

    processes = []

    while skiprows < total_rows:
        data_chunk = pd.read_csv(main_file, skiprows=skiprows, header=None, chunksize=chunksize,
                                 nrows=chunksize * per_process_chunk)
        processes.append(Process(target=iterate_chunk, args=(data_chunk, file_process_queue)))
        processes[-1].start()
        skiprows += chunksize * per_process_chunk

    for proc in processes:
        proc.join()

    file_process_queue.put('Fin.')


def main():
    main_file = 'sample_50M_rows_taq_aug_2019_quotes_500_tickers.csv'
    output_file_path = 'taq_aug_2019_quotes_parallel'

    file_process_queue = Queue(maxsize=5)
    file_write_process = Process(target=file_write_queue_processor, args=(output_file_path, file_process_queue))
    file_write_process.start()

    file_reader(main_file, file_process_queue)

    file_write_process.join()


if __name__ == '__main__':
    main()
