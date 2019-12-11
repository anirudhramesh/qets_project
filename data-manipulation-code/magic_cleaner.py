from os import system, walk, path
from multiprocessing import Pool, Queue, Process
import pandas as pd
from sys import argv
from numpy import any, array


print_queue = Queue()
quotes_columns = array(['DATE', 'TIME_M', 'EX', 'BID', 'BIDSIZ', 'ASK', 'ASKSIZ', 'SYM_ROOT', 'SYM_SUFFIX'])
trades_columns = array(['DATE', 'TIME_M', 'EX', 'SYM_ROOT', 'SYM_SUFFIX', 'SIZE', 'PRICE', 'TR_ID'])


def printer():
    while True:
        val = print_queue.get()
        if val == 'fin':
            break
        print(val)


def sorter(original_file):
    final_file = original_file.replace('.csv', '_sorted.csv')
    temp_file = 'temp_'+path.split(final_file)[1]
    for df in pd.read_csv(original_file, chunksize=10 ** 6, parse_dates=[[0, 1]], infer_datetime_format=True):
        df.sort_values('DATE_TIME_M', inplace=True)
        df.drop(['SYM_SUFFIX'], axis=1, inplace=True)

        try:
            other_frame = pd.read_csv(final_file, nrows=2, parse_dates=[0], infer_datetime_format=True)
        except FileNotFoundError:
            df.to_csv(final_file, index=False)
            continue

        if df.DATE_TIME_M.iloc[0] > other_frame.DATE_TIME_M.iloc[0]:
            with open(final_file, 'a') as f:
                df.to_csv(f, header=False, index=False)
                f.close()
        else:
            df.to_csv(temp_file, index=False)
            system('{ tail -n +1 ' + temp_file + ' | tail -n +2 ' + final_file + ' } | cat > ' + final_file)
    system('rm ' + temp_file)


def checker(file):
    # print_queue.put(file)
    all_chunks = pd.read_csv(file, chunksize=10**6)
    first_chunk = next(all_chunks)
    try:
        if (('quotes' in file) and any(first_chunk.columns != quotes_columns)) or (('trades' in file) and any(first_chunk.columns != trades_columns)):
            print_queue.put('Column mismatch: ' + file)
            return
    except ValueError:
        print_queue.put('Size difference: ' + file + str(first_chunk.columns))
        return
    ticker = first_chunk.SYM_ROOT.unique()[0]
    for chunk in all_chunks:
        if chunk.SYM_ROOT.unique()[0] != ticker:
            print_queue.put('Ticker mismatch: File starts with ' + ticker + ' and also contains ' +
                            chunk.SYM_ROOT.unique()[0])


def sort_caller():
    all_pools = []
    results = []
    for i in range(1, len(argv)):
        original_path = argv[i]
        # print(original_path)
        files = next(walk(original_path))[2]
        all_pools.append(Pool(processes=10, maxtasksperchild=3))
        results.append(all_pools[-1].map_async(sorter, [path.join(original_path, f) for f in files]))
        all_pools[-1].close()

    for i in range(len(all_pools)):
        all_pools[i].join()
        print_queue.put(results[i].get())


def check_caller():
    all_pools = []
    results = []
    for i in range(1, len(argv)):
        original_path = argv[i]
        print(original_path)
        files = next(walk(original_path))[2]
        # print_queue.put(files)
        all_pools.append(Pool(processes=10, maxtasksperchild=3))
        results.append(all_pools[-1].map_async(checker, [path.join(original_path, f) for f in files]))
        all_pools[-1].close()

    for i in range(len(all_pools)):
        all_pools[i].join()
        print_queue.put(results[i].get())


def main():
    printer_process = Process(target=printer)
    printer_process.start()

    # check_caller()
    sort_caller()

    print_queue.put('fin')
    printer_process.join()


if __name__ == '__main__':
    main()
