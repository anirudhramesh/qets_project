from os import system, walk, path
from multiprocessing import Pool, Queue, Process
import pandas as pd
from sys import argv


print_queue = Queue()
quotes_columns = ['DATE', 'TIME_M', 'EX', 'BID', 'BIDSIZ', 'ASK', 'ASKSIZ', 'SYM_ROOT', 'SYM_SUFFIX']
trades_columns = ['DATE', 'TIME_M', 'SYM_ROOT', 'SYM_SUFFIX', 'SIZE', 'PRICE', 'TR_ID']


def printer():
    while True:
        val = print_queue.get()
        if val == 'fin':
            break
        print(val)


def sorter(original_file):
    final_file = original_file.replace('.csv', '_sorted.csv')
    temp_file = 'temp_'+path.split(final_file)[1]
    for df in pd.read_csv(original_file, chunksize=10 ** 6):
        try:
            df['DATE_TIME'] = df.DATE.astype(str) + ' ' + df.TIME_M.astype(str)
        except AttributeError as e:
            print(e)
            print(path.split(final_file)[1].replace('.csv', ''))
            break
        df.DATE_TIME = pd.to_datetime(df.DATE_TIME)
        df.sort_values('DATE_TIME', inplace=True)
        df.drop(['DATE', 'TIME_M', 'SYM_SUFFIX'], axis=1, inplace=True)

        try:
            other_frame = pd.read_csv(final_file, nrows=2)
        except FileNotFoundError:
            df.to_csv(final_file, index=False)
            continue
        other_frame.DATE_TIME = pd.to_datetime(other_frame.DATE_TIME)

        if df.DATE_TIME.iloc[0] > other_frame.DATE_TIME.iloc[0]:
            with open(final_file, 'a') as f:
                df.to_csv(f, header=False, index=False)
                f.close()
        else:
            df.to_csv(temp_file, index=False)
            system('{ tail -n +1 ' + temp_file + ' | tail -n +2 ' + final_file + ' } | cat > ' + final_file)
    system('rm ' + temp_file)


def checker(file, chunksize):
    all_chunks = pd.read_csv(file, chunksize=chunksize)
    first_chunk = all_chunks.get()
    if (('quotes' in file) and (first_chunk.columns != quotes_columns)) or (first_chunk.columns != trades_columns):
        print_queue.put('Column mismatch: ' + file)
        return
    ticker = first_chunk.SYM_ROOT.unique()[0]
    for chunk in all_chunks:
        if chunk.SYM_ROOT.unique()[0] != ticker:
            print_queue.put('Ticker mismatch: File starts with ' + ticker + ' and also contains ' +
                            chunk.SYM_ROOT.unique()[0])


def sort_caller():
    original_path = argv[1]
    print(original_path)
    files = next(walk(original_path))[2]
    pool = Pool(processes=30, maxtasksperchild=3)
    results = pool.map_async(sorter, [path.join(original_path, f) for f in files])
    pool.close()
    pool.join()
    print(results.get())


def check_caller():
    original_path = argv[1]
    # print(original_path)
    files = next(walk(original_path))[2]
    pool = Pool(processes=30, maxtasksperchild=3)
    results = pool.map_async(checker, [path.join(original_path, f) for f in files])
    pool.close()
    pool.join()
    print(results.get())


def main():
    printer_process = Process(target=printer)
    printer_process.start()

    check_caller()

    print_queue.put('fin')
    printer_process.join()


if __name__ == '__main__':
    main()
