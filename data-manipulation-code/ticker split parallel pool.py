import pandas as pd
import os
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from sys import argv


# output_file_path = argv[2]
output_file_path = 'taq_jul_2019_quotes'
if 'quotes' in output_file_path:
    column_names = ['DATE', 'TIME_M', 'EX', 'BID', 'BIDSIZ', 'ASK', 'ASKSIZ', 'SYM_ROOT', 'SYM_SUFFIX']
else:
    column_names = ['DATE', 'TIME_M', 'EX', 'SYM_ROOT', 'SYM_SUFFIX', 'SIZE', 'PRICE', 'TR_ID']


def file_writer(chunk):
    # print(chunk.SYM_ROOT.unique())
    chunk.columns = column_names
    # print(chunk.SYM_ROOT.unique())
    for ticker in chunk.SYM_ROOT.unique():
        print(ticker)
        if os.path.exists(os.path.join(output_file_path, ticker+'.csv')):
            header = False
            while os.path.getsize(os.path.join(output_file_path, ticker+'.csv')) == 0:
                pass
        else:
            header = True
        with open(os.path.join(output_file_path, ticker+'.csv'), 'a') as f:
            # print(ticker+'.csv')
            chunk[chunk.SYM_ROOT == ticker].to_csv(f, index=False, header=header)
            f.close()


def chunk_splitter(main_file, chunksize, skiprows, per_process_chunk):
    with ThreadPool(processes=10) as pool:
        print(skiprows)
        results = pool.imap(file_writer, pd.read_csv(main_file, skiprows=skiprows, chunksize=chunksize,
                                           nrows=chunksize*per_process_chunk, header=None))
        pool.close()
        pool.join()
    for r in results:
        print(r)


def main():
    # main_file = 'taq_sep_2019_quotes_500_tickers.csv'
    main_file = 'archive/taq_sep_2019_trades_500_tickers.csv'
    # print(argv[1], argv[2], argv[3])
    # main_file = argv[1]

    chunksize = 10 ** 6
    per_process_chunk = 10 * 5
    skiprows = 1
    # total_rows = int(argv[3])
    # total_rows = chunksize*per_process_chunk+skiprows
    total_rows = 284191154

    with Pool(processes=5, maxtasksperchild=1) as pool:
        result = pool.starmap(chunk_splitter, [(main_file, chunksize, s, per_process_chunk) for s in
                                      range(skiprows, total_rows, chunksize*per_process_chunk)])
        pool.close()
        pool.join()

    # for r in result:
    #     print(r)


if __name__ == '__main__':
    main()
