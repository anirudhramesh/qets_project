import pandas as pd
from sqlalchemy import create_engine
from multiprocessing import Pool


def write_db(file_path, skiprows, chunksize, nrows, sql_engine):
    # print('entered')
    sql_engine = create_engine(sql_engine)
    chunks = pd.read_csv(file_path, skiprows=skiprows, chunksize=chunksize, nrows=nrows, header=None)
    # print('read')
    if 'quotes' in file_path:
        column_names = ['DATE', 'TIME_M', 'EX', 'BID', 'BIDSIZ', 'ASK', 'ASKSIZ', 'SYM_ROOT', 'SYM_SUFFIX']
    else:
        column_names = ['DATE', 'TIME_M', 'EX', 'SYM_ROOT', 'SYM_SUFFIX', 'SIZE', 'PRICE', 'TR_ID']
    for chunk in chunks:
        print('chunking')
        chunk.columns = column_names
        for ticker in chunk.SYM_ROOT.unique():
            chunk[chunk.SYM_ROOT == ticker].to_sql('quotes_'+ticker if 'quotes' in file_path else 'trades_'+ticker,
                                                   sql_engine, if_exists='append')


def main():
    files = ['archive/taq_aug_2019_quotes_500_tickers.csv', 'archive/taq_aug_2019_trades_500_tickers.csv']
    total_rows = [4636032216, 314208630]
    chunksize = 10**6
    per_process_chunk = 10*5
    skiprows=1
    sql_engine = 'sqlite:///archive/taq_aug_2019.db'
    # print('reached')

    proc_pools = []
    results = []
    for i in range(len(files)):
        proc_pools.append(Pool(processes=10, maxtasksperchild=5))
        results.append(proc_pools[-1].starmap_async(write_db, [(files[i], s, chunksize, chunksize*per_process_chunk, sql_engine) for
                                                s in range(skiprows, total_rows[i], chunksize*per_process_chunk)]))
        proc_pools[-1].close()
    # print('set up')

    # print(result.get())

    for i in range(len(proc_pools)):
        proc_pools[i].join()
    print('done')


if __name__ == '__main__':
    main()
