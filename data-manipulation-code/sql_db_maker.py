import pandas as pd
from sqlalchemy import create_engine
from multiprocessing import Pool, Queue, Process


ticker_queue = Queue()
def ticker_queue_proc():
    tickers_quotes = []
    tickers_trades = []
    while True:
        val = ticker_queue.get()
        if val == 'fin':
            tickers_quotes = set(tickers_quotes)
            tickers_trades = set(tickers_trades)
            print('Quotes: ', tickers_quotes)
            print('Quotes Count: ', len(tickers_quotes))
            print('Trades: ', tickers_trades)
            print('Trades Count: ', len(tickers_trades))
            break
        if 'quotes' in val:
            tickers_quotes.append(val.replace('quotes_', ''))
        else:
            tickers_trades.append(val.replace('trades_', ''))


print_queue = Queue()
def print_queue_proc():
    while True:
        print(print_queue.get())


def write_db(file_path, skiprows, chunksize, nrows, sql_engine_pool):
    sql_engine = sql_engine_pool.connect()
    chunks = pd.read_csv(file_path, skiprows=skiprows, chunksize=chunksize, nrows=nrows, header=None, parse_dates=[[0,1]],
                         infer_datetime_format=True)
    if 'quotes' in file_path:
        column_names = ['DATE', 'TIME_M', 'EX', 'BID', 'BIDSIZ', 'ASK', 'ASKSIZ', 'SYM_ROOT', 'SYM_SUFFIX']
    else:
        column_names = ['DATE', 'TIME_M', 'EX', 'SYM_ROOT', 'SYM_SUFFIX', 'SIZE', 'PRICE', 'TR_ID']
    for chunk in chunks:
        chunk.columns = column_names
        chunk.to_sql('all_quotes', sql_engine, if_exists='append')
        for ticker in chunk.SYM_ROOT.unique():
            ticker_queue.put('quotes_'+ticker if 'quotes' in file_path else 'trades_'+ticker)
            chunk[chunk.SYM_ROOT == ticker].to_sql('quotes_'+ticker if 'quotes' in file_path else 'trades_'+ticker,
                                                   sql_engine, if_exists='append', index=False)
    # sql_engine.dispose()


def main():
    # files = ['archive/taq_aug_2019_quotes_500_tickers.csv']#, 'archive/taq_aug_2019_trades_500_tickers.csv']
    files = ['/scratch/ar1515/QETS_Project/archive/taq_aug_2019_trades_500_tickers.csv']
    # total_rows = [4636032216]#, 314208630]
    total_rows = [314208630]
    chunksize = 10**6
    per_process_chunk = 10*5
    skiprows = 1
    sql_engine = create_engine('sqlite:////scratch/ar1515/archive/taq_aug_2019_trades.db', pool_size=20)

    ticker_process = Process(target=ticker_queue_proc)
    ticker_process.start()

    print_proc = Process(target=print_queue_proc, daemon=True)
    print_proc.start()

    proc_pools = []
    results = []
    for i in range(len(files)):
        proc_pools.append(Pool(processes=10, maxtasksperchild=5))
        results.append(proc_pools[-1].starmap_async(write_db, [(files[i], s, chunksize, chunksize*per_process_chunk, sql_engine) for
                                                s in range(skiprows, total_rows[i], chunksize*per_process_chunk)]))
        proc_pools[-1].close()

    for i in range(len(proc_pools)):
        proc_pools[i].join()
        print_queue.put(results[i].get())

    ticker_queue.put('fin')
    ticker_process.join()


if __name__ == '__main__':
    main()
