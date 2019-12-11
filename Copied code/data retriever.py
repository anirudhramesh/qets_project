import pandas as pd
from os.path import split, join
from os import walk
from multiprocessing import Pool, Queue, Process
from datetime import time
from scipy.stats import zscore
from numpy import nan

final_frame_queue = Queue()
def final_frame_func():
    final_frame = pd.DataFrame()
    while True:
        val = final_frame_queue.get()
        if isinstance(val, str):
            break
        if final_frame.empty:
            final_frame = final_frame.append(val)
        elif val.columns.isin(final_frame.columns).any():
            final_frame = final_frame.append(val)
        else:
            final_frame = pd.merge(final_frame, val, left_index=True, right_index=True, how='outer')
    final_frame = final_frame.reset_index().drop_duplicates('DATE_TIME_M', keep='first').set_index('DATE_TIME_M',
                                                                                                   drop=True)
    final_frame.where((abs((final_frame-final_frame.mean(axis=0))/final_frame.std(axis=0)) < 3) & (final_frame != 0) &
                      (final_frame < 1000), nan, inplace=True)
    final_frame.interpolate('linear', axis=1, inplace=True)
    final_frame.fillna(method='ffill', inplace=True, axis=0)
    final_frame.fillna(method='bfill', inplace=True, axis=0)
    final_frame.to_csv('some_data.csv')


def read_file(file_path, chunksize):
    frame = pd.DataFrame()
    ticker = split(file_path)[1].replace('_sorted.csv', '')
    for chunk in pd.read_csv(file_path, chunksize=chunksize, parse_dates=[0], infer_datetime_format=True):
        chunk = chunk.resample('5min', on='DATE_TIME_M')[['BID', 'BIDSIZ', 'ASK', 'ASKSIZ']].mean()
        # chunk = chunk[[x for x in chunk.columns if x[1] == 'close']].droplevel(1, axis=1)
        chunk.columns = ['BidPrice-'+ticker, 'BidVolume-'+ticker, 'AskPrice-'+ticker, 'AskVolume-'+ticker]
        frame = frame.append(chunk)
    frame.drop(frame[(frame.index.time > time(20, 0)) | (frame.index.time < time(4, 0))].index, inplace=True)
    frame.drop(frame[frame.index.weekday.isin([5, 6])].index, inplace=True)
    final_frame_queue.put(frame)


def main():
    tickers = open('tickers.txt').readline().split(', ')
    # tickers = tickers[:5]

    final_frame_proc = Process(target=final_frame_func)
    final_frame_proc.start()

    folders = ['/scratch/ar1515/QETS_Project/taq_aug_2019_quotes_sorted/',
               '/scratch/ar1515/QETS_Project/taq_sep_2019_quotes_sorted']
    chunksize = 10**6
    for folder in folders:
        files = next(walk(folders[0]))[2]
        files = [f for f in files if f.replace('_sorted.csv', '') in tickers]
        pool = Pool(processes=10, maxtasksperchild=3)
        pool.starmap(read_file, [(join(folder, f), chunksize) for f in files])
        pool.close()
        pool.join()

    final_frame_queue.put('fin')
    final_frame_proc.join()


if __name__ == '__main__':
    main()