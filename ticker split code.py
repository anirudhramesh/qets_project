import pandas as pd
import os

main_file = 'taq_aug_2019_quotes_500_tickers.csv'


def process_chunk(chunk, file_process_queue):
    if len(chunk.ticker.unique()) == 1:
        ticker = chunk.ticker.unique()[0]
        file_name = os.path.join('taq_aug_2019_quotes', ticker+'.csv')
        if os.path.exists(file_name):
            header = False
        else:
            header = True


def file_writer(file_process_queue):



def file_reader():
    chunksize = 10**8

