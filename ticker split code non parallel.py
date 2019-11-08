import pandas as pd
import os


def main():
    main_file = 'taq_aug_2019_quotes_500_tickers.csv'
    output_file_path = 'taq_aug_2019_quotes'

    column_names = ['DATE', 'TIME_M', 'EX', 'BID', 'BIDSIZ', 'ASK', 'ASKSIZ', 'SYM_ROOT', 'SYM_SUFFIX']

    chunksize = 10**6
    chunk_generator = pd.read_csv(main_file, chunksize=chunksize, skiprows=chunksize*(500+3213) + 320307, header=None)

    for chunk in chunk_generator:
        chunk.columns = column_names
        for ticker in chunk.SYM_ROOT.unique():
            print(ticker)
            if os.path.exists(os.path.join(output_file_path, ticker+'.csv')):
                header = False
            else:
                header = True
            with open(os.path.join(output_file_path, ticker+'.csv'), 'a') as f:
                chunk[chunk.SYM_ROOT == ticker].to_csv(f, header=header, index=False)
                f.close()


if __name__ == '__main__':
    main()
