import pandas as pd
import os


def main():
    main_file = 'taq_aug_2019_quotes_500_tickers.csv'
    output_file_path = 'taq_aug_2019_quotes'

    chunksize = 10**6
    chunk_generator = pd.read_csv(main_file, chunksize=chunksize)

    for chunk in chunk_generator:
        for ticker in chunk.SYM_ROOT.unique():
            if os.path.exists(os.path.join(output_file_path, ticker+'.csv')):
                header = False
            else:
                header = True
            with open(os.path.join(output_file_path, ticker+'.csv'), 'a') as f:
                chunk.to_csv(f, header=header, index=False)
                f.close()


if __name__ == '__main__':
    main()
