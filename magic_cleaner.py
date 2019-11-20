from os import system
import pandas as pd

# drop DATE, TIME_M, SYM_SUFFIX
def sorter(original_file):
    final_file = 'AAPL sorted trades.csv'
    for df in pd.read_csv('taq_aug_2019_trades/AAPL.csv', chunksize=10 ** 6):
        df['DATE_TIME'] = df.DATE.astype(str) + ' ' + df.TIME_M.astype(str)
        df.DATE_TIME = pd.to_datetime(df.DATE_TIME)
        df.sort_values('DATE_TIME', inplace=True)

        try:
            other_frame = pd.read_csv(final_file, nrows=2)
        except FileNotFoundError:
            df.to_csv(final_file, index=False)
            frame_length = len(df)
            continue
        other_frame.DATE_TIME = pd.to_datetime(other_frame.DATE_TIME)

        if df.DATE_TIME.iloc[0] > other_frame.DATE_TIME.iloc[0]:
            with open(final_file, 'a') as f:
                df.to_csv(f, header=False, index=False)
                f.close()
        else:
            df.to_csv('temp.csv', index=False)
            system('{ tail -n +1 temp.csv | tail -n +2 ' + final_file + ' } | cat > ' + final_file)
    system('rm temp.csv')


def main():
    pass


if __name__ == '__main__':
    main()
