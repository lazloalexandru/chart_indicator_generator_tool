import os
import pandas as pd
import multiprocessing as mp
import mplfinance as mpf
from termcolor import colored
import sys

__input_dir = "input_raw_chart_data\\intraday_charts"
__output_dir = "output_generated_chart_data\\intraday_charts_with_indicators"
__output_dir_root = "output_generated_chart_data"


def get_list_of_symbols_with_intraday_chart():
    symbols = []

    if os.path.isdir(__input_dir):
        for root, dirs, files in os.walk(__input_dir + "\\", topdown=False):
            for name in dirs:
                symbols.append(name)
    else:
        print(colored(__input_dir + ' not found!', color='yellow'))

    return symbols


def show_1min_chart_ms(symbol, date):
    date = date.replace("-", "")
    path = __output_dir + "\\" + symbol + "\\" + symbol + "_" + date + ".csv"

    if os.path.isfile(path):
        df = pd.read_csv(path)
        df.Time = pd.to_datetime(df.Time, format="%Y-%m-%d  %H:%M:%S")

        df = df.set_index(pd.Index(df.Time))

        ##################################
        # Plot charts

        adp = [mpf.make_addplot(df['atr'].tolist(), color='green')]

        mpf.plot(df, type='candle', ylabel='Price', ylabel_lower='Volume', mav=[21],
                 volume=True, figscale=1, figratio=[16, 9], addplot=adp)
    else:
        print(path)
        print(colored('Warning! Daily chart for ' + symbol + ' not available', color='yellow'))


def _generate_indicators(symbol, file):
    path = __input_dir + "\\" + symbol + "\\" + file

    result_file = __output_dir + "\\" + symbol + "\\" + file
    result_dir = __output_dir + "\\" + symbol

    if not os.path.isdir(result_dir):
        try:
            os.mkdir(result_dir)
        except OSError:
            print("Creation of the directory %s failed" % result_dir, flush=True)

    if os.path.isfile(result_file):
        print(result_file, " Already generated.", flush=True)
    else:

        df = pd.read_csv(path)

        df_range = pd.DataFrame(columns=['idx', 'range'])

        tr = []
        vwap = []
        cur_vol = []
        num_vol_lower = []

        sum_vxp = 0
        sumv = 0

        n = len(df)
        for i in range(0, n):

            ####################################################################
            # Range Score

            data = {'idx': i,
                    'range': df.iloc[i]['High'] - df.iloc[i]['Low']}
            df_range = df_range.append(data, ignore_index=True)

            ####################################################################
            # VWAP & Current volume

            sum_vxp = sum_vxp + df.loc[i]['Volume'] * (df.loc[i]['High'] + df.loc[i]['Close'] + df.loc[i]['Low'])/3
            sumv = sumv + df.loc[i]['Volume']

            vwap.append(0 if sumv == 0 else sum_vxp / sumv)
            cur_vol.append(sumv)

            ####################################################################
            # Count how many bars have lower volume before finding a bigger volume

            j = i
            is_bigger = True
            cnt = 0
            while j > 0 and is_bigger:
                j = j - 1
                is_bigger = df.iloc[j]['Volume'] < df.iloc[i]['Volume']
                if is_bigger:
                    cnt = cnt + 1
            num_vol_lower.append(cnt)

            ####################################################################
            # TR

            tr.append(df.loc[j]['High'] - df.loc[j]['Low'])

        ##################################################################
        # Range Score Continued ...

        df_range = df_range.set_index(df_range.idx)
        df_range = df_range.sort_values(by='range', ascending=True)

        range_score = [0] * n
        for i in range(0, n):
            range_score[int(df_range.iloc[i]['idx'])] = int(100 * i / n)

        df["range_score"] = range_score
        df["vol_high_count"] = num_vol_lower
        df["vwap"] = vwap
        df["current_volume"] = cur_vol
        df["trading_range"] = tr

        df["atr3"] = df["trading_range"].rolling(window=3).mean()
        df["atr5"] = df["trading_range"].rolling(window=5).mean()
        df["atr8"] = df["trading_range"].rolling(window=8).mean()
        df["atr13"] = df["trading_range"].rolling(window=13).mean()

        df['mav3'] = df['Close'].rolling(window=3).mean()
        df['mav5'] = df['Close'].rolling(window=5).mean()
        df['mav8'] = df['Close'].rolling(window=8).mean()
        df['mav9'] = df['Close'].rolling(window=9).mean()
        df['mav13'] = df['Close'].rolling(window=13).mean()
        df['mav21'] = df['Close'].rolling(window=21).mean()

        df['vmav3'] = df['Volume'].rolling(window=3).mean()
        df['vmav5'] = df['Volume'].rolling(window=5).mean()
        df['vmav8'] = df['Volume'].rolling(window=8).mean()
        df['vmav13'] = df['Volume'].rolling(window=13).mean()
        df['vmav21'] = df['Volume'].rolling(window=21).mean()

        df.to_csv(result_file)
        print(result_file, flush=True)

    return 1


def check_and_create_output_directories():
    if not os.path.isdir(__output_dir_root):
        try:
            os.mkdir(__output_dir_root)
        except OSError:
            print("Creation of the directory %s failed" % __output_dir_root)

    if not os.path.isdir(__output_dir):
        try:
            os.mkdir(__output_dir)
        except OSError:
            print("Creation of the directory %s failed" % __output_dir)


def add_metrics_to_intraday_charts():
    params = []

    check_and_create_output_directories()

    print("Creating list of intraday chart files ... ")

    symbols = get_list_of_symbols_with_intraday_chart()

    for symbol in symbols:
        dir_path = __input_dir + "\\" + symbol
        if os.path.isdir(dir_path):
            for file in os.listdir(dir_path):
                if file.endswith(".csv"):
                    params.append([symbol, file])

    print("Found", len(params), "input files")

    if len(params) > 0:

        print("\nNum CPUs: ", mp.cpu_count())

        ''' THIS DOES NOT WORK ...
        
        result_objs = []
        with mp.Pool(processes=mp.cpu_count()) as pool:
            for param in params:
                result = pool.apply_async(_generate_indicators, args=(param[0], param[1]))
                result_objs.append(result)

            results = [result.get() for result in result_objs]
        '''

        ##################################################################
        # Used for debugging Single process ... to see errors
        '''
        for param in params:
            _generate_indicators(param[0], param[1])

        '''
        ###################################################################

        pool = mp.Pool(mp.cpu_count())
        for param in params:
            pool.apply_async(_generate_indicators, args=(param[0], param[1]))

        pool.close()
        pool.join()


def main():
    add_metrics_to_intraday_charts()
    # show_1min_chart_ms("AAL", "2020-03-13")


if __name__ == "__main__":
    main()
