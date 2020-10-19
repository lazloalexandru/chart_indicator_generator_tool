import os
import pandas as pd
import common
import multiprocessing as mp
import mplfinance as mpf
from termcolor import colored

__generated_charts_dir = "__generated_chart_data__\\intraday_charts_with_indicators"


def show_1min_chart_ms(symbol, date):
    date = date.replace("-", "")
    path = __generated_charts_dir + "\\" + symbol + "\\" + symbol + "_" + date + ".csv"

    df = None
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
    path = common.__intraday_charts_dir + "\\" + symbol + "\\" + file

    result_file = __generated_charts_dir + "\\" + symbol + "\\" + file
    result_dir = __generated_charts_dir + "\\" + symbol

    if not os.path.isdir(result_dir):
        try:
            os.mkdir(result_dir)
        except OSError:
            print("Creation of the directory %s failed" % result_dir)

    if os.path.isfile(result_file):
        print("Already generated.")
    else:

        df = pd.read_csv(path)

        df_range = pd.DataFrame(columns=['idx', 'range'])

        atr = []
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
            # ATR
            atr_length = 9

            j = i - atr_length
            if j < 0:
                j = 0
            m = 0
            sm = 0
            while 0 < j < i:
                sm = sm + df.loc[j]['High'] - df.loc[j]['Low']
                j = j + 1
                m = m + 1

            if m > 0:
                atr.append(sm / m)
            else:
                atr.append(0)

        ##################################################################
        # Range Score Continued ...

        df_range = df_range.set_index(df_range.idx)
        df_range = df_range.sort_values(by='range', ascending=True)

        range_score = [0] * n
        for i in range(0, n):
            range_score[int(df_range.iloc[i]['idx'])] = int(100 * i / n)

        df["range_score"] = range_score
        df["atr"] = atr
        df["vol_high_count"] = num_vol_lower
        df["vwap"] = vwap
        df["current_volume"] = cur_vol

        df['mav3'] = df['Close'].rolling(window=3).mean()
        df['mav4'] = df['Close'].rolling(window=4).mean()
        df['mav5'] = df['Close'].rolling(window=5).mean()
        df['mav8'] = df['Close'].rolling(window=8).mean()
        df['mav9'] = df['Close'].rolling(window=9).mean()
        df['mav13'] = df['Close'].rolling(window=13).mean()
        df['mav21'] = df['Close'].rolling(window=21).mean()

        print(path, "    >>>>    ", result_file, flush=True)
        df.to_csv(result_file)

    return 1


def add_metrics_to_intraday_charts():
    params = []

    symbols = common.get_list_of_symbols_with_intraday_chart()

    print("Creating list of intraday chart files")

    for symbol in symbols:
        dir_path = common.__intraday_charts_dir + "\\" + symbol
        if os.path.isdir(dir_path):
            for file in os.listdir(dir_path):
                if file.endswith(".csv"):
                    params.append([symbol, file])

    print("\nNum CPUs: ", mp.cpu_count())

    pool = mp.Pool(mp.cpu_count())
    for param in params:
        pool.apply_async(_generate_indicators, args=(param[0], param[1]))

    pool.close()
    pool.join()


def save_intraday_data(symbol, filename, df):
    if len(df) > 0:

        dir_path = __generated_charts_dir + "\\" + symbol
        if not os.path.isdir(dir_path):
            try:
                os.mkdir(dir_path)
            except OSError:
                print("Creation of the directory %s failed" % dir_path)

        df.to_csv(filename, index=False)


def main():
    add_metrics_to_intraday_charts()
    # show_1min_chart_ms("AAL", "2020-03-13")


if __name__ == "__main__":
    main()
