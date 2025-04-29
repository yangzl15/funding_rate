# main.py
import pandas as pd
import sys
import glob
import os
os.environ["PYDEVD_WARN_SLOW_RESOLVE_TIMEOUT"] = "5.0"
import numpy as np
import logging
from datetime import datetime
import concurrent.futures
import warnings
warnings.filterwarnings('ignore')
import config

def setup_logger():
    log_dir = config.log_dir
    print(f"Log directory: {log_dir}")
    date_str = datetime.today().strftime('%Y-%m-%d')
    time_str = datetime.today().strftime('%H%M%S')
    daily_log_dir = os.path.join(log_dir, date_str)
    os.makedirs(daily_log_dir, exist_ok=True)
    log_filename = os.path.join(daily_log_dir, f"processing_{time_str}.log")
    logging.basicConfig(filename=log_filename, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

def process_csv_file(csv_path, start_date, end_date):
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.lower()
    df["date"] = pd.to_datetime(df["date"])
    df['dt'] = df['date']
    shifted = df['dt'] - pd.Timedelta(minutes=5)
    df['date_only'] = pd.to_datetime(shifted.dt.date)
    df["Label"] = (shifted.dt.hour * 12 + shifted.dt.minute // 5 + 1).astype(int)
    df.set_index(["date_only", "Label"], inplace=True)
    df.index.set_names(["date", "Label"], inplace=True)
    full_dates = pd.date_range(start_date, end_date, freq="D")
    full_labels = range(1, 289)
    full_index = pd.MultiIndex.from_product([full_dates, full_labels], names=["date", "Label"])
    series = df["funding rate"].reindex(full_index)
    return series


# --- MOD ---
def combine_all_symbols(csv_dir, start_date, end_date, start_date_override=None, start_label_override=None):
    file_list = [os.path.join(csv_dir, f) for f in os.listdir(csv_dir) if f.endswith('.csv')]
    print(f"len(file_list): {len(file_list)}")
    combined_df = None
    for fp in file_list:
        basename = os.path.basename(fp)
        parts = basename.split("_")[0]
        if parts:
            symbol = parts
            series = process_csv_file(fp, start_date, end_date)
            if start_date_override is not None and start_label_override is not None: ## 开始5min增量
                mask = (
                    (series.index.get_level_values("date") > pd.to_datetime(start_date_override)) |
                    (
                        (series.index.get_level_values("date") == pd.to_datetime(start_date_override)) &
                        (series.index.get_level_values("Label") > start_label_override)
                    )
                )
                series = series.loc[mask]
            elif start_date_override is not None: #第一次跑历史+增量
                series = series.loc[series.index.get_level_values("date") >= pd.to_datetime(start_date_override)]
            if combined_df is None:
                combined_df = pd.DataFrame({symbol: series})
            else:
                combined_df[symbol] = series
    combined_df.columns = combined_df.columns.str.lower()
    logging.info(f"processed and combined to 5min data but unfilled")
    return combined_df
# --- /MOD ---


def process_symbol(args):
    symbol, s_orig, dates = args
    idx = pd.MultiIndex.from_product([dates, range(1, 289)], names=["date", "Label"])
    target = pd.Series(index=idx, dtype="float64")
    prev_day_last = None
    for d, daily in s_orig.groupby(level=0):
        vals = daily.dropna().sort_index().values
        labs = daily.dropna().index.get_level_values(1).sort_values().to_numpy()
        if labs.size == 0:
            continue
        if prev_day_last is None:
            for i, lab in enumerate(labs):
                start = lab
                end = labs[i+1] - 1 if i+1 < len(labs) else 288
                target.loc[(d, slice(start, end))] = vals[i]
            prev_day_last = vals[-1]
            continue
        start_lab = 1
        prev_val = prev_day_last
        end_lab = labs[0] - 1
        if end_lab >= start_lab:
            target.loc[(d, slice(start_lab, end_lab))] = prev_val
        for i, lab in enumerate(labs):
            prev_val = vals[i]
            start_lab = lab
            end_lab = labs[i+1] - 1 if i+1 < len(labs) else 288
            target.loc[(d, slice(start_lab, end_lab))] = prev_val
        prev_day_last = vals[-1]
    return symbol.lower(), target

def fill_target_from_orig_parallel(orig_df, start_date, end_date):
    dates = pd.date_range(start_date, end_date, freq="D")
    tasks = [(symbol, orig_df[symbol], dates) for symbol in orig_df.columns]
    results = {}
    with concurrent.futures.ProcessPoolExecutor(max_workers=64) as executor:
        for symbol, series in executor.map(process_symbol, tasks):
            results[symbol] = series
    df_target = pd.DataFrame(results)
    logging.info(f"filled the history data in 5min scale")
    return df_target

def check_zero_std(df):
    mask = df.std(axis=1) == 0
    if mask.any():
        df.loc[mask] = np.nan
    return df

def check_inf(df):
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df


def process_funding_rate(input_df, configurations):
    df = input_df
    output_dfs = {}
    for cfg in configurations:
        interval = cfg["interval"]
        divisor = cfg["divisor"]
        labels_to_pick = list(range(divisor, 289, divisor))
        df_resampled = df.loc[df.index.get_level_values("Label").isin(labels_to_pick)]
        df_resampled = df_resampled.dropna(how='all')
        # df_resampled = check_zero_std(df_resampled)
        # df_resampled = check_inf(df_resampled)
        logging.info(f"Processed {interval} data")
        output_dfs[interval] = df_resampled
    logging.info("All funding rate frequencies processed successfully.")
    return output_dfs

def save_all_results(df_target_history, df_target_rt, df_target, output_dfs, debug_data_dir, output_dir):
    df_target_history.to_parquet(os.path.join(debug_data_dir, "funding_5min_history.parquet"))
    df_target_rt.to_parquet(os.path.join(debug_data_dir, "funding_5min_realtime.parquet"))
    # raw_df.to_parquet(os.path.join(debug_data_dir, "funding_5min_history_unfilled.parquet"))
    df_target.to_parquet(os.path.join(debug_data_dir, "funding_5min_combined.parquet"))

    for interval, df_resampled in output_dfs.items():
        output_subdir = os.path.join(output_dir, interval)
        os.makedirs(output_subdir, exist_ok=True)
        output_path = os.path.join(output_subdir, "funding_rate.parquet")
        df_resampled.to_parquet(output_path)
        logging.info(f"Saved {interval} data to {output_path}")

if __name__ == "__main__":
    setup_logger()
    logging.info("Starting the funding rate processing.")
    os.makedirs(config.debug_data_dir, exist_ok=True)

    first_run = True
    if first_run == True: ## 第一次跑历史+增量
        history_start_date = config.start_date
        history_end_date = (pd.to_datetime(config.end_date) - pd.Timedelta(days=2)).strftime("%Y-%m-%d")
        rt_start_date = (pd.to_datetime(config.end_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        rt_end_date = config.end_date
        print(f"History start date: {history_start_date}")
        print(f"History end date: {history_end_date}")
        print(f"Real-time start date: {rt_start_date}")
        print(f"Real-time end date: {rt_end_date}")

        raw_df = combine_all_symbols(
            config.csv_history_dir,
            history_start_date,
            history_end_date,
            start_date_override = None,
            start_label_override=None
        )
        df_target_history = fill_target_from_orig_parallel(raw_df, config.start_date, history_end_date)

        last_date_in_history = df_target_history.index.get_level_values("date").max()
        logging.info(f"last_date_in_history: {last_date_in_history}")

        df_target_rt = combine_all_symbols(
            config.csv_realtime_dir,
            rt_start_date,
            rt_end_date,
            start_date_override= last_date_in_history,
            start_label_override=None
        )

        # columns取并集
        df_target = pd.concat([df_target_history, df_target_rt], axis=0, ignore_index=False)
        output_dfs = process_funding_rate(df_target, config.configurations)

        save_all_results(df_target_history, df_target_rt, df_target, output_dfs, config.debug_data_dir, config.output_dir)

    else: ## 增量更新
        df_target = pd.read_parquet(os.path.join(config.debug_data_dir, "funding_5min_combined_test.parquet"))
        df_target = df_target.dropna(how='all')
        last_date = df_target.index.get_level_values("date").max()  # --- MOD ---
        last_label = df_target.loc[last_date].index.get_level_values("Label").max()  # --- MOD ---
        rt_start_date = last_date.strftime("%Y-%m-%d")  # --- MOD ---
        rt_end_date = config.end_date  # --- MOD ---

        df_target_addition = combine_all_symbols(  # --- MOD ---
            config.csv_realtime_dir,
            rt_start_date,
            rt_end_date,
            start_date_override=last_date,
            start_label_override=last_label
        )

        df_target_new = pd.concat([df_target, df_target_addition], axis=0, ignore_index=False)

        output_dfs = process_funding_rate(df_target_new, config.configurations)  # --- MOD ---

        save_all_results(df_target, df_target_addition, df_target_new, output_dfs, config.debug_data_dir, config.output_dir)  # --- MOD ---
