import time
from datetime import datetime
import pandas as pd  # pip install pandas
import yaml  # pip install pyyaml
import os
import sys

dir_path = "../../"

def make_api_call(row, custom_config):
    start_time_from_config = custom_config.get("start_time", "").strip()
    try:
        start_time_from_config = start_time_from_config + " 00:00:00"
        time_obj = time.strptime(start_time_from_config, "%d-%m-%Y %H:%M:%S")
    except:
        today = datetime.now()
        start_time_from_config = today.strftime("%d-%m-%Y") + " 00:00:00"
        time_obj = time.strptime(start_time_from_config, "%d-%m-%Y %H:%M:%S")
    finally:
        start_time = time.mktime(time_obj)
    one_day_seconds = 86400.0
    end_time = str(start_time + one_day_seconds)
    start_time = str(start_time)
    token = broker.instrument_symbol(
            row["exchange"], row["symbol"]
        )
    historical_data: list[dict] | None = broker.historical(
            row["exchange"], token, start_time, end_time
        )
    columns_to_keep = ["time", "into","inth","intl","intc", "v"]
    historical_data_df = pd.DataFrame([{k: d[k] for k in columns_to_keep} for d in historical_data])
    historical_data_df["exchange"] = row["exchange"]
    historical_data_df["symbol"] = row["symbol"]
    return historical_data_df


if __name__ == "__main__":
    from omspy_brokers.finvasia import Finvasia
    BROKER = Finvasia
    dir_path = "../../"
    with open(dir_path + "config.yaml", "r") as f:
        config = yaml.safe_load(f)
        broker = BROKER(**config["finvasia"])
        if broker.authenticate():
            print("success")
        custom_config = config["custom_config"]
    input_file_name = custom_config["input_file_name"]
    if not os.path.exists(dir_path+input_file_name):
        sys.exit(-1)
    input_df = pd.read_csv(dir_path+input_file_name)
    input_df['historical_data'] = input_df.apply(make_api_call, custom_config=custom_config, axis=1)
    final_df = pd.concat(input_df['historical_data'].tolist(), ignore_index=True)
    chosen_date = final_df["time"].to_list()[0].split()[0]
    final_df.to_csv(f"{chosen_date}_{input_file_name}")

