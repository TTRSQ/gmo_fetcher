import gzip
import subprocess
from datetime import date, datetime, timezone, timedelta
from typing_extensions import Self
import json

data_root = "./datas/"
interval_sec = 900

result = subprocess.run(["ls", data_root], stdout=subprocess.PIPE)
result_str = result.stdout.decode("utf-8")
results = result_str.split("\n")

idxs = {
    "symbol": 0,
    "side": 1,
    "size": 2,
    "price": 3,
    "timestamp": 4,
}


class ohlcv:
    def __init__(self, start_at: int, execution_hash: dict) -> None:
        self.start_at = start_at
        self.open = float(execution_hash[idxs["price"]])
        self.high = float(execution_hash[idxs["price"]])
        self.low = float(execution_hash[idxs["price"]])
        self.close = float(execution_hash[idxs["price"]])
        self.vol = float(execution_hash[idxs["size"]])

    def append(self, execution_hash: dict):
        self.high = max(float(execution_hash[idxs["price"]]), self.high)
        self.low = min(float(execution_hash[idxs["price"]]), self.low)
        self.close = float(execution_hash[idxs["price"]])
        self.vol += float(execution_hash[idxs["size"]])

    def hash(self):
        return {
            "start_at": self.start_at,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "vol": self.vol,
        }


candle_map = {}

for item in results:
    if -1 != item.find("gz"):

        with gzip.open(data_root + item, "rb") as f:
            print("convert", item)
            file_content = f.read()
            rows = str(file_content).split("\\n")

            head = True
            for row in rows:
                if head:
                    head = False
                    continue
                cols = row.split(",")
                if len(cols) == 5:
                    time_str = cols[idxs["timestamp"]]
                    dt = datetime.strptime(
                        time_str + " +0900", "%Y-%m-%d %H:%M:%S.%f %z"
                    )
                    start_at = int(dt.timestamp() / interval_sec) * interval_sec
                    date_str = dt.strftime("%Y%m%d")

                    if date_str not in candle_map:
                        candle_map[date_str] = {}

                    if start_at in candle_map[date_str]:
                        candle_map[date_str][start_at].append(cols)
                    else:
                        candle_map[date_str][start_at] = ohlcv(start_at, cols)

for key1 in candle_map.keys():
    for key2 in candle_map[key1]:
        hash = candle_map[key1][key2].hash()
        hash["vol"] = round(hash["vol"], 4)
        candle_map[key1][key2] = hash

ret_json = json.dumps(candle_map)

print(ret_json)

with open(data_root + "output.json", "w+") as f:
    f.write(ret_json)

print("all done")
