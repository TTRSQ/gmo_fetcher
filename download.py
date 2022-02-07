from datetime import datetime, timedelta
import subprocess

symbol = "BTC_JPY"

from_date = 20180905
to_date = 20220207

from_dt = datetime.strptime(str(from_date) + " +0900", "%Y%m%d %z")
to_dt = datetime.strptime(str(to_date) + " +0900", "%Y%m%d %z")
idx_dt = datetime.strptime(str(from_date) + " +0900", "%Y%m%d %z")
to_int = int(to_dt.strftime("%Y%m%d"))

while True:
    idx_int = int(idx_dt.strftime("%Y%m%d"))
    if idx_int > to_int:
        break

    year = idx_dt.year
    month = idx_dt.strftime("%m")
    date_str = str(idx_int)

    url = f"https://api.coin.z.com/data/trades/{symbol}/{year}/{month}/{date_str}_{symbol}.csv.gz"

    result = subprocess.run(["wget", "-P", "./datas", url], stderr=subprocess.PIPE)
    result_str = result.stderr.decode("utf-8")
    print(result_str)

    idx_dt += timedelta(days=1)
