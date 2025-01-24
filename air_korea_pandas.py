import pandas as pd
import os
import cx_Oracle as db
from datetime import datetime
import warnings


os.environ["NLS_LANG"] = (
    ".UTF8"  # UTF 환경으로 설정-> DB 업로드시 문제가 될 수도 있어서 설정
)
warnings.filterwarnings("ignore")

# 경로지정
cur_path = os.getcwd()
code_path = os.path.join(cur_path, "code")
data_path = os.path.join(cur_path, "data")
before_processing = os.path.join(data_path, "beforeProcessingData")
after_processing = os.path.join(data_path, "afterProcessingData")
measure_point = os.path.join(data_path, "measurePointData")
before_processing_data_path = os.path.join(
    before_processing, "beforeProcessingSepReal.xlsx"
)
after_processing_data_path = os.path.join(after_processing, "afterProcessingSep.xlsx")
measure_point_data_path = os.path.join(measure_point, "outdoorMeasurePoint.xlsx")


before_processing_data = pd.read_excel(before_processing_data_path)

# 데이터 처리
current_datetime = datetime.now()
before_processing_data = before_processing_data[
    before_processing_data["망"] == "도시대기"
]
before_processing_data = before_processing_data.drop(
    columns=["지역", "망", "측정소명", "주소"], inplace=True
)
before_processing_data["MEASURE_SRC_SEQ"] = 3
before_processing_data["TEMP"] = None
before_processing_data["HUMI"] = None
before_processing_data["MEASURE_DATA_SEQ"] = None
before_processing_data["MEASURE_DATE"] = None
before_processing_data["MEASURE_TIME"] = None
before_processing_data["DEL_YN"] = "N"
before_processing_data["FRST_REG_DT"] = current_datetime
before_processing_data["FRST_REG_USER"] = "admin"
before_processing_data["LAST_UPDT_DT"] = current_datetime
before_processing_data["LAST_UPDT_USER"] = "admin"

# DB
con = db.connect("CIN_EDU/cheminet@cheminet.webhop.net:4101/ORA11G")
cursor = con.cursor()

# 쿼리문
sql = """
SELECT * FROM KAPEX_TEST
"""

cursor.exeute(sql)
rows = cursor.fetchall()

measure_data_seq = int(30806623)

before_processing_data["MEASURE_DATA_SEQ"] = measure_data_seq + pd.RangeIndex(
    start=1, stop=len(before_processing_data) + 1
)
before_processing_data["O3"] = before_processing_data["O3"].apply(
    lambda x: x * 1000 if pd.notnull(x) else x
)
before_processing_data["측정일시"] = before_processing_data["측정일시"].astype(str)
before_processing_data["MEASURE_DATE"] = before_processing_data["측정일시"].str[:8]
before_processing_data["MEASURE_TIME"] = (
    before_processing_data["측정일시"].str[8:] + ":00"
)
before_processing_data["MEASURE_DATE"] = pd.to_datetime(
    before_processing_data["MEASURE_DATE"], format="%Y%m%d"
)
condition = before_processing_data["MEASURE_TIME"] == "24:00"
before_processing_data.loc[condition, "MEASURE_TIME"] = "00:00"
before_processing_data.loc[condition, "MEASURE_TIME"] += pd.Timedelta(days=1)
before_processing_data["MEASURE_DATE"] = before_processing_data["MEASURE_DATE"].astype(
    str
)
before_processing_data.drop(columns=["측정일시"], inplace=True)
new_column_names = {"측정소코드": "MEASURE_POINT_ID"}
before_processing_data = before_processing_data.rename(columns=new_column_names)
columns_order = [
    "MEASURE_DATA_SEQ",
    "MEASURE_SRC_SEQ",
    "MEASURE_POINT_ID",
    "MEASURE_DATE",
    "MEASURE_TIME",
    "PM25",
    "PM10",
    "O3",
    "CO",
    "SO2",
    "NO2",
    "TEMP",
    "HUMI",
    "DEL_YN",
    "FRST_REG_DT",
    "FRST_REG_USER",
    "LAST_UPDT_DT",
    "LAST_UPDT_USER",
]
before_processing_data = before_processing_data[columns_order]
before_processing_data.fillna("NULL", inplace=True)
before_processing_data.replace("NULL", None, inplace=True)

before_processing_data.to_excel("kapexPreprocessing.xlsx", index=False)
