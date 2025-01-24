import pandas as pd
import os
import cx_Oracle as db
from datetime import datetime
import warnings

os.environ["NLS_LANG"] = ".UTF8"
warnings.filterwarnings("ignore")

cur_path = os.getcwd()
code_path = os.path.join(cur_path, "code")
data_path = os.path.join(cur_path, "data")
beforeProcessingSep_data_path = os.path.join(data_path, "beforeProcessingData")
beforeProcessingSep_data = os.path.join(
    beforeProcessingSep_data_path, "beforeProcessingSep.xlsx"
)
beforeProcessingSep = pd.read_excel(beforeProcessingSep_data)

current_datetime = datetime.now()
beforeProcessingSep.drop(
    columns=[
        "지역",
        "망",
        "주소",
        "Material",
        "Cent.X",
        "Cent.Y",
        "Match.Max",
        "Match.Min",
        "Match.Avg",
    ],
    inplace=True,
)
beforeProcessingSep["MEASURE_DATA_SEQ"] = None
beforeProcessingSep["MEASURE_SRC_SEQ"] = 3
beforeProcessingSep["TEMP"] = None
beforeProcessingSep["HUMI"] = None
beforeProcessingSep["MEASURE_DATE"] = None
beforeProcessingSep["MEASURE_TIME"] = None
beforeProcessingSep["DEL_YN"] = "N"
beforeProcessingSep["FRST_REG_DT"] = current_datetime
beforeProcessingSep["FRST_REG_USER"] = "admin"
beforeProcessingSep["LAST_UPDT_DT"] = current_datetime
beforeProcessingSep["LAST_UPDT_USER"] = "admin"

con = db.connect("CIN_EDU/cheminet@cheminet.webhop.net:4101/ORA11G")
cursor = con.cursor()

sql = """
SELECT * FROM KAPEX_TEST
"""

cursor.execute(sql)
rows = cursor.fetchall()

measure_data_seq = int(rows[0][0])

beforeProcessingSep["MEASURE_DATA_SEQ"] = measure_data_seq + pd.RangeIndex(
    start=0, stop=len(beforeProcessingSep), step=1
)
beforeProcessingSep["O3"] = beforeProcessingSep["O3"].apply(
    lambda a: a * 1000 if pd.notnull(a) else a
)
beforeProcessingSep["측정일시"] = beforeProcessingSep["측정일시"].astype(str)
beforeProcessingSep["MEASURE_DATE"] = beforeProcessingSep["측정일시"].str[:8]
beforeProcessingSep["MEASURE_DATE"] = pd.to_datetime(
    beforeProcessingSep["MEASURE_DATE"], format="%Y%m%d"
)
beforeProcessingSep["MEASURE_TIME"] = beforeProcessingSep["측정일시"].str[8:] + ":00"
beforeProcessingSep.loc[
    (beforeProcessingSep["MEASURE_TIME"] == "24:00"), "MEASURE_TIME"
] = "00:00"
beforeProcessingSep.loc[
    (beforeProcessingSep["MEASURE_TIME"] == "00:00"), "MEASURE_DATE"
] += pd.Timedelta(days=1)
beforeProcessingSep["MEASURE_DATE"] = beforeProcessingSep["MEASURE_DATE"].astype(str)
beforeProcessingSep.drop(columns=["측정일시"], inplace=True)
beforeProcessingSep.rename(columns={"측정소코드": "MEASURE_POINT_ID"}, inplace=True)

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

beforeProcessingSep = beforeProcessingSep[columns_order]
beforeProcessingSep.fillna("NULL", inplace=True)
beforeProcessingSep.replace("NULL", None, inplace=True)

# beforeProcessingSep.to_excel("kapexPreprocessing.xlsx", index=False)
# print(beforeProcessingSep)

for index, row in beforeProcessingSep.iterrows():
    sql = """
    INSERT INTO KAPEX_TEST (
        MEASURE_DATA_SEQ,
        MEASURE_SRC_SEQ,
        MEASURE_POINT_ID,
        MEASURE_DATE,
        MEASURE_TIME,
        PM25,
        PM10,
        O3,
        CO,
        SO2,
        NO2,
        TEMP,
        HUMI,
        DEL_YN,
        FRST_REG_DT,
        FRST_REG_USER,
        LAST_UPDT_DT,
        LAST_UPDT_USER
    ) VALUES (
        :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18
    )
    """
    cursor.execute(
        sql,
        (
            row["MEASURE_DATA_SEQ"],
            row["MEASURE_SRC_SEQ"],
            row["MEASURE_POINT_ID"],
            row["MEASURE_DATE"],
            row["MEASURE_TIME"],
            row["PM25"],
            row["PM10"],
            row["O3"],
            row["CO"],
            row["SO2"],
            row["NO2"],
            row["TEMP"],
            row["HUMI"],
            row["DEL_YN"],
            row["FRST_REG_DT"],
            row["FRST_REG_USER"],
            row["LAST_UPDT_DT"],
            row["LAST_UPDT_USER"],
        ),
    )

    con.commit()

cursor.close()
con.close()
