# 통상적으로 pandas = pd, numpy as np로 alis 지정 함. 아무렇게나 실행상 문제없음.
import pandas as pd
import os
import cx_Oracle as db
from datetime import datetime
import warnings

os.environ["NLS_LANG"] = (
    ".UTF8"  # UTF 환경으로 설정-> DB 업로드시 문제가 될 수도 있어서 설정
)
warnings.filterwarnings("ignore")  # 워닝 무시 코드(없어도 무방)

# 파일 경로 세팅(OS PATH 지정 방법이 가장 굿)
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

# CSV Load (pandas 함수는 반드시 공부할 것!!!-> 활용도가 많습니다.ㅎㅎ)
before_processing_data = pd.read_excel(before_processing_data_path)

# 데이터 처리
current_datetime = datetime.now()  # 현재시각 초까지 다 나옴
before_processing_data = before_processing_data[
    before_processing_data["망"] == "도시대기"
]
before_processing_data.drop(columns=["지역", "망", "측정소명", "주소"], inplace=True)
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

# DB 컨트롤러 인스턴스 생성
con = db.connect("CIN_EDU/cheminet@cheminet.webhop.net:4101/ORA11G")
cursor = con.cursor()

# 쿼리문
sql = """
SELECT * FROM KAPEX_TEST
"""
# DB에서 쿼리 실행
cursor.execute(sql)
rows = cursor.fetchall()

measure_data_seq = int(rows[0][0])
print(measure_data_seq)
# measure_data_seq = int(30806623)


# 데이터 처리 --> Pandas 및 기본 데이터 처리 공부할 것!!! 이것도 알아두면 좋아요 ㅎㅎ
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
before_processing_data.loc[condition, "MEASURE_DATE"] += pd.Timedelta(days=1)
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

# 여기까지가 데이터 전처리 끝!!!!!!!!!!!!!!!
####################################################################

# before_processing_data.to_excel("kapexPreprocessing.xlsx", index = False)
# print(before_processing_data)


####################################################################
# 여기서부터가 전처리를 한 데이터를 오라클 DB에 업로드하는 코드입니다
# 쿼리는 바인딩해서 한번에 올리는 인서트 문입니다
for index, row in before_processing_data.iterrows():
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
