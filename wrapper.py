from dataprocessing import DataProcssing
from DBuploader import DBupl


class WrapperClass:
    def 경로세팅(self):
        cur_path = os.getcwd()

    code_path = os.path.join(cur_path, "code")
    data_path = os.path.join(cur_path, "data")
    beforeProcessingSep_data_path = os.path.join(data_path, "beforeProcessingData")
    beforeProcessingSep_data = os.path.join(
        beforeProcessingSep_data_path, "beforeProcessingSep.xlsx"
    )

    def run(dd):
        DataProcssing
