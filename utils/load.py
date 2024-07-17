from utils.dataloader import DataLoader
import sys
# from strgen import StringGenerator as SG
import numpy as np
import logging
import datetime


PURPLE = "\033[0;35m"
OK = '\033[92m' #GREEN
WARNING = '\033[93m' #YELLOW
FAIL = '\033[91m' #RED
RESET = '\033[0m' #RESET COLOR
class Load:

    def __init__(self,function_name,path_name):
        self.data_loader = DataLoader()
        self.function_name = function_name
        self.path_name = path_name
        pass

    def drop(self,sql_query, all = False):
        try:
            if all:
                questdb_data = self.data_loader.load_database_questdb(sql_query,self.path_name)
                print(questdb_data)
                for i in questdb_data['table_name'].unique():
                    self.data_loader._drop(i)
                return questdb_data
            else:
                questdb_data = self.data_loader._drop(sql_query)
            return questdb_data
        except Exception as e:
            timestamp = datetime.datetime.today()
            logging.error(f'Error drop {timestamp}: {str(e)}')
            raise ValueError(str(e))
    
    def loading(self,sql_query = None, process = False):
        try:
            # Lógica para determinar qual função do DataLoader chamar
            if self.function_name == 'excel':
                # Carregar dados de um arquivo Excel
                excel_data = self.data_loader.load_from_excel(self.path_name)
                if excel_data is not None:
                    print(PURPLE+"Dados do Excel carregados com sucesso:"+RESET)
                    return excel_data
            elif self.function_name == 'questdb':
                # Consulta ao QuestDB
                # questdb_data = self.data_loader.load_from_questdb(sql_query,self.path_name)
                questdb_data = self.data_loader.load_database_questdb(sql_query,self.path_name)
                timestamp = datetime.datetime.today()
                logging.info(f'Info QuestDB {timestamp}: Success')
                for i in questdb_data.columns:
                    questdb_data[i].replace('NULL', np.nan, inplace=True)
                    questdb_data[i].replace('NaN', np.nan, inplace=True)  
                
                if questdb_data is not None:
                    print(OK + 'Success !'+ RESET)
                    return questdb_data
            elif self.function_name == 'csv':
                # Carregar dados de um arquivo CSV
                csv_data = self.data_loader.load_from_csv(self.path_name)
                if csv_data is not None:
                    print("Dados do CSV carregados com sucesso:")
                    return csv_data
            elif self.function_name == 'tcp':
                pass
            else:
                timestamp = datetime.datetime.today()
                logging.warning(f'Error Storaged {timestamp}: Função {self.function_name} não reconhecida. Opções: excel, questdb, json, csv')

        except Exception as e:
            timestamp = datetime.datetime.today()
            logging.error(f'Error loading data {timestamp}: {str(e)}')
            raise ValueError('Error loading data'+str(e))
        
    # def storage(self, data):
    #     # print('1')
    #     # self.data_loader.data_storage(data)
    #     try:
    #         table_name =  'temp_'+SG(r"[\w]{6}").render()
    #         self.data_loader.save_dataframe_to_questdb(data, table_name)
    #         timestamp = datetime.datetime.today()
    #         logging.info(f'Info Storaged {timestamp}: Success')
    #         return table_name
    #     except Exception as e:
    #         timestamp = datetime.datetime.today()
    #         logging.error(f'Error Storaged {timestamp}: {str(e)}')
    #         raise ValueError('Erro Storage: '+str(e))
        
