import pandas as pd
import requests
import pandas as pd
import io
import questdb.ingress as qdb
from questdb.ingress import Sender,Buffer
import logging
import datetime
import urllib.parse
from socket import socket
from time import sleep
from sys import exit
from os import environ
from tqdm import tqdm
import logging
from threading import Thread
from warnings import filterwarnings
filterwarnings("ignore")

PURPLE = "\033[0;35m"
OK = '\033[92m' #GREEN
WARNING = '\033[93m' #YELLOW
FAIL = '\033[91m' #RED
RESET = '\033[0m' #RESET COLOR

def progress(memories, memory_type):
    if len(memories[memory_type]) < 80:
        pbar = tqdm(total=180)
        pbar.update(len(memories[memory_type]))
    else:
        pbar.update(100)
        pbar.close()


class DataLoader:
    def __init__(self, questdb_url='http://localhost:9000'):
        self.questdb_url = questdb_url

    def load_from_excel(self, file_path):
        try:
            # Carrega dados do Excel usando o pandas
            data = pd.read_excel(file_path)
            return data
        except Exception as e:
            print(f"Erro ao carregar dados do Excel: {e}")
            return None

    def load_database_questdb(self, sql_query, questdb_url):
        try:

            sql_query = urllib.parse.quote(sql_query)
            if questdb_url:
                self.questdb_url = questdb_url  
            # Endpoint para enviar consultas SQL
            query_endpoint = f"{self.questdb_url}/exec?query="
            
            # Envia uma solicitação GET com a consulta SQL para o QuestDB
            response = requests.get(query_endpoint + sql_query)
            # print(query_endpoint+sql_query)
            # Verifica se a solicitação foi bem-sucedida (código 200)
            if response.status_code == 200:
                # Verifica se o conteúdo da resposta é um CSV
                    try:
                        queryData = response.json()
                        if queryData:
                            columns = [queryData['columns'][x]['name'] for x in range(len(queryData['columns']))]
                            data = pd.DataFrame(queryData['dataset'],columns=columns)
                            return data
                        else:
                            timestamp = datetime.datetime.today()
                            logging.error(f'Erro ao executar a consulta {timestamp}: Consulta com atributos errados, não foi possível carregar os dados')
                            raise ValueError("Consulta com atributos errados, não foi possível carregar os dados")
                    except pd.errors.EmptyDataError as e:
                        # print(str(e))
                        timestamp = datetime.datetime.today()
                        logging.error(f'Erro ao executar a consulta {timestamp}: {str(e)}')
                        raise ValueError("Os dados retornados estão vazios.")
                        
            else:
                # Se a resposta não foi bem-sucedida, imprime o status code e a mensagem de erro
                timestamp = datetime.datetime.today()
                logging.error(f'Erro ao executar a consulta {timestamp}: Status code {response.status_code}.{response.text}')
                raise ValueError(f"Erro ao executar a consulta. Status code: {response.status_code}.{response.text}")

        except requests.RequestException as e:
            # Manipula exceções de solicitação
            timestamp = datetime.datetime.today()
            logging.error(f'Erro de requisição {timestamp}: {str(e)}')
            raise ValueError(f"Erro de requisição: {e}")

    def save_dataframe_to_questdb(self,data, table_name='storage_temp'):
        try:
            self._drop(table_name)
            self.data_storage(data, table_name)
            with Sender('localhost', 9009) as sender:
                # buf = Buffer()
                # buf.dataframe(data,  table_name=table_name)
                x = data.to_dict('records')
                for i in x:
                    sender.row(
                    table_name,
                    columns=i)
                    sender.flush()
            timestamp = datetime.datetime.today()
            logging.info(f'Info saved {timestamp}: Success')
        except Exception as e:
            timestamp = datetime.datetime.today()
            logging.error(f'Erro save dataframe {timestamp}: {str(e)}')
            raise ValueError('Erro save dataframe: '+str(e))
       
    
    def load_from_csv(self,file_path):
        try:
            # Carrega dados do CSV usando o pandas
            data = pd.read_csv(file_path, delimiter=';',header=0)
            return data
        except Exception as e:
            raise ValueError(f"Erro ao carregar dados do CSV: {e}")
           

    def receiving(self,tcpClient,thread):
        '''Receives, via TCP and Thread, RTP inputs'''
        received = tcpClient.recv(1024).decode()
        if received == 'Clear':
            memories = dict()
        elif received == 'Exit':
            sleep(2)
            thread.join()
            exit()
        return received 

    def handshake(self,ip, port):
        tcpClient = socket()
        while True:
            try:
                tcpClient.connect((ip, port))
                return tcpClient
            except:
                tcpClient.shutdown(10)
    

    def conect_tcp(self):
        '''Main function, receives the analyzed data and from this data sends a speed or error to RTP'''
        memories = dict()
        while True:
            try:
                received = self.receiving()
                return received
            except Exception as e:
                pass


    def _drop(self, table_name):
        try:
            url = f"{self.questdb_url}/exec?query="
            endpoint = f"DROP TABLE IF EXISTS {table_name};"
            response = requests.get(url + endpoint)
            if response.status_code == 200:
                timestamp = datetime.datetime.today()
                logging.info(f'Info drop {timestamp}: Success, {response.status_code}')
                # print('Sucesso drop', response.status_code)
                return None
            else:
                timestamp = datetime.datetime.today()
                logging.error(f'Erro! Exclusão de tabela falhou, verifique se nome da tabela esta correta')
                raise ValueError("Erro! Exclusão de tabela falhou, verifique se nome da tabela esta correta")
        except Exception as e:
            timestamp = datetime.datetime.today()
            logging.error(f'Erro drop dataframe {timestamp}: {str(e)}')
            raise ValueError(str(e))


    def data_storage(self, qdb_dataframe,table_name = 'storage_temp'):
        try:
            data_dict = qdb_dataframe.to_dict(orient='records')
            # Create table efficiently
            url = f"{self.questdb_url}/exec?query="
            endpoint = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                   # Obtém os tipos de dados do DataFrame para criar a tabela no QuestDB
            for column, dtype in qdb_dataframe.dtypes.items():
                if pd.api.types.is_float_dtype(dtype):
                    endpoint += f"'{column}' DOUBLE,"
                elif pd.api.types.is_integer_dtype(dtype):
                    endpoint += f"'{column}' INT,"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    endpoint += f"'{column}' TIMESTAMP,"
                elif pd.api.types.is_bool_dtype(dtype):
                    endpoint += f"'{column}' BOOLEAN,"
                else:
                    endpoint += f"'{column}' SYMBOL,"
            # Remove a última vírgula e fecha parênteses para finalizar a definição da tabela
            endpoint = endpoint[:-1] + ")"
            response = requests.get(url + endpoint)
            if response.status_code == 200:
                timestamp = datetime.datetime.today()
                logging.debug(f'Success created {timestamp}: Data_storage, Success, {response.status_code}')
                # print('Sucesso create',response.status_code)
            else:
                timestamp = datetime.datetime.today()
                logging.error(f'Erro {timestamp}: Não foi possível criar a tabela com os argumentos passado')
                raise ValueError('Erro! Não foi possível criar a tabela com os argumentos passado')
        except Exception as e:
            timestamp = datetime.datetime.today()
            logging.error(f'Erro {timestamp}: {str(e)}')
            raise ValueError(str(e))

   