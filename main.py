import mysql.connector
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def connect_db(user, password, db, host='localhost', port='sua porta'):
    connection = False
    try:
        connection = mysql.connector.connect(host=host,
                                             port=port,
                                             database=db,
                                             user=user,
                                             passwd=password)
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    return connection

def get_unwritten_vagas(connection):
    cursor = connection.cursor()
    query = '''
        SELECT
            id, nome, idade, cidade
        FROM
            vagas
        WHERE
            escrito = 0
    '''
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def mark_vagas_as_written(connection, ids):
    cursor = connection.cursor()
    update_query = '''
        UPDATE
            vagas
        SET
            escrito = 1
        WHERE
            id IN ({})
    '''.format(','.join(map(str, ids)))

    cursor.execute(update_query)
    connection.commit()

# Classe para manipulação do Google Sheets
class Spreadsheet:
    def __init__(self, sheet_url, sheet_name, credentials_path):
        self.sheet_url = sheet_url
        self.sheet_name = sheet_name
        self.credentials_path = credentials_path
        self.creds = self.authenticate()
        self.service = build('sheets', 'v4', credentials=self.creds)
        self.sheet = self.service.spreadsheets()
        self.SPREADSHEET_ID = self.sheet_url.split('/')[-2]

    def authenticate(self):
        flow = InstalledAppFlow.from_client_secrets_file(self.credentials_path, SCOPES)
        creds = flow.run_local_server(port=0)
        return creds

    def append_values(self, values: list):
        existing_rows_set = self.get_existing_rows_set()
        values_to_insert = []

        for value in values:
            if tuple(value[:-1]) not in existing_rows_set:
                values_to_insert.append(value)

        if values_to_insert:
            request = self.sheet.values().append(
                spreadsheetId=self.SPREADSHEET_ID,
                range=f"{self.sheet_name}!A:Z",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": values_to_insert})
            response = request.execute()
            inserted_rows = response.get('updates').get('updatedRows')
            return inserted_rows
        else:
            return 0

    def get_existing_rows_set(self):
        try:
            result = self.sheet.values().get(
                spreadsheetId=self.SPREADSHEET_ID,
                range=f'{self.sheet_name}!A:Z').execute().get('values', [])
            existing_rows_set = set(tuple(row[:-1]) for row in result)
            return existing_rows_set
        except Exception as e:
            print(f"Erro ao obter valores existentes: {e}")
            return set()

def main():
    connection = connect_db(user='seu user', password='sua senha', db='db', host='localhost', port='sua porta')
    if not connection:
        print("Error connecting to Database")
        exit(0)

    vagas = get_unwritten_vagas(connection)
    
    if not vagas:
        print("Não há vagas para serem escritas.")
        exit(0)

    sheet_url = 'sheet url'
    sheet_name = 'sheet name'
    credentials_path = 'credentials.json'
    spreadsheet = Spreadsheet(sheet_url, sheet_name, credentials_path)

    values_to_insert = []

    for vaga in vagas:
        vaga_list = list(vaga)
        values_to_insert.append(vaga_list)

    inserted_rows = spreadsheet.append_values(values_to_insert)
    
    if inserted_rows:
        ids_to_mark_written = [vaga_list[0] for vaga_list in values_to_insert]
        mark_vagas_as_written(connection, ids_to_mark_written)
        print(f"Foram escritas {inserted_rows} vagas no Google Sheets.")
    else:
        print("Nenhuma vaga foi escrita no Google Sheets.")

    connection.close()

if __name__ == '__main__':
    main()
