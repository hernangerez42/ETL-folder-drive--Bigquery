import csv
import io
import logging
import os
import tempfile
import shutil


import google.auth
import pandas as pd
from google.cloud import bigquery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#id_carpeta = '139xf3AnC3jnKV0MmVzzgpawB6k9B1iDe'
id_carpeta = '1IW1HAv9p-1ksAcUhS8RqI1lpYbFFPNJ4'

columnas = ['CustomerID',
            'CustomerName', 
            'Invoice_No', 
            'Transaction_Date',
            'Date_Due_Parsed',
            'Transaction',
            'Trans_No',
            'Amount',
            'Open_PO',
            'Amount_Received',
            'Amount_Remaining',
            'Invoice_Amount',
            'Credit_Amount',
            'Receipt_Amount',
            'Days_To_Pay',
            'Age',
            'Invoice_Status',
            'Customer_PO',
            'Payment_Method',
            'InvoiceID',
            'Company',
            'Estado',
            'Invoice_Date',
            'Pagos_Registrados',
            'Days_Until_First_Payment',
            'Tipo_Factura']
nuevas_posiciones = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]


# Configura el controlador de consola
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Define el formato de los registros en la consola
console_formatter = logging.Formatter('%(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Configura el sistema de registro
logging.basicConfig(level=logging.INFO, handlers=[console_handler])


def obtener_logger(nombre):
    return logging.getLogger(nombre)


def crear_carpeta_temporal():
    temp_folder = tempfile.mkdtemp()
    logging.info(f'Carpeta temporal {temp_folder} creada exitosamente.')
    return temp_folder


def eliminar_carpeta_temporal(temp_folder):
    try:
        shutil.rmtree(temp_folder)
        logging.info(f'Carpeta temporal {temp_folder} eliminada exitosamente.')
    except Exception as e:
        logging.info(f'Error al eliminar la carpeta temporal {temp_folder}: {str(e)}')


def buscar_archivos_csv(gdrive_folder_id):

    gdrive_api_query = (
        "mimeType = 'text/csv' "
        f"and '{gdrive_folder_id}' in parents"
    )

    creds, _ = google.auth.default()

    try:
        # Create Drive API client
        service = build("drive", "v3", credentials=creds)
        files = []
        page_token = None
        while True:
            response = service.files().list(q=gdrive_api_query,
                                            spaces="drive",
                                            includeItemsFromAllDrives=True,
                                            supportsAllDrives=True,
                                            corpora="allDrives",
                                            fields="nextPageToken, "
                                                   "files(id, name)",
                                            pageToken=page_token).execute()
            files.extend(response.get("files", []))
            if not files:
                logging.info("No CSV files found.")
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

    except HttpError as error:
        logging.info(f"An error occurred: {error}")
        files = None

    return files


def obtener_valores_csv(file_id):

    try:
        # Obtener credenciales automáticamente
        creds, _ = google.auth.default()
        
        drive_service = build('drive', 'v3', credentials=creds)
        request = drive_service.files().get_media(fileId=file_id)
        
        # Create an in-memory file-like object to store the downloaded data
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        
        # Download the file
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        # Reset the file-like object for reading
        fh.seek(0)
        
        # Read the CSV data
        csv_data = fh.read().decode('windows-1252')
        
        # Parse the CSV data into a list of lists
        csv_reader = csv.reader(csv_data.splitlines()[4:])
        rows = [row for row in csv_reader]
        
        return rows

    except Exception as error:
        # Handle exceptions
        print(f"An error occurred: {error}")
        return []


def renombra_columnas(df):
    df = df.rename(columns={'Customer ID': 'CustomerID',
                            'Customer Name': 'CustomerName',
                            'Invoice No.': 'Invoice_No',
                            'Transaction': 'Transaction',
                            'Trans No.': 'Trans_No',
                            'Date':'Transaction_Date',
                            'Amount': 'Amount',
                            'Open PO #': 'Open_PO',
                            'Amnt Received': 'Amount_Received',
                            'Amnt Remaining': 'Amount_Remaining',
                            'Days To Pay': 'Days_To_Pay',
                            'Date Due': 'Date_Due_Parsed',
                            'Age': 'Age',
                            'Invoice Status': 'Invoice_Status',
                            'Customer PO': 'Customer_PO',
                            'Payment Method': 'Payment_Method',
                            'company': 'Company'
                            })
    return df

#funcion para añadir columnas
def añadir_col(df):
    #añadimos una columna le seteamos none
    df['Invoice_Amount'] = None
    df['Credit_Amount'] = None
    df['Receipt_Amount'] = None
    df['InvoiceID'] = None
    df['Estado'] = None
    df['Invoice_Date'] = None
    df['Pagos_Registrados'] = None
    df['Days_Until_First_Payment'] = None
    df['Tipo_Factura'] = None

    return df


def mover_col(df, columnas, nuevas_posiciones):
    # Obtener el nombre de todas las columnas en el DataFrame
    columnas_originales = list(df.columns)

    # Iterar sobre las columnas a mover
    for columna, nueva_posicion in zip(columnas, nuevas_posiciones):
        # Obtener el índice actual de la columna
        indice_actual = columnas_originales.index(columna)

        # Quitar la columna del DataFrame
        columna_movida = df.pop(columna)

        # Insertar la columna en la nueva posición
        columnas_originales.insert(nueva_posicion, columna)
        df.insert(nueva_posicion, columna, columna_movida)

    return df


if __name__ == '__main__':
    print('Ejecutando como programa principal')
