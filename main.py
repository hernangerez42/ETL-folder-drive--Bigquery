import csv
import datetime
import io
import logging
import os
import tempfile

import google.auth
import pandas as pd
from google.cloud import bigquery

import config
from config import (obtener_valores_csv, mover_col,
                    renombra_columnas, buscar_archivos_csv,
                    añadir_col, crear_carpeta_temporal, eliminar_carpeta_temporal)

id_carpeta = config.id_carpeta
columnas = config.columnas
nuevas_posiciones = config.nuevas_posiciones
logger = config.obtener_logger(__name__)


def extracion():
    logging.basicConfig(filename="extraccion.log", level=logging.INFO)
    logging.info('Se ha iniciado la extraccion de los datos')
    # Crear una lista para almacenar los DataFrames
    dfs = []
    # Obtener la ubicación de la carpeta temporal
    temp_folder = tempfile.mkdtemp()
    # Obtener la lista de archivos CSV en la carpeta de Google Drive
    csv_files = buscar_archivos_csv(id_carpeta)
    
    # Iterar sobre la lista de archivos CSV
    for csv_file in csv_files:
        file_id = csv_file['id']
        file_name = csv_file['name']
    
        # Obtener los valores del archivo CSV
        csv_data = obtener_valores_csv(file_id)
        #guardamos los datos en un dataframe con su nombre correspondiente
        df = pd.DataFrame(csv_data, columns=csv_data[0])
        #eliminamos la primera filad si esta duplicada
        df = df.drop([0])
        #quitamos el .csv del nombre
        file_name = file_name[:2]
        df['company'] = file_name
        #añadimos el dataframe a la lista con su nombre
        dfs.append(df)
    #Concatenar los DataFrames
    df = pd.concat(dfs)
    # Guardar el DataFrame en un archivo CSV en la carpeta temporal
    ruta_csv_salida = os.path.join(temp_folder, 'data.csv')
    df.to_csv(ruta_csv_salida, index=False)
    logging.info('Se ha extraido, concatenado y guardado los datos en un dataframe')
    #retornamos el dataframe guardado
    return ruta_csv_salida


def abrir_df(df):
    df = pd.read_csv(df)
    return df



def transformacion(df):

    logging.basicConfig(filename="transformacion.log", level=logging.INFO)
    logging.info('Se ha iniciado la transformacion de los datos')
    # Reemplaza los valores faltantes en `Customer Name` con el último valor válido
    df['CustomerID'] = df['CustomerID'].ffill()
    df['CustomerName'] = df['CustomerName'].ffill()
    # Reemplaza los valores faltantes en `Invoice No` con el siguiente o anterior valor válido.
    df["Fill_InvoiceNo"] = df["Invoice_No"].bfill(limit=1)
    df["Fill_InvoiceNo2"] = df["Fill_InvoiceNo"].ffill()
    #limpiamos las filas que no tienen datos
    df = df.dropna(subset=['Transaction_Date'])
    # eliminamos las columna Invoice No, Fill_InvoiceNo
    df = df.drop(['Invoice_No', 'Fill_InvoiceNo'], axis=1)
    # renombramos la columna Fill_InvoiceNo.2 a Invoice No
    df.rename(columns={'Fill_InvoiceNo2': 'Invoice_No'}, inplace=True)
    df = añadir_col(df)
    df = mover_col(df, columnas, nuevas_posiciones)
    # Quitar las comas
    df["Amount"] = df["Amount"].str.replace(",", "").astype(float)
    df["Amount_Received"] = df["Amount_Received"].str.replace(",", "").astype(float)
    df['Amount_Remaining'] = df['Amount_Remaining'].str.replace(",", "").astype(float)
    # Convertir las columnas a numéricas
    df['Invoice_Amount'] = pd.to_numeric(df['Invoice_Amount'], errors='coerce')
    df['Credit_Amount'] = pd.to_numeric(df['Credit_Amount'], errors='coerce')
    df['Receipt_Amount'] = pd.to_numeric(df['Receipt_Amount'], errors='coerce')
    df['Days_Until_First_Payment'] = pd.to_numeric(df['Days_Until_First_Payment'], errors='coerce')
    df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
    df['Days_To_Pay'] = pd.to_numeric(df['Days_To_Pay'], errors='coerce')
    df['Open_PO'] = df['Open_PO'].astype(object)
    # y una columna llamada 'fecha' con las fechas en formato '8/23/16'
    df['Transaction_Date'] = pd.to_datetime(df['Transaction_Date'], format='%m/%d/%y')
    df['Date_Due_Parsed'] = pd.to_datetime(df['Date_Due_Parsed'], format='%m/%d/%y')
    df['Invoice_Date'] = pd.to_datetime(df['Invoice_Date'], format='%m/%d/%y')

    logging.info('Se ha finalizado la transformacion de los datos')

    return df



def cargar_dataframe_bigquery(df, project_id, dataset_id, table_id):
    
    logging.basicConfig(filename="carga.log", level=logging.INFO)
    logging.info('Se ha iniciado la carga de los datos a BigQuery')

    credentials, project = google.auth.default()
    bq_client = bigquery.Client(credentials=credentials, project=project_id)
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
                bigquery.SchemaField("CustomerID", "STRING"),
                bigquery.SchemaField("CustomerName", "STRING"),
                bigquery.SchemaField("Invoice_No", "STRING"),
                bigquery.SchemaField("Transaction_Date", "DATETIME"),
                bigquery.SchemaField("Date_Due_Parsed", "DATETIME"),
                bigquery.SchemaField("Transaction", "STRING"),
                bigquery.SchemaField("Trans_No", "STRING"),
                bigquery.SchemaField("Amount", "FLOAT64"),
                bigquery.SchemaField("Open_PO", "STRING"),
                bigquery.SchemaField("Amount_Received", "FLOAT64"),
                bigquery.SchemaField("Amount_Remaining", "FLOAT64"),
                bigquery.SchemaField("Invoice_Amount", "FLOAT64"),
                bigquery.SchemaField("Credit_Amount", "FLOAT64"),
                bigquery.SchemaField("Receipt_Amount", "FLOAT64"),
                bigquery.SchemaField("Days_To_Pay", "INT64"),
                bigquery.SchemaField("Age", "INT64"),
                bigquery.SchemaField("Invoice_Status", "STRING"),
                bigquery.SchemaField("Customer_PO", "STRING"),
                bigquery.SchemaField("Payment_Method", "STRING"),
                bigquery.SchemaField("InvoiceID", "STRING"),
                bigquery.SchemaField("Company", "STRING"),
                bigquery.SchemaField("Estado", "STRING"),
                bigquery.SchemaField("Invoice_Date", "DATETIME"),
                bigquery.SchemaField("Pagos_Registrados", "STRING"),
                bigquery.SchemaField("Days_Until_First_Payment", "INT64"),
                bigquery.SchemaField("Tipo_Factura", "STRING")
        ]
    )
    
    try:
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logging.info(f"Se ha cargado el DataFrame en la tabla {project_id}.{dataset_id}.{table_id} de BigQuery.")
    except Exception as e:
        logging.info(f"Error al cargar el DataFrame en BigQuery: {str(e)}")

    logging.info('Se ha finalizado la carga de los datos a BigQuery')


def main(*args):
    logging.basicConfig(filename="main.log", level=logging.INFO)
    temp_folder = crear_carpeta_temporal()
    ruta_csv_salida = extracion()
    df = abrir_df(ruta_csv_salida)
    df = renombra_columnas(df)
    df = transformacion(df)
    cargar_dataframe_bigquery(df, 'kpi-process', 'peachtree', 'DR-LT')
    eliminar_carpeta_temporal(temp_folder)
    logging.info('ETL finalizado')


if __name__ == '__main__':
    main()
