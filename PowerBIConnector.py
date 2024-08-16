# -*- coding: UTF-8 -*-
###################################################################
# Programa para crear entidades en Power BI
# Autor: Marlos E. Gomez
# Creado el: 24/07/2024 05:12 pm
###################################################################

from datetime import date
from datetime import timedelta
import pyodbc
from datetime import datetime
import urllib3
import requests
import pandas as pd
import matplotlib


# Variables generales de entidad
EntityDesde = date(2022, 1, 1)
# Obtiene el ultimo día del mes actual
EntityHasta = date(date.today().year, (date.today().month + 1), 1) - timedelta(days=1)
EntityHasta = date(2024, 3, 5)

# Variables de conexion
SQLServer = '192.168.1.164'
SQLDataBase = 'QuerySAP'
SQLUser = 'soporte'
SQLPassword = 'soporte'
SAPUser = "WSAPIREST"
SAPPassword = "123456"

#Metas = None
#Vendedores = None
pdSQL = None
pdSSAP = None
preVENTAS = None

#Se realiza la conexion a la BD
try:
    conexion = pyodbc.connect( f'DRIVER={{SQL Server}};SERVER={SQLServer};DATABASE={SQLDataBase};UID={SQLUser};PWD={SQLPassword}')
    print("Conexión exitosa!")
except pyodbc.Error as pyerr:
    print(f'Error al conectar en SQL\nError No:{pyerr.args[1]}\nError:{pyerr.args[0]}')


###########################################################################################
# Devuelve la ultima fecha almacenada en BD
###########################################################################################
def GetLastDateInSQL():
    #tableDate = conexion.cursor().execute("Select Max(FECHA) As LastDate From Ventas Where FECHA <= '2024-02-29'")
    tableDate = conexion.cursor().execute("Select Max(FECHA) As LastDate From Ventas")

    for row in tableDate:
        try:
            toDate = datetime.strptime(row.LastDate, '%Y-%m-%d')
            #toDate = date(toDate.year, (toDate.month + 1), 1) - timedelta(days=1)
            toDate = date(toDate.year, toDate.month, toDate.day)
        except Exception as err:
            toDate = date(2021, 12, 31)

        return toDate

###########################################################################################
### Se obtienen las metas
###########################################################################################
def GetMetas():
    try:
        SQL_Command = "Select * From Metas Order By ANO, MES, SOCIEDAD, ID_MARCA1, ID_MARCA2"

        tableDataSQL = conexion.cursor().execute(SQL_Command)

        columns = [column[0] for column in tableDataSQL.description]
        return pd.DataFrame.from_records(data=tableDataSQL, columns=columns)
    except pyodbc.Error as pyerr:
        print(f'Error al ejecutar en SQL\nError No:{pyerr.args[1]}\nError:{pyerr.args[0]}')


###########################################################################################
### Se obtienen los vendedores
###########################################################################################
def GetVendedores():
    APIurl = f'https://200.125.191.156:44300/sap/bc/zapi_salesman?sap-client=400'

    urllib3.disable_warnings()
    try:
        petition = requests.get(APIurl, auth=(SAPUser, SAPPassword), verify=False)
        
        if petition.status_code == 200:
            dataSAP = petition.json()
            pdData = pd.DataFrame(dataSAP["VENDEDORES"])
            return pdData
        else:
            return None
        
    except requests.exceptions.Timeout as errT: 
        print(f'Error de tiempo: {petition.status_code} | {errT}'.json())
    except requests.exceptions.HTTPError as errH: 
        print(f'Error de HTTP: {petition.status_code} | {errH}'.json())
    except requests.exceptions.RequestException as errX:
        print(f'Error: {petition.status_code} | {errX}'.json())


###########################################################################################
### Se obtienen los clientes
###########################################################################################
def GetClientes():
    APIurl = f'https://200.125.191.156:44300/sap/bc/zapi_salesman?sap-client=400'

    urllib3.disable_warnings()
    try:
        petition = requests.get(APIurl, auth=(SAPUser, SAPPassword), verify=False)
        
        if petition.status_code == 200:
            dataSAP = petition.json()
            pdData = pd.DataFrame(dataSAP["VENTAS"])
            return pdData
        else:
            return None
        
    except requests.exceptions.Timeout as errT: 
        print(f'Error de tiempo: {petition.status_code} | {errT}'.json())
    except requests.exceptions.HTTPError as errH: 
        print(f'Error de HTTP: {petition.status_code} | {errH}'.json())
    except requests.exceptions.RequestException as errX:
        print(f'Error: {petition.status_code} | {errX}'.json())



# Se obtiene la ultima fecha almacenada en BD
lastDateInBD = GetLastDateInSQL()
Metas = GetMetas()
Vendedores = GetVendedores()
Clientes = GetClientes()

# Se consulta la BD para optimizar la consulta
try:

    ##########################
    ### Se obtienen las ventas
    ##########################
    SQL_Command = """
        Select
            ID_VENTAS,
            SOCIEDAD,
            FACTURA,
            ANULADA,
            FECHA,
            TIPO_DOCUMENTO_SAP, CLASE_DOCUMENTO, TIPO_DOCUMENTO,
            ID_CONDICION, CONDICION,
            FECHA_VENCE_FISCAL, FECHA_DESPACHO, FECHA_RECEPCION, FECHA_VENCE_PVENTAS, FECHA_VENCE_ADMINISTRATIVA,
            ID_CLIENTE, CLIENTE, RIF, TIPO_CLIENTE,
            ID_CLASIFICACION, CLASIFICACION,
            CODIGO_ESTADO, ESTADO,
            ID_PAIS, PAIS,
            DIRECCION_FISCAL, DIRECCION_FISCAL_FACTURA, DIRECCION_ENTREGA,
            IIF(Replace(Replace(SubString([UBICACION_GPS], 8, Len([UBICACION_GPS])), ')', ''), ' ', ', ') = '0, 0', '',
            RTrim(LTrim(Replace(Replace(SubString([UBICACION_GPS], 8, Len([UBICACION_GPS])), ')', ''), ' ', ', ')))) As UBICACION_GPS,
            IDIOMA,
            TELEFONO1, EMAIL,
            ID_ZONAVENTA, ZONAVENTA,
            ID_VENDEDOR, VENDEDOR, EMAIL_VENDEDOR, TIPO_VENDEDOR, CANAL_VENDEDOR,
            MONEDA,
            TASA,
            FECHA_TASA,
            BASEIMPONIBLE, IVA, TOTAL,
            POSICION,
            ID_MATERIAL, ID_COMERCIAL, MATERIAL,
            ID_GRUPO_MAT1, GRUPO_MAT1,
            ID_GRUPO_MAT2, GRUPO_MAT2,
            ID_GRUPO_MAT3, GRUPO_MAT3,
            ID_GRUPO_MAT4, GRUPO_MAT4,
            CATEGORIA_RENTABILIDAD, PUNTAJE_RENTABILIDAD,
            CANTIDAD,
            PRECIO_UNITARIO, BASEIMPONIBLE_RENGLON, PORCENTAJE_IVA, IVA_RENGLON, TOTAL_RENGLON
        From Ventas
        Where FECHA >= ?"""

    tableDataSQL = conexion.cursor().execute(SQL_Command, EntityDesde.strftime('%Y-%m-%d'))

    columns = [column[0] for column in tableDataSQL.description]
    pdSQL = pd.DataFrame.from_records(data=tableDataSQL, columns=columns)
except pyodbc.Error as pyerr:
    print(f'Error al ejecutar en SQL\nError No:{pyerr.args[1]}\nError:{pyerr.args[0]}')


# Se consultan los datos faltantes desde SAP
if lastDateInBD < EntityHasta:
    SAPUser = "WSAPIREST"
    SAPPassword = "123456"
    fec_Desde = lastDateInBD + timedelta(days=1)
    APIurl = f'https://200.125.191.156:44300/sap/bc/zapi_sales01?sap-client=400&FECHA_DESDE={fec_Desde.strftime('%d/%m/%Y')}&FECHA_HASTA={EntityHasta.strftime('%d/%m/%Y')}'

    urllib3.disable_warnings()
    try:
        petition = requests.get(APIurl, auth=(SAPUser, SAPPassword), verify=False)
        
        if petition.status_code == 200:
            dataSAP = petition.json()
            pdSSAP = pd.DataFrame(dataSAP["VENTAS"])
            dataSAP = None
        else:
            pdSSAP = None
        
    except requests.exceptions.Timeout as errT: 
        print(f'Error de tiempo: {petition.status_code} | {errT}'.json())
    except requests.exceptions.HTTPError as errH: 
        print(f'Error de HTTP: {petition.status_code} | {errH}'.json())
    except requests.exceptions.RequestException as errX:
        print(f'Error: {petition.status_code} | {errX}'.json())

preVENTAS = pd.concat([pdSQL, pdSSAP], axis=0)

# Se ordena el Dataframe
Ventas = preVENTAS.sort_values(by=["FECHA", "SOCIEDAD", "FACTURA"])

pdSQL = None
pdSSAP = None
preVENTAS = None
conexion.close()

print("Terminado!")


#for i in range(len(Ventas)):
#    print(f"Factura: {Ventas.iloc[i]['FACTURA']} | Fecha: {Ventas.iloc[i]['FECHA']}")