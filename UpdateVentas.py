# -*- coding: UTF-8 -*- 
###################################################################
# Programa que actualiza las ventas desde API de SAP a uns BD SQL
# Autor: Marlos E. Gomez
# Creado el: 15/07/2024 06:12 pm
###################################################################

# librerias
from datetime import date
from datetime import timedelta
import urllib3
import requests
import pandas as pd
import sys
import pyodbc


#Variables generales de entidad
#fec_Desde = date(2022, 1, 1)
fec_Desde = date(2024, 8, 1)
# Obtiene el ultimo día del mes actual
#fec_Hasta = date(date.today().year, (date.today().month + 1), 1) - timedelta(days=1)
fec_Hasta = date(2024, 8, 31)
SAPUser = "WSAPIREST"
SAPPassword = "123456"
SAPPassword = "APIR3st@123"
APIurlVentas = f'https://200.125.191.156:44301/sap/bc/zapi_sales01?sap-client=400&FECHA_DESDE={fec_Desde.strftime('%d/%m/%Y')}&FECHA_HASTA={fec_Hasta.strftime('%d/%m/%Y')}'
#print(APIurlMetas)



# Funciones
def GetAPI_SalesSAP():
    # Deshabilitar el certificado SSL
    urllib3.disable_warnings()
    try:
        print("Iniciando peticion a SAP")
        petition = requests.get(APIurlVentas, auth=(SAPUser, SAPPassword), verify=False)
        
        if petition.status_code == 200:
            print("Peticion a SAP finalizada con exito!")
            return petition.json()
        else:
            print(petition.text)
            sys.exit(1)
        
    except requests.exceptions.Timeout as errT: 
        return f'Error de tiempo: {petition.status_code} | {errT}'.json()
    except requests.exceptions.HTTPError as errH: 
        return f'Error de HTTP: {petition.status_code} | {errH}'.json()
    except requests.exceptions.RequestException as errX:
        return f'Error: {petition.status_code} | {errX}'.json()

def repeat(stringValue, length):
    return stringValue * length


# Obtener datos desde SAP
dataV = GetAPI_SalesSAP()

ventasSAP = pd.DataFrame(dataV["VENTAS"])
#returnSAP = pd.DataFrame(data["RETURN"])
#print(ventasSAP)
#print(returnSAP)

print('Datos obtenidos desde SAP')


# Inserta los datos en la BD
SQLServer = '192.168.1.164'
SQLDataBase = 'QuerySAP'
SQLUser = 'soporte'
SQLPassword = 'soporte'

try:
    conexion = pyodbc.connect( f'DRIVER={{SQL Server}};SERVER={SQLServer};DATABASE={SQLDataBase};UID={SQLUser};PWD={SQLPassword}')
    print('Conexión SQL exitosa')

    # Se eliminan los datos de ventas ya existentes
    conexion.cursor().execute("Delete From Ventas Where FECHA Between ? AND ?", fec_Desde.strftime('%Y-%m-%d'), fec_Hasta.strftime('%Y-%m-%d'))

    # Se graban los cambios
    conexion.cursor().commit()

    # Se obtiene el total de registros a insertar
    iRegs = len(ventasSAP)
    nPorc = 100 / iRegs
    TotalRegs = 0

    SQL_Command = """Insert Ventas (
        SOCIEDAD
        ,FACTURA
        ,ANULADA
        ,FECHA
        ,TIPO_DOCUMENTO_SAP
        ,CLASE_DOCUMENTO
        ,TIPO_DOCUMENTO
        ,ID_CONDICION
        ,CONDICION
        ,FECHA_VENCE_FISCAL
        ,FECHA_DESPACHO
        ,FECHA_RECEPCION
        ,FECHA_VENCE_PVENTAS
        ,FECHA_VENCE_ADMINISTRATIVA
        ,ID_CLIENTE
        ,CLIENTE
        ,RIF
        ,TIPO_CLIENTE
        ,ID_CLASIFICACION
        ,CLASIFICACION
        ,CODIGO_ESTADO
        ,ESTADO
        ,ID_PAIS
        ,PAIS
        ,DIRECCION_FISCAL
        ,DIRECCION_FISCAL_FACTURA
        ,DIRECCION_ENTREGA
        ,GEOLOC
        ,IDIOMA
        ,TELEFONO1
        ,EMAIL
        ,ID_ZONAVENTA
        ,ZONAVENTA
        ,ID_VENDEDOR
        ,VENDEDOR
        ,EMAIL_VENDEDOR
        ,TIPO_VENDEDOR
        ,CANAL_VENDEDOR
        ,MONEDA
        ,TASA
        ,FECHA_TASA
        ,BASEIMPONIBLE
        ,IVA
        ,TOTAL
        ,POSICION
        ,ID_MATERIAL
        ,ID_COMERCIAL
        ,MATERIAL
        ,ID_GRUPO_MAT1
        ,GRUPO_MAT1
        ,ID_GRUPO_MAT2
        ,GRUPO_MAT2
        ,ID_GRUPO_MAT3
        ,GRUPO_MAT3
        ,ID_GRUPO_MAT4
        ,GRUPO_MAT4
        ,CATEGORIA_RENTABILIDAD
        ,PUNTAJE_RENTABILIDAD
        ,CANTIDAD
        ,PRECIO_UNITARIO
        ,BASEIMPONIBLE_RENGLON
        ,PORCENTAJE_IVA
        ,IVA_RENGLON
        ,TOTAL_RENGLON)
        VALUES (?"""

    SQL_Command += repeat(", ?", 63) + ")"

    for iVenta in range(len(ventasSAP)):
        TotalRegs += nPorc

        print(f"Factura No.: {ventasSAP.iloc[iVenta]['FACTURA']} | Cliente: {ventasSAP.iloc[iVenta]['ID_CLIENTE']} | Producto: {ventasSAP.iloc[iVenta]['ID_COMERCIAL']} | Avance: {int(TotalRegs)}%                          ", end="\r")

        if ventasSAP.iloc[iVenta]['FECHA_DESPACHO'] == '0000-00-00':
            fecha_Despacho = None
        else:
            fecha_Despacho = ventasSAP.iloc[iVenta]['FECHA_DESPACHO']

        if ventasSAP.iloc[iVenta]['FECHA_RECEPCION'] == '0000-00-00':
            fecha_Recepcion = None
        else:
            fecha_Recepcion = ventasSAP.iloc[iVenta]['FECHA_RECEPCION']

        if ventasSAP.iloc[iVenta]['UBICACION_GPS'] == '':
            gpsPoint = 'POINT(0 0)'
        else:
            if ("," in ventasSAP.iloc[iVenta]['UBICACION_GPS']):
                gpsPoint = (('POINT(' + ventasSAP.iloc[iVenta]['UBICACION_GPS'] + ')').replace('  ', ' '))

                if (", " in gpsPoint):
                    gpsPoint = gpsPoint.replace(',', '')

                if (" " not in gpsPoint and "," in gpsPoint):
                    gpsPoint = gpsPoint.replace(',', ' ')

            else:
                gpsPoint = (('POINT(' + ventasSAP.iloc[iVenta]['UBICACION_GPS'] + ')').replace('  ', ' '))

        #gpsPoint = 'geography::POINT(40.7128, -74.0060, 4326)'
        #gpsPoint = 'POINT(40.7128 -74.0060)'
        #print(gpsPoint)

        conexion.cursor().execute(SQL_Command,
            ventasSAP.iloc[iVenta]['SOCIEDAD']
           ,ventasSAP.iloc[iVenta]['FACTURA']
           ,ventasSAP.iloc[iVenta]['ANULADA']
           ,ventasSAP.iloc[iVenta]['FECHA']
           ,ventasSAP.iloc[iVenta]['TIPO_DOCUMENTO_SAP']
           ,ventasSAP.iloc[iVenta]['CLASE_DOCUMENTO']
           ,ventasSAP.iloc[iVenta]['TIPO_DOCUMENTO']
           ,ventasSAP.iloc[iVenta]['ID_CONDICION']
           ,ventasSAP.iloc[iVenta]['CONDICION']
           ,ventasSAP.iloc[iVenta]['FECHA_VENCE_FISCAL']
           ,fecha_Despacho
           ,fecha_Recepcion
           ,ventasSAP.iloc[iVenta]['FECHA_VENCE_PVENTAS']
           ,ventasSAP.iloc[iVenta]['FECHA_VENCE_ADMINISTRATIVA']
           ,ventasSAP.iloc[iVenta]['ID_CLIENTE']
           ,ventasSAP.iloc[iVenta]['CLIENTE']
           ,ventasSAP.iloc[iVenta]['RIF']
           ,ventasSAP.iloc[iVenta]['TIPO_CLIENTE']
           ,ventasSAP.iloc[iVenta]['ID_CLASIFICACION']
           ,ventasSAP.iloc[iVenta]['CLASIFICACION']
           ,ventasSAP.iloc[iVenta]['CODIGO_ESTADO']
           ,ventasSAP.iloc[iVenta]['ESTADO']
           ,ventasSAP.iloc[iVenta]['ID_PAIS']
           ,ventasSAP.iloc[iVenta]['PAIS']
           ,ventasSAP.iloc[iVenta]['DIRECCION_FISCAL']
           ,ventasSAP.iloc[iVenta]['DIRECCION_FISCAL_FACTURA']
           ,ventasSAP.iloc[iVenta]['DIRECCION_ENTREGA']
           ,gpsPoint
           ,ventasSAP.iloc[iVenta]['IDIOMA']
           ,ventasSAP.iloc[iVenta]['TELEFONO1']
           ,ventasSAP.iloc[iVenta]['EMAIL']
           ,ventasSAP.iloc[iVenta]['ID_ZONAVENTA']
           ,ventasSAP.iloc[iVenta]['ZONAVENTA']
           ,ventasSAP.iloc[iVenta]['ID_VENDEDOR']
           ,ventasSAP.iloc[iVenta]['VENDEDOR']
           ,ventasSAP.iloc[iVenta]['EMAIL_VENDEDOR']
           ,ventasSAP.iloc[iVenta]['TIPO_VENDEDOR']
           ,ventasSAP.iloc[iVenta]['CANAL_VENDEDOR']
           ,ventasSAP.iloc[iVenta]['MONEDA']
           ,ventasSAP.iloc[iVenta]['TASA']
           ,ventasSAP.iloc[iVenta]['FECHA_TASA']
           ,ventasSAP.iloc[iVenta]['BASEIMPONIBLE']
           ,ventasSAP.iloc[iVenta]['IVA']
           ,ventasSAP.iloc[iVenta]['TOTAL']
           ,int(ventasSAP.iloc[iVenta]['POSICION'])
           ,ventasSAP.iloc[iVenta]['ID_MATERIAL']
           ,ventasSAP.iloc[iVenta]['ID_COMERCIAL']
           ,ventasSAP.iloc[iVenta]['MATERIAL']
           ,ventasSAP.iloc[iVenta]['ID_GRUPO_MAT1']
           ,ventasSAP.iloc[iVenta]['GRUPO_MAT1']
           ,ventasSAP.iloc[iVenta]['ID_GRUPO_MAT2']
           ,ventasSAP.iloc[iVenta]['GRUPO_MAT2']
           ,ventasSAP.iloc[iVenta]['ID_GRUPO_MAT3']
           ,ventasSAP.iloc[iVenta]['GRUPO_MAT3']
           ,ventasSAP.iloc[iVenta]['ID_GRUPO_MAT4']
           ,ventasSAP.iloc[iVenta]['GRUPO_MAT4']
           ,ventasSAP.iloc[iVenta]['CATEGORIA_RENTABILIDAD']
           ,int(ventasSAP.iloc[iVenta]['PUNTAJE_RENTABILIDAD'])
           ,ventasSAP.iloc[iVenta]['CANTIDAD']
           ,ventasSAP.iloc[iVenta]['PRECIO_UNITARIO']
           ,ventasSAP.iloc[iVenta]['BASEIMPONIBLE_RENGLON']
           ,ventasSAP.iloc[iVenta]['PORCENTAJE_IVA']
           ,ventasSAP.iloc[iVenta]['IVA_RENGLON']
           ,ventasSAP.iloc[iVenta]['TOTAL_RENGLON']
        )

        #resultID = conexion.cursor().fetchval()
        #print(f"Insertado: {ventasSAP.iloc[iVenta]['FACTURA']}", end='\r' )

        # Se graban los cambios
        conexion.cursor().commit()

    # Se cierra la conexion
    conexion.close()

except pyodbc.Error as pyerr:
    print(f'\nError al operar en SQL | Documento: {ventasSAP.iloc[iVenta]['FACTURA']} | Producto: {ventasSAP.iloc[iVenta]['ID_COMERCIAL']}\nError No:{pyerr.args[1]}\nError:{pyerr.args[0]}')

    conexion.cursor().rollback()
    conexion.close()

print("\nProceso terminado!")


#https://jsonplaceholder.typicode.com/guide/
#https://jsonplaceholder.typicode.com/posts
#https://gorest.co.in/