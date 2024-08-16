# -*- coding: UTF-8 -*- 
###################################################################
# Programa que actualiza las metas desde API de SAP a uns BD SQL
# Autor: Marlos E. Gomez
# Creado el: 13/08/2024 03:23 pm
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
ANO = 2024
MES = ''
SAPUser = "WSAPIREST"
SAPPassword = "APIR3st@123"
APIurlMetas = f'https://200.125.191.156:44301/sap/bc/zapi_metas?sap-client=400&SOCIEDADFROM=1010&SOCIEDADTO=1020&ANO={ANO}&MES={MES}'
print(APIurlMetas)


# Funciones
def GetAPI_MetasSAP():
    # Deshabilitar el certificado SSL
    urllib3.disable_warnings()
    try:
        print("Iniciando peticion a SAP")
        petition = requests.get(APIurlMetas, auth=(SAPUser, SAPPassword), verify=False)
        
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
dataM = GetAPI_MetasSAP()

metasSAP = pd.DataFrame(dataM["METAS"])
#returnSAP = pd.DataFrame(data["RETURN"])
#print(metasSAP)
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

    # Se eliminan los datos de metas ya existentes
    #conexion.cursor().execute("Delete From Metas Where ANO = ? AND MES = ?", ANO, MES)
    #conexion.cursor().execute("Delete From Metas Where ANO = ?", ANO)

    # Se graban los cambios
    #conexion.cursor().commit()

    # Se obtiene el total de registros a insertar
    iRegs = len(metasSAP)
    nPorc = 100 / iRegs
    TotalRegs = 0

    for iMeta in range(len(metasSAP)):
        TotalRegs += nPorc

        print(f"Sociedad.: {metasSAP.iloc[iMeta]['SOCIEDAD']} | Marca: {metasSAP.iloc[iMeta]['MARCA1']} | Avance: {int(TotalRegs)}%                          ", end="\r")

        SQL_Command = """Insert Metas (
            SOCIEDAD,
            ID_MARCA1,
            ID_MARCA2,
            MES,
            ANO,
            MARCA1,
            MARCA2,
            DIAS_HAB,
            METAS_UNIDAD,
            METAS_NACIONAL_DOLARES,
            METAS_EXPORTACION_DOLARES,
            BACKORDER,
            META_RENTABILIDAD)
            VALUES (?"""
        
        SQL_Command += repeat(", ?", 12) + ")"

        conexion.cursor().execute(SQL_Command,
            metasSAP.iloc[iMeta]['SOCIEDAD']
           ,metasSAP.iloc[iMeta]['ID_MARCA1']
           ,metasSAP.iloc[iMeta]['ID_MARCA2']
           ,int(metasSAP.iloc[iMeta]['MES'])
           ,int(metasSAP.iloc[iMeta]['ANO'])
           ,metasSAP.iloc[iMeta]['MARCA1']
           ,metasSAP.iloc[iMeta]['MARCA2']
           ,int(metasSAP.iloc[iMeta]['DIAS_HAB'])
           ,float(metasSAP.iloc[iMeta]['METAS_UNIDAD'])
           ,float(metasSAP.iloc[iMeta]['METAS_NACIONAL_DOLARES'])
           ,float(metasSAP.iloc[iMeta]['METAS_EXPORTACION_DOLARES'])
           ,float(metasSAP.iloc[iMeta]['BACKORDER'])
           ,float(metasSAP.iloc[iMeta]['META_RENTABILIDAD'])
        )

        #resultID = conexion.cursor().fetchval()
        #print(f"Insertado: {ventasSAP.iloc[iMeta]['FACTURA']}", end='\r' )

        # Se graban los cambios
        conexion.cursor().commit()

    # Se cierra la conexion
    conexion.close()

except pyodbc.Error as pyerr:
    print(f'Error al operar en SQL | Documento: {metasSAP.iloc[iMeta]['SOCIEDAD']} | Marca: {metasSAP.iloc[iMeta]['MARCA1']}\nError No:{pyerr.args[1]}\nError:{pyerr.args[0]}')

    conexion.cursor().rollback()
    conexion.close()

print("Proceso terminado!")



#https://jsonplaceholder.typicode.com/guide/
#https://jsonplaceholder.typicode.com/posts
#https://gorest.co.in/