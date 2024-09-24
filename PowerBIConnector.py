# -*- coding: UTF-8 -*-
###################################################################
# Programa para crear entidades en Power BI
# Autor: Marlos E. Gomez
# Creado el: 24/07/2024 05:12 pm
###################################################################

import urllib3
import requests
import pandas as pd


###########################################################################################
### Se obtienen los clientes, vendedores y ventas desde SAP
###########################################################################################
def GetEntitySAP():
    SAPUser = "WSAPIREST"
    SAPPassword = "123456"
    #SAPPassword = "APIR3st@123"

    '''fec_Desde = lastDateInBD + timedelta(days=1)
    fec_Desde = lastDateInBD + timedelta(days=1)
    APIurl = f'https://200.125.191.156:44300/sap/bc/zapi_sales_pbi?sap-client=400&FECHA_DESDE={fec_Desde.strftime('%d/%m/%Y')}&FECHA_HASTA={EntityHasta.strftime('%d/%m/%Y')}'
    '''
    APIurl = f'https://200.125.191.156:44300/sap/bc/zapi_sales_pbi?sap-client=400'

    urllib3.disable_warnings()
    try:
        petition = requests.get(APIurl, auth=(SAPUser, SAPPassword), verify=False)
        
        if petition.status_code == 200:
            dataSAP = petition.json()
            return dataSAP
            #pdSSAP = pd.DataFrame(dataSAP["VENTAS"])
            #dataSAP = None
        else:
            pdSSAP = None
        
    except requests.exceptions.Timeout as errT: 
        print(f'Error de tiempo: {petition.status_code} | {errT}'.json())
    except requests.exceptions.HTTPError as errH: 
        print(f'Error de HTTP: {petition.status_code} | {errH}'.json())
    except requests.exceptions.RequestException as errX:
        print(f'Error: {petition.status_code} | {errX}'.json())


EntitySAP = GetEntitySAP()
print("Ventas SAP listas!")
Clientes = pd.DataFrame(EntitySAP["CLIENTES"])
Vendedores = pd.DataFrame(EntitySAP["VENDEDORES"])

