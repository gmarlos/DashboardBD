# -*- coding: UTF-8 -*-
###################################################################
# Programa para actualizar las ventas temporales en SQL
# Autor: Marlos E. Gomez
# Creado el: 24/09/2024 04:26 pm
###################################################################

import telebot
from telebot import types
import pyodbc
from datetime import date
from datetime import timedelta
from datetime import datetime
import urllib3
import requests
import pandas as pd


TOKEN = "7787511967:AAFsabpX6yr-T8PjbcpU0VpINIpFHlM5T08"

# Variables de conexion SQL
SQLServer = '192.168.1.164'
SQLDataBase = 'QuerySAP'
SQLUser = 'soporte'
SQLPassword = 'soporte'

# Variables de conexion SAP
SAPUser = "WSAPIREST"
SAPPassword = "123456"
SAPPassword = "APIR3st@123"

conexion = pyodbc.connect( f'DRIVER={{SQL Server}};SERVER={SQLServer};DATABASE={SQLDataBase};UID={SQLUser};PWD={SQLPassword}')


###################################################################
## Funciones
###################################################################

## Obtiene el ID del chat
def chat_id_assign(message):
    """
    Allows the bot to keep track of the chat_id of the user who sent 
    a message, providing a way to send the user messages or perform 
    other chat-related actions.
    """
    global chat_id
    chat_id = message.chat.id 

## Se inicia el Bot
bot = telebot.TeleBot(TOKEN)

commands = ('Comandos disponibles:' + '\n' + 
    '/start - Iniciar bot' + '\n' + 
    '/start_task - Iniciar Tarea' + '\n' + 
    '/update - Actualizar Ventas' + '\n' + 
    '/stop_task - Detener Tarea')

bot.delete_my_commands(scope=None, language_code=None)

bot.set_my_commands(
    commands=[
        telebot.types.BotCommand("start", "Iniciar bot"),
        #telebot.types.BotCommand("update", "Actualizar Ventas"),
        #telebot.types.BotCommand("pizza", "Pizza"),
        #telebot.types.BotCommand("start_task", "Iniciar tareas programadas"),
        #telebot.types.BotCommand("stop_task", "Detener tareas programadas")
    ],
)


## Mensaje de bienvenida
@bot.message_handler(commands=['start'])
def start_command(message):
    chat_id_assign(message)
    emo = '\U0000270b'

    markup = types.InlineKeyboardMarkup(row_width=2)

    # Botones
    btnTaskIni = types.InlineKeyboardButton('Iniciar tarea', callback_data='task_start')
    btnTaskStop = types.InlineKeyboardButton('Detener tarea', callback_data='task_stop')
    btnUpdate = types.InlineKeyboardButton('Actualizar Ventas', callback_data='update')

    #Agregar los botones
    markup.add(btnTaskIni, btnTaskStop, btnUpdate)

    globalmessage_chat_id = message.chat.id

    bot.send_message(message.chat.id,
                     f'{emo} Bienvenido al Bot de actualización de ventas de Grupo Pacífico.',
                     reply_markup=markup)



@bot.callback_query_handler(func=lambda call:True)
def callback_query(call):
    if call.data == 'update':
        Update_SalesSQL(call)
    elif call.data == "task_start":
        Start_Task(call)
    elif call.data == "task_stop":
        Stop_Task(call)


def repeat(stringValue, length):
    return stringValue * length

def Update_SalesSQL(call):
    fec_Desde = GetLastDateInSQL()
    fec_Desde = fec_Desde + timedelta(days=1)
    fec_Hasta = date(date.today().year, (date.today().month + 1), 1) - timedelta(days=1)
    APIurlVentas = f'https://200.125.191.156:44301/sap/bc/zapi_sales01?sap-client=400&FECHA_DESDE={fec_Desde.strftime('%d/%m/%Y')}&FECHA_HASTA={fec_Hasta.strftime('%d/%m/%Y')}'

    # Deshabilitar el certificado SSL
    urllib3.disable_warnings()
    
    try:
        #bot.answer_callback_query(call.id, 'Iniciando peticion a SAP')
        print('Iniciando peticion a SAP')
        petition = requests.get(APIurlVentas, auth=(SAPUser, SAPPassword), verify=False)
        
        if petition.status_code == 200:
            #bot.answer_callback_query(call.id, 'Peticion a SAP finalizada con exito!')
            print('Peticion a SAP finalizada con exito!')
            dt = petition.json()
            ventasSAP = pd.DataFrame(dt["VENTAS"])

            # Se eliminan los datos de ventas ya existentes
            conexion.cursor().execute("Truncate Table VentasTMP")
            
            # Se graban los cambios
            conexion.cursor().commit()

            print('Tabla truncada!')

            SQL_Command = """Insert VentasTMP (
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

            # Se obtiene el total de registros a insertar
            iRegs = len(ventasSAP)
            nPorc = 100 / iRegs
            TotalRegs = 0

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

        else:
            print(petition.text)

    except requests.exceptions.Timeout as errT: 
        return f'Error de tiempo: {petition.status_code} | {errT}'.json()
    except requests.exceptions.HTTPError as errH: 
        return f'Error de HTTP: {petition.status_code} | {errH}'.json()
    except requests.exceptions.RequestException as errX:
        return f'Error: {petition.status_code} | {errX}'.json()

    bot.answer_callback_query(call.id, 'Proceso terminado!')


def Start_Task(call):
    bot.answer_callback_query(call.id, "Iniciar")


def Stop_Task(call):
    bot.answer_callback_query(call.id, "Detener")




###########################################################################################
# Devuelve la ultima fecha almacenada en BD
###########################################################################################
def GetLastDateInSQL():
    tableDate = conexion.cursor().execute("Select Max(FECHA) As LastDate From Ventas")

    for row in tableDate:
        try:
            toDate = datetime.strptime(row.LastDate, '%Y-%m-%d')
            toDate = date(toDate.year, toDate.month, toDate.day)
        except Exception as err:
            toDate = date(2021, 12, 31)

    return toDate



## No permitir otros mensajes
'''
@bot.message_handler(func=lambda message:True)
def echo_all(message):
    """
    Handles all incoming text messages that do not match any of the 
    available commands. The bot will then delete the message and send 
    a message stating that the command is invalid and providing a 
    list of available commands.
    """
    message_id = message.message_id
    bot.delete_message(chat_id, message_id)

    emo = '\U0000274c'
    mensaje = (f'{emo} <b>Comando no valido</b>' + 
                '\n \n' +
                commands)
    bot.send_message(chat_id, mensaje, parse_mode="HTML")'''



if __name__ == '__main__':
    global chat_id
    chat_id = ""
    
    # Automatic tasks are scheduled # https://pypi.org/project/schedule/
    #schedule.every().day.at("06:00").do(sap_update)
    #schedule.every().day.at("18:00").do(sap_update)
    
    #bot.polling(non_stop=True)
    bot.infinity_polling()
    #bot.send_message()
