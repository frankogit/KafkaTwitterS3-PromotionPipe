#!/usr/bin/env python
import os
from confluent_kafka import Consumer
import time
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import json
import re
import pandas as pd
import pandas as Dataframe
import csv
import mimetypes

def get_mail(text):
    try:
        return re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", text.lower())
    except:
        return []

def get_wsp(text):

     try:
        return(re.findall("[\d]{9}", text))
     except:
        return []

def send_mail(send_from,send_to,Asunto,CuerpoMensaje,fileToSend,server,port,username='',password=''):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ", ".join(send_to)
    #print(msg['To'])
    msg['Subject'] = Asunto
    msg.attach(MIMEText(CuerpoMensaje))

    if fileToSend:
        ctype, encoding = mimetypes.guess_type(fileToSend)
        if ctype is None or encoding is not None:
            ctype = "application/octet-stream"

        maintype, subtype = ctype.split("/", 1)

        #attachment = MIMEText(reporte.to_csv(), _subtype='vnd.ms-excel', _charset='utf8')
        #fp.close()
        #attachment.add_header("Content-Disposition", "attachment", filename=NombreArchivo)
        #msg.attach(attachment)
        #logging.info('Existe archivo a considerar')
        if maintype == "text":
            fp = open(fileToSend)
            # Note: we should handle calculating the charset
            attachment = MIMEText(fp.read(), _subtype=subtype)
            fp.close()
        else:
            fp = open(fileToSend, "rb")
            attachment = MIMEBase(maintype, subtype)
            attachment.set_payload(fp.read())
            fp.close()
            encoders.encode_base64(attachment)

        attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
        msg.attach(attachment)

    # Ingresar las credenciales de tu correo
    username = username
    password = password

    # The actual mail send
    server = smtplib.SMTP(server, port,'STARTTLS')
    server.starttls()
    server.login(username,password)
    server.sendmail(msg['From'],send_to, msg.as_string())
    server.quit()

c = Consumer({
    'bootstrap.servers': 'broker:9092',
    'group.id': 'simpleconsumerpythony',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['COEDATA_STREAM_TOPIC'])
mail_list=[]
wsp_list=[]
wsp_txt_list=[]
wsp_nro_list=[]

while True:
    start_time = time.time()
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    msg=format(msg.value().decode('utf-8'))
    res = json.loads(msg)

    user_name = res.get('USER_NAME')
    user_followers = res.get('USER_FOLLOWERS')
    user_friends = res.get('USER_FRIENDS')
    id_str = res.get('ID_STR')
    user_years_ago =res.get('USER_YEARS_AGO')
    user_os =res.get('USER_OS')
    user_mail = get_mail(res.get('FULL_TEXT'))
    user_wsp = get_wsp(res.get('FULL_TEXT'))
    cus_msg = res.get('CUSTOM_MESSAGE')
    print(cus_msg)
    if cus_msg =='cat1':
        custom_message = "Hola "+user_name+"!\n\nNos podemos dar cuenta que asististe al Comité Trimestral División Data y Analytics BCP.\n\tEstas proximo a cumplir: "+ str(user_years_ago)+ " años en Twitter y puedes celebrarlo regalandote un viaje con tu nueva tarjeta de crédito pre aprobada y asi sorprender a tus: "+ str(user_followers)+" seguidores y "+str(user_friends)+" amigos.\n\nEntra a banca movil y solicitalo ya mismo!"
        custom_message_wsp = "** Hola "+user_name+"! **      \n\n\tCumpliras: "+ str(user_years_ago)+ " años en Twitter, celebralo regalandote un viaje con tu nueva tarjeta de crédito pre aprobada y asi sorprender a tus: "+ str(user_followers)+" seguidores y "+str(user_friends)+" amigos. \n\nEntra a banca movil y solicitalo !"
    elif cus_msg =='cat2':
        custom_message = "Hola "+user_name+"!\n\nNos podemos dar cuenta que asististe al Comité Trimestral División Data y Analytics BCP.\n\tActualmente tienes "+ str(user_followers)+" seguidores y "+str(user_friends)+" amigos, esos son muchos seguidores.\n\tSorprende a tus seguidores subiendo fotos profesionales con una camara que puedes adquirir con un prestamos personal que tienes aprobado. \n\nEntra a banca movil y solicitalo ya mismo!"
        custom_message_wsp = "** Hola "+user_name+"! **      \n\n\tCuentas con "+ str(user_followers)+" seguidores y "+str(user_friends)+" amigos, esos son muchos seguidores.\n\t Sorprendelos subiendo fotos profesionales con una camara que puedes adquirir con un prestamos personal que tienes aprobado. \n\nEntra a banca movil y solicitalo ya mismo!"
    elif cus_msg =='cat3':
        custom_message = "Hola "+user_name+"!\n\nNos podemos dar cuenta que asististe al Comité Trimestral División Data y Analytics BCP.\n\tTienes disponible un adelanto de sueldo para que puedas renovar tu apreciado: "+str(user_os) +".\n\nEntra a banca movil y solicitalo ya mismo!"
        custom_message_wsp = "** Hola "+user_name+"! **      \n\n\tTienes disponible un adelanto de sueldo para que puedas renovar tu apreciado: "+str(user_os) +".\n\n Entra a banca movil y solicitalo ya mismo!"
    else:
        custom_message = "Hola "+user_name+"!\n\nNos podemos dar cuenta que asististe al Comité Trimestral División Data y Analytics BCP.\n\tActualmente tienes "+ str(user_followers)+" seguidores y "+str(user_friends)+" amigos.\nSe agradece tu presencia y te contamos que tenemos estos productos para que le saques todo el provecho:\n\t- Aplicacion web Yape! »\n\t- Seguros en el BCP »\n\t- Créditos Hipotecarios en el BCP »\n\t- Créditos Personales del BCP »\n\t- Tarjetas de Débito del BCP »\n\t- Tarjetas de Crédito del BCP »\n\t- Cuentas para recibir sueldo en el BCP »\n\nEntra a banca movil y solicita ya mismo!"
        custom_message_wsp = "** Hola "+user_name+"! **      \n\n\tNos podemos dar cuenta que asististe al DemoDay del BCP, vemos que tienes "+ str(user_followers)+" seguidores y "+str(user_friends)+" amigos.\n Te contamos que tenemos estos productos para que le saques todo el provecho:\n\t >> Aplicacion web Yape!\n\t >> Seguros en el BCP \n\t >> Créditos Hipotecarios en el BCP >>\n\t Créditos Personales >> \n\tTarjetas de Débito >>\n\t Tarjetas de Crédito »\n\t Cuentas para recibir sueldo \n\n ** Entra a banca movil y solicita ya mismo!**"

    #print("\n")
    print( user_mail)
    #check if mail is in list
    if user_mail:
        if id_str not  in mail_list:
            send_mail('YOUR-MAIL',
                  user_mail,
                  'PROMOTION FOR YOU !',
                  custom_message,
                  None,
                  'smtp.gmail.com',
                  587,
                  'YOUR-MAIL',
                  'YOUR-PWD')

            mail_list.append(id_str)
    #print("\n")
    if user_wsp:
        if id_str not  in wsp_list:
            print('wsp number: '+str(user_wsp))                
            # INVOKE TWILIO SENTENCE FOR SEND WSP
            wsp_list.append(id_str)

    print("Message received in {} seconds.".format(time.time() - start_time))
    print("")

c.close()










    







