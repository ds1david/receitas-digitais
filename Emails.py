# -*- coding: utf-8 -*-
import smtplib
import traceback
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from Logger import *

logger = Logger(__name__)


servidor_smtp = 'smtp.dimed.local'
porta_servidor = '25'
remetente_emails = 'Receitas Convenios <receitas_convenios@dimed.com.br>'
email_administrador = 'David Oliveira <doliveira@dimed.com.br>'

def EnviarEmailERRO(mensagem_erro):
    corpo_mensagem = '''\
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Erro Rotinas Receitas Conveios</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    * { margin: 0; padding: 0; }
    body { font: 12px/1.2 Verdana, Arial, Serif; }
    #page-wrap { margin: 5px; }
    p { margin: 20px 0; }
    table { width: 100%; border-collapse: collapse; }
    tr:nth-of-type(odd) { background: #eee; }
    th { background: #333; color: white; font-weight: bold; }
    td, th { padding: 6px; border: 1px solid #ccc; text-align: left; }
  </style>
  <!--[if !IE]><!-->
  <style>
  @media
  only screen and (max-width: 760px), (min-device-width: 768px) and (max-device-width: 1024px) {
    table, thead, tbody, th, td, tr { display: block; }
    thead tr { position: absolute; top: -9999px; left: -9999px; }
    tr { border: 1px solid #ccc; }
    td { border: none; border-bottom: 1px solid #eee; position: relative; padding-left: 50%; }
    td:before { position: absolute; top: 6px; left: 6px; width: 45%; padding-right: 10px; white-space: nowrap; }
    td:nth-of-type(1):before { content: "Data"; }
    td:nth-of-type(2):before { content: "Processo"; }
    td:nth-of-type(3):before { content: "Hora Inicio"; }
    td:nth-of-type(4):before { content: "Tempo Execucao"; }
    td:nth-of-type(5):before { content: "Retorno"; }
  }
  @media only screen and (min-device-width : 320px) and (max-device-width : 480px) {
    body { padding: 0; margin: 0; width: 320px; }
  }
  @media only screen and (min-device-width: 768px) and (max-device-width: 1024px) {
    body { width: 495px; }
  }
  </style>
  <!--<![endif]-->
</head>
<body>
  <div id="page-wrap">
  <h2>Erro Rotinas Receitas Convenios</h2>
  <p>As Rotinas de Receitas Convenios, finalizaram com erros que nao estao previstos nos processos.</p>
  <h3>Detalhes do erro:</h3>
  <table>
    <thead>
    <tr>
      <th>Data</th>
      <th>Hora</th>
      <th>Erro</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td>#DATA#</td>
      <td>#HORA#</td>
      <td>#ERRO#</td>
    </tr>
    </tbody>
  </table>
  </div>
</body>
</html>
'''.replace('#DATA#', datetime.now().strftime('%d/%M/%Y')).replace('#HORA#', datetime.now().strftime('%H:%M:%S')).replace('#ERRO#', mensagem_erro)

    mensagem = MIMEMultipart('alternative')
    mensagem['Subject'] = 'Erro Rotinas Receitas Conveios'
    mensagem['From'] = remetente_emails
    mensagem['To'] = email_administrador
    mensagem.attach(MIMEText(corpo_mensagem, 'html'))

    conexao_smtp = smtplib.SMTP('smtp.dimed.local:25')
    conexao_smtp.ehlo_or_helo_if_needed()
    conexao_smtp.sendmail(remetente_emails, email_administrador, mensagem.as_string())
    conexao_smtp.quit()
