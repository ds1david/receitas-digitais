# -*- coding: utf-8 -*-
import BD as banco_dados
from Logger import *

logger = Logger(__name__)


class Parametros(object):

    email_administrador = None
    url_servidor_proxy = None
    usuario_proxy = None
    senha_usuario_proxy = None
    url_servidor_fluig = None
    usuario_fluig = None
    senha_fluig = None

    def __init__(self):
        banco_dados.Conectar()
        try:
            comando_sql = '''SELECT PARC.EMAIL_ADMINISTRADOR
                                   ,PARC.URL_SERVIDOR_PROXY
                                   ,PARC.USUARIO_PROXY
                                   ,PARC.SENHA_PROXY
                                   ,PARC.URL_SERVIDOR_FLUIG
                                   ,PARC.USUARIO_FLUIG
                                   ,PARC.SENHA_FLUIG
                               FROM MTZ_PARAM_ARQS_RECS_CONVENIOS PARC'''
            banco_dados.Executar(comando_sql)
            parametros = banco_dados.BuscarUm()

            self.email_administrador = parametros[0]
            self.utl_servidor_proxy = parametros[1]
            self.usuario_proxy = parametros[2]
            self.senha_proxy = parametros[3]
            self.url_servidor_fluig = parametros[4]
            self.usuario_fluig = parametros[5]
            self.senha_fluig = parametros[6]
        except Exception as e:
            raise
