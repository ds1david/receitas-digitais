# -*- coding: utf-8 -*-
import os
import BD as banco_dados
from Fluig import *
from Parametros import *
from Logger import *


logger = Logger(__name__)

class ValidadorCodigoBarras(object):

    nome_arquivo_pdf = None
    nome_arquivo_thumbnail = None
    codigo_barras = None
    codigo_filial = None
    nsu_transacao = None
    log_processamento = None

    def __init__(self):
        self.nome_arquivo_pdf = None
        self.nome_arquivo_thumbnail = None
        self.codigo_barras = None
        self.codigo_filial = None
        self.nsu_transacao = None
        self.log_processamento = None


    def Selecionar(self, sequencia_arq_rec_convenio):
        banco_dados.Conectar()

        try:
            variaveis_bind = { 'sequencia_arq_rec_convenio' : sequencia_arq_rec_convenio }
            comando_sql = '''SELECT ARC.CODIGO_DA_FILIAL
                             ,      ARC.NSU_DA_TRANSACAO
                             FROM   MTZ_ARQS_RECEITAS_CONVENIOS ARC
                             WHERE  ARC.SEQUENCIA_ARQ_REC_CONVENIO = :sequencia_arq_rec_convenio'''
            banco_dados.Executar(comando_sql, variaveis_bind)
            dados_arquivo = banco_dados.BuscarUm()

            if dados_arquivo != None:
                self.codigo_filial = dados_arquivo[0]
                self.nsu_transacao = dados_arquivo[1]

                status_processamento =  banco_dados.ExecutarFuncao('PCK_MTZ_ARQS_RECS_CONVENIOS.FNC_PROCESSA_ARQUIVO', 'CHAR', [sequencia_arq_rec_convenio])
                banco_dados.Comitar()

                if status_processamento == 'A':
                    return True

                else:
                    comando_sql = '''SELECT ARC.LOG_PROCESSAMENTO
                                     FROM   MTZ_ARQS_RECEITAS_CONVENIOS ARC
                                     WHERE  ARC.SEQUENCIA_ARQ_REC_CONVENIO = :sequencia_arq_rec_convenio'''
                    banco_dados.Executar(comando_sql, variaveis_bind)
                    dados_arquivo = banco_dados.BuscarUm()
                    raise Exception(str(dados_arquivo[0]))

            else:
                return False

            return True

        except Exception as e:
            raise

        finally:
            banco_dados.Desconectar()


    def Integrar(self, sequencia_arq_rec_convenio):
        banco_dados.Conectar()

        try:
            fluig = Fluig()
            fluig.sequencia_arq_rec_convenio = sequencia_arq_rec_convenio
            if fluig.UploadArquivo():
                variaveis_bind = { 'company_id_fluig'           : fluig.company_id,
                                   'document_id_fluig'          : fluig.document_id,
                                   'document_version_fluig'     : fluig.document_version,
                                   'document_description_fluig' : fluig.document_description,
                                   'document_folder_fluig'      : fluig.document_folder,
                                   'mensagem_retorno_fluig'     : fluig.mensagem_retorno,
                                   'log_processamento'          : 'ARQUIVO DISPONIBILIZADO COM SUCESSO!',
                                   'sequencia_arq_rec_convenio' : fluig.sequencia_arq_rec_convenio }
                comando_sql = '''UPDATE MTZ_ARQS_RECEITAS_CONVENIOS ARC
                                    SET ARC.COMPANY_ID_FLUIG           = :company_id_fluig
                                       ,ARC.DOCUMENT_ID_FLUIG          = :document_id_fluig
                                       ,ARC.DOCUMENT_VERSION_FLUIG     = :document_version_fluig
                                       ,ARC.DOCUMENT_DESCRIPTION_FLUIG = :document_description_fluig
                                       ,ARC.DOCUMENT_FOLDER_FLUIG      = :document_folder_fluig
                                       ,ARC.MENSAGEM_RETORNO_FLUIG     = :mensagem_retorno_fluig
                                       ,ARC.LOG_PROCESSAMENTO          = :log_processamento
                                       ,ARC.DTHR_PROCESSAMENTO         = SYSDATE
                                       ,ARC.STATUS_PROCESSAMENTO       = 'B'
                                  WHERE ARC.SEQUENCIA_ARQ_REC_CONVENIO = :sequencia_arq_rec_convenio'''
                banco_dados.Executar(comando_sql, variaveis_bind, True)

                if fluig.UploadCard():
                    variaveis_bind = { 'card_id_fluig'              : fluig.card_id,
                                       'card_version_fluig'         : fluig.card_version,
                                       'card_description_fluig'     : fluig.card_description,
                                       'card_folder_fluig'          : fluig.card_folder,
                                       'mensagem_retorno_fluig'     : fluig.mensagem_retorno,
                                       'log_processamento'          : 'CARD DISPONIBILIZADO COM SUCESSO!',
                                       'sequencia_arq_rec_convenio' : fluig.sequencia_arq_rec_convenio }
                    comando_sql = '''UPDATE MTZ_ARQS_RECEITAS_CONVENIOS ARC
                                        SET ARC.CARD_ID_FLUIG              = :card_id_fluig
                                           ,ARC.CARD_VERSION_FLUIG         = :card_version_fluig
                                           ,ARC.CARD_DESCRIPTION_FLUIG     = :card_description_fluig
                                           ,ARC.CARD_FOLDER_FLUIG          = :card_folder_fluig
                                           ,ARC.MENSAGEM_RETORNO_FLUIG     = :mensagem_retorno_fluig
                                           ,ARC.LOG_PROCESSAMENTO          = :log_processamento
                                           ,ARC.DTHR_PROCESSAMENTO         = SYSDATE
                                           ,ARC.STATUS_PROCESSAMENTO       = 'F'
                                      WHERE ARC.SEQUENCIA_ARQ_REC_CONVENIO = :sequencia_arq_rec_convenio'''
                    banco_dados.Executar(comando_sql, variaveis_bind, True)
                else:
                    variaveis_bind = { 'mensagem_retorno_fluig'     : fluig.mensagem_retorno,
                                       'log_processamento'          : 'DETALHES DO ARQUIVO NAO DISPONIBILIZADOS (ERRO INTEGRACAO FLUIG)!',
                                       'sequencia_arq_rec_convenio' : fluig.sequencia_arq_rec_convenio }
                    comando_sql = '''UPDATE MTZ_ARQS_RECEITAS_CONVENIOS ARC
                                        SET ARC.MENSAGEM_RETORNO_FLUIG     = :mensagem_retorno_fluig
                                           ,ARC.LOG_PROCESSAMENTO          = :log_processamento
                                           ,ARC.DTHR_PROCESSAMENTO         = SYSDATE
                                           ,ARC.STATUS_PROCESSAMENTO       = 'E'
                                      WHERE ARC.SEQUENCIA_ARQ_REC_CONVENIO = :sequencia_arq_rec_convenio'''
                    banco_dados.Executar(comando_sql, variaveis_bind, True)

                    return False
            else:
                variaveis_bind = { 'mensagem_retorno_fluig'     : fluig.mensagem_retorno,
                                   'log_processamento'          : 'ARQUIVO NAO DISPONIBILIZADO (ERRO INTEGRACAO FLUIG)!',
                                   'sequencia_arq_rec_convenio' : fluig.sequencia_arq_rec_convenio }
                comando_sql = '''UPDATE MTZ_ARQS_RECEITAS_CONVENIOS ARC
                                    SET ARC.MENSAGEM_RETORNO_FLUIG     = :mensagem_retorno_fluig
                                       ,ARC.LOG_PROCESSAMENTO          = :log_processamento
                                       ,ARC.DTHR_PROCESSAMENTO         = SYSDATE
                                       ,ARC.STATUS_PROCESSAMENTO       = 'E'
                                  WHERE ARC.SEQUENCIA_ARQ_REC_CONVENIO = :sequencia_arq_rec_convenio'''
                banco_dados.Executar(comando_sql, variaveis_bind, True)

                return False

            return True

        except Exception as e:
            raise

        finally:
            banco_dados.Desconectar()


    def Conciliar(self, sequencia_arq_rec_convenio):
        banco_dados.Conectar()

        try:
            variaveis_bind = { 'sequencia_arq_rec_convenio' : sequencia_arq_rec_convenio }
            comando_sql = '''SELECT ARC.CODIGO_DA_FILIAL
                             ,      ARC.NSU_DA_TRANSACAO
                             FROM   MTZ_ARQS_RECEITAS_CONVENIOS ARC
                             WHERE  ARC.SEQUENCIA_ARQ_REC_CONVENIO = :sequencia_arq_rec_convenio'''
            banco_dados.Executar(comando_sql, variaveis_bind)
            dados_arquivo = banco_dados.BuscarUm()

            if dados_arquivo != None:
                self.codigo_filial = dados_arquivo[0]
                self.nsu_transacao = dados_arquivo[1]

                status_processamento =  banco_dados.ExecutarFuncao('PCK_MTZ_ARQS_RECS_CONVENIOS.FNC_CONCILIA_VENDA', 'CHAR', [sequencia_arq_rec_convenio])
                banco_dados.Comitar()

                if status_processamento != 'F':
                    return False

            else:
                return False

            return True

        except Exception as e:
            raise

        finally:
            banco_dados.Desconectar()

    def Processar(self, validar_codigo_barras=False):
        if len(self.codigo_barras) != 12 and validar_codigo_barras == True:
            return False

        banco_dados.Conectar()

        try:
            if validar_codigo_barras == True:
                variaveis_bind = { 'codigo_barras' : self.codigo_barras }
                comando_sql = '''WITH DADOS AS
                                  (SELECT /*+ MATERIALIZE */
                                          FNC_COR_DECODE_BASE61(SUBSTR(:codigo_barras, 1, 3)) CODIGO_DA_FILIAL
                                         ,FNC_COR_DECODE_BASE61(SUBSTR(:codigo_barras, -9)) NSU_DA_TRANSACAO
                                     FROM DUAL)
                                 SELECT D.CODIGO_DA_FILIAL
                                       ,D.NSU_DA_TRANSACAO
                                   FROM DADOS                D
                                       ,MTZ_VENDAS_DE_CARTAO VC
                                  WHERE VC.CODIGO_DA_FILIAL = D.CODIGO_DA_FILIAL
                                    AND VC.NSU_DA_TRANSACAO = D.NSU_DA_TRANSACAO'''
                banco_dados.Executar(comando_sql, variaveis_bind)
                dados_transacao = banco_dados.BuscarUm()

                if dados_transacao != None:
                    self.codigo_filial = dados_transacao[0]
                    self.nsu_transacao = dados_transacao[1]
                else:
                    return False

            blob_arquivo = open(self.nome_arquivo_pdf, 'rb')
            thumbnail_arquivo = open(self.nome_arquivo_thumbnail, 'rb')

            variaveis_bind = { 'blob_arquivo'               : blob_arquivo,
                               'nome_arquivo'               : os.path.basename(self.nome_arquivo_pdf),
                               'thumbnail_arquivo'          : thumbnail_arquivo,
                               'codigo_barras_arquivo'      : self.codigo_barras,
                               'codigo_da_filial'           : self.codigo_filial,
                               'nsu_da_transacao'           : self.nsu_transacao,
                               'sequencia_arq_rec_convenio' : None }
            comando_sql = '''INSERT INTO MTZ_ARQS_RECEITAS_CONVENIOS(BLOB_ARQUIVO
                                                                    ,NOME_ARQUIVO
                                                                    ,MIMETYPE_ARQUIVO
                                                                    ,THUMBNAIL_ARQUIVO
                                                                    ,CODIGO_BARRAS_ARQUIVO
                                                                    ,CODIGO_DA_FILIAL
                                                                    ,NSU_DA_TRANSACAO
                                                                    ,DTHR_RECEBIMENTO)
                                                              VALUES(:blob_arquivo
                                                                    ,:nome_arquivo
                                                                    ,'application/pdf'
                                                                    ,:thumbnail_arquivo
                                                                    ,SUBSTR(:codigo_barras_arquivo,1,200)
                                                                    ,:codigo_da_filial
                                                                    ,:nsu_da_transacao
                                                                    ,SYSDATE)
                                    RETURNING SEQUENCIA_ARQ_REC_CONVENIO INTO :sequencia_arq_rec_convenio'''
            sequencia_arq_rec_convenio = banco_dados.ExecutarComRetorno(comando_sql, variaveis_bind, 'sequencia_arq_rec_convenio', 'NUMBER')

            blob_arquivo.close()
            thumbnail_arquivo.close()

            del(blob_arquivo)
            del(thumbnail_arquivo)

            del(variaveis_bind)

            status_processamento =  banco_dados.ExecutarFuncao('PCK_MTZ_ARQS_RECS_CONVENIOS.FNC_PROCESSA_ARQUIVO', 'CHAR', [sequencia_arq_rec_convenio])
            banco_dados.Comitar()

            if status_processamento == 'A':
                if self.Integrar(sequencia_arq_rec_convenio):
                    self.Conciliar(sequencia_arq_rec_convenio)

            return True

        except Exception as e:
            raise

        finally:
            banco_dados.Desconectar()
