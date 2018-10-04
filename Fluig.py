# -*- coding: utf-8 -*-
import base64
import json
import os
import requests
import BD as banco_dados
from bs4 import BeautifulSoup
from Parametros import *
from Logger import *

logger = Logger(__name__)

xml_anexo = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="http://ws.dm.ecm.technology.totvs.com/">
   <soapenv:Header/>
   <soapenv:Body>
      <ws:createSimpleDocument>
         <username>#USERNAME#</username>
         <password>#PASSWORD#</password>
         <companyId>#COMPANY_ID#</companyId>
         <parentDocumentId>#DOCUMENT_FOLDER#</parentDocumentId>
         <publisherId>#USERNAME#</publisherId>
         <documentDescription>#FILENAME#</documentDescription>
         <Attachments>
            <item>
               <attach>false</attach>
               <descriptor>#FILENAME#</descriptor>
               <fileName>#FILENAME#</fileName>
               <fileSelected/>
               <filecontent>#FILECONTENT#</filecontent>
               <principal>false</principal>
            </item>
         </Attachments>
      </ws:createSimpleDocument>
   </soapenv:Body>
</soapenv:Envelope>
'''

xml_card = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="http://ws.dm.webdesk.technology.datasul.com/">
   <soapenv:Header/>
   <soapenv:Body>
      <ws:create>
         <username>#USERNAME#</username>
         <password>#PASSWORD#</password>
         <companyId>#COMPANY_ID#</companyId>
         <card>
            <item>
               <additionalComments>Registro incluido via webservice</additionalComments>
               <cardData>
                  <field>codigoFilial</field>
                  <value>#CODIGO_DA_FILIAL#</value>
               </cardData>
               <cardData>
                  <field>numeroCupomFiscal</field>
                  <value>#NUMERO_DO_CUPOM_FISCAL#</value>
               </cardData>
               <cardData>
                  <field>dataEfetivaVenda</field>
                  <value>#DATA_EFETIVA_DA_VENDA#</value>
               </cardData>
               <cardData>
                  <field>valorConvenio</field>
                  <value>#VALOR_CONVENIO#</value>
               </cardData>
               <cardData>
                  <field>matriculaAssociado</field>
                  <value>#MATRICULA_DO_ASSOCIADO#</value>
               </cardData>
               <cardData>
                  <field>nomeAssociado</field>
                  <value>#NOME_DO_ASSOCIADO#</value>
               </cardData>
#ITENS_DA_NOTA_FISCAL#
               <cardData>
                  <field>empresaFluig</field>
                  <value>#COMPANY_ID#</value>
               </cardData>
               <cardData>
                  <field>idFluig</field>
                  <value>#DOCUMENT_ID#</value>
               </cardData>
               <cardData>
                  <field>versaoFluig</field>
                  <value>#DOCUMENT_VERSION#</value>
               </cardData>
               <cardData>
                  <field>descricaoFluig</field>
                  <value>#DOCUMENT_DESCRIPTION#</value>
               </cardData>
               <cardData>
                  <field>pastaFluig</field>
                  <value>#DOCUMENT_FOLDER#</value>
               </cardData>
               <colleagueId>#USERNAME#</colleagueId>
               <parentDocumentId>#CARD_FOLDER#</parentDocumentId>
               <reldocs>
                  <companyId>#COMPANY_ID#</companyId>
                  <documentId>#DOCUMENT_ID#</documentId>
                  <relatedDocumentId>#DOCUMENT_ID#</relatedDocumentId>
                  <version>#DOCUMENT_VERSION#</version>
               </reldocs>
            </item>
         </card>
      </ws:create>
   </soapenv:Body>
</soapenv:Envelope>
'''
xml_card_itens = '''               <cardData>
                  <field>codigo___#NUMERO_DO_ITEM#</field>
                  <value>#CODIGO_DO_ITEM#</value>
               </cardData>
               <cardData>
                  <field>ean___#NUMERO_DO_ITEM#</field>
                  <value>#CODIGO_DE_BARRAS#</value>
               </cardData>
               <cardData>
                  <field>descricao___#NUMERO_DO_ITEM#</field>
                  <value>#NOMENCLATURA_VAREJO#</value>
               </cardData>
               <cardData>
                  <field>categoria___#NUMERO_DO_ITEM#</field>
                  <value>#CATEGORIA#</value>
               </cardData>
               <cardData>
                  <field>quantidade___#NUMERO_DO_ITEM#</field>
                  <value>#QUANTIDADE#</value>
               </cardData>
               <cardData>
                  <field>valorUnitario___#NUMERO_DO_ITEM#</field>
                  <value>#VALOR_UNITARIO#</value>
               </cardData>
               <cardData>
                  <field>valorTotal___#NUMERO_DO_ITEM#</field>
                  <value>#VALOR_TOTAL#</value>
               </cardData>
               <cardData>
                  <field>registroCRM___#NUMERO_DO_ITEM#</field>
                  <value>#NUMERO_DO_REGISTRO_CRM#</value>
               </cardData>
'''

class Fluig(object):

    sequencia_arq_rec_convenio = None
    nome_do_arquivo = None
    company_id = None
    document_id = None
    document_version = None
    document_description = None
    document_folder = None
    card_id = None
    card_version = None
    card_description = None
    card_folder = None
    mensagem_retorno = None

    def __init__(self):
        self.sequencia_arq_rec_convenio = None
        self.nome_do_arquivo = None
        self.company_id = None
        self.document_id = None
        self.document_version = None
        self.document_description = None
        self.document_folder = None
        self.card_id = None
        self.card_version = None
        self.card_description = None
        self.card_folder = None
        self.mensagem_retorno = None

    def UploadArquivo(self):
        parametros = Parametros()
        banco_dados.Conectar()
        try:
            variaveis_bind = { 'sequencia_arq_rec_convenio' : self.sequencia_arq_rec_convenio }
            comando_sql = '''SELECT ARC.SEQUENCIA_ARQ_REC_CONVENIO
                                   ,'PV' || LPAD(ARC.CODIGO_DA_FILIAL, 4, '0') || '-' ||
                                            LPAD(NFS.NUMERO_DO_CUPOM_FISCAL, 9, '0') || '-' ||
                                            TO_CHAR(NFS.DATA_EFETIVA_DA_VENDA, 'DDMMYYYY') || '-' ||
                                            LPAD(NVL(FPV.ASSO_MATRICULA_DO_ASSOCIADO, '0'), 16, '0') || '.pdf'
                                   ,ARC.BLOB_ARQUIVO
                                   ,PFC.COMPANY_ID_FLUIG
                                   ,PFC.DOCUMENT_FOLDER_FLUIG
                               FROM MTZ_ARQS_RECEITAS_CONVENIOS    ARC
                                   ,MTZ_PARAMETROS_FLUIG_CONVENIOS PFC
                                   ,NOTAS_FISCAIS_DE_SAIDA         NFS
                                   ,FORMAS_DE_PAGAMENTO_DA_VENDA   FPV
                              WHERE ARC.CODIGO_DA_FILIAL_DO_CONVENIO = PFC.CODIGO_DA_FILIAL_DO_CONVENIO
                                AND ARC.CODIGO_DO_CONVENIO           = PFC.CODIGO_DO_CONVENIO
                                AND ARC.CODIGO_DA_FILIAL             = NFS.CODIGO_DA_FILIAL
                                AND ARC.CODIGO_DO_DOCUMENTO_DE_SAIDA = NFS.CODIGO_DO_DOCUMENTO_DE_SAIDA
                                AND ARC.CODIGO_DA_FILIAL             = FPV.CODIGO_DA_FILIAL
                                AND ARC.CODIGO_DO_DOCUMENTO_DE_SAIDA = FPV.CODIGO_DO_DOCUMENTO_DE_SAIDA
                                AND ARC.CODIGO_DA_FORMA_DE_PAGAMENTO = FPV.CODIGO_DA_FORMA_DE_PAGAMENTO
                                AND ARC.SEQUENCIA_FORMA_PAGTO_VENDA  = FPV.SEQUENCIA_FORMA_PAGTO_VENDA
                                AND ARC.SEQUENCIA_ARQ_REC_CONVENIO   = :sequencia_arq_rec_convenio
                                AND NFS.SITUACAO_DA_NOTA_FISCAL = 1'''
            banco_dados.Executar(comando_sql, variaveis_bind)
            dados_arquivo = banco_dados.BuscarUm()
            if dados_arquivo != None:
                arquivo_temp_base64 = base64.b64encode(dados_arquivo[2].read())
                xml_envio = xml_anexo
                xml_envio = xml_envio.replace('#USERNAME#', parametros.usuario_fluig)
                xml_envio = xml_envio.replace('#PASSWORD#', parametros.senha_fluig)
                xml_envio = xml_envio.replace('#COMPANY_ID#', dados_arquivo[3])
                xml_envio = xml_envio.replace('#DOCUMENT_FOLDER#', dados_arquivo[4])
                xml_envio = xml_envio.replace('#FILENAME#', os.path.basename(dados_arquivo[1]))
                xml_envio = xml_envio.replace('#FILECONTENT#', arquivo_temp_base64)

                url_webservice = parametros.url_servidor_fluig + '/webdesk/ECMDocumentService?wsdl'
                headers = { 'content-type' : 'text/xml',
                            'SOAPAction'   : 'createSimpleDocument' }
                try:
                    response = requests.post(url_webservice, data=xml_envio, headers=headers)
                    if response.status_code != requests.codes.ok:
                        response.raise_for_status()

                except Exception as e:
                    self.mensagem_retorno = str(e.message)
                    return False

                if response.content:
                    bs = BeautifulSoup(response.content, 'html.parser')
                    if bs.documentid.text == '0':
                        self.mensagem_retorno = bs.webservicemessage.text
                        return False

                    self.company_id = dados_arquivo[3]
                    self.document_id = bs.documentid.text
                    self.document_version = bs.version.text
                    self.document_description = bs.documentdescription.text
                    self.document_folder = dados_arquivo[4]
                    self.mensagem_retorno = bs.webservicemessage.text
                    return True
                else:
                    self.mensagem_retorno = response.content
                    return False
            else:
                self.mensagem_retorno = 'Dados do Arquivo / Cupom nao localizados.'
                return False
        except Exception as e:
            self.mensagem_retorno = str(e.message)
            return False

    def UploadCard(self):
        parametros = Parametros()
        banco_dados.Conectar()
        try:
            variaveis_bind = { 'sequencia_arq_rec_convenio' : self.sequencia_arq_rec_convenio }
            comando_sql = '''SELECT LPAD(ARC.CODIGO_DA_FILIAL, 4, '0')
                                   ,LPAD(NFS.NUMERO_DO_CUPOM_FISCAL, 9, '0')
                                   ,TO_CHAR(NFS.DATA_EFETIVA_DA_VENDA, 'DD/MM/YYYY')
                                   ,TO_CHAR(FPV.VALOR, 'FM999999990D00', 'NLS_NUMERIC_CHARACTERS=.,')
                                   ,LPAD(NVL(FPV.ASSO_MATRICULA_DO_ASSOCIADO, '0'), 16, '0')
                                   ,EC.NOME_DA_ENTIDADE_COMERCIAL
                                   ,PFC.CARD_FOLDER_FLUIG
                                   ,ARC.COMPANY_ID_FLUIG
                                   ,ARC.DOCUMENT_ID_FLUIG
                                   ,ARC.DOCUMENT_VERSION_FLUIG
                                   ,ARC.DOCUMENT_DESCRIPTION_FLUIG
                                   ,ARC.DOCUMENT_FOLDER_FLUIG
                               FROM MTZ_ARQS_RECEITAS_CONVENIOS    ARC
                                   ,MTZ_PARAMETROS_FLUIG_CONVENIOS PFC
                                   ,NOTAS_FISCAIS_DE_SAIDA         NFS
                                   ,FORMAS_DE_PAGAMENTO_DA_VENDA   FPV
                                   ,ENTIDADES_COMERCIAIS           EC
                              WHERE ARC.CODIGO_DA_FILIAL_DO_CONVENIO = PFC.CODIGO_DA_FILIAL_DO_CONVENIO
                                AND ARC.CODIGO_DO_CONVENIO           = PFC.CODIGO_DO_CONVENIO
                                AND ARC.CODIGO_DA_FILIAL             = NFS.CODIGO_DA_FILIAL
                                AND ARC.CODIGO_DO_DOCUMENTO_DE_SAIDA = NFS.CODIGO_DO_DOCUMENTO_DE_SAIDA
                                AND ARC.CODIGO_DA_FILIAL             = FPV.CODIGO_DA_FILIAL
                                AND ARC.CODIGO_DO_DOCUMENTO_DE_SAIDA = FPV.CODIGO_DO_DOCUMENTO_DE_SAIDA
                                AND ARC.CODIGO_DA_FORMA_DE_PAGAMENTO = FPV.CODIGO_DA_FORMA_DE_PAGAMENTO
                                AND ARC.SEQUENCIA_FORMA_PAGTO_VENDA  = FPV.SEQUENCIA_FORMA_PAGTO_VENDA
                                AND FPV.ASSO_CODIGO_DO_ASSOCIADO     = EC.CODIGO_DA_ENTIDADE_COMERCIAL
                                AND ARC.SEQUENCIA_ARQ_REC_CONVENIO   = :sequencia_arq_rec_convenio
                                AND NFS.SITUACAO_DA_NOTA_FISCAL      = 1'''
            banco_dados.Executar(comando_sql, variaveis_bind)
            dados_cupom_fiscal = banco_dados.BuscarUm()
            if dados_cupom_fiscal != None:
                xml_envio = xml_card
                xml_envio = xml_envio.replace('#USERNAME#', parametros.usuario_fluig)
                xml_envio = xml_envio.replace('#PASSWORD#', parametros.senha_fluig)
                xml_envio = xml_envio.replace('#COMPANY_ID#', dados_cupom_fiscal[7])
                xml_envio = xml_envio.replace('#CARD_FOLDER#', dados_cupom_fiscal[6])
                xml_envio = xml_envio.replace('#CODIGO_DA_FILIAL#', dados_cupom_fiscal[0])
                xml_envio = xml_envio.replace('#NUMERO_DO_CUPOM_FISCAL#', dados_cupom_fiscal[1])
                xml_envio = xml_envio.replace('#DATA_EFETIVA_DA_VENDA#', dados_cupom_fiscal[2])
                xml_envio = xml_envio.replace('#VALOR_CONVENIO#', dados_cupom_fiscal[3])
                xml_envio = xml_envio.replace('#MATRICULA_DO_ASSOCIADO#', dados_cupom_fiscal[4])
                xml_envio = xml_envio.replace('#NOME_DO_ASSOCIADO#', dados_cupom_fiscal[5])
                xml_envio = xml_envio.replace('#COMPANY_ID#', dados_cupom_fiscal[7])
                xml_envio = xml_envio.replace('#DOCUMENT_ID#', dados_cupom_fiscal[8])
                xml_envio = xml_envio.replace('#DOCUMENT_VERSION#', dados_cupom_fiscal[9])
                xml_envio = xml_envio.replace('#DOCUMENT_DESCRIPTION#', dados_cupom_fiscal[10])
                xml_envio = xml_envio.replace('#DOCUMENT_FOLDER#', dados_cupom_fiscal[11])

                try:
                    comando_sql = '''SELECT LPAD(I.CODIGO_DO_ITEM, 10, 0)
                                           ,LPAD(NVL(I.CODIGO_DE_BARRAS_FORNECEDOR, NVL(I.CODIGO_DE_BARRAS_DIMED, 0)), 18, '0')
                                           ,I.NOMENCLATURA_VAREJO
                                           ,DECODE(I.CATEGORIA, 'R', 'ETICO','G', 'GENERICO', 'S', 'SIMILAR', 'M', 'MANIPULADO' ,'OUTRO')
                                           ,TO_CHAR(INFS.QUANTIDADE, 'FM999999990D00', 'NLS_NUMERIC_CHARACTERS=.,')
                                           ,TO_CHAR(TRUNC(((NVL(INFS.VALOR_UNITARIO, 0) * NVL(INFS.QUANTIDADE, 0)) - NVL(INFS.VALOR_DO_DESCONTO, 0)  - NVL(INFS.VALOR_DE_REPASSE_PBMS, 0)) / NVL(INFS.QUANTIDADE, 0), 2), 'FM999999990D00', 'NLS_NUMERIC_CHARACTERS=.,')
                                           ,TO_CHAR(INFS.VALOR_TOTAL_DO_ITEM, 'FM999999990D00', 'NLS_NUMERIC_CHARACTERS=.,')
                                           ,LPAD(NVL(R.NUMERO_DE_REGISTRO, '0'), 12, '0')
                                       FROM MTZ_ARQS_RECEITAS_CONVENIOS    ARC
                                           ,MTZ_PARAMETROS_FLUIG_CONVENIOS PFC
                                           ,NOTAS_FISCAIS_DE_SAIDA         NFS
                                           ,ITENS_DA_NOTA_FISCAL_DE_SAIDA  INFS
                                           ,ITENS                          I
                                           ,PHW_ITENS_DAS_RECEITAS         IR
                                           ,PHW_RECEITAS                   R
                                      WHERE ARC.CODIGO_DA_FILIAL_DO_CONVENIO  = PFC.CODIGO_DA_FILIAL_DO_CONVENIO
                                        AND ARC.CODIGO_DO_CONVENIO            = PFC.CODIGO_DO_CONVENIO
                                        AND ARC.CODIGO_DA_FILIAL              = NFS.CODIGO_DA_FILIAL
                                        AND ARC.CODIGO_DO_DOCUMENTO_DE_SAIDA  = NFS.CODIGO_DO_DOCUMENTO_DE_SAIDA
                                        AND NFS.CODIGO_DA_FILIAL              = INFS.CODIGO_DA_FILIAL
                                        AND NFS.CODIGO_DO_DOCUMENTO_DE_SAIDA  = INFS.CODIGO_DO_DOCUMENTO_DE_SAIDA
                                        AND INFS.CODIGO_DO_ITEM               = I.CODIGO_DO_ITEM
                                        AND INFS.CODIGO_DA_FILIAL             = IR.INFS_CODIGO_DA_FILIAL (+)
                                        AND INFS.CODIGO_DO_DOCUMENTO_DE_SAIDA = IR.INFS_CODIGO_DO_DOCUMENTO_SAIDA (+)
                                        AND INFS.NUMERO_DO_ITEM               = IR.INFS_NUMERO_DO_ITEM (+)
                                        AND IR.CODIGO_DA_FILIAL               = R.CODIGO_DA_FILIAL (+)
                                        AND IR.CODIGO_DA_RECEITA              = R.CODIGO_DA_RECEITA (+)
                                        AND ARC.SEQUENCIA_ARQ_REC_CONVENIO    = :sequencia_arq_rec_convenio
                                        AND NFS.SITUACAO_DA_NOTA_FISCAL       = 1
                                        AND INFS.SITUACAO_ITEM_DA_NF          = 1'''
                    banco_dados.Executar(comando_sql, variaveis_bind)
                    dados_itens_cupom_fiscal = banco_dados.BuscarTodos()
                    if len(dados_itens_cupom_fiscal) != 0:
                        xml_envio_itens = ''
                        contador = 0
                        for dados_item_cupom_fiscal in dados_itens_cupom_fiscal:
                            contador += 1
                            xml_envio_itens += xml_card_itens
                            xml_envio_itens = xml_envio_itens.replace('#NUMERO_DO_ITEM#', str(contador))
                            xml_envio_itens = xml_envio_itens.replace('#CODIGO_DO_ITEM#', dados_item_cupom_fiscal[0])
                            xml_envio_itens = xml_envio_itens.replace('#CODIGO_DE_BARRAS#', dados_item_cupom_fiscal[1])
                            xml_envio_itens = xml_envio_itens.replace('#NOMENCLATURA_VAREJO#', dados_item_cupom_fiscal[2])
                            xml_envio_itens = xml_envio_itens.replace('#CATEGORIA#', dados_item_cupom_fiscal[3])
                            xml_envio_itens = xml_envio_itens.replace('#QUANTIDADE#', dados_item_cupom_fiscal[4])
                            xml_envio_itens = xml_envio_itens.replace('#VALOR_UNITARIO#', dados_item_cupom_fiscal[5])
                            xml_envio_itens = xml_envio_itens.replace('#VALOR_TOTAL#', dados_item_cupom_fiscal[6])
                            xml_envio_itens = xml_envio_itens.replace('#NUMERO_DO_REGISTRO_CRM#', dados_item_cupom_fiscal[7])

                        xml_envio = xml_envio.replace('#ITENS_DA_NOTA_FISCAL#', xml_envio_itens)

                        file = open('tmp/teste.xml', 'w')
                        file.write(xml_envio)
                        file.close()

                        url_webservice = parametros.url_servidor_fluig + '/webdesk/ECMCardService?wsdl'
                        headers = { 'content-type' : 'text/xml',
                                    'SOAPAction'   : 'createCard' }
                        try:
                            response = requests.post(url_webservice, data=xml_envio, headers=headers)
                            if response.status_code != requests.codes.ok:
                                response.raise_for_status()

                        except Exception as e:
                            self.mensagem_retorno = str(e.message)
                            return False

                        if response.content:
                            bs = BeautifulSoup(response.content, 'html.parser')
                            if bs.documentid.text == '0':
                                self.mensagem_retorno = bs.webservicemessage.text
                                return False

                            self.card_id = bs.documentid.text
                            self.card_version = bs.version.text
                            self.card_description = bs.documentdescription.text
                            self.card_folder = dados_cupom_fiscal[6]
                            self.mensagem_retorno = bs.webservicemessage.text
                            return True
                        else:
                            self.mensagem_retorno = response.content
                            return False
                    else:
                        self.mensagem_retorno = 'Dados do Arquivo / Itens do Cupom nao localizados.'
                        return False
                except Exception as e:
                    raise
            else:
                self.mensagem_retorno = 'Dados do Arquivo / Cupom nao localizados.'
                return False
        except Exception as e:
            raise
