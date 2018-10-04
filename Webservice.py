# -*- coding: utf-8 -*-
import cherrypy
import bottle
from cherrypy.wsgiserver import *
from ValidadorCodigoBarras import *
from Logger import *

logger = Logger(__name__)


app = application = bottle.default_app()

@bottle.route('/receitas_convenios/integrar_arquivo/<sequencia_arq_rec_convenio>', method='POST')
def IntegrarArquivo(sequencia_arq_rec_convenio=None):
    if sequencia_arq_rec_convenio == None:
        return { 'Status'   : False,
                 'Tipo'     : 'Exceção',
                 'Mensagem' : 'Chamada sem sequencia de arquivo para processamento!'}

    else:
        arquivo = ValidadorCodigoBarras();
        try:
            if arquivo.Selecionar(sequencia_arq_rec_convenio) == False:
                return { 'Status'   : False,
                         'Tipo'     : 'Processo',
                         'Mensagem' : 'Arquivo com a sequência ' + str(sequencia_arq_rec_convenio) + ' não localizado!'}

        except Exception as e:
            logger.error(e, exc_info=True)
            return { 'Status'   : False,
                     'Tipo'     : 'Erro',
                     'Mensagem' : str(e).decode('iso-8859-1')}

        else:
            try:
                if arquivo.Integrar(sequencia_arq_rec_convenio) == False:
                    return { 'Status'   : False,
                             'Tipo'     : 'Processo',
                             'Mensagem' : 'Erro ao integrar ao Fluig o arquivo com a sequência ' + str(sequencia_arq_rec_convenio)}

            except Exception as e:
                logger.error(e, exc_info=True)
                return { 'Status'   : False,
                         'Tipo'     : 'Erro',
                         'Mensagem' : str(e).decode('iso-8859-1')}

            else:
                try:
                    if arquivo.Conciliar(sequencia_arq_rec_convenio) == False:
                        return { 'Status'   : False,
                                 'Tipo'     : 'Processo',
                                 'Mensagem' : 'Erro ao conciliar a venda do arquivo com a sequência ' + str(sequencia_arq_rec_convenio)}

                except Exception as e:
                    logger.error(e, exc_info=True)
                    return { 'Status'   : False,
                             'Tipo'     : 'Erro',
                             'Mensagem' : str(e).decode('iso-8859-1')}

                else:
                    return { 'Status'  : True,
                             'Tipo'    : 'Processo',
                             'Mensagem': 'Arquivo sequencia ' + str(sequencia_arq_rec_convenio) + ' reprocessado com sucesso!'}


if __name__ == '__main__':
    server = CherryPyWSGIServer(('0.0.0.0', 8081), app, numthreads=30)
    try:
        server.start()

    except KeyboardInterrupt:
        server.stop()
