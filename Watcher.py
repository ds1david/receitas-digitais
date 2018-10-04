# -*- coding: utf-8 -*-
import cv2
import numpy as np
import os
import PyPDF2
import re
import time
import urllib
import zbar
from mimetypes import MimeTypes
from wand.image import Image
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from PIL import Image as PI
from Emails import *
from ValidadorCodigoBarras import *
from Logger import *

logger = Logger(__name__)


patterns = ['*.pdf']
diretorio_ftp = 'ftp/'
diretorio_temp = 'tmp/'


class WatcherHandler(PatternMatchingEventHandler):

    def EscanearArquivo(self, event):
        processo = ValidadorCodigoBarras()
        scanner = zbar.Scanner()
        try:
            codigos_barras_arquivo = []
            temp_url = urllib.pathname2url(event.src_path)
            mimetypes = MimeTypes()
            mimetype_file = mimetypes.guess_type(temp_url)
            del(mimetypes)
            if mimetype_file[0] != 'application/pdf':
                EnviarEmailERRO('Formato de arquivo recebido nao reconhecido ou nao aceito. Arquivo: ' + event.src_path + ' - Mimetype: ' + mimetype_file[0])
                return

            arquivo_ftp = event.src_path

            arquivo_pdf_temp = re.sub('(?i)'+re.escape(diretorio_ftp), lambda m: diretorio_temp, arquivo_ftp)
            pdfWriter = PyPDF2.PdfFileWriter()
            pdfFileObj = open(arquivo_ftp, 'rb')
            pdfReader = PyPDF2.PdfFileReader(pdfFileObj)

            pageObj = pdfReader.getPage(0)
            pdfWriter.addPage(pageObj)

            with open(arquivo_pdf_temp, 'wb') as output:
                pdfWriter.write(output)

            pdfFileObj.close()

            del(pdfWriter)
            del(pdfFileObj)
            del(pdfReader)

            arquivo_imagem = re.sub('(?i)'+re.escape('.pdf'), lambda m: '.jpg', arquivo_pdf_temp)

            imagem_temp = Image(filename=arquivo_pdf_temp, resolution=(400,400))
            imagem_temp = imagem_temp.convert('jpeg')
            imagem_temp.save(filename=arquivo_imagem)
            del(imagem_temp)

            processo.nome_arquivo_pdf = arquivo_ftp
            processo.nome_arquivo_thumbnail = arquivo_imagem

            # colorido
            #print(arquivo_ftp + ' - colorido - sem efeito')
            codigos_barras = []
            imagem_temp = PI.open(arquivo_imagem).convert('L')
            imagem_array_temp = np.array(imagem_temp)
            del(imagem_temp)
            codigo_barras_temp = scanner.scan(imagem_array_temp)
            del(imagem_array_temp)
            for codigo_barras in codigo_barras_temp:
                if codigo_barras.type == 'QR-Code':
                    continue
                codigos_barras.append(codigo_barras.data)
            if len(codigos_barras) > 0:
                for codigo_barras in codigos_barras:
                    processo.codigo_barras = codigo_barras
                    if processo.Processar(True):
                        logger.info(arquivo_ftp + ' - colorido - sem efeito - ' + codigo_barras)
                        os.remove(arquivo_ftp)
                        os.remove(arquivo_pdf_temp)
                        os.remove(arquivo_imagem)
                        return
                    else:
                        if codigo_barras not in codigos_barras_arquivo:
                            codigos_barras_arquivo.append(codigo_barras)

            #print(arquivo_ftp + ' - colorido - gaussian blur')
            codigos_barras = []
            imagem_temp = cv2.imread(arquivo_imagem)
            imagem_temp = cv2.GaussianBlur(imagem_temp, (5, 5), 0)
            imagem_temp = PI.fromarray(imagem_temp).convert('L')
            imagem_temp = np.array(imagem_temp)
            codigo_barras_temp = scanner.scan(imagem_temp)
            for codigo_barras in codigo_barras_temp:
                if codigo_barras.type == 'QR-Code':
                    continue
                codigos_barras.append(codigo_barras.data)
            del(imagem_temp)
            if len(codigos_barras) > 0:
                for codigo_barras in codigos_barras:
                    processo.codigo_barras = codigo_barras
                    if processo.Processar(True):
                        logger.info(arquivo_ftp + ' - colorido - gaussian blur - ' + codigo_barras)
                        os.remove(arquivo_ftp)
                        os.remove(arquivo_pdf_temp)
                        os.remove(arquivo_imagem)
                        return
                    else:
                        if codigo_barras not in codigos_barras_arquivo:
                            codigos_barras_arquivo.append(codigo_barras)

            #print(arquivo_ftp + ' - colorido - median blur')
            codigos_barras = []
            imagem_temp = cv2.imread(arquivo_imagem)
            imagem_temp = cv2.medianBlur(imagem_temp, 5)
            imagem_temp = PI.fromarray(imagem_temp).convert('L')
            imagem_temp = np.array(imagem_temp)
            codigo_barras_temp = scanner.scan(imagem_temp)
            for codigo_barras in codigo_barras_temp:
                if codigo_barras.type == 'QR-Code':
                    continue
                codigos_barras.append(codigo_barras.data)
            del(imagem_temp)
            if len(codigos_barras) > 0:
                for codigo_barras in codigos_barras:
                    processo.codigo_barras = codigo_barras
                    if processo.Processar(True):
                        logger.info(arquivo_ftp + ' - colorido - median blur - ' + codigo_barras)
                        os.remove(arquivo_ftp)
                        os.remove(arquivo_pdf_temp)
                        os.remove(arquivo_imagem)
                        return
                    else:
                        if codigo_barras not in codigos_barras_arquivo:
                            codigos_barras_arquivo.append(codigo_barras)

            for angle in range(5, 90, 5):
                #print(arquivo_ftp + ' - colorido - angulo - ' + str(angle))
                codigos_barras = []
                imagem_temp = cv2.imread(arquivo_imagem)
                (oldY,oldX) = imagem_temp.shape[:-1]
                rotation_matrix = cv2.getRotationMatrix2D((oldX/2,oldY/2),angle,1)
                (newX,newY) = (oldX*1,oldY*1)
                r = np.deg2rad(angle)
                (newX,newY) = (abs(np.sin(r)*newY)+abs(np.cos(r)*newX),abs(np.sin(r)*newX)+abs(np.cos(r)*newY))
                (tx,ty) = ((newX-oldX)/2,(newY-oldY)/2)
                rotation_matrix[0,2] += tx
                rotation_matrix[1,2] += ty
                imagem_temp = cv2.warpAffine(imagem_temp, rotation_matrix, dsize=(int(newX),int(newY)), flags=cv2.INTER_LINEAR)
                imagem_temp = PI.fromarray(imagem_temp).convert('L')
                imagem_temp = np.array(imagem_temp)
                codigo_barras_temp = scanner.scan(imagem_temp)
                for codigo_barras in codigo_barras_temp:
                    if codigo_barras.type == 'QR-Code':
                        continue
                    codigos_barras.append(codigo_barras.data)
                del(imagem_temp)
                if len(codigos_barras) > 0:
                    for codigo_barras in codigos_barras:
                        processo.codigo_barras = codigo_barras
                        if processo.Processar(True):
                            logger.info(arquivo_ftp + ' - colorido - angulo - ' + angle + ' - ' + codigo_barras)
                            os.remove(arquivo_ftp)
                            os.remove(arquivo_pdf_temp)
                            os.remove(arquivo_imagem)
                            return
                        else:
                            if codigo_barras not in codigos_barras_arquivo:
                                codigos_barras_arquivo.append(codigo_barras)

            # preto e branco
            #print(arquivo_ftp + ' - preto e branco - sem efeito')
            codigos_barras = []
            imagem_temp = cv2.imread(arquivo_imagem)
            imagem_temp = cv2.cvtColor(imagem_temp, cv2.COLOR_RGB2GRAY)
            cv2.imwrite(arquivo_imagem, imagem_temp)
            imagem_temp = PI.open(arquivo_imagem).convert('L')
            imagem_temp = np.array(imagem_temp)
            codigo_barras_temp = scanner.scan(imagem_temp)
            for codigo_barras in codigo_barras_temp:
                if codigo_barras.type == 'QR-Code':
                    continue
                codigos_barras.append(codigo_barras.data)
            del(imagem_temp)
            if len(codigos_barras) > 0:
                for codigo_barras in codigos_barras:
                    processo.codigo_barras = codigo_barras
                    if processo.Processar(True):
                        logger.info(arquivo_ftp + ' - preto e branco - sem efeito - ' + codigo_barras)
                        os.remove(arquivo_ftp)
                        os.remove(arquivo_pdf_temp)
                        os.remove(arquivo_imagem)
                        return
                    else:
                        if codigo_barras not in codigos_barras_arquivo:
                            codigos_barras_arquivo.append(codigo_barras)

            #print(arquivo_ftp + ' - preto e branco - gaussian blur')
            codigos_barras = []
            imagem_temp = cv2.imread(arquivo_imagem)
            imagem_temp = cv2.GaussianBlur(imagem_temp, (5, 5), 0)
            imagem_temp = PI.fromarray(imagem_temp).convert('L')
            imagem_temp = np.array(imagem_temp)
            codigo_barras_temp = scanner.scan(imagem_temp)
            for codigo_barras in codigo_barras_temp:
                if codigo_barras.type == 'QR-Code':
                    continue
                codigos_barras.append(codigo_barras.data)
            del(imagem_temp)
            if len(codigos_barras) > 0:
                for codigo_barras in codigos_barras:
                    processo.codigo_barras = codigo_barras
                    if processo.Processar(True):
                        logger.info(arquivo_ftp + ' - preto e branco - gaussian blur - ' + codigo_barras)
                        os.remove(arquivo_ftp)
                        os.remove(arquivo_pdf_temp)
                        os.remove(arquivo_imagem)
                        return
                    else:
                        if codigo_barras not in codigos_barras_arquivo:
                            codigos_barras_arquivo.append(codigo_barras)

            #print(arquivo_ftp + ' - preto e branco - median blur')
            codigos_barras = []
            imagem_temp = cv2.imread(arquivo_imagem)
            imagem_temp = cv2.medianBlur(imagem_temp, 5)
            imagem_temp = PI.fromarray(imagem_temp).convert('L')
            imagem_temp = np.array(imagem_temp)
            codigo_barras_temp = scanner.scan(imagem_temp)
            for codigo_barras in codigo_barras_temp:
                if codigo_barras.type == 'QR-Code':
                    continue
                codigos_barras.append(codigo_barras.data)
            del(imagem_temp)
            if len(codigos_barras) > 0:
                for codigo_barras in codigos_barras:
                    processo.codigo_barras = codigo_barras
                    if processo.Processar(True):
                        logger.info(arquivo_ftp + ' - preto e branco - median blur - ' + codigo_barras)
                        os.remove(arquivo_ftp)
                        os.remove(arquivo_pdf_temp)
                        os.remove(arquivo_imagem)
                        return
                    else:
                        if codigo_barras not in codigos_barras_arquivo:
                            codigos_barras_arquivo.append(codigo_barras)

            for angle in range(5, 90, 5):
                #print(arquivo_ftp + ' - preto e branco - angulo - ' + str(angle))
                codigos_barras = []
                imagem_temp = cv2.imread(arquivo_imagem)
                (oldY,oldX) = imagem_temp.shape[:-1]
                rotation_matrix = cv2.getRotationMatrix2D((oldX/2,oldY/2),angle,1)
                (newX,newY) = (oldX*1,oldY*1)
                r = np.deg2rad(angle)
                (newX,newY) = (abs(np.sin(r)*newY)+abs(np.cos(r)*newX),abs(np.sin(r)*newX)+abs(np.cos(r)*newY))
                (tx,ty) = ((newX-oldX)/2,(newY-oldY)/2)
                rotation_matrix[0,2] += tx
                rotation_matrix[1,2] += ty
                imagem_temp = cv2.warpAffine(imagem_temp, rotation_matrix, dsize=(int(newX),int(newY)), flags=cv2.INTER_LINEAR)
                imagem_temp = PI.fromarray(imagem_temp).convert('L')
                imagem_temp = np.array(imagem_temp)
                codigo_barras_temp = scanner.scan(imagem_temp)
                for codigo_barras in codigo_barras_temp:
                    if codigo_barras.type == 'QR-Code':
                        continue
                    codigos_barras.append(codigo_barras.data)
                del(imagem_temp)
                if len(codigos_barras) > 0:
                    for codigo_barras in codigos_barras:
                        processo.codigo_barras = codigo_barras
                        if processo.Processar(True):
                            logger.info(arquivo_ftp + ' - preto e branco - angulo - ' + angle + ' - ' + codigo_barras)
                            os.remove(arquivo_ftp)
                            os.remove(arquivo_pdf_temp)
                            os.remove(arquivo_imagem)
                            return
                        else:
                            if codigo_barras not in codigos_barras_arquivo:
                                codigos_barras_arquivo.append(codigo_barras)

            imagem_temp = Image(filename=arquivo_pdf_temp, resolution=(400,400))
            imagem_temp.convert('jpeg')
            imagem_temp.save(filename=arquivo_imagem)
            del(imagem_temp)

            processo.codigo_barras = '#-#'.join(str(codigo_barras) for codigo_barras in codigos_barras_arquivo)
            if processo.Processar(False):
                logger.info(arquivo_ftp + ' - ultimo recurso - ' + processo.codigo_barras)
                os.remove(arquivo_ftp)
                os.remove(arquivo_pdf_temp)
                os.remove(arquivo_imagem)
                return
        finally:
            if isinstance(scanner,zbar.Scanner):
                del(scanner)
            if isinstance(processo,ValidadorCodigoBarras):
                del(processo)

    def on_created(self, event):
        try:
            tamanho_arquivo = -1
            while True:
                if tamanho_arquivo == os.path.getsize(event.src_path):
                    break

                tamanho_arquivo = os.path.getsize(event.src_path)
                time.sleep(5)

            self.EscanearArquivo(event)

        except Exception as e:
            logger.error(e, exc_info=True)
            EnviarEmailERRO(traceback.format_exc())

if __name__ == '__main__':
    observer = Observer()
    observer.schedule(WatcherHandler(patterns=patterns, case_sensitive=False), path=diretorio_ftp)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
