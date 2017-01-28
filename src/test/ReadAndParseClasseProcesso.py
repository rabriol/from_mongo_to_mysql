# coding=utf-8

__author__ = 'brito'

from pymongo import MongoClient

CLASSE_PROCESSO_TO_DISCARD = '2ºJULG'

if __name__ == '__main__':
    connection = MongoClient('localhost', 27017)
    db = connection['DJs']

    #acordaos = db.all.find({"acordaoId" : "AGR PET 2627"})
    acordaos = db.all.find()

    adiCount = 1
    dic = dict()

    print 'total de documentos encontrados ', db.all.find().count()

    for acordao in acordaos:
        cl_processo = acordao['acordaoId'].encode('utf-8').strip(' ').split(' ')[0]
        if cl_processo is not None:
            if CLASSE_PROCESSO_TO_DISCARD == cl_processo\
                    or 'SEGUNDO' == cl_processo \
                    or 'SEGUNDOS' == cl_processo \
                    or 'EXTENSÃO' == cl_processo \
                    or 'JULGAMENTO' == cl_processo \
                    or 'NONO' == cl_processo \
                    or 'QUARTOS' == cl_processo \
                    or 'TERC' in cl_processo \
                    or 'NONA' == cl_processo \
                    or 'QUART' in cl_processo \
                    or 'EXTN' == cl_processo \
                    or 'TIMOS' in cl_processo \
                    or 'TERCEIROS' == cl_processo \
                    or 'SEGUND' in cl_processo \
                    or 'VIG' in cl_processo \
                    or 'REFERENDO' == cl_processo \
                    or 'PRIMEIROS' == cl_processo \
                    or 'AUTOS' == cl_processo \
                    or 'SEXTOS' == cl_processo \
                    or 'DEN\xc3\x9aNCIA' == cl_processo \
                    or 'D\xc3\x89CIMA' == cl_processo \
                    or 'MC' == cl_processo \
                    or 'QUINTOS' == cl_processo \
                    or 'ANTECIPADA' == cl_processo \
                    or 'SEXTO' == cl_processo:
                cl_processo = acordao['acordaoId'].encode('utf-8').strip(' ').split(' ')[1]

            if 'SEGUNDO' == cl_processo\
                    or 'VIG\xc3\x89SIMO' == cl_processo \
                    or 'D\xc3\x89CIMO' == cl_processo \
                    or 'DILIG\xc3\x8aNCIA' == cl_processo \
                    or 'VIG\xc3\x89SIMOS' == cl_processo \
                    or 'D\xc3\x89CIMOS' == cl_processo \
                    or 'EXTENS\xc3\x83O' == cl_processo \
                    or 'S\xc3\x89TIMA' == cl_processo \
                    or 'RECONSTITUI\xc3\x87\xc3\x83O' == cl_processo \
                    or 'S\xc3\x89TIMO' == cl_processo \
                    or 'EXTENS\xc3\x83O' == cl_processo \
                    or 'OITAVO' == cl_processo \
                    or 'TUTELA' == cl_processo \
                    or 'ANTECIPADA' == cl_processo \
                    or 'SEXTA' == cl_processo \
                    or 'QUINTO' == cl_processo \
                    or 'SEGUNDA' == cl_processo:
                cl_processo = acordao['acordaoId'].encode('utf-8').strip(' ').split(' ')[2]
        else:
            continue

        if cl_processo == 'ADI':
            adiCount +=1

        if cl_processo not in dic:
            dic[cl_processo] = 1
        else:
            dic[cl_processo] += 1

        print cl_processo

    print adiCount
    print dic