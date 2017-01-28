# coding=utf-8

__author__ = 'brito'

from pymongo import MongoClient
import re

if __name__ == '__main__':
    connection = MongoClient('localhost', 27017)
    db = connection['DJs']

    #acordao = db.all.find_one({"acordaoId" : "HC 91207"})
    acordaos = db.all.find()

    for acordao in acordaos:
        cabecalho = acordao['cabecalho'].encode('utf-8')

        pattern = re.compile("p/ Acórdão:&nbsp Min.")
        stIndex = -1

        for p in pattern.finditer(cabecalho):
            #print p.start(), p.group()
            stIndex = p.end()

        if (stIndex > -1):
            fromRelatorName = cabecalho[stIndex:]
            relatorName = ''

            for word in fromRelatorName.strip(' ').split(' '):
                if word.isupper() and '(' not in word:
                    relatorName += ' ' + word
                else:
                    break;

            print relatorName