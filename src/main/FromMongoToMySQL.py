# coding=utf-8

__author__ = 'brito'

from pymongo import MongoClient
import MySQLdb
from string import replace

import re
import warnings

errors = dict()

CLASSE_PROCESSO_TO_DISCARD = '2ºJULG'

#classes_processos = ['ADI', 'ADO', 'ADC', 'ADPF', 'Rcl', 'PSV', 'AR', 'AC', 'HC', 'MS', 'MI', 'SL', 'SS', 'STA']

def insert_tables_on_mysql(document, decisao_trained):
    db = MySQLdb.connect('localhost', 'root', '', 'acordaos_v1')

    cursor = db.cursor()

    # warnings.filterwarnings('ignore', category = MySQLdb.Warning)

    try:
        check_acordao = ("SELECT ID FROM ACORDAO WHERE ID = '%s';") % document['acordaoId'].encode('utf-8')
        cursor.execute(check_acordao)
        acordao_result = cursor.fetchone()

        # print 'Resultado da consulta %s' % str(acordao_total)

        if acordao_result == None:
            acordao_id = clean_acordao_id(document['acordaoId'].encode('utf-8'))
            local = document['local'].encode('utf-8')
            tribunal = document['tribunal'].encode('utf-8')
            relator = document['relator'].encode('utf-8')
            relator_para_acordao = get_relator_para_acordao_name(document['cabecalho'])
            classe_processo_id = get_or_insert_classe_processo(cursor, document)
            decisao_trained = get_decisao_classificada(decisao_trained)
            orgao_julgador = document['orgaoJulg'].encode('utf-8')
            doutrinas = document['doutrinas'].encode('utf-8')

            publicacao = clean_string(document['publicacao'])
            decisao = clean_string(document['decisao'])
            partesTexto = clean_string(document['partesTexto'])
            similaresTexto = clean_string(document['similaresTexto'])
            ementa = clean_string(document['ementa'])

            legislacaoTexto = replace(document['legislacaoTexto'].encode('utf-8'), '\n', '\\n')
            legislacaoTexto = replace(legislacaoTexto, '\'', '\\\'')

            decisao_id = None
            try:
                insert_citacoes = ("""INSERT INTO DECISAO(DESCRICAO, CLASSIFICACAO) VALUES ('%s', '%s');""") % \
                              (decisao, decisao_trained)
                cursor.execute(insert_citacoes)
                decisao_id = cursor.lastrowid
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            insert_acordao = ("""INSERT INTO ACORDAO(ID, LOCAL, TRIBUNAL,
                            PUBLICACAO, PARTES_TEXTO, SIMILARES_TEXTO, EMENTA, ORGAO_JULGADOR,
                            LEGISLACAO_TEXTO, DOUTRINAS, RELATOR, ID_CLASSE_PROCESSO, RELATOR_PARA_ACORDAO, ID_DECISAO)
                            VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', %s);
                            """) % (acordao_id,
                                    local,
                                    tribunal,
                                    publicacao,
                                    partesTexto,
                                    similaresTexto,
                                    ementa,
                                    orgao_julgador,
                                    legislacaoTexto,
                                    doutrinas,
                                    relator,
                                    classe_processo_id,
                                    relator_para_acordao,
                                    decisao_id)
            cursor.execute(insert_acordao)

            try:
                for acordao_citado in document['citacoes']:
                    insert_citacoes = ("""INSERT INTO CITACAO(ID_ACORDAO, ID_ACORDAO_CITADO) VALUES ('%s', '%s');""") % \
                                  (acordao_id, acordao_citado)
                    cursor.execute(insert_citacoes)
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                for acordao_similar in document['similares']:
                    insert_citacoes = ("""INSERT INTO SIMILAR(ID_ACORDAO, ID_ACORDAO_SIMILAR) VALUES ('%s', '%s');""") % \
                                  (acordao_id, acordao_similar['acordaoId'])
                    cursor.execute(insert_citacoes)
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(acordao_id, document['partes']['intimado'], cursor, 'intimado', 'partes')
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(acordao_id, document['partes']['agravante'], cursor, 'agravante', 'partes')
            except Exception,e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(acordao_id, document['partes']['agravado'], cursor, 'agravado', 'partes')
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(acordao_id, document['partes']['advogado'], cursor, 'advogado', 'partes')
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(acordao_id, document['partes']['procurador'], cursor, 'procurador', 'partes')
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(acordao_id, document['tags'], cursor, 'tag', 'acordao')
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_legislacao(cursor, document)
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            db.commit()
    except Exception, e:
        error = str(e)

        if error in errors:
            errors[error] = errors[error] + 1
        else:
            errors[error] = 1

        print error

        db.rollback()

    db.close()


def get_decisao_classificada(decisao_trained):
    if decisao_trained is None:
        return ''
    return decisao_trained['classificao'].encode('utf-8')


def get_relator_para_acordao_name(cabecalho):
    cabecalho = cabecalho.encode('utf-8')

    pattern = re.compile("p/ Acórdão:&nbsp Min.")
    stIndex = -1

    for p in pattern.finditer(cabecalho):
        # print p.start(), p.group()
        stIndex = p.end()

    if (stIndex > -1):
        fromRelatorName = cabecalho[stIndex:]
        relatorName = ''

        for word in fromRelatorName.strip(' ').split(' '):
            if word.isupper() and '(' not in word:
                relatorName += ' ' + word
            else:
                break

        print relatorName
    else:
        return ''

def clean_acordao_id(acordao_id):
    word = replace(acordao_id, '2ºJULG', '')
    word = replace(word.strip(' '), ' ', '_')
    return word

def clean_string(word):
    word = replace(word.encode('utf-8'), '\n', '\\n')
    word = replace(word, '\'', '\\\'')
    word = replace(word, '  ', '')
    return word


def get_classe_processo(acordao_id):
    cl_processo = acordao_id.encode('utf-8').strip(' ').split(' ')[0]
    if cl_processo is not None:
        if CLASSE_PROCESSO_TO_DISCARD == cl_processo \
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

        if 'SEGUNDO' == cl_processo \
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

    return cl_processo


# def get_or_insert_relator(cursor, document):
#     relator_id = None;
#     try:
#         check_relator = ("SELECT ID FROM RELATOR WHERE NOME = '%s';") % document['relator'].encode('utf-8')
#         cursor.execute(check_relator)
#         relator_result = cursor.fetchone()
#
#         if relator_result == None:
#             insert_relator = ("""INSERT INTO RELATOR(NOME) VALUES ('%s');""") % \
#                              (document['relator'].encode('utf-8'))
#             cursor.execute(insert_relator)
#
#         check_relator = ("SELECT ID FROM RELATOR WHERE NOME = '%s';") % document['relator'].encode('utf-8')
#         cursor.execute(check_relator)
#         relator_result = cursor.fetchone()
#
#         relator_id = relator_result[0]
#
#     except Exception, e:
#         error = str(e)
#
#         if error in errors:
#             errors[error] = errors[error] + 1
#         else:
#             errors[error] = 1
#
#     return relator_id

# def get_or_insert_relator_para_acordao(cursor, document):
#     relator_para_acordao_name = get_relator_para_acordao_name(document['cabecalho'])
#
#     if relator_para_acordao_name is None:
#         return None
#
#         relator_para_acordao_id = None;
#     try:
#         check_relator_para_acordao = ("SELECT ID FROM RELATOR_PARA_ACORDAO WHERE NOME = '%s';") % relator_para_acordao_name.encode('utf-8')
#         cursor.execute(check_relator_para_acordao)
#         relator_para_acordao_result = cursor.fetchone()
#
#         if relator_para_acordao_result == None:
#             insert_relator_para_acordao = ("""INSERT INTO RELATOR_PARA_ACORDAO(NOME) VALUES ('%s');""") % relator_para_acordao_name.encode('utf-8')
#             cursor.execute(insert_relator_para_acordao)
#
#         check_relator_para_acordao = ("SELECT ID FROM RELATOR_PARA_ACORDAO WHERE NOME = '%s';") % relator_para_acordao_name.encode('utf-8')
#         cursor.execute(check_relator_para_acordao)
#         relator_para_acordao_result = cursor.fetchone()
#
#         relator_para_acordao_id = relator_para_acordao_result[0]
#
#     except Exception, e:
#         error = str(e)
#
#         if error in errors:
#             errors[error] = errors[error] + 1
#         else:
#             errors[error] = 1
#
#     return relator_para_acordao_id

def get_or_insert_classe_processo(cursor, document):
    classe_processo_id = None;
    try:
        check_classe_processo = ("SELECT ID FROM CLASSE_PROCESSO WHERE NOME = '%s';") % \
                                get_classe_processo(document['acordaoId']);
        cursor.execute(check_classe_processo)
        classe_processo_result = cursor.fetchone()

        if classe_processo_result == None:
            insert_classe_processo = ("""INSERT INTO CLASSE_PROCESSO(NOME) VALUES ('%s');""") % \
                                     (get_classe_processo(document['acordaoId']))
            cursor.execute(insert_classe_processo)

        check_classe_processo = ("SELECT ID FROM CLASSE_PROCESSO WHERE NOME = '%s';") % get_classe_processo(
            document['acordaoId'])
        cursor.execute(check_classe_processo)
        classe_processo_result = cursor.fetchone()

        classe_processo_id = classe_processo_result[0]

    except Exception, e:
        error = str(e)

        if error in errors:
            errors[error] = errors[error] + 1
        else:
            errors[error] = 1
    return classe_processo_id


def insert_legislacao(cursor, document):
    try:
        for legislacao in document['legislacao']:
            if (legislacao != {} and legislacao['sigla'] != '' and legislacao['sigla'] != None \
                and legislacao['tipo'] != '' and legislacao['tipo'] != None \
                and legislacao['descricao'] != '' and legislacao['descricao'] != None):

                check_legislacao = ("SELECT ID FROM LEGISLACAO WHERE ID = '%s' AND TIPO = '%s';") \
                                   % (legislacao['sigla'], legislacao['tipo'])
                cursor.execute(check_legislacao)
                legislacao_result = cursor.fetchone()

                if legislacao_result == None:
                    insert_legislacao = ("""INSERT INTO LEGISLACAO(ID, TIPO, DESCRICAO, ANO) VALUES ('%s', '%s', '%s', '%s');""") % \
                                  (legislacao['sigla'], legislacao['tipo'], legislacao['descricao'], legislacao['ano'])
                    cursor.execute(insert_legislacao)

                check_legislacao = ("SELECT ID FROM LEGISLACAO WHERE ID = '%s' AND TIPO = '%s';") \
                                   % (legislacao['sigla'], legislacao['tipo'])
                cursor.execute(check_legislacao)
                legislacao_result = cursor.fetchone()

                try:
                    check_acordao_legislacao = ("SELECT ID_ACORDAO FROM ACORDAO_LEGISLACAO WHERE ID_ACORDAO = '%s' AND ID_LEGISLACAO = '%s';") \
                                   % (document['acordaoId'], legislacao_result[0])
                    cursor.execute(check_acordao_legislacao)
                    acordao_legislacao_result = cursor.fetchone()

                    if legislacao_result != None and acordao_legislacao_result == None:
                        insert_partes = ("INSERT INTO ACORDAO_LEGISLACAO(ID_ACORDAO, ID_LEGISLACAO) VALUES('%s', '%s');") \
                                        % (document['acordaoId'], legislacao_result[0])
                        cursor.execute(insert_partes)
                except Exception, e:
                    pass

    except Exception, e:
        print str(e)

        error = str(e)

        if error in errors:
            errors[error] = errors[error] + 1
        else:
            errors[error] = 1

def insert_related_tables(acordaoId, document, cursor, table_name, table_prefix_name):
    try:
        for partes in document:
            check_partes = ("SELECT ID FROM " + table_name.upper() + " WHERE NOME = '%s';") % partes.encode('utf-8')
            cursor.execute(check_partes)

            partes_result = cursor.fetchone()

            if partes_result == None:
                insert_partes = ("INSERT INTO " + table_name.upper() + "(NOME) VALUES ('%s');") % (partes.encode('utf-8'))
                cursor.execute(insert_partes)

            check_partes = ("SELECT ID FROM " + table_name.upper() + " WHERE NOME = '%s';") % partes.encode('utf-8')
            cursor.execute(check_partes)

            result = cursor.fetchone()
            partes_id = result[0]

            check_partes = ("SELECT ID_ACORDAO, ID_" + table_name.upper() + " FROM " + table_prefix_name.upper() + "_"
                                    + table_name.upper() + " WHERE ID_ACORDAO = '%s' AND ID_" + table_name.upper()
                                    + " = %s;") % (acordaoId.encode('utf-8'), int(partes_id))

            cursor.execute(check_partes)

            result = cursor.fetchone()

            if result == None:
                insert_partes = ("INSERT INTO " + table_prefix_name.upper() + "_" + table_name.upper()
                                 + " (ID_ACORDAO, ID_" + table_name.upper() + ") VALUES('%s', %s);") \
                                % (acordaoId.encode('utf-8'), int(partes_id))
                cursor.execute(insert_partes)

    except Exception, e:
        print str(e)

        error = str(e)

        if error in errors:
            errors[error] = errors[error] + 1
        else:
            errors[error] = 1

def create_mysql_tables():
    db = MySQLdb.connect('localhost', 'root', '', 'acordaos_v1')

    cursor = db.cursor()

    # warnings.filterwarnings('ignore', category = MySQLdb.Warning)

    try:
        cursor.execute('DROP TABLE IF EXISTS CITACAO;')
        cursor.execute('DROP TABLE IF EXISTS SIMILAR;')
        cursor.execute('DROP TABLE IF EXISTS ACORDAO_TAG;')
        cursor.execute('DROP TABLE IF EXISTS ACORDAO_LEGISLACAO;')
        cursor.execute('DROP TABLE IF EXISTS PARTES_PROCURADOR;')
        cursor.execute('DROP TABLE IF EXISTS PARTES_INTIMADO;')
        cursor.execute('DROP TABLE IF EXISTS PARTES_ADVOGADO;')
        cursor.execute('DROP TABLE IF EXISTS PARTES_AGRAVANTE;')
        cursor.execute('DROP TABLE IF EXISTS PARTES_AGRAVADO;')
        cursor.execute("DROP TABLE IF EXISTS PROCURADOR;")
        cursor.execute("DROP TABLE IF EXISTS INTIMADO;")
        cursor.execute("DROP TABLE IF EXISTS ADVOGADO;")
        cursor.execute("DROP TABLE IF EXISTS AGRAVANTE;")
        cursor.execute("DROP TABLE IF EXISTS AGRAVADO;")
        cursor.execute("DROP TABLE IF EXISTS TAG;")
        cursor.execute("DROP TABLE IF EXISTS LEGISLACAO;")
        cursor.execute("DROP TABLE IF EXISTS ACORDAO;")
        cursor.execute("DROP TABLE IF EXISTS RELATOR;")
        cursor.execute("DROP TABLE IF EXISTS CLASSE_PROCESSO;")
        cursor.execute("DROP TABLE IF EXISTS RELATOR_PARA_ACORDAO;")
        cursor.execute('DROP TABLE IF EXISTS DECISAO;')

        # relator = """CREATE TABLE RELATOR (
        #       ID INT NOT NULL AUTO_INCREMENT,
        #       NOME VARCHAR(50) NOT NULL,
        #       PRIMARY KEY(ID)
        #       ) ENGINE=InnoDB"""
        # cursor.execute(relator)
        # print 'TABELA RELATOR CRIADA COM SUCESSO'

        classe_processo = """CREATE TABLE CLASSE_PROCESSO (
                      ID INT NOT NULL AUTO_INCREMENT,
                      NOME VARCHAR(10) NOT NULL,
                      PRIMARY KEY(ID)
                      ) ENGINE=InnoDB"""
        cursor.execute(classe_processo)
        print 'TABELA CLASSE_PROCESSO CRIADA COM SUCESSO'

        # relator_para_acordao = """CREATE TABLE RELATOR_PARA_ACORDAO (
        #                       ID INT NOT NULL AUTO_INCREMENT,
        #                       NOME VARCHAR(50) NOT NULL,
        #                       PRIMARY KEY(ID)
        #                       ) ENGINE=InnoDB"""
        # cursor.execute(relator_para_acordao)
        # print 'TABELA RELATOR_PARA_ACORDAO CRIADA COM SUCESSO'

        classe_processo = """CREATE TABLE DECISAO (
                              ID INT NOT NULL AUTO_INCREMENT,
                              DESCRICAO VARCHAR(5000) NOT NULL,
                              CLASSIFICACAO VARCHAR(4),
                              PRIMARY KEY(ID)
                              ) ENGINE=InnoDB"""
        cursor.execute(classe_processo)
        print 'TABELA DECISAO CRIADA COM SUCESSO'

        acordao = """CREATE TABLE ACORDAO (
                  ID VARCHAR(50) NOT NULL,
                  DAT_JULGAMENTO DATE,
                  LOCAL VARCHAR(50),
                  TRIBUNAL VARCHAR(150),
                  PUBLICACAO VARCHAR(1000),
                  RELATOR VARCHAR(40),
                  PARTES_TEXTO VARCHAR(2000),
                  SIMILARES_TEXTO VARCHAR(7920),
                  EMENTA VARCHAR(6000),
                  ORGAO_JULGADOR VARCHAR(50),
                  LEGISLACAO_TEXTO VARCHAR(2000),
                  DOUTRINAS VARCHAR(300),
                  ID_CLASSE_PROCESSO INT,
                  RELATOR_PARA_ACORDAO VARCHAR(40),
                  ID_DECISAO INT,
                  PRIMARY KEY (ID),
                  FOREIGN KEY CLASSE_PROCESSO_FK(ID_CLASSE_PROCESSO) REFERENCES CLASSE_PROCESSO(ID),
                  FOREIGN KEY ID_DECISAO_FK(ID_DECISAO) REFERENCES DECISAO(ID)
                  ) ENGINE=InnoDB;"""
        cursor.execute(acordao)
        print 'TABELA ACORDAO CRIADA COM SUCESSO'

        legislacao = """CREATE TABLE LEGISLACAO (
              ID VARCHAR(50) NOT NULL,
              TIPO VARCHAR(15) NOT NULL,
              DESCRICAO VARCHAR(100) NOT NULL,
              ANO VARCHAR(5),
              PRIMARY KEY(ID)
              ) ENGINE=InnoDB"""
        cursor.execute(legislacao)
        print 'TABELA LEGISLACAO CRIADA COM SUCESSO'

        tag = """CREATE TABLE TAG (
              ID INT NOT NULL AUTO_INCREMENT,
              NOME VARCHAR(100) NOT NULL,
              PRIMARY KEY(ID)
              ) ENGINE=InnoDB"""
        cursor.execute(tag)
        print 'TABELA TAG CRIADA COM SUCESSO'

        agravado = """CREATE TABLE AGRAVADO (
                ID INT NOT NULL AUTO_INCREMENT,
                NOME VARCHAR(150) NOT NULL,
                PRIMARY KEY(ID)
                ) ENGINE=InnoDB;"""
        cursor.execute(agravado)
        print 'TABELA AGRAVADO CRIADA COM SUCESSO'

        agravante = """CREATE TABLE AGRAVANTE (
                ID INT NOT NULL AUTO_INCREMENT,
                NOME VARCHAR(150) NOT NULL,
                PRIMARY KEY(ID)
                ) ENGINE=InnoDB;"""
        cursor.execute(agravante)
        print 'TABELA AGRAVANTE CRIADA COM SUCESSO'

        advogado = """CREATE TABLE ADVOGADO (
                ID INT NOT NULL AUTO_INCREMENT,
                NOME VARCHAR(100) NOT NULL,
                PRIMARY KEY(ID)
                ) ENGINE=InnoDB;"""
        cursor.execute(advogado)
        print 'TABELA ADVOGADO CRIADA COM SUCESSO'

        intimado = """CREATE TABLE INTIMADO (
                ID INT NOT NULL AUTO_INCREMENT,
                NOME VARCHAR(100) NOT NULL,
                PRIMARY KEY(ID)
                ) ENGINE=InnoDB;"""
        cursor.execute(intimado)
        print 'TABELA INTIMADO CRIADA COM SUCESSO'

        procurador = """CREATE TABLE PROCURADOR (
                ID INT NOT NULL AUTO_INCREMENT,
                NOME VARCHAR(100) NOT NULL,
                PRIMARY KEY(ID)
                ) ENGINE=InnoDB;"""
        cursor.execute(procurador)
        print 'TABELA PROCURADOR CRIADA COM SUCESSO'
        db.commit()

        acordao_tag = """CREATE TABLE ACORDAO_TAG (
                ID_ACORDAO VARCHAR(50),
                ID_TAG INT,
                FOREIGN KEY ACORDAO_FK(ID_ACORDAO) REFERENCES ACORDAO(ID),
                FOREIGN KEY TAG_FK(ID_TAG) REFERENCES TAG(ID),
                UNIQUE KEY(ID_ACORDAO, ID_TAG)
                ) ENGINE=InnoDB"""
        cursor.execute(acordao_tag)
        print 'TABELA ACORDAO_TAG CRIADA COM SUCESSO'

        acordao_tag = """CREATE TABLE ACORDAO_LEGISLACAO (
                ID_ACORDAO VARCHAR(50),
                ID_LEGISLACAO VARCHAR(50),
                FOREIGN KEY ACORDAO_FK(ID_ACORDAO) REFERENCES ACORDAO(ID),
                FOREIGN KEY LEGISLACAO_FK(ID_LEGISLACAO) REFERENCES LEGISLACAO(ID),
                UNIQUE KEY(ID_ACORDAO, ID_LEGISLACAO)
                ) ENGINE=InnoDB"""
        cursor.execute(acordao_tag)
        print 'TABELA ACORDAO_LEGISLACAO CRIADA COM SUCESSO'

        partes_agravado = """CREATE TABLE PARTES_AGRAVADO (
                ID_ACORDAO VARCHAR(50),
                ID_AGRAVADO INT,
                FOREIGN KEY ACORDAO_FK(ID_ACORDAO) REFERENCES ACORDAO(ID),
                FOREIGN KEY AGRAVADO_FK(ID_AGRAVADO) REFERENCES AGRAVADO(ID),
                UNIQUE KEY(ID_ACORDAO, ID_AGRAVADO)
                ) ENGINE=InnoDB"""
        cursor.execute(partes_agravado)
        print 'TABELA PARTES_AGRAVADO CRIADA COM SUCESSO'

        partes_agravante = """CREATE TABLE PARTES_AGRAVANTE (
                ID_ACORDAO VARCHAR(50),
                ID_AGRAVANTE INT,
                FOREIGN KEY ACORDAO_FK(ID_ACORDAO) REFERENCES ACORDAO(ID),
                FOREIGN KEY AGRAVANTE_FK(ID_AGRAVANTE) REFERENCES AGRAVANTE(ID),
                UNIQUE KEY(ID_ACORDAO, ID_AGRAVANTE)
                ) ENGINE=InnoDB"""
        cursor.execute(partes_agravante)
        print 'TABELA PARTES_AGRAVANTE CRIADA COM SUCESSO'

        partes_procurador = """CREATE TABLE PARTES_PROCURADOR (
                ID_ACORDAO VARCHAR(50),
                ID_PROCURADOR INT,
                FOREIGN KEY ACORDAO_FK(ID_ACORDAO) REFERENCES ACORDAO(ID),
                FOREIGN KEY PROCURADOR_FK(ID_PROCURADOR) REFERENCES PROCURADOR(ID),
                UNIQUE KEY(ID_ACORDAO, ID_PROCURADOR)
                ) ENGINE=InnoDB"""
        cursor.execute(partes_procurador)
        print 'TABELA PARTES_PROCURADOR CRIADA COM SUCESSO'

        partes_intimado = """CREATE TABLE PARTES_INTIMADO (
                ID_ACORDAO VARCHAR(50),
                ID_INTIMADO INT,
                FOREIGN KEY ACORDAO_FK(ID_ACORDAO) REFERENCES ACORDAO(ID),
                FOREIGN KEY INTIMADO_FK(ID_INTIMADO) REFERENCES INTIMADO(ID),
                UNIQUE KEY(ID_ACORDAO, ID_INTIMADO)
                ) ENGINE=InnoDB"""
        cursor.execute(partes_intimado)
        print 'TABELA PARTES_INTIMADO CRIADA COM SUCESSO'

        partes_advogado = """CREATE TABLE PARTES_ADVOGADO (
                ID_ACORDAO VARCHAR(50),
                ID_ADVOGADO INT,
                FOREIGN KEY ACORDAO_FK(ID_ACORDAO) REFERENCES ACORDAO(ID),
                FOREIGN KEY ADVOGADO_FK(ID_ADVOGADO) REFERENCES ADVOGADO(ID),
                UNIQUE KEY(ID_ACORDAO, ID_ADVOGADO)
                ) ENGINE=InnoDB"""
        cursor.execute(partes_advogado)
        print 'TABELA PARTES_ADVOGADO CRIADA COM SUCESSO'

        citacao = """CREATE TABLE CITACAO (
                ID_ACORDAO VARCHAR(50),
                ID_ACORDAO_CITADO VARCHAR(50),
                FOREIGN KEY ACORDAO_FK(ID_ACORDAO) REFERENCES ACORDAO(ID)
                ) ENGINE=InnoDB"""
        cursor.execute(citacao)
        print 'TABELA CITACAO CRIADA COM SUCESSO'

        similar = """CREATE TABLE SIMILAR (
                ID_ACORDAO VARCHAR(50),
                ID_ACORDAO_SIMILAR VARCHAR(50),
                FOREIGN KEY ACORDAO_FK(ID_ACORDAO) REFERENCES ACORDAO(ID)
                ) ENGINE=InnoDB"""
        cursor.execute(similar)
        print 'TABELA SIMILAR CRIADA COM SUCESSO'

        print 'TABELAS CRIADAS COM SUCESSO'

        db.commit()
    except Exception, e:
        print str(e)
        db.rollback()

    db.close()


if __name__ == '__main__':
    connection = MongoClient('localhost', 27017)
    db = connection['DJs']

    create_mysql_tables()

    for acordao in db.all.find(no_cursor_timeout=True).batch_size(5):
        decisao_trained = db.trained.find_one({"acordaoId" : acordao['acordaoId'].encode('utf-8')})
        # print 'iterando sobre os acordaos %s' % str(acordao)
        insert_tables_on_mysql(acordao, decisao_trained)

    print errors