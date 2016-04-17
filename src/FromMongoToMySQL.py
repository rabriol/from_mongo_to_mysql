# coding=utf-8

__author__ = 'brito'

from pymongo import MongoClient
import MySQLdb
from string import replace
import warnings

errors = dict()

def insert_tables_on_mysql(document):
    db = MySQLdb.connect('localhost', 'root', '', 'ACORDAOS')

    cursor = db.cursor()

    # warnings.filterwarnings('ignore', category = MySQLdb.Warning)

    try:
        check_acordao = ("SELECT ID FROM ACORDAO WHERE ID = '%s';") % document['acordaoId'].encode('utf-8')
        cursor.execute(check_acordao)
        acordao_result = cursor.fetchone()

        # print 'Resultado da consulta %s' % str(acordao_total)

        if acordao_result == None:
            relator_id = None;
            try:
                check_relator = ("SELECT ID FROM RELATOR WHERE NOME = '%s';") % document['relator'].encode('utf-8')
                cursor.execute(check_relator)
                relator_result = cursor.fetchone()

                if relator_result == None:
                    insert_relator = ("""INSERT INTO RELATOR(NOME) VALUES ('%s');""") % \
                              (document['relator'].encode('utf-8'))
                    cursor.execute(insert_relator)

                check_relator = ("SELECT ID FROM RELATOR WHERE NOME = '%s';") % document['relator'].encode('utf-8')
                cursor.execute(check_relator)
                relator_result = cursor.fetchone()

                relator_id = relator_result[0]

            except Exception,e:
                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            publicacao = replace(document['publicacao'].encode('utf-8'), '\n', '\\n')
            publicacao = replace(publicacao, '\'', '\\\'')
            publicacao = replace(publicacao, '  ', '')

            decisao = replace(document['decisao'].encode('utf-8'), '\n', '\\n')
            decisao = replace(decisao, '\'', '\\\'')
            decisao = replace(decisao, '  ', '')

            partesTexto = replace(document['partesTexto'].encode('utf-8'), '\n', '\\n')
            partesTexto = replace(partesTexto, '\'', '\\\'')
            partesTexto = replace(partesTexto, '  ', '')

            similaresTexto = replace(document['similaresTexto'].encode('utf-8'), '\n', '\\n')
            similaresTexto = replace(similaresTexto, '\'', '\\\'')
            similaresTexto = replace(similaresTexto, '  ', '')

            ementa = replace(document['ementa'].encode('utf-8'), '\n', '\\n')
            ementa = replace(ementa, '\'', '\\\'')
            ementa = replace(ementa, '  ', '')

            legislacaoTexto = replace(document['legislacaoTexto'].encode('utf-8'), '\n', '\\n')
            legislacaoTexto = replace(legislacaoTexto, '\'', '\\\'')

            insert_acordao = ("""INSERT INTO ACORDAO(ID, LOCAL, TRIBUNAL,
                            PUBLICACAO, DECISAO, PARTES_TEXTO, SIMILARES_TEXTO, EMENTA, ORGAO_JULGADOR,
                            LEGISLACAO_TEXTO, DOUTRINAS, ID_RELATOR)
                            VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s);
                            """) % (document['acordaoId'].encode('utf-8'),
                                    document['local'].encode('utf-8'),
                                    document['tribunal'].encode('utf-8'),
                                    publicacao,
                                    decisao,
                                    partesTexto,
                                    similaresTexto,
                                    ementa,
                                    document['orgaoJulg'].encode('utf-8'),
                                    legislacaoTexto,
                                    document['doutrinas'].encode('utf-8'),
                                    relator_id
                                )
            cursor.execute(insert_acordao)

            try:
                for acordao_citado in document['citacoes']:
                    insert_citacoes = ("""INSERT INTO CITACAO(ID_ACORDAO, ID_ACORDAO_CITADO) VALUES ('%s', '%s');""") % \
                                  (document['acordaoId'], acordao_citado)
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
                                  (document['acordaoId'], acordao_similar['acordaoId'])
                    cursor.execute(insert_citacoes)
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(document['acordaoId'], document['partes']['intimado'], cursor, 'intimado', 'partes')
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(document['acordaoId'], document['partes']['agravante'], cursor, 'agravante', 'partes')
            except Exception,e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(document['acordaoId'], document['partes']['agravado'], cursor, 'agravado', 'partes')
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(document['acordaoId'], document['partes']['advogado'], cursor, 'advogado', 'partes')
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(document['acordaoId'], document['partes']['procurador'], cursor, 'procurador', 'partes')
            except Exception, e:
                print str(e)

                error = str(e)

                if error in errors:
                    errors[error] = errors[error] + 1
                else:
                    errors[error] = 1

            try:
                insert_related_tables(document['acordaoId'], document['tags'], cursor, 'tag', 'acordao')
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
    db = MySQLdb.connect('localhost', 'root', '', 'acordaos')

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

        relator = """CREATE TABLE RELATOR (
              ID INT NOT NULL AUTO_INCREMENT,
              NOME VARCHAR(50) NOT NULL,
              PRIMARY KEY(ID)
              ) ENGINE=InnoDB"""
        cursor.execute(relator)
        print 'TABELA RELATOR CRIADA COM SUCESSO'

        acordao = """CREATE TABLE ACORDAO (
                  ID VARCHAR(50) NOT NULL,
                  DAT_JULGAMENTO DATE,
                  LOCAL VARCHAR(50),
                  TRIBUNAL VARCHAR(50),
                  PUBLICACAO VARCHAR(500),
                  ID_RELATOR INT,
                  DECISAO VARCHAR(5000),
                  PARTES_TEXTO VARCHAR(2000),
                  SIMILARES_TEXTO VARCHAR(7000),
                  EMENTA VARCHAR(5000),
                  ORGAO_JULGADOR VARCHAR(50),
                  LEGISLACAO_TEXTO VARCHAR(2000),
                  DOUTRINAS VARCHAR(100),
                  PRIMARY KEY (ID),
                  FOREIGN KEY RELATOR_FK(ID_RELATOR) REFERENCES RELATOR(ID)
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
              NOME VARCHAR(50) NOT NULL,
              PRIMARY KEY(ID)
              ) ENGINE=InnoDB"""
        cursor.execute(tag)
        print 'TABELA TAG CRIADA COM SUCESSO'

        agravado = """CREATE TABLE AGRAVADO (
                ID INT NOT NULL AUTO_INCREMENT,
                NOME VARCHAR(100) NOT NULL,
                PRIMARY KEY(ID)
                ) ENGINE=InnoDB;"""
        cursor.execute(agravado)
        print 'TABELA AGRAVADO CRIADA COM SUCESSO'

        agravante = """CREATE TABLE AGRAVANTE (
                ID INT NOT NULL AUTO_INCREMENT,
                NOME VARCHAR(100) NOT NULL,
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

    for acordao in db.all.find():
        # print 'iterando sobre os acordaos %s' % str(acordao)
        insert_tables_on_mysql(acordao)

    print errors