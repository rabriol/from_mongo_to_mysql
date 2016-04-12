# coding=utf-8

__author__ = 'brito'

from pymongo import MongoClient
import MySQLdb
from string import replace
import warnings



def save_on_mysql(document):
    db = MySQLdb.connect('localhost', 'root', '', 'ACORDAOS')

    cursor = db.cursor()

    # warnings.filterwarnings('ignore', category = MySQLdb.Warning)

    try:
        check_acordao = ("SELECT ID FROM ACORDAO WHERE ID = '%s';") % document['acordaoId'].encode('utf-8')
        cursor.execute(check_acordao)

        acordao_result = cursor.fetchone()

        # print 'Resultado da consulta %s' % str(acordao_total)

        if acordao_result == None:
            # print 'INSERINDO REGISTRO EM ACORDAOS'

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
                            LEGISLACAO_TEXTO, DOUTRINAS)
                            VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');
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
                                    document['doutrinas'].encode('utf-8')
                                )
            cursor.execute(insert_acordao)

            # for acordao_citado in document['citacoes']:
            #     insert_citacoes = ("""INSERT INTO CITACAO(ID_ACORDAO, ID_ACORDAO_CITADO) VALUES ('%s', '%s');""") % \
            #                   (document['acordaoId'], acordao_citado)
            #     cursor.execute(insert_citacoes)

            # for acordao_citado in document['similares']:
            #     insert_citacoes = ("""INSERT INTO SIMILAR(IDT_ACORDAO, ID_ACORDAO_CITADO) VALUES ('%s', '%s');""") % \
            #                   (document['acordaoId'], acordao_citado)
            #     cursor.execute(insert_citacoes)

            # INSERT PROCURADOR E PARTES_PROCURADOR
            try:
                for procurador in document['partes']['procurador']:
                    check_procurador = ("SELECT ID FROM PROCURADOR WHERE NOME = '%s';") % procurador.encode('utf-8')
                    cursor.execute(check_procurador)

                    procurador_result = cursor.fetchone()

                    if procurador_result == None:
                        insert_procurador = ("INSERT INTO PROCURADOR(NOME) VALUES ('%s');") % (procurador.encode('utf-8'))
                        cursor.execute(insert_procurador)

                    check_procurador = ("SELECT ID FROM PROCURADOR WHERE NOME = '%s';") % procurador.encode('utf-8')
                    cursor.execute(check_procurador)

                    result = cursor.fetchone()
                    procurador_id = result[0]

                    check_partes_procurador = ("""SELECT ID_ACORDAO, ID_PROCURADOR FROM PARTES_PROCURADOR WHERE ID_ACORDAO = '%s'
                                                AND ID_PROCURADOR = %s;""") % (document['acordaoId'].encode('utf-8'), int(procurador_id))

                    cursor.execute(check_partes_procurador)

                    result = cursor.fetchone()

                    if result == None:
                        insert_partes_procurador = ("""INSERT INTO PARTES_PROCURADOR(ID_ACORDAO, ID_PROCURADOR) VALUES
                                            ('%s', %s);""") % (document['acordaoId'].encode('utf-8'), int(procurador_id))
                        cursor.execute(insert_partes_procurador)

            except Exception:
                pass

            # INSERT INTIMADO E PARTES_INTIMADO
            try:
                for intimado in document['partes']['intimado']:
                    check_intimado = ("SELECT ID FROM INTIMADO WHERE NOME = '%s';") % intimado.encode('utf-8')
                    cursor.execute(check_intimado)

                    intimado_result = cursor.fetchone()

                    if intimado_result == None:
                        insert_intimado = ("INSERT INTO INTIMADO(NOME) VALUES ('%s');") % (intimado.encode('utf-8'))
                        cursor.execute(insert_intimado)

                    check_intimado = ("SELECT ID FROM INTIMADO WHERE NOME = '%s';") % intimado.encode('utf-8')
                    cursor.execute(check_intimado)

                    result = cursor.fetchone()
                    intimado_id = result[0]

                    check_partes_intimado = ("""SELECT ID_ACORDAO, ID_INTIMADO FROM PARTES_INTIMADO WHERE ID_ACORDAO = '%s'
                                                AND ID_INTIMADO = %s;""") % (document['acordaoId'].encode('utf-8'), int(intimado_id))

                    cursor.execute(check_partes_intimado)

                    result = cursor.fetchone()

                    if result == None:
                        insert_partes_intimado = ("""INSERT INTO PARTES_INTIMADO(ID_ACORDAO, ID_INTIMADO) VALUES
                                            ('%s', %s);""") % (document['acordaoId'].encode('utf-8'), int(intimado_id))
                        cursor.execute(insert_partes_intimado)

            except Exception:
                pass

        db.commit()
    except Exception, e:
        print str(e)
        db.rollback()

    db.close()


def create_mysql_tables():
    db = MySQLdb.connect('localhost', 'root', '', 'acordaos')

    cursor = db.cursor()

    # warnings.filterwarnings('ignore', category = MySQLdb.Warning)

    try:
        cursor.execute('DROP TABLE IF EXISTS CITACAO')
        cursor.execute('DROP TABLE IF EXISTS SIMILAR')
        cursor.execute('DROP TABLE IF EXISTS PARTES_PROCURADOR')
        cursor.execute('DROP TABLE IF EXISTS PARTES_INTIMADO')
        cursor.execute('DROP TABLE IF EXISTS PARTES_ADVOGADO')
        cursor.execute('DROP TABLE IF EXISTS PARTES_AGRAVANTE')
        cursor.execute('DROP TABLE IF EXISTS PARTES_AGRAVADO')
        cursor.execute("DROP TABLE IF EXISTS PROCURADOR;")
        cursor.execute("DROP TABLE IF EXISTS INTIMADO;")
        cursor.execute("DROP TABLE IF EXISTS ADVOGADO;")
        cursor.execute("DROP TABLE IF EXISTS AGRAVANTE;")
        cursor.execute("DROP TABLE IF EXISTS AGRAVADO;")
        cursor.execute("DROP TABLE IF EXISTS TAG;")
        cursor.execute("DROP TABLE IF EXISTS ACORDAO;")

        acordao = """CREATE TABLE ACORDAO (
                  ID VARCHAR(50) NOT NULL,
                  DAT_JULGAMENTO DATE,
                  LOCAL VARCHAR(50),
                  TRIBUNAL VARCHAR(50),
                  PUBLICACAO VARCHAR(500),
                  DECISAO VARCHAR(5000),
                  PARTES_TEXTO VARCHAR(2000),
                  SIMILARES_TEXTO VARCHAR(7000),
                  EMENTA VARCHAR(5000),
                  ORGAO_JULGADOR VARCHAR(50),
                  LEGISLACAO_TEXTO VARCHAR(2000),
                  DOUTRINAS VARCHAR(100),
                  PRIMARY KEY (ID)
                  ) ENGINE=InnoDB;"""
        cursor.execute(acordao)
        print 'TABELA ACORDAO CRIADA COM SUCESSO'

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
                FOREIGN KEY ACORDAO_FK(ID_ACORDAO) REFERENCES ACORDAO(ID),
                FOREIGN KEY CITADO_FK(ID_ACORDAO_CITADO) REFERENCES ACORDAO(ID),
                UNIQUE KEY(ID_ACORDAO, ID_ACORDAO_CITADO)
                ) ENGINE=InnoDB"""
        cursor.execute(citacao)
        print 'TABELA CITACAO CRIADA COM SUCESSO'

        similar = """CREATE TABLE SIMILAR (
                ID_ACORDAO VARCHAR(50),
                ID_ACORDAO_SIMILAR VARCHAR(50),
                FOREIGN KEY ACORDAO_FK(ID_ACORDAO) REFERENCES ACORDAO(ID),
                FOREIGN KEY ACORDAO_SIMILAR_FK(ID_ACORDAO_SIMILAR) REFERENCES ACORDAO(ID),
                UNIQUE KEY(ID_ACORDAO, ID_ACORDAO_SIMILAR)
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
        save_on_mysql(acordao)