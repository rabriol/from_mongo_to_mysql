# coding=utf-8

__author__ = 'brito'

from string import replace

def clean_acordao_id(acordao_id):
    word = replace(acordao_id, '2ºJULG', '')
    word = replace(word.strip(' '), ' ', '_')
    return word

if __name__ == '__main__':
    print clean_acordao_id('2ºJULG AGR AI 699063')