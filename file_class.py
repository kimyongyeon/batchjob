#!/usr/bin/python
#-*- coding: utf-8 -*-

import os
import sys
import crawl

class FileManagement(object):

    def __init__(self):
        print ("FileManagement Class start")
        self._rootPath = {}

    # 파일 이름
    def file_find(self, file_name):
        return os.path.split(file_name)[1]

    # 폴더 이름
    def folder_find(self, folder_name):
        return os.path.split(folder_name)[0]

    # 확장자만 구하기
    def ext_find(self, ext_name):
        return os.path.splitext(ext_name)

    # 패스 리스트 , 정렬
    def path_list(self, path_dir):
        return os.listdir(path_dir).sort()

    # 파일 쓰기
    def file_write(self, file_path, file_str):
        print(file_str + " 파일을 씁니다.")
        f = open(file_path, 'w', encoding="utf8")
        f.write(file_str)
        f.close()

    # 파일 한줄 읽기
    def file_read(self, file_path):
        print("파일을 읽습니다.")
        f = open(file_path, 'r', encoding='utf8')
        line = f.readline()
        print(line)
        f.close()

    # 전체 파일 읽기
    def file_total_read(self, file_path):
        f = open(file_path, 'r', encoding='utf8')
        while True:
            line = f.readline()
            if not line: break
            print(line)

crawl.hello()
crawl.hello_world("world")
print (sys.stdout.encoding)

file = FileManagement()
file_str = '''select * from query;
insert query into name, phone, password values 'admin', '010-1234-5678', 'password';
'''
file.file_write('file.txt', file_str)
file.file_read('file.txt')
print ('=' * 50)
file.file_total_read('file.txt')

multiline_str = '''
select * from {table_name}
'''.format(table_name='tonyspark')

print(multiline_str)