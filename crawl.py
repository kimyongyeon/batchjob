#-*- coding: utf-8 -*-
def hello():
    print ("def hello")

def hello_world(data):
    print ("hello " + data)

class Sample(object):

    def __init__(self):
        self._groups = {}

    def member_init(self, name):
        self._groups[name] = []

    def add_member(self, name, jumsu):
        self._groups[name].append(jumsu)

    def get_member(self, name):
        sum_str = self._groups[name]
        sum_str += "," + name
        return sum_str

class Crawl(object):

    def __init__(self):
        self._url = {}
        self._item = {}

    def data_receive(self, data):
        print ("데이터 받음: " + data)

    def data_send(self, data):
        print ("데이터 보냄: " + data )


class Parser(object):
    def __init__(self):
        self._url = {}
        self._item = {}

# sample = Sample()
# sample.member_init("admin")
# sample.add_member("admin", 80)
# print(sample.get_member("admin"))
#
# crawl = Crawl()
# crawl.data_receive("http://receive.com")
# crawl.data_send("http://send.com")

