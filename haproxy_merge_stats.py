#!/usr/bin/env python

""" Pulls and merges statistics from HAProxy """

import asyncore
import socket
import os
import sys
#from collections import defaultdict
#import logging, logging.handlers
import json

class SocketClient(asyncore.dispatcher):
    """ Basic asyncore client """

    def __init__(self, path, command):
        asyncore.dispatcher.__init__(self)
        if path[0] == '/':
            self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(path)
        self.buffer = command
        self.output = ''

    def handle_connect(self):
        pass

    def handle_close(self):
        self.close()

    def handle_read(self):
        self.output += self.recv(8192)

    def writable(self):
        return len(self.buffer) > 0

    def handle_write(self):
        sent = self.send(self.buffer)
        self.buffer = self.buffer[sent:]

class HAProxyClient(SocketClient):
    """ Basic HAProxy client """

    def __init__(self, path, command):
        SocketClient.__init__(self, path, command)
        self.result = None

    def __parse(self):
        parsed = []
        data = self.output.split('\n')[:-2]
        for line in data:
            parsed.append(line.split(',')[:-1])
        self.result = parsed

    def handle_close(self):
        self.__parse()
        SocketClient.handle_close(self)

class MergeHAProxyStats(object):
    """ Merge Haproxy stats """

    # Common stats across all servers. These shold be the same across all servers
    MERGE_COMPARE = [
        '# pxname',
        'svname',
        'slim',
        'iid',
        'sid',
        'tracked',
        'type',
        'addr',
        'cookie',
        'mode',
        'algo'
    ]

    # Common stats across all servers. These can be different, so use data from first server only
    MERGE = [
        'status',
        'weight',
        'act',
        'bck',
        'lastchg',
        'downtime',
        'pid',
        'check_status',
        'check_code',
        'check_duration',
        'hanafail',
        'lastsess',
        'last_chk',
        'last_agt',
        'agent_status',
        'agent_code',
        'agent_duration',
        'check_desc',
        'agent_desc',
        'check_rise',
        'check_fall',
        'check_health',
        'agent_rise',
        'agent_fall',
        'agent_health',
    ]

    # Add stats across different servers
    ADD = [
        'qcur',
        'qmax',
        'scur',
        'smax',
        'stot',
        'bin',
        'bout',
        'dreq',
        'dresp',
        'ereq',
        'econ',
        'eresp',
        'wretr',
        'wredis',
        'chkfail',
        'chkdown',
        'qlimit',
        'lbtot',
        'rate',
        'rate_lim',
        'rate_max'
        'hrsp_1xx',
        'hrsp_2xx',
        'hrsp_3xx',
        'hrsp_4xx',
        'hrsp_5xx',
        'hrsp_other',
        'req_rate',
        'req_rate_max',
        'req_tot',
        'cli_abrt',
        'srv_abrt'
        'comp_in',
        'comp_out',
        'comp_byp',
        'comp_rsp',
        'conn_rate',
        'conn_rate_max',
        'conn_tot',
        'intercepted',
        'dcon',
        'dses'
    ]

    # Average across different servers
    AVERAGE = [
        'throttle',
        'qtime',
        'ctime',
        'rtime',
        'ttime'
    ]

    def __init__(self, in_results):
        self.results = in_results
        # populate result with data from first backend
        self.result = self.results.pop(0)
        self.length = int(len(self.results))
        self.__merge()
        self.__normalize()

    def __merge(self):
        for backend in self.results:
            if len(self.result) != len(backend):
                self.__error('Wrong number of stats row across backends')
            if self.result[0] != backend[0]:
                self.__error('Stats header does not match acrosss backends')
            for row in range(1, len(backend)):
                for col in range(len(backend[0])):
                    name = self.__get_name_by_index(col)
                    if name in self.MERGE_COMPARE:
                        if backend[row][col] != self.result[row][col]:
                            self.__error('Stats do not match acrosss some backends row=' + str(row) + \
                              ' , col=' + str(col) + ', field=' + name + ' , name=' + str(backend[row]))
                    if name in self.MERGE:
                        pass
                    if name in self.ADD:
                        self.result[row][col] = \
                            self.__add_elements(self.result[row][col], backend[row][col])
                    if name in self.AVERAGE:
                        self.result[row][col] = \
                            self.__add_elements(self.result[row][col], backend[row][col])

    def __normalize(self):
        for row in range(1, len(self.result)):
            for col in range(len(self.result[0])):
                name = self.__get_name_by_index(col)
                if name in self.AVERAGE:
                    if isinstance(self.result[row][col], float):
                        self.result[row][col] = self.result[row][col]/self.length
                if isinstance(self.result[row][col], float):
                    self.result[row][col] = str(int(self.result[row][col]))

    def __get_name_by_index(self, index):
        return self.result[0][index]

    @staticmethod
    def __add_elements(first, second):
        if not first:
            return second
        if not second:
            return first
        return float(first) + float(second)

    @staticmethod
    def __error(message):
        raise RuntimeError, message

class HAProxyStats(object):
    """ Get and merge HAProxy from multiple servers """

    def __init__(self, sockets):
        self.sockets = sockets

    def read(self):
        """ Read, parse and merge stats from HAProxy """
        clients = []
        results = []
        for socket in self.sockets:
            client = HAProxyClient(socket, 'show stat\n')
            clients.append(client)
        asyncore.loop(timeout=1, count=None)
        for client in clients:
            results.append(client.result)
        result = MergeHAProxyStats(results).result
        output = ''
        for line in result:
            for value in line:
                output += str(value) + ','
            output += '\n'
        output += '\n'
        #print json.dumps(output, sort_keys=True, indent=2)
        return output

class StatsServerHandler(asyncore.dispatcher):
    """ Basic asyncore show stat hander """

    def __init__(self, sock, output, async_map):
        asyncore.dispatcher.__init__(self, sock=sock, map=async_map)
        self.output = output
        self.buffer = ''
        self.is_readable = True

    def handle_read(self):
        data = self.recv(8192)
        if data == 'show stat\n':
            self.buffer = self.output.read()
        else:
            self.buffer = 'Unknown command. Please enter one of the following commands only :\n'
            self.buffer += '  show stat      : report counters for each proxy and server\n\n'
        self.is_readable = False

    def writable(self):
        return len(self.buffer) > 0

    def readable(self):
        return self.is_readable

    def handle_write(self):
        sent = self.send(self.buffer)
        self.buffer = self.buffer[sent:]
        if not self.writable():
            self.close()

    def handle_close(self):
        self.close()

class StatsServer(asyncore.dispatcher):
    """ Basic asyncore unix socket sever """

    def __init__(self, path, output, async_map):
        asyncore.dispatcher.__init__(self, map=async_map)
        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(path)
        self.listen(5)
        self.output = output
        self.map = async_map

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            #sock, addr = pair
            sock = pair[0]
            StatsServerHandler(sock, self.output, self.map)

if __name__ == '__main__':
    sockets = sys.argv
    COMMAND = sockets.pop(0)
    LISTEN_SOCKET = sockets.pop(0)
    USAGE = 'Usage: ' + COMMAND + ' <listen socket> <haproxy sockets>'

    if len(sockets) < 1:
        print USAGE
        exit(1)
    if not os.path.isabs(LISTEN_SOCKET):
        print 'Error: <listen socket> is not abolute path : ' + LISTEN_SOCKET
        print USAGE
        exit(1)
    for path in sockets:
        if not os.path.isabs(path):
            print 'Error: <haproxy socket> is not abolute path : ' + path
            print USAGE
            exit(1)

    async_map = {}
    StatsServer(LISTEN_SOCKET, HAProxyStats(sockets), async_map)
    try:
        asyncore.loop(timeout=1, count=None, map=async_map)
    finally:
        if os.path.exists(LISTEN_SOCKET):
            os.unlink(LISTEN_SOCKET)
