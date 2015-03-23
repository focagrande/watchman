#!/usr/bin/python

from __future__ import print_function

import time
import redis
import yaml
from subprocess import call

from daemon import runner


class Watchman():

    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path = '/tmp/sentinel_listener.pid'
        self.pidfile_timeout = 5
        self.nutcracker_config = {}
        self.masterchange = {'prev': [], 'curr': []}

    def __read_configuration(self):
        self.config = yaml.load(file('./watchman.yml', 'r'))

    def run(self):

        self.__read_configuration()

        self.get_nutcracker_config()

        r = redis.StrictRedis(host=self.config['sentinel_host'], port=self.config['sentinel_port'])
        p = r.pubsub()
        p.psubscribe('+switch-master')

        while True:
            message = p.get_message()
            if message and message['channel'] == '+switch-master' and message['type'] == 'pmessage':
                
                self.get_masterchange(message)
                self.update_nutcracker_config()
                self.save_nutcracker_config()
                self.restart_nutcracker()

            time.sleep(0.001)

    def get_nutcracker_config(self):
        self.nutcracker_config = yaml.load(file(self.config['nutcracker_config_file'], 'r'))

    def get_masterchange(self, message):
        msgdata = str(message['data']).split()
        self.masterchange['prev'] = msgdata[1:3]
        self.masterchange['curr'] = msgdata[3:]

    def update_nutcracker_config(self):

        servers = self.nutcracker_config[self.config['clustername']]['servers']

        for index, server in enumerate(servers):
            serverdet, servername = server.split()
            host, port, num = serverdet.split(':')

            if (host, port) == (self.masterchange['prev'][0], self.masterchange['prev'][1]):

                self.nutcracker_config[self.config['clustername']]['servers'][index] = '{}:{}:{} {}'.format(
                    self.masterchange['curr'][0], self.masterchange['curr'][1], num, servername)

    def save_nutcracker_config(self):
        yaml.dump(self.nutcracker_config, file(self.config['nutcracker_config_file'], 'w'), default_flow_style=False)

    
    def restart_nutcracker(self):
        call(['/etc/init.d/nutcracker', 'restart'])

if __name__ == '__main__':

    watchman = Watchman()
    daemon_runner = runner.DaemonRunner(watchman)
    daemon_runner.daemon_context.working_directory = '/etc/watchman'
    daemon_runner.do_action()
