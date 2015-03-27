#!/usr/bin/python

from __future__ import print_function

import time
import redis
import yaml
import logging
from subprocess import call

from daemon import runner


class Watchman():

    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path = '/tmp/watchman.pid'
        self.pidfile_timeout = 5
        self.nutcracker_config = {}
        self.masterchange = {'prev': [], 'curr': []}

    def __read_configuration(self):
        self.config = yaml.load(file('./watchman.yml', 'r'))

    def run(self):

        logger.info('Watchman started')	

        self.__read_configuration()

        logger.info('Watchman configuration: {}'.format(str(self.config)))

        self.get_nutcracker_config()

        r = redis.StrictRedis(host=self.config['sentinel_host'], port=self.config['sentinel_port'])
        p = r.pubsub()
        p.psubscribe('+switch-master')

        while True:
            message = p.get_message()
            if message and message['channel'] == '+switch-master' and message['type'] == 'pmessage':

                logger.info('Got +switch-master message')
                
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

        logger.info('Master change: {}'.format(str(self.masterchange)))

    def update_nutcracker_config(self):

        servers = self.nutcracker_config[self.config['clustername']]['servers']

        for index, server in enumerate(servers):
            serverdet, servername = server.split()
            host, port, num = serverdet.split(':')

            if (host, port) == (self.masterchange['prev'][0], self.masterchange['prev'][1]):

                self.nutcracker_config[self.config['clustername']]['servers'][index] = '{}:{}:{} {}'.format(
                    self.masterchange['curr'][0], self.masterchange['curr'][1], num, servername)

                logger.info('Nutcracker configuration updated')

    def save_nutcracker_config(self):
        yaml.dump(self.nutcracker_config, file(self.config['nutcracker_config_file'], 'w'), default_flow_style=False)
        logger.info('Nutcracker configuration saved')

    
    def restart_nutcracker(self):
        call(['/etc/init.d/nutcracker', 'restart'])
        logger.info('Nutcracker restarted')


watchman = Watchman()

logger = logging.getLogger("watchmanlog")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler = logging.FileHandler("/var/log/watchman/watchman.log")
handler.setFormatter(formatter)
logger.addHandler(handler)

daemon_runner = runner.DaemonRunner(watchman)
daemon_runner.daemon_context.working_directory = '/etc/watchman'
daemon_runner.daemon_context.files_preserve=[handler.stream]
daemon_runner.do_action()
