#!/usr/bin/env python
'''
    Elastic Search plugin for Server Density
'''

import requests
import json

class ElasticSearch:
    def __init__(self, agentConfig, checksLogger, rawConfig):
        self.agentConfig = agentConfig
        self.checksLogger = checksLogger
        self.rawConfig = rawConfig

    def getCluster(self):
        session = requests.session()
        session.config.update({'max_retries':3})
        url = 'http://%s:%s/_cluster/nodes/_local/stats?all=true' % (
            self.agentConfig.get('elasticsearch_host', '127.0.0.1'),
            self.agentConfig('elasticsearch_port', 9200)
        )
        response = session.get(url)
        return json.loads(response.content)

    def load_data(self):
        response = self.getCluster()
        stats = json.loads(response)

        results = dict()
        for node in stats['nodes'].values():
            results['DB Size']      = node['indices']['store']['size_in_bytes'] / 1048576
            results['Num Docs']     = node['indices']['docs']['count']
            results['CPU %']        = node['process']['cpu']['percent']
            results['Mem Resident'] = node['process']['mem']['resident_in_bytes'] / 1048576
            results['Mem Shared']   = node['process']['mem']['share_in_bytes']  / 1048576
            results['Mem Virtual']  = node['process']['mem']['total_virtual_in_bytes']  / 1048576
            return results

    def run(self):
        return self.load_data()

