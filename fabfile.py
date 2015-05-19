#!/usr/bin/env python

from __future__ import with_statement

import json
import time
from fabric.api import *
# from fabric.colors import red, green
# from fabric.contrib import files
# from functools import wraps
from kazoo.client import KazooClient

class SolrCloudManager:
    def __init__(self, zk_host):
        self.__zk = KazooClient(hosts=zk_host)
        self.__zk.start()

    def __del__(self):
        self.__zk.stop()

    def get_cluster_state(self):
        cs_tuple = self.__zk.retry(self.__zk.get, 'clusterstate.json')
        cs = json.loads(cs_tuple[0])
        return cs

    # Check all replicas that contain node_name
    # Return true if ALL nodes are in the active state
    def replicas_are_active(self, node_name):
        cluster_state = self.get_cluster_state()
        active = True
        for cn, cdata in cluster_state.iteritems():
            for sn, sdata in cdata['shards'].iteritems():
                replica_down = False
                node_in_replica = False
                for rn, rdata in sdata['replicas'].iteritems():
                    if rdata['node_name'] == node_name:
                        node_in_replica = True
                    if rdata['state'] != "active":
                        replica_down = True
                if replica_down and node_in_replica:
                    active = False
            if not active:
                break
        return active

    # Wait for all replicas to enter the active state
    def wait_for_replicas(self, node_name, timeout):
        start_time = time.time()
        ra = self.replicas_are_active(node_name)
        while ((start_time + timeout) > time.time()) and (not ra):
            print "Waiting for replication to finish"
            time.sleep(3)
            ra = self.replicas_are_active(node_name)
        return ra

    def node_is_live(self, node_name):
        live_nodes = self.__zk.retry(self.__zk.get_children, 'live_nodes')
        return (node_name in live_nodes)

    def wait_for_live_node(self, node_name, timeout):
        start_time = time.time()
        lv = self.node_is_live(node_name)
        while ((start_time + timeout) > time.time()) and (not lv):
            print "Waiting for live node"
            time.sleep(3)
            lv = self.node_is_live(node_name)
        return lv

    def _remove_live_node(self, node_name):
        # self.__zk.retry(self.__zk.delete, 'live_nodes/' + node_name)
        print 'Pretend... Delete: ' + 'live_nodes/' + node_name
        return True

    def _restart_host_solr_service(self, host):
        # restart_command = '/usr/bin/sudo /sbin/restart solr-undertow'
        print 'Pretend... Restarting: %s' % (host)
        # result = sudo("echo '/sbin/restart solr-undertow'")
        # if result.failed:
        #     return 1
        return True

    def restart_host_solr(self, host, host_port='8983', force=False):
        if host is None:
            return self._return_message(1, 'host is required')

        node_name = host + ':' + host_port + '_solr'
        if (not force) and (not self.node_is_live(node_name)):
            return self._return_message(10, 'Node is not live')

        # Don't restart if any other replicas are down
        if (not force) and (not self.replicas_are_active(node_name)):
            return self._return_message(20, 'Not all replicas are not active')

        # LATER Make sure a reindex isn't in progress

        if not self._remove_live_node(node_name):
            return self._return_message(30, 'Error removing live node')

        if not self._restart_host_solr_service(host):
            return self._return_message(40, 'Error restarting solr service')

        if not self.wait_for_live_node(node_name, 240):
            return self._return_message(50, 'Timeout waiting for live node')

        if not self.wait_for_replicas(node_name, 600):
            return self._return_message(60, 'Timeout waiting for replicas')

    def _return_message(self, error_code, message):
        return {'status': error_code, 'message': message}

env.zkhost = 'zookeeper-dev-1.build.internal:2181/solr/5.1.0/sandbox'
env.host_port = '8983'
env.force = False
env.user = 'thartman'


@task
def solrRestart():
    scman = SolrCloudManager(env.zkhost)
    # Only supports restart_host_solr operation
    results = scman.restart_host_solr(host=env.host, host_port=env.host_port, force=env.force)

    print results
