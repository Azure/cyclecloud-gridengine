import logging

from hpc.autoscale.job.computenode import SchedulerNode


class MockGridEngineDriver:
    
    def __init__(self, scheduler_nodes, jobs):
        self.queues = {}
        self.scheduler_nodes = scheduler_nodes
        self.jobs = jobs
        self.drained = {}
    
    @property
    def current_hostnames(self):
        return [n.hostname for n in self.scheduler_nodes]
    
    def handle_draining(self, unmatched_nodes):
        for node in unmatched_nodes:
            if node.hostname not in self.drained:
                if node.hostname in self.current_hostnames:
                    self.drained[node.hostname] = node
            else:
                assert self.drained[node.hostname] == node
    
    def handle_join_cluster(self, matched_nodes):
        for node in matched_nodes:
            if node.hostname not in self.current_hostnames:
                self.scheduler_nodes.append(SchedulerNode(node.hostname, node.resources))
        
    def handle_post_delete(self, nodes_to_delete):
        if not nodes_to_delete:
            logging.warn("Empty or None passed into handle_post_delete")
            return
        
        for node in nodes_to_delete:
            if node.hostname in self.current_hostnames:
                self.scheduler_nodes = [t for t in self.scheduler_nodes if t.hostname != node.hostname]
        
    def handle_undraining(self, matched_nodes):
        for node in matched_nodes:
            self.drained.pop(node.hostname, node)
