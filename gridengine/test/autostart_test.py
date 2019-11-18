import unittest
from copy import deepcopy

from cyclecloud import machine
from cyclecloud.autoscaler import Autoscaler
from cyclecloud.machine import MachineRequest
from cyclecloud.nodearrays import NodearrayDefinitions

import autostart
import complexes
import logging_init


class MockSGELib:
    
    def __init__(self, jobs=None, details=None):
        self.jobs = jobs or []
        self.details = details or {}
        
    def get_sge_job_details(self):
        return self.details
    
    def get_sge_jobs(self):
        return self.jobs
        
    def get_host_complexes(self):
        return complexes.EXAMPLE_COMPLEX.splitlines(False)
        

def _nodearray_definitions_as_cluster_status(*machinetypes):
    machinetypes = sorted(machinetypes, key=lambda x: -x.get("priority", 100))
    cluster_status = {}
    ret = {}
    
    for machinetype in machinetypes:
        nodearray = machinetype.get("nodearray")
        if nodearray not in ret:
            ret[nodearray] = {"nodearray": {},
                              "name": nodearray,
                              "buckets": []}
        
        name = machinetype.get("machinetype")
        ret[nodearray]["buckets"].append({"definition": {"machineType": name},
                                          "virtualMachine": machinetype})

    cluster_status["nodearrays"] = ret.values()
    return cluster_status


def _nodearray_definitions(*machinetypes):
    ret = NodearrayDefinitions()
    for mt in machinetypes:
        if mt.get("placeby"):
            ret.add_machinetype_with_placement_group(mt.get(mt.get("placeby")), mt)
        else:
            ret.add_machinetype(mt)

    return ret
# 
# 
# def machine.new_machinetype(nodearray, name, ncpus, mem_gb, disk_gb, **extra_resources):
#     ret = {"nodearray": nodearray,
#            "name": "name",
#            "machinetype": name,
#            "ncpus": ncpus,  # h_core
#            "mem": mem_gb,  # h_vmem
#            "available": 100,
#            "h_fsize": disk_gb}
#     ret.update(extra_resources)
#     return ret


def _machine_instance(nodearray, mt_name, placement_group, nodearray_definitions):
    na_def = nodearray_definitions.get_machinetype(nodearray, mt_name, placement_group)
    return machine.new_machine_instance(na_def)

        
class SGEQ:
    
    def __init__(self):
        self.jobs = []
        self.details = {}
        self.job_id = 1
    
    def qsub(self, **jobdef):
        jobdef["job_number"] = jobdef.get("job_number", str(self.job_id))
        self.job_id += 1
        
        for _ in range(jobdef.pop("t", 1)):
            jobdeftmp = deepcopy(jobdef)
            job = {}
            job["job_state"] = jobdeftmp.pop("job_state", "qw")
            job["job_number"] = jobdeftmp.pop("job_number")
            
            if "nodes" in jobdeftmp:
                jobdeftmp["m_numa_nodes"] = jobdeftmp.pop("nodes")
            
            details = {}
            
            if "pe" in jobdeftmp:
                details["pe"] = jobdeftmp.pop("pe")
                # TODO this is wrong. need to support a stepped range
                pecount = jobdeftmp.pop("pe_range")
                details["pe_range"] = {"min": pecount, "max": pecount}
            
            if "ac" in jobdeftmp:
                details["context"] = jobdeftmp.pop("ac")
            
            details["hard_resources"] = {}
            for key, value in jobdeftmp.items():
                details["hard_resources"][key] = str(value)
            
            self.jobs.append(job)
            self.details[job["job_number"]] = details
        
    def autoscale_requests(self, nodearray_defs=None, existing_machines=None):
        existing_machines = existing_machines or []
        if not nodearray_defs:
            nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 16, 8, 100, astring="aaa"),
                                                    _new_machinetype("execute", "a4", 32, 64, 500, abool=False))

        allocator = Autoscaler(nodearray_defs, existing_machines)
        mocklib = MockSGELib(self.jobs, self.details)
        a = autostart.GridEngineAutostart({"mpi_exclusive": {"allocation_rule": "1"},
                                                  "smpslots": {"allocation_rule": "$pe_slots"},
                                                  "mpi_pack": {"allocation_rule": "$fill_up"},
                                                  "mpi_single_proc": {"allocation_rule": "1"},
                                                  "mpi_round_robin": {"allocation_rule": "$round_robin"}}, mocklib, True)
        
        for job in a.query_jobs():
            allocator.add_job(job)
        
        return allocator.get_new_machine_requests()


class AutostartTest(unittest.TestCase):
    

    def test_pbsuserguide_ex1(self):
        '''
            1. A job that will fit in a single host but not in any of the vnodes, packed into the fewest vnodes:
                -pe mpi
                -l select=1:ncpus=10:mem=20gb
                -l place=pack
            In earlier versions, this would have been:
                -lncpus=10,mem=20gb
        '''
        q = SGEQ()
        q.qsub(h_core=10, h_vmem="20G")
#         q.qsub(machinetype="a4")
        self.assertEquals([MachineRequest("execute", "a4", 1, "", "")], q.autoscale_requests())
        
    def test_pbsuserguide_ex2(self):
        '''
            2. Request four chunks, each with 1 CPU and 4GB of memory taken from anywhere.
            -l select=4:ncpus=1:mem=4GB
            -l place=free
        '''
        q = SGEQ()
        q.qsub(nodes=4, h_core=1, h_vmem=4, machinetype="a4")
        self.assertEquals([MachineRequest("execute", "a4", 1, "", "")], q.autoscale_requests())
        
        q = SGEQ()
        q.qsub(nodes=4, h_core=1, h_vmem=4, machinetype="a4")
        self.assertEquals([MachineRequest("execute", "a4", 1, "", "")], q.autoscale_requests())
        
    def test_pbsuserguide_ex6(self):
        '''
            6. This selects exactly 4 vnodes where the arch is linux, and each vnode will be on a separate host. Each vnode will
            have 1 CPU and 2GB of memory allocated to the job.
            -lselect=4:mem=2GB:ncpus=1:arch=linux -lplace=scatter
        '''
        q = SGEQ()
        q.qsub(h_vmem=2, pe="mpi_round_robin", pe_range=4)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 32, 128, 100, arch="linux", placeby="placement_group", placement_group="123"))
        self.assertEquals([MachineRequest("execute", "a2", 1, "placement_group", "123")], q.autoscale_requests(nodearray_defs))
        
        q = SGEQ()
        # if you don't specify nodes, t is assumed to be the # of nodes
        q.qsub(h_vmem=2, pe="mpi_round_robin", pe_range=4, t=4)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 32, 128, 100, arch="linux", placeby="placement_group", placement_group="123"))
        self.assertEquals([MachineRequest("execute", "a2", 4, "placement_group", "123")], q.autoscale_requests(nodearray_defs))
        
        q = SGEQ()
        # specifying nodes is used in stead of t
        q.qsub(h_vmem=2, pe="mpi_round_robin", pe_range=4, t=4, nodes=2)
        self.assertEquals([MachineRequest("execute", "a2", 2, "placement_group", "123")], q.autoscale_requests(nodearray_defs))
        
        q.qsub(h_vmem=2, pe="mpi_round_robin", pe_range=4, exclusive=True)
        self.assertEquals([MachineRequest("execute", "a2", 3, "placement_group", "123")], q.autoscale_requests(nodearray_defs))
        
    def test_mpi_pack(self):
        '''
        '''
        # TODO how do we distinguish smp vs mpi pe?
        q = SGEQ()
        q.qsub(h_vmem=2, pe="mpi_pack", pe_range=4, placement_group="123")
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 32, 128, 100, arch="linux"))
        self.assertEquals([], q.autoscale_requests(nodearray_defs))
 
        q = SGEQ()
        q.qsub(h_vmem=2, pe="mpi_pack", pe_range=4, placement_group="123")
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 32, 128, 100, arch="linux", placeby="placement_group", placement_group="123"))
        # only machines with a placement_group exist
        self.assertEquals([MachineRequest("execute", "a2", 1, "placement_group", "123")], q.autoscale_requests(nodearray_defs))
          
        q = SGEQ()
        # unlike round_robin, if you don't specify nodes, t is _not_ assumed to be the # of nodes
        q.qsub(h_vmem=2, pe="mpi_pack", pe_range=4, t=4, placement_group="123")
        self.assertEquals([MachineRequest("execute", "a2", 1, "placement_group", "123")], q.autoscale_requests(nodearray_defs))
          # TODO I don't know that this is even possible - num nodes with a pe?
#         q = SGEQ()
#         # specifying nodes is used instead of t
#         q.qsub(h_vmem=2, pe="mpi_pack", pe_range=4, t=4, nodes=4)
#         self.assertEquals([MachineRequest("execute", "a2", 4, "placement_group", "123")], q.autoscale_requests(nodearray_defs))
#          
#         q = SGEQ()
#         # nodes can be reused if exclusive is false
#         q.qsub(h_vmem=2, pe="mpi_pack", pe_range=4, t=4, nodes=4)
#         q.qsub(h_vmem=2, pe="mpi_pack", pe_range=4, exclusive=False)
#         self.assertEquals([MachineRequest("execute", "a2", 4, "placement_group", "123")], q.autoscale_requests(nodearray_defs))
#         
#         q = SGEQ()
#         # specifying nodes is used in stead of t
#         q.qsub(h_vmem=2, pe="mpi_pack", pe_range=4, t=4, nodes=4)
#         q.qsub(h_vmem=2, pe="mpi_pack", pe_range=4, exclusive=True, placement_group="123")
#         self.assertEquals([MachineRequest("execute", "a2", 5, "placement_group", "123")], q.autoscale_requests(nodearray_defs))
        
    def test_playground(self):
        q = SGEQ()
        # unlike round_robin, if you don't specify nodes, t is _not_ assumed to be the # of nodes
        
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a4", 32, 128, 100, arch="linux", placeby="placement_group", placement_group="123"),
                                                _new_machinetype("execute", "a2", 16, 64, 50, arch="linux"))
        q.qsub(h_vmem=2, t=1000)
        self.assertEquals([MachineRequest("execute", "a2", 1000 / 16 + 1, "", "")], q.autoscale_requests(nodearray_defs))
        
    def test_existing_machines(self):
        q = SGEQ()
        
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a4", 32, 128, 100, arch="linux", placeby="placement_group", placement_group="123"),
                                                _new_machinetype("execute", "a2", 16, 64, 50, priority=50, arch="linux"),
                                                _new_machinetype("execute", "a2v2", 16, 64, 50, priority=25, arch="linux"))
        q.qsub(h_vmem=2, t=1000)
        
        existing_a2 = _machine_instance("execute", "a2", None, nodearray_defs)
        existing_a2v2 = _machine_instance("execute", "a2v2", None, nodearray_defs)
        existing_a4 = _machine_instance("execute", "a4", "123", nodearray_defs)
        required_instances = 1000 / 16 + 1
        
        self.assertEquals([MachineRequest("execute", "a2", required_instances, "", "")], 
                          q.autoscale_requests(nodearray_defs, []))
        
        # there is already an existing a2, so allocate one fewer 
        self.assertEquals([MachineRequest("execute", "a2", required_instances - 1, "", "")], 
                          q.autoscale_requests(nodearray_defs, [existing_a2]))
        
        # there is an a4 up and running, but it is in a placement group so we will ignore it
        self.assertEquals([MachineRequest("execute", "a2", required_instances, "", "")], 
                          q.autoscale_requests(nodearray_defs, [existing_a4]))
        
        # we would prefer the 2 according to priority, but there already is an a2v2 up and running, so use that
        self.assertEquals([MachineRequest("execute", "a2", required_instances - 1, "", "")],
                          sorted(q.autoscale_requests(nodearray_defs, [existing_a2v2]), key=lambda x: x.machinetype))
        
    def test_pe_slots(self):
        # no machine has enough slots
        q = SGEQ()
        q.qsub(h_vmem=2, pe="smpslots", pe_range=4)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 2, 128, 100))
        self.assertEquals([], q.autoscale_requests(nodearray_defs))
        
        q = SGEQ()
        # has plenty of slots
        q.qsub(h_vmem=2, pe="smpslots", pe_range=4)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 32, 128, 100))
        self.assertEquals([MachineRequest("execute", "a2", 1, "", "")], q.autoscale_requests(nodearray_defs))
        
        q = SGEQ()
        # has plenty of slots
        q.qsub(h_vmem=2, pe="smpslots", pe_range=4, t=8)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 32, 128, 100))
        self.assertEquals([MachineRequest("execute", "a2", 1, "", "")], q.autoscale_requests(nodearray_defs))
        
        q = SGEQ()
        # t * 9 is 36 cpus, so we should spill over to another box
        q.qsub(h_vmem=2, pe="smpslots", pe_range=4, t=9)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 32, 128, 100))
        self.assertEquals([MachineRequest("execute", "a2", 2, "", "")], q.autoscale_requests(nodearray_defs))
        
        q = SGEQ()
        # t * 9 is 36 cpus, so we should spill over to another box
        q.qsub(h_vmem=2, pe="smpslots", pe_range=4, t=4)
        q.qsub(h_vmem=2, pe="smpslots", pe_range=4, t=4)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 32, 128, 100))
        self.assertEquals([MachineRequest("execute", "a2", 2, "", "")], q.autoscale_requests(nodearray_defs))
        
    def test_pbsuserguide_ex11(self):
        '''
           11. Here is an odd-sized job that will fit on a single SGI system, but not on any one node-board. We request an odd
                number of CPUs that are not shared, so they must be "rounded up":
                -l select=1:ncpus=3:mem=6gb
                -l place=pack:excl
        '''
        q = SGEQ()
        # for allocation testing purposes, I'm going to add a free job here too to ensure we don't try to use that one.
        q.qsub(h_core=1, h_vmem=1)
        q.qsub(h_core=3, h_vmem=6, exclusive=True)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 8, 24, 100, priority=100))
          
        self.assertEquals([MachineRequest("execute", "a2", 2, "", "")], q.autoscale_requests(nodearray_defs))
        
        q = SGEQ()
        q.qsub(h_core=1, h_vmem=1)
        q.qsub(nodes=3, h_core=3, h_vmem=6, exclusive=True)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 8, 24, 100, priority=100))
        
        self.assertEquals([MachineRequest("execute", "a2", 3, "", "")], q.autoscale_requests(nodearray_defs))
        
    def test_pbsuserguide_ex14(self):
        '''
            14. Submit a job that must be run across multiple SGI systems, packed into the fewest vnodes:
            -l select=2:ncpus=10:mem=12gb
            -l place=scatter
        '''
        q = SGEQ()
        q.qsub(h_vmem=12, pe="mpi_round_robin", pe_range=10, t=2)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 16, 36, 100,
                                                              placeby="placement_group", placement_group="123", priority=100))
        self.assertEquals([MachineRequest("execute", "a2", 2, "placement_group", "123")], q.autoscale_requests(nodearray_defs))
         
        # add enough CPUs and it still requests 2 machines
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 24, 36, 100,
                                                              placeby="placement_group", placement_group="123", priority=100))
        self.assertEquals([MachineRequest("execute", "a2", 2, "placement_group", "123")], q.autoscale_requests(nodearray_defs))
        
        q = SGEQ()
        q.qsub(nodes=2, h_vmem=12, pe="mpi_round_robin", pe_range=5)
        # add another job
        q.qsub(nodes=2, h_vmem=12, pe="mpi_round_robin", pe_range=5)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 24, 36, 100,
                                                              placeby="placement_group", placement_group="123", priority=100))
        self.assertEquals([MachineRequest("execute", "a2", 2, "placement_group", "123")], q.autoscale_requests(nodearray_defs))

    def test_mpi_single_proc(self):
        q = SGEQ()
        q.qsub(pe="mpi_single_proc", pe_range=5)
        nodearray_defs = _nodearray_definitions(_new_machinetype("execute", "a2", 24, 36, 100,
                                                              placeby="placement_group", placement_group="123", priority=100))
        self.assertEquals([MachineRequest("execute", "a2", 5, "placement_group", "123")], q.autoscale_requests(nodearray_defs))


def _new_machinetype(nodearray, name, ncpus, mem_gb, disk_gb, **extra):
    extra["h_vmem"] = mem_gb
    extra["h_core"] = ncpus
    extra["h_fsize"] = disk_gb
    return machine.new_machinetype(nodearray, name, ncpus, mem_gb, disk_gb, **extra)


if __name__ == "__main__":
    unittest.main()
