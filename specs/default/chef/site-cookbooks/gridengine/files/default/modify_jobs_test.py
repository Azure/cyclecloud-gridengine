import unittest
import modify_jobs
import parallel_environments
import random
from modify_jobs import assign_job_to_pg, _build_placement_groups
from placement_groups import PlacementGroup, PlacementGroupSet


def _test_job(cores="8.000", slot_type=None, placement_group=None, state="pending", job_number=100, pe="mpi", **hard_resources):
    ret = {"job_number": job_number, "pe_range": {"max": cores}, "pe": pe, "state": state}
    if slot_type:
        hard_resources["slot_type"] = slot_type
    if placement_group:
        hard_resources["placement_group"] = placement_group
    ret["hard_resources"] = hard_resources
    return ret


class ModifyJobsTest(unittest.TestCase):

    def setUp(self):
        modify_jobs.jetpack_config["gridengine.parallel_environment.mpi.use_placement_groups"] = True
        parallel_environments.set_pe("mpi", {"allocation_rule": random.choice(["$round_robin", "$fill_up"])})

    def test_existing_pg(self):
        pg_set = PlacementGroupSet()
        pg = pg_set.create_placement_group("execute", "123")
        pg.add_slots(16)
        pg.request_slots(16)

        assert pg_set.get_placement_group("123").queue_depth() == 0

        # so this was already assigned, so we don't need a delta for aff group...
        # BUT it should increase the slots assigned.
        job = _test_job(placement_group="123")
        details = assign_job_to_pg(job, pg_set, True)
        self.assertEquals(details, {"slot_type": "execute"})
        self.assertEquals(pg_set.get_placement_group("123").queue_depth(), 1)

    def test_assign_job_to_pg(self):
        pg_set = PlacementGroupSet()
        
        pg_set.create_placement_group("execute", "123").add_slots(16)
        pg_set.create_placement_group("other", "234").add_slots(6)
        queue_depth = lambda pg_id: pg_set.get_placement_group(pg_id).queue_depth()
        
        details = assign_job_to_pg(_test_job(8), pg_set, True)
        
        self.assertEquals(details, {"slot_type": "execute", "placement_group": "123"})
        self.assertEquals(queue_depth("123"), -1)
        details = assign_job_to_pg(_test_job(8), pg_set, True)
        
        self.assertEquals(details, {"slot_type": "execute", "placement_group": "123"})
        self.assertEquals(queue_depth("123"), 0, str(pg_set))
        
        details = assign_job_to_pg(_test_job(8), pg_set, True)
        self.assertEquals(queue_depth("123"), 1)
        
        self.assertEquals(assign_job_to_pg(_test_job(9), pg_set, True), {"slot_type": "execute"})
        self.assertEquals(assign_job_to_pg(_test_job(8, "other"), pg_set, True), {})

    def test_assign_to_jobs(self):
        pg_set = PlacementGroupSet()
        pg_set.create_placement_group("execute", "aid-123").add_slots(8)
        pg_set.create_placement_group("execute", "aid-234").add_slots(16)
        pg_set.create_placement_group("execute", "aid-345").add_slots(24)
        
        for _ in range(6):
            assign_job_to_pg(_test_job(), pg_set, True)
        self.assertEquals(0, sum(x.queue_depth() for x in pg_set.query_placement_groups("execute")))

    def test_queue_depth(self):
        # we no longer know the size of the group, so the depth is basically the number of assigned hosts
        self.assertEquals(PlacementGroup(group_id="abc", slot_type="execute", slots_per_host=4, slots_total=8, slots_requested=8).queue_depth(), 0)
        self.assertEquals(PlacementGroup(group_id="abc", slot_type="execute", slots_per_host=8, slots_total=8, slots_requested=12).queue_depth(), 1)

    def test_build_placement_groups(self):
        expected_pg_set = PlacementGroupSet()
        pg = expected_pg_set.create_placement_group("execute", "123")
        pg.add_host("host-123")
        pg.add_slots(2)

        self.assertEquals(expected_pg_set,
            _build_placement_groups({"host-123": {"slot_type": "execute", "placement_group": "123", "num_proc": "2.0000"}}))

    def test_pg_set(self):
        pg_set = PlacementGroupSet()
        pg1 = pg_set.create_placement_group("execute", "123")
        for _ in range(4):
            pg1.add_slots(12)
        pg2 = pg_set.create_placement_group("other", "234")
        for _ in range(4):
            pg2.add_slots(12)
        pg3 = pg_set.create_placement_group("execute", "345")
        for _ in range(2):
            pg3.add_slots(12)
        
        self.assertEqual(2, len(pg_set.query_placement_groups("execute")))
        self.assertEqual(1, len(pg_set.query_placement_groups("execute", 48)))
        self.assertEqual(1, len(pg_set.query_placement_groups("execute", 36)))
        self.assertEqual(2, len(pg_set.query_placement_groups("execute", 24)))
        
        self.assertEqual(3, len(pg_set.query_placement_groups(None)))
        self.assertEqual(1, len(pg_set.query_placement_groups("other")))

    def test_smp(self):
        jetpack_config = {}
        parallel_environments.set_pe("smp", {"allocation_rule": "$pe_slots"})
        parallel_environments.set_pe("mpir", {"allocation_rule": "$round_robin"})
        parallel_environments.set_pe("mpif", {"allocation_rule": "$fill_up"})  # only mpi that is not grouped
        parallel_environments.set_pe("mpi1", {"allocation_rule": "1"})
        parallel_environments.set_pe("mpi2", {"allocation_rule": "2"})

        def _assertEquals(boolean, pe_name):
            actual = parallel_environments.is_job_candidate_for_placement({"pe_range": {"max": 8}, "pe": pe_name}, jetpack_config)
            self.assertEquals(boolean, actual)

        # Test default behaviors
        _assertEquals(False, "smp")
        _assertEquals(True, "mpir")
        _assertEquals(True, "mpif")
        _assertEquals(False, "mpi1")
        _assertEquals(False, "mpi2")

        # cluster defaults
        jetpack_config["gridengine.parallel_environment.default.use_placement_groups"] = False
        jetpack_config["gridengine.parallel_environment.%s.use_placement_groups" % "mpir"] = True
        _assertEquals(False, "smp")
        _assertEquals(True, "mpir")
        _assertEquals(False, "mpif")
        _assertEquals(False, "mpi1")
        _assertEquals(False, "mpi2")

        jetpack_config["gridengine.parallel_environment.default.use_placement_groups"] = True
        jetpack_config["gridengine.parallel_environment.%s.use_placement_groups" % "smp"] = False
        _assertEquals(False, "smp")
        _assertEquals(True, "mpir")
        _assertEquals(True, "mpif")
        _assertEquals(False, "mpi1")
        _assertEquals(False, "mpi2")

        # non-booleans are ignored
        jetpack_config["gridengine.parallel_environment.default.use_placement_groups"] = None
        jetpack_config["gridengine.parallel_environment.%s.use_placement_groups" % "mpi1"] = "ture"

        _assertEquals(False, "mpi1")
       

if __name__ == "__main__":
    unittest.main()