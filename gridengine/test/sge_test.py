import tempfile
import unittest

import sge


class SGETest(unittest.TestCase):


    def test_read_complexes(self):
        data = """#name               shortcut     type        relop   requestable consumable default  urgency 
#--------------------------------------------------------------------------------------------
affinity_group      affinity_group        RESTRING    ==      YES      NO         NONE     0
affinity_group_cores affinity_group_cores  INT         ==      YES         NO         0        0
arch                a            RESTRING    ==      YES         NO         NONE     0
average_runtime     avg          INT         <=      YES         NO         0        0
calendar            c            RESTRING    ==      YES         NO         NONE     0
cpu                 cpu          DOUBLE      >=      YES         NO         0        0
d_rt                d_rt         TIME        <=      YES         NO         0:0:0    0
display_win_gui     dwg          BOOL        ==      YES         NO         0        0
exclusive           exclusive    BOOL        EXCL    YES         YES        0        1000
h_core              h_core       MEMORY      <=      YES         NO         0        0
h_cpu               h_cpu        TIME        <=      YES         NO         0:0:0    0
h_data              h_data       MEMORY      <=      YES         NO         0        0
h_fsize             h_fsize      MEMORY      <=      YES         NO         0        0
h_rss               h_rss        MEMORY      <=      YES         NO         0        0
h_rt                h_rt         TIME        <=      YES         NO         0:0:0    0
h_stack             h_stack      MEMORY      <=      YES         NO         0        0
h_vmem              h_vmem       MEMORY      <=      YES         NO         0        0
hostname            h            HOST        ==      YES         NO         NONE     0
load_avg            la           DOUBLE      >=      NO          NO         0        0
load_long           ll           DOUBLE      >=      NO          NO         0        0
load_medium         lm           DOUBLE      >=      NO          NO         0        0
load_short          ls           DOUBLE      >=      NO          NO         0        0
m_cache_l1          mcache1      MEMORY      <=      YES         NO         0        0
m_cache_l2          mcache2      MEMORY      <=      YES         NO         0        0
m_cache_l3          mcache3      MEMORY      <=      YES         NO         0        0
m_core              core         INT         <=      YES         NO         0        0
m_mem_free          mfree        MEMORY      <=      YES         YES        0        0
m_mem_free_n0       mfree0       MEMORY      <=      YES         YES        0        0
m_mem_free_n1       mfree1       MEMORY      <=      YES         YES        0        0
m_mem_free_n2       mfree2       MEMORY      <=      YES         YES        0        0
m_mem_free_n3       mfree3       MEMORY      <=      YES         YES        0        0
m_mem_total         mtotal       MEMORY      <=      YES         YES        0        0
m_mem_total_n0      mmem0        MEMORY      <=      YES         YES        0        0
m_mem_total_n1      mmem1        MEMORY      <=      YES         YES        0        0
m_mem_total_n2      mmem2        MEMORY      <=      YES         YES        0        0
m_mem_total_n3      mmem3        MEMORY      <=      YES         YES        0        0
m_mem_used          mused        MEMORY      >=      YES         NO         0        0
m_mem_used_n0       mused0       MEMORY      >=      YES         NO         0        0
m_mem_used_n1       mused1       MEMORY      >=      YES         NO         0        0
m_mem_used_n2       mused2       MEMORY      >=      YES         NO         0        0
m_mem_used_n3       mused3       MEMORY      >=      YES         NO         0        0
m_numa_nodes        nodes        INT         <=      YES         NO         0        0
m_socket            socket       INT         <=      YES         NO         0        0
m_thread            thread       INT         <=      YES         NO         0        0
m_topology          topo         RESTRING    ==      YES         NO         NONE     0
m_topology_inuse    utopo        RESTRING    ==      YES         NO         NONE     0
m_topology_numa     unuma        RESTRING    ==      YES         NO         NONE     0
mem_free            mf           MEMORY      <=      YES         NO         0        0
mem_total           mt           MEMORY      <=      YES         NO         0        0
mem_used            mu           MEMORY      >=      YES         NO         0        0
min_cpu_interval    mci          TIME        <=      NO          NO         0:0:0    0
nodearray           nodearray    RESTRING    ==      YES         NO         NONE     0
np_load_avg         nla          DOUBLE      >=      NO          NO         0        0
np_load_long        nll          DOUBLE      >=      NO          NO         0        0
np_load_medium      nlm          DOUBLE      >=      NO          NO         0        0
np_load_short       nls          DOUBLE      >=      NO          NO         0        0
num_proc            p            INT         ==      YES         NO         0        0
onsched             os           BOOL        ==      YES         NO         0        0
placement_group     group        RESTRING    ==      FORCED      NO         NONE     0
placement_group_cores group_size  INT         ==      YES         NO         0        0
qname               q            RESTRING    ==      YES         NO         NONE     0
rerun               re           BOOL        ==      NO          NO         0        0
s_core              s_core       MEMORY      <=      YES         NO         0        0
s_cpu               s_cpu        TIME        <=      YES         NO         0:0:0    0
s_data              s_data       MEMORY      <=      YES         NO         0        0
s_fsize             s_fsize      MEMORY      <=      YES         NO         0        0
s_rss               s_rss        MEMORY      <=      YES         NO         0        0
s_rt                s_rt         TIME        <=      YES         NO         0:0:0    0
s_stack             s_stack      MEMORY      <=      YES         NO         0        0
s_vmem              s_vmem       MEMORY      <=      YES         NO         0        0
seq_no              seq          INT         ==      NO          NO         0        0
slot_type           slot_type    RESTRING    ==      FORCED      NO         NONE     0
slots               s            INT         <=      YES         YES        1        1000
swap_free           sf           MEMORY      <=      YES         NO         0        0
swap_rate           sr           MEMORY      >=      YES         NO         0        0
swap_rsvd           srsv         MEMORY      >=      YES         NO         0        0
swap_total          st           MEMORY      <=      YES         NO         0        0
swap_used           su           MEMORY      >=      YES         NO         0        0
tmpdir              tmp          RESTRING    ==      NO          NO         NONE     0
virtual_free        vf           MEMORY      <=      YES         YES        0        0
virtual_total       vt           MEMORY      <=      YES         NO         0        0
virtual_used        vu           MEMORY      >=      YES         NO         0        0
# >#< starts a comment but comments are not saved across edits --------
"""
        tmp = tempfile.mktemp()
        with open(tmp, "w") as fw:
            fw.write(data)
            
        complexes = sge.read_complexes(tmp)
        self.assertEquals(1, len(list(complexes.query(name="swap_free"))))
        self.assertEquals(data.count("MEMORY"), len(list(complexes.query(type="MEMORY"))))
        self.assertEquals(1, len(list(complexes.query(type="INT", urgency=1000))))
        
        swap_free = complexes.query_single(name="swap_free")
        self.assertEquals("swap_free", swap_free.name)
        self.assertEquals("sf", swap_free.shortcut)
        self.assertEquals("MEMORY", swap_free.type)
        self.assertEquals("<=", swap_free.relop)
        self.assertEquals("YES", swap_free.requestable)
        self.assertEquals("NO", swap_free.consumable)
        self.assertEquals("0", swap_free.default)
        self.assertEquals("0", swap_free.urgency)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()