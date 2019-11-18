EXAMPLE_COMPLEX = '''#name               shortcut     type        relop   requestable consumable default  urgency
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
machinetype         machinetype  RESTRING    ==      YES         NO         NONE     0
nodearray           nodearray  RESTRING    ==      YES         NO         NONE     0
'''
import logging


class Complex:
    
    def __init__(self, name, shortcut, type, relop, requestable, consumable, default, urgency):
        self.name = name
        self.shortcut = shortcut
        self.type = type
        self.relop = relop
        self.requestable = requestable if isinstance(requestable, bool) else requestable.lower() == "yes"
        self.consumable = consumable if isinstance(consumable, bool) else consumable.lower() == "yes"
        self.default = None if default == "NONE" else default
        self.value = self.default
        self.urgency = int(urgency)
        
        
class Complexes:
    def __init__(self, complexes):
        self.complexes = complexes
        
    def query(self, **kwargs):
        for cplex in self.complexes:
            matches = True
            for key, value in kwargs.items():
                if getattr(cplex, key) != value:
                    if key != "name" or getattr(cplex, "shortcut") != value:
                        matches = False
                        break
            if matches:
                yield cplex
                
                
def parse(lines):
    
    cplexes = []
    for line in lines:
        line = line.strip()
        
        if line.startswith("#"):
            continue
        
        toks = line.split()
        
        if len(toks) != 8:
            logging.warn("Unknown complex line - expected 8 columns, got '%s'", line)
            
        name, shortcut, type, relop, requestable, consumable, default, urgency = toks 
        cplexes.append(Complex(name, shortcut, type, relop, requestable, consumable, default, urgency))
    
    return Complexes(cplexes)


def func(complexes):
    
    complexes.query(name="h_vmem")
    complexes.query()
