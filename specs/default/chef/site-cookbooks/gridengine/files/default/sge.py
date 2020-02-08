# Library functions for querying SGE jobs (could use drmaa in the future)
import xml.sax
from datetime import datetime
import subprocess
import re
import xml.etree.ElementTree as ET

_BAD_TASK_RE = re.compile("^\s*(<JATASK:(?:.|\n)*?</JATASK.*\.>)", re.MULTILINE)


def _fix_detail_output(detail_output):
    '''
    Some versions of Grid Engine will give bad XML detail output like this:

    <JB_ja_tasks>
        <JATASK:      34.>
            <JAT_status>512</JAT_status>
            <JAT_task_number>168</JAT_task_number>
        </JATASK:      34.>
    </JB_ja_tasks>

    To get around this bug, pre-process the output to remove these invalid
    tags. The example above will become this:

    <JB_ja_tasks>
    </JB_ja_tasks>
    '''
    # Quickly determine if the bad task replacement regexp is needed
    if "JATASK:" in detail_output:
        # Fix up the output so the xml parser doesn't explode later
        return re.sub(_BAD_TASK_RE, '', detail_output)
    else:
        return detail_output


class JobDetails(xml.sax.ContentHandler):
    """ Parses the SGE 'job details' output from a command:

     qstat -u "*" -j "*" -xml

     stores the job details in a dict that is a subset of what you would see from a stanard "qstat -j <job_number>" command:

     {
        "job_number": 5,
        "job_name": "test.sh",
        "script_file": "test.sh",
        "account": "sge",
        "owner": "cluster.user",
        "uid": 100,
        "group": "cluster.user",
        "gid": 200,
        "submission_time": "2013-10-09T09:09:09",
        "pe": "mpi",
        "job_args": ['arg1', 'arg2', 'arg'3'],
        "context": {
            'average_runtime': '10',
        },
        "env_list": {
            '__SGE_PREFIX__O_HOME' : "/shared/home/cluster.user",
            '__SGE_PREFIX__O_SHELL' : "/bin/bash"
        }
        "hard_queue": {
            'name': "framework.q" }
        ,
        "hard_resources": {
            'mem_free': '15G',
            'slot_type': 'execute'
        },
        "soft_resources": {
            'foo_bar': 'baz',
        },
        "pe_range": {
            'min': 16,
            'max': 32,
            'step': 1
        }
     }

     Note: Job args are converged into a python array, resources from '-l' flags are converted into name-value items in a dict

     Jobs are stored in the 'jobs' attribute in the object when parsing is complete:

     parser = JobDetails()
     xml.sax.parse(file_handle, parser)
     jobs = parser.jobs

    """
    def __init__(self):
        self.buffer = []
        self.job = {}
        self.jobs = {}   # Mapping of JobID -> Job

        self.in_arguments = False
        self.in_context = False
        self.in_context_list = False
        self.in_job_env_list = False
        self.in_job_env_job_sublist = False
        self.in_hard_queue_list = False
        self.in_hard_resource_list = False
        self.in_soft_resource_list = False
        self.in_hard_resource_qstat_l_requests = False
        self.in_soft_resource_qstat_l_requests = False
        self.in_pe_range = False

        # The name value pair for the current qstat l request
        self.qstat_l_request_name = None
        self.qstat_l_request_value = None

        # The name value pair for the current VA_var
        self.va_variable_name = None
        self.va_variable_value = None

        self.attributes = {
            'JB_job_number': {'name': 'job_number', 'type': int},
            'JB_job_name': {'name': 'job_name', 'type': unicode},
            'JB_script_file': {'name': 'script_file', 'type': unicode},
            'JB_account': {'name': 'account', 'type': unicode},
            'JB_owner': {'name': 'owner', 'type': unicode},
            'JB_uid': {'name': 'uid', 'type': int},
            'JB_group': {'name': 'group', 'type': unicode},
            'JB_gid': {'name': 'gid', 'type': int},
            'JB_pe': {'name': 'pe', 'type': unicode}
        }

    def startElement(self, name, attrs):
        if name == "JB_job_number":
            self.completeJob()
        elif name == "JB_job_args":
            self.in_arguments = True
        elif name == "JB_context":
            self.in_context = True
        elif self.in_context and name == "context_list":
            self.in_context_list = True
            if 'context' not in self.job:
                self.job['context'] = {}
        elif name == "JB_env_list":
            self.in_job_env_list = True
        elif self.in_job_env_list and name == "job_sublist":
            self.in_job_env_job_sublist = True
            if 'env_list' not in self.job:
                self.job['env_list'] = {}
        elif name == "JB_hard_queue_list":
            self.in_hard_queue_list = True
        elif name == "JB_hard_resource_list":
            self.in_hard_resource_list = True
        elif name == "JB_soft_resource_list":
            self.in_soft_resource_list = True
        # NOTE: We check for both qstat_l_requests AND element because SGE outputs
        # different XML based on if you ran a qalter on the resources... stupid...
        elif self.in_hard_resource_list and (name == "qstat_l_requests" or name == "element"):
            self.in_hard_resource_qstat_l_requests = True
            if 'hard_resources' not in self.job:
                self.job['hard_resources'] = {}
        elif self.in_soft_resource_list and (name == "qstat_l_requests" or name == "element"):
            self.in_soft_resource_qstat_l_requests = True
            if 'soft_resources' not in self.job:
                self.job['soft_resources'] = {}
        elif name == "JB_pe_range":
            self.in_pe_range = True
            if 'pe_range' not in self.job:
                self.job['pe_range'] = {}

    def endElement(self, name):
        value = "".join(self.buffer).strip()
        self.buffer = []

        if name == "detailed_job_info":
            self.completeJob()
        elif name == "JB_job_args":
            self.in_arguments = False
        elif name == "JB_context":
            self.in_context_list = False
        elif name == "JB_context":
            self.in_context = False
        elif self.in_context_list and name == "context_list":
            self.in_context_list = False
            if self.va_variable_name and self.va_variable_value:
                self.job['context'][self.va_variable_name] = self.va_variable_value
            self.va_variable_name = None
            self.va_variable_value = None
        elif name == "JB_env_list":
            self.in_job_env_list = False
        elif self.in_job_env_list and name == "job_sublist":
            self.in_job_env_job_sublist = False
            if self.va_variable_name and self.va_variable_value:
                self.job['env_list'][self.va_variable_name] = self.va_variable_value
            self.va_variable_name = None
            self.va_variable_value = None
        elif name == "JB_hard_queue_list":
            self.in_hard_queue_list = False
        elif name == "JB_hard_resource_list":
            self.in_hard_resource_list = False
        elif name == "JB_soft_resource_list":
            self.in_soft_resource_list = False
        elif self.in_hard_resource_list and (name == "qstat_l_requests" or name == "element"):
            self.in_hard_resource_qstat_l_requests = False
            if self.qstat_l_request_name and self.qstat_l_request_value:
                self.job['hard_resources'][self.qstat_l_request_name] = self.qstat_l_request_value
            self.qstat_l_request_name = None
            self.qstat_l_request_value = None
        elif self.in_soft_resource_list and (name == "qstat_l_requests" or name == "element"):
            self.in_soft_resource_qstat_l_requests = False
            if self.qstat_l_request_name and self.qstat_l_request_value:
                self.job['soft_resources'][self.qstat_l_request_name] = self.qstat_l_request_value
            self.qstat_l_request_name = None
            self.qstat_l_request_value = None
        elif name == "JB_pe_range":
            self.in_pe_range = False
        elif self.in_pe_range:
            if name == "RN_min":
                self.job['pe_range']['min'] = value
            elif name == "RN_max":
                self.job['pe_range']['max'] = value
            elif name == "RN_step":
                self.job['pe_range']['step'] = value

        # Handle the individual arguments for a job
        elif name == "ST_name" and self.in_arguments:
            if 'job_args' not in self.job:
                self.job['job_args'] = []
            self.job['job_args'].append(value)

        elif name == "QR_name" and self.in_hard_queue_list:
            self.job['hard_queue'] = value

        # Parse the qstat_l_resources
        # TODO: We always use a string val for now, but depending on CE_valtype we may want
        # to use CE_doubleval instead for things like "-l mem_free=1G" which will return the
        # number of bytes instead of the string 1G
        elif name == "CE_name":
            self.qstat_l_request_name = value
        elif name == "CE_stringval":
            self.qstat_l_request_value = value

        elif name == "VA_variable":
            self.va_variable_name = value
        elif name == "VA_value":
            self.va_variable_value = value

        elif name == "JB_submission_time":
            self.job['submission_time'] = datetime.fromtimestamp(int(value[:10]))
        elif name in self.attributes.keys():
            self.job[self.attributes[name]['name']] = self.attributes[name]['type'](value)

    def characters(self, ch):
        self.buffer.append(ch)

    def completeJob(self):
        if 'job_number' in self.job:

            # Make sure we have empty resources if they weren't imported
            if 'hard_resources' not in self.job:
                self.job['hard_resources'] = {}
            if 'job_args' not in self.job:
                self.job['job_args'] = []

            self.jobs[int(self.job['job_number'])] = self.job
            self.job = {}


class Jobs(xml.sax.ContentHandler):
    """ Parse job data into a list of dictionaries from the qstat command, this command 'unrolls' task arrays:

    qstat -g d -xml

    stores generic job information in a dictionary:

    {
        'job_number': 9,
        'state': 'pending',
        'slots': 1,
        'startTime': datetime(2013, 10, 10, 9, 9, 9)
    }

    Note: Job start time (if available) is stored as a python datetime object for ease of use.

    Jobs are stored in the 'jobs' attribute of the parser object:

    parser = Jobs()
    xml.sax.parse(file_handle, parser)
    parser.jobs
    """

    def __init__(self):
        self.buffer = []
        self.jobs = []
        self.job = {}

    def startElement(self, name, attrs):
        if name == "job_list":
            self.job['state'] = attrs.get('state')

    def endElement(self, name):
        value = "".join(self.buffer).strip()
        self.buffer = []

        if name == "job_list":
            self.completeJob()
        elif name == "JB_job_number":
            self.job['job_number'] = int(value)
        elif name == "state":
            self.job['job_state'] = str(value)
        elif name == "JAT_start_time":
            self.job['start_time'] = str(value)
        elif name == 'slots':
            self.job['slots'] = int(value)

    def completeJob(self):
        self.jobs.append(self.job)
        self.job = {}

    def characters(self, ch):
        self.buffer.append(ch)


def _parse_job_details(fp):
    """ Parse a file-like object using the JobDetails XML parser and return the dictionary of job details """
    details = JobDetails()
    output = _fix_detail_output(fp.read())
    xml.sax.parseString(output, details)
    return details.jobs


def _parse_jobs(fp):
    """ Parse a file-like object using the Jobs XML parser and return the dictionary of jobs """
    jobs = Jobs()
    output = _fix_detail_output(fp.read())
    xml.sax.parseString(output, jobs)
    return jobs.jobs


def _has_jobs():
    output = subprocess.Popen('. /etc/cluster-setup.sh && qstat -u "*" | wc -l', shell=True, stdout=subprocess.PIPE)
    qstatLines = int(output.stdout.read())
    if qstatLines <= 2:
        return False
    return True


def get_sge_job_details(parse=True):
    class JobDetails:
        def __init__(self, p=None):
            self._process = p
        def parse(self):
            if self._process:
                return _parse_job_details(self._process.stdout)
            else:
                return {}

    if _has_jobs():
        details = JobDetails(subprocess.Popen('. /etc/cluster-setup.sh && qstat -u "*" -j "*" -xml', shell=True, stdout=subprocess.PIPE))
    else:
        details = JobDetails()

    if parse:
        return details.parse()
    else:
        return details

def get_sge_jobs(parse=True):
    class Jobs:
        def __init__(self, p=None):
            self._process = p 
        def parse(self):
            if self._process:
                return _parse_jobs(self._process.stdout)
            else:
                return []
    
    if _has_jobs():
        jobs = Jobs(subprocess.Popen('. /etc/cluster-setup.sh && qstat -g d -xml -u "*"', shell=True, stdout=subprocess.PIPE))    
    else:
        jobs = Jobs()

    if parse:
        return jobs.parse()
    else:
        return jobs

def get_host_complexes(complexes=None):
    """ Get a dict of hostnames to complex values 

    Example: complexes=['slot_type', 'placement_group']

    {
        "ip-10-10-10-10": {'slot_type': 'execute', 'placement_group': 'group1'},
        "ip-11-11-11-11": {'slot_type': 'execute', 'placement_group': 'group2'}
    }
    """

    if not _has_jobs() or complexes is None:
        return {}
    if not isinstance(complexes, list):
        complexes = [complexes]

    cmd = ". /etc/cluster-setup.sh && qhost -F %s -xml" % " ".join(complexes)
    qhost = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    
    root = ET.fromstring(qhost.stdout.read())
    hosts = root.findall("./host[resourcevalue]")
    data = {}
    for host in hosts:
        data[host.get("name")] = {}
        for c in complexes:
            v = host.find("resourcevalue[@name='%s']" % c)
            if v is not None:
                data[host.get("name")][c] = v.text
            else:
                data[host.get("name")][c] = None
    return data
