import unittest
import subprocess


class TestSimple(unittest.TestCase):

    def setUp(self):
        self.userhome = "/shared/home/cluster.user"

    def test_execute_nfs_mounts_present(self):
        output = subprocess.check_output(['mount', '-l', '-t', 'nfs'])
        self.assertTrue('/shared type nfs' in output, 'NFS mount for /shared missing')
        self.assertTrue('/sched type nfs' in output, 'NFS mount for /sched missing')

    def test_execute_connect_master(self):
        hostname = subprocess.check_output(['hostname', '-s']).strip()
        env = {'SGE_ROOT': '/sched/sge/sge-2011.11', 'SGE_QMASTER_PORT': '537'} 
        p = subprocess.Popen('/sched/sge/sge-2011.11/bin/linux-x64/qhost', stderr=subprocess.PIPE,
                             stdout=subprocess.PIPE, env=env)
        stdout, stderr = p.communicate()
        error_message = 'Not able to connect to SGE master.' + \
                        'Return code was %d\nStdout: %s\nStderr:%s' % (p.returncode, stdout, stderr)
        self.assertEqual(0, p.returncode, error_message)

        # converting to lowercase, because windows hostnames randomly are upper case or
        # lowercase the status output
        self.assertTrue(hostname.lower() in stdout.lower(),
                        'Current hostname %s not found in qhost output\nStdout: %s'
                        % (hostname, stdout))
