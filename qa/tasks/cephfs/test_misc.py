
from unittest import SkipTest
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.orchestra.run import CommandFailedError
import errno

class TestMisc(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    def test_getattr_caps(self):
        """
        Check if MDS recognizes the 'mask' parameter of open request.
        The paramter allows client to request caps when opening file
        """

        if not isinstance(self.mount_a, FuseMount):
            raise SkipTest("Require FUSE client")

        # Enable debug. Client will requests CEPH_CAP_XATTR_SHARED
        # on lookup/open
        self.mount_b.umount_wait()
        self.set_conf('client', 'client debug getattr caps', 'true')
        self.mount_b.mount()
        self.mount_b.wait_until_mounted()

        # create a file and hold it open. MDS will issue CEPH_CAP_EXCL_*
        # to mount_a
        p = self.mount_a.open_background("testfile")
        self.mount_b.wait_for_visible("testfile")

        # this tiggers a lookup request and an open request. The debug
        # code will check if lookup/open reply contains xattrs
        self.mount_b.run_shell(["cat", "testfile"])

        self.mount_a.kill_background(p)

    def test_fs_new(self):
        data_pool_name = self.fs.get_data_pool_name()

        self.fs.mds_stop()
        self.fs.mds_fail()

        self.fs.mon_manager.raw_cluster_cmd('fs', 'rm', self.fs.name,
                                            '--yes-i-really-mean-it')

        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'delete',
                                            self.fs.metadata_pool_name,
                                            self.fs.metadata_pool_name,
                                            '--yes-i-really-really-mean-it')
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                            self.fs.metadata_pool_name,
                                            self.fs.get_pgs_per_fs_pool().__str__())

        dummyfile = '/etc/fstab'

        self.fs.put_metadata_object_raw("key", dummyfile)

        timeout = 10

        get_pool_df = self.fs.get_pool_df
        self.wait_until_true(
                lambda: get_pool_df(self.fs.metadata_pool_name)['objects'] > 0,
                timeout=timeout)

        try:
            self.fs.mon_manager.raw_cluster_cmd('fs', 'new', self.fs.name,
                                                self.fs.metadata_pool_name,
                                                data_pool_name)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            raise AssertionError("Expected EINVAL")

        self.fs.mon_manager.raw_cluster_cmd('fs', 'new', self.fs.name,
                                            self.fs.metadata_pool_name,
                                            data_pool_name, "--force")

        self.fs.mon_manager.raw_cluster_cmd('fs', 'rm', self.fs.name,
                                            '--yes-i-really-mean-it')


        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'delete',
                                            self.fs.metadata_pool_name,
                                            self.fs.metadata_pool_name,
                                            '--yes-i-really-really-mean-it')
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                            self.fs.metadata_pool_name,
                                            self.fs.get_pgs_per_fs_pool().__str__())
        self.fs.mon_manager.raw_cluster_cmd('fs', 'new', self.fs.name,
                                            self.fs.metadata_pool_name,
                                            data_pool_name)
