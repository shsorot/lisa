# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
import os
import string
import sys
from pathlib import Path
from typing import Any, Dict, cast

myDir = os.getcwd()
sys.path.append(myDir)
path = Path(myDir)
a = str(path.parent.absolute())
sys.path.append(a)

from lisa import (
    Logger,
    Node,
    RemoteNode,
    SkippedException,
    TestCaseMetadata,
    TestSuite,
    TestSuiteMetadata,
    UnsupportedDistroException,
    schema,
    search_space,
    simple_requirement,
)
from lisa.features import Disk, Nvme
from lisa.operating_system import BSD, Oracle, Redhat, Windows
from lisa.platform_ import PlatformMessage, load_platform
from lisa.sut_orchestrator import AZURE
from lisa.sut_orchestrator.azure.features import AzureFileShare
from lisa.sut_orchestrator.azure.platform_ import AzurePlatform
from lisa.testsuite import TestResult
from lisa.tools import Echo, FileSystem, KernelConfig, Mkfs, Mount, Parted
from lisa.util import BadEnvironmentStateException, generate_random_chars
from xfstestingazfiles.testsuites.xfstests.xfstests import Xfstests

# Constants
_scratch_folder = "/root/scratch"
_test_folder = "/root/test"

ENVIRONMENT_KEEP_ALWAYS = "always"
ENVIRONMENT_KEEP_NO = "no"
ENVIRONMENT_KEEP_FAILED = "failed"


def _prepare_data_disk(
    node: Node,
    disk_name: str,
    disk_mount: Dict[str, str],
    file_system: FileSystem = FileSystem.xfs,
) -> None:
    mount = node.tools[Mount]
    parted = node.tools[Parted]
    mkfs = node.tools[Mkfs]

    for disk, mount_point in disk_mount.items():
        mount.umount(disk, mount_point)

    parted.make_label(disk_name)
    parted.make_partition(disk_name, "primary", "1", "50%")
    node.execute("sync")
    parted.make_partition(disk_name, "secondary", "50%", "100%")
    node.execute("sync")

    for disk, mount_point in disk_mount.items():
        mkfs.format_disk(disk, file_system)
        node.execute(f"mkdir {mount_point}", sudo=True)


def _get_smb_version(node: Node) -> str:
    if node.tools[KernelConfig].is_enabled("CONFIG_CIFS_SMB311"):
        version = "3.1.1"
    else:
        version = "3.0"
    return version


def _prepare_azure_file_share(
    node: Node,
    account_credential: Dict[str, str],
    test_folders_share_dict: Dict[str, str],
    fstab_info: str,
) -> None:
    folder_path = node.get_pure_path("/etc/smbcredentials")
    if node.shell.exists(folder_path):
        node.execute(f"rm -rf {folder_path}", sudo=True)
    node.shell.mkdir(folder_path)
    file_path = node.get_pure_path("/etc/smbcredentials/lisa.cred")
    echo = node.tools[Echo]
    username = account_credential["account_name"]
    password = account_credential["account_key"]
    echo.write_to_file(f"username={username}", file_path, sudo=True, append=True)
    echo.write_to_file(f"password={password}", file_path, sudo=True, append=True)
    node.execute("cp -f /etc/fstab /etc/fstab_cifs", sudo=True)
    for folder_name, share in test_folders_share_dict.items():
        node.execute(f"mkdir {folder_name}", sudo=True)
        echo.write_to_file(
            f"{share} {folder_name} cifs {fstab_info}",
            node.get_pure_path("/etc/fstab"),
            sudo=True,
            append=True,
        )


@TestSuiteMetadata(
    area="storage",
    category="community",
    description="""
    This test suite is to validate different types of data disk on Linux VM
     using xfstests.
    """,
)
class Xfstesting(TestSuite):
    # Use xfstests benchmark to test the different types of data disk,
    #  it will run many cases, so the runtime is longer than usual case.
    TIME_OUT = 14400
    # TODO: will include btrfs/244 once the kernel contains below fix.
    # exclude btrfs/244 temporarily for below commit not picked up by distro vendor.
    # https://git.kernel.org/pub/scm/linux/kernel/git/next/linux-next.git/commit/fs/btrfs/volumes.c?id=e4571b8c5e9ffa1e85c0c671995bd4dcc5c75091 # noqa: E501
    # TODO: will include ext4/054 once the kernel contains below fix.
    # This is a regression test for three kernel commit:
    # 1. 0f2f87d51aebc (ext4: prevent partial update of the extent blocks)
    # 2. 9c6e071913792 (ext4: check for inconsistent extents between index
    #    and leaf block)
    # 3. 8dd27fecede55 (ext4: check for out-of-order index extents in
    #    ext4_valid_extent_entries())
    # TODO: will include ext4/058 once the kernel contains below fix.
    # Regression test for commit a08f789d2ab5 ext4: fix bug_on ext4_mb_use_inode_pa
    # TODO: will include ext4/059 once the kernel contains below fix.
    # A regression test for b55c3cd102a6 ("ext4: add reserved GDT blocks check")
    # xfs/081 case will hung for long time
    # a1de97fe296c ("xfs: Fix the free logic of state in xfs_attr_node_hasname")
    # ext4/056 will trigger OOPS, reboot the VM, miss below kernel patch
    # commit b1489186cc8391e0c1e342f9fbc3eedf6b944c61
    # ext4: add check to prevent attempting to resize an fs with sparse_super2
    # VM will hung during running case xfs/520
    # commit d0c7feaf8767 ("xfs: add agf freeblocks verify in xfs_agf_verify")
    # generic/738 case might cause hang more than 4 hours on old kernel
    # TODO: will figure out the detailed reason of every excluded case.
    # exclude generic/680 for security reason.
    excluded_tests = (
        "generic/211 generic/430 generic/431 generic/434 generic/738 xfs/438 xfs/490"
        + " btrfs/007 btrfs/178 btrfs/244 btrfs/262"
        + " xfs/030 xfs/032 xfs/050 xfs/052 xfs/106 xfs/107 xfs/122 xfs/132 xfs/138"
        + " xfs/144 xfs/148 xfs/175 xfs/191-input-validation xfs/289 xfs/293 xfs/424"
        + " xfs/432 xfs/500 xfs/508 xfs/512 xfs/514 xfs/515 xfs/516 xfs/518 xfs/521"
        + " xfs/528 xfs/544 ext4/054 ext4/056 ext4/058 ext4/059 xfs/081 xfs/520"
        + " generic/680"
    )
    excluded_smb3_tests = (
        "generic/011 generic/020 generic/023 generic/035 generic/037"
        + " generic/062 generic/070 generic/071 generic/074 generic/075 generic/081 generic/087"
        + " generic/088 generic/089 generic/091 generic/097 generic/117 generic/120 generic/126"
        + " generic/127 generic/130 generic/131 generic/184 generic/192 generic/193 generic/209"
        + " generic/236 generic/237 generic/245 generic/258  generic/263 generic/270 generic/277"
        + " generic/313 generic/314 generic/294 generic/306 generic/313 generic/314 generic/317"
        + " generic/318 generic/319 generic/337 generic/377 generic/379 generic/380 generic/381"
        + " generic/382 generic/383 generic/385 generic/387 generic/388 generic/390 generic/392"
        + " generic/393 generic/395 generic/396 generic/397 generic/398 generic/399 generic/401"
        + " generic/402 generic/403 generic/405 generic/406 generic/409 generic/410 generic/411"
        + " generic/412 generic/413 generic/414 generic/415 generic/416 generic/417 generic/419"
        + " generic/421 generic/422 generic/423 generic/424 generic/425 generic/427 generic/429"
        + " generic/430 generic/431 generic/434 generic/435 generic/439 generic/440 generic/441"
        + " generic/442 generic/446 generic/447 generic/449 generic/452 generic/453 generic/454"
        + " generic/455 generic/456 generic/457 generic/458 generic/459 generic/460 generic/461"
        + " generic/462 generic/464 generic/466"
    )

    excluded_smb2_tests = ()
    _fs_sku: str = ""
    _fs_kind: str = ""
    _smb_version: str = ""

    def before_case(self, log: Logger, **kwargs: Any) -> None:
        node = kwargs["node"]
        if isinstance(node.os, Oracle) and (node.os.information.version <= "9.0.0"):
            self.excluded_tests = self.excluded_tests + " btrfs/299"

        # code for cifs tests only
        variables: Dict[str, Any] = kwargs["variables"]
        self._fs_sku = variables.get("fs_sku", "Standard_LRS")
        # Use this to set storage account type
        self._fs_kind = variables.get("fs_kind", "StorageV2")
        # Note: we fetch SMB version to check from user instead of using kernel function.
        self._smb_version = variables.get("smb_version", "3.1.1")

    @TestCaseMetadata(
        description="""
        This test case will run cifs xfstests testing against
        azure file share. Accepted protocols are SMB3.11,3.0 and 2.0
        currently.
        TODO: add support domain joined share
        """,
        requirement=simple_requirement(
            min_core_count=16,
            supported_platform_type=[AZURE],
            unsupported_os=[BSD, Windows],
        ),
        timeout=TIME_OUT,
        use_new_environment=True,
        priority=1,
    )
    def verify_azure_file_share(
        self, log: Logger, log_path: Path, result: TestResult
    ) -> None:
        environment = result.environment
        assert environment, "fail to get environment from testresult"
        assert isinstance(environment.platform, AzurePlatform)
        node = cast(RemoteNode, environment.nodes[0])
        if not node.tools[KernelConfig].is_enabled("CONFIG_CIFS"):
            raise UnsupportedDistroException(
                node.os, "current distro not enable cifs module."
            )
        xfstests = self._install_xfstests(node)

        azure_file_share = node.features[AzureFileShare]
        # Deprecated, CONFIG_CIFS_SMB311  is no longer used in kernel, instead replaced with CONFIG_CIFS=y
        # version = azure_file_share.get_smb_version()
        mount_opts = (
            f"-o vers={self._smb_version},credentials=/etc/smbcredentials/lisa.cred"
            ",dir_mode=0777,file_mode=0777,serverino"
        )

        random_str = generate_random_chars(string.ascii_lowercase + string.digits, 10)
        file_share_name = f"lisa{random_str}fs"
        scratch_name = f"lisa{random_str}scratch"

        # fs_url_dict: Dict[str, str] = {file_share_name: "", scratch_name: ""}
        try:
            log.info(
                f"Creating storage account with name: {file_share_name}, type: {self._fs_kind}, sku: {self._fs_sku}"
            )
            fs_url_dict = azure_file_share.create_file_share(
                file_share_names=[file_share_name, scratch_name],
                environment=environment,
                sku=self._fs_sku,
                kind=self._fs_kind,
                allow_shared_key_access=True,
            )
            test_folders_share_dict = {
                _test_folder: fs_url_dict[file_share_name],
                _scratch_folder: fs_url_dict[scratch_name],
            }
            azure_file_share.create_fileshare_folders(test_folders_share_dict)

            self._execute_xfstests(
                log_path,
                xfstests,
                result,
                test_dev=fs_url_dict[file_share_name],
                scratch_dev=fs_url_dict[scratch_name],
                excluded_tests=self.excluded_tests + self.excluded_smb3_tests,
                mount_opts=mount_opts,
            )
        finally:
            # azure_file_share.delete_azure_fileshare([file_share_name, scratch_name])
            # TODO: Figure out error code on failure so that this share can be preserved for inspection.
            log.info(f"Deleting storage account with name: {file_share_name}")

    def after_case(self, log: Logger, **kwargs: Any) -> None:
        try:
            node: Node = kwargs.pop("node")
            for path in [
                "/dev/mapper/delay-test",
                "/dev/mapper/huge-test",
                "/dev/mapper/huge-test-zero",
            ]:
                if 0 == node.execute(f"ls -lt {path}", sudo=True).exit_code:
                    node.execute(f"dmsetup remove {path}", sudo=True)
            for mount_point in [_scratch_folder, _test_folder]:
                node.tools[Mount].umount("", mount_point, erase=False)
        except Exception as identifier:
            raise BadEnvironmentStateException(f"after case, {identifier}")

    def _execute_xfstests(
        self,
        log_path: Path,
        xfstests: Xfstests,
        result: TestResult,
        data_disk: str = "",
        test_dev: str = "",
        scratch_dev: str = "",
        file_system: FileSystem = FileSystem.xfs,
        test_type: str = "generic",
        excluded_tests: str = "",
        mount_opts: str = "",
    ) -> Any:
        environment = result.environment
        assert environment, "fail to get environment from testresult"

        node = cast(RemoteNode, environment.nodes[0])
        # TODO: will include generic/641 once the kernel contains below fix.
        # exclude this case generic/641 temporarily
        # it will trigger oops on RHEL8.3/8.4, VM will reboot
        # lack of commit 5808fecc572391867fcd929662b29c12e6d08d81
        if (
            test_type == "generic"
            and isinstance(node.os, Redhat)
            and node.os.information.version >= "8.3.0"
        ):
            excluded_tests += " generic/641"

        # prepare data disk when xfstesting target is data disk
        if data_disk:
            _prepare_data_disk(
                node,
                data_disk,
                {test_dev: _test_folder, scratch_dev: _scratch_folder},
                file_system=file_system,
            )

        xfstests.set_local_config(
            scratch_dev,
            _scratch_folder,
            test_dev,
            _test_folder,
            test_type,
            mount_opts,
        )
        xfstests.set_excluded_tests(excluded_tests)
        # Reduce run_test timeout by 30s to let it complete before case Timeout
        # wait_processes interval in run_test is 10s, set to 30 for safety check
        xfstests.run_test(test_type, log_path, result, data_disk, self.TIME_OUT - 30)

    def _install_xfstests(self, node: Node) -> Xfstests:
        try:
            xfstests = node.tools[Xfstests]
            return xfstests
        except UnsupportedDistroException as identifier:
            raise SkippedException(identifier)

    def _check_btrfs_supported(self, node: Node) -> None:
        if not node.tools[KernelConfig].is_enabled("CONFIG_BTRFS_FS"):
            raise SkippedException("Current distro doesn't support btrfs file system.")
