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
from lisa.util import BadEnvironmentStateException, LisaException, generate_random_chars
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
        + " generic/680 generic/476 generic/551 generic/598 generic/642 generic/643"
    )
    excluded_smb3_tests = (
        "cifs/001 generic/002 generic/089 generic/236 generic/378 generic/480 generic/498"
        + " generic/502 generic/526 generic/527 generic/597generic/023 generic/035 generic/037"
        + " generic/126 generic/127 generic/193 generic/213 generic/245 generic/309 generic/314"
        + " generic/355 generic/453 generic/468 generic/471 generic/495 generic/623 generic/647"
        + " generic/003 generic/192 generic/004 generic/389 generic/509 generic/530 generic/531"
        + " generic/008 generic/009 generic/011 generic/610 generic/012 generic/015 generic/019"
        + " generic/027 generic/034 generic/039 generic/040 generic/041 generic/050 generic/056"
        + " generic/057 generic/059 generic/065 generic/066 generic/067 generic/073 generic/076"
        + " generic/081 generic/083 generic/090 generic/096 generic/101 generic/102 generic/104"
        + " generic/106 generic/107 generic/108 generic/114 generic/204 generic/218 generic/223"
        + " generic/224 generic/226 generic/250 generic/252 generic/269 generic/273 generic/274"
        + " generic/275 generic/299 generic/300 generic/311 generic/312 generic/320 generic/321"
        + " generic/322 generic/325 generic/335 generic/336 generic/338 generic/341 generic/342"
        + " generic/343 generic/347 generic/348 generic/361 generic/371 generic/376 generic/388"
        + " generic/405 generic/409 generic/410 generic/411 generic/416 generic/418 generic/427"
        + " generic/441 generic/442 generic/455 generic/456 generic/459 generic/466 generic/470"
        + " generic/475 generic/481 generic/482 generic/483 generic/484 generic/487 generic/488"
        + " generic/489 generic/500 generic/510 generic/512 generic/520 generic/534 generic/535"
        + " generic/536 generic/547 generic/552 generic/557 generic/558 generic/559 generic/560"
        + " generic/561 generic/562 generic/570 generic/589 generic/619 generic/620 generic/640"
        + " generic/016 generic/017 generic/018 generic/324 generic/021 generic/022 generic/351"
        + " generic/025 generic/078 generic/626 generic/026 generic/054 generic/077 generic/099"
        + " generic/105 generic/237 generic/307 generic/318 generic/319 generic/375 generic/444"
        + " generic/449 generic/529 generic/031 generic/033 generic/349 generic/038 generic/251"
        + " generic/260 generic/288 generic/537 generic/042 generic/032 generic/043 generic/044"
        + " generic/045 generic/046 generic/048 generic/049 generic/064 generic/225 generic/425"
        + " generic/436 generic/445 generic/448 generic/473 generic/490 generic/519 generic/052"
        + " generic/054 generic/055 generic/058 generic/060 generic/061 generic/063 generic/177"
        + " generic/255 generic/256 generic/316 generic/350 generic/392 generic/420 generic/439"
        + " generic/446 generic/469 generic/494 generic/503 generic/539 generic/567 generic/062"
        + " generic/401 generic/423 generic/434 generic/479 generic/564 generic/072 generic/079"
        + " generic/424 generic/555 generic/082 generic/219 generic/230 generic/231 generic/232"
        + " generic/233 generic/234 generic/235 generic/244 generic/270 generic/280 generic/379"
        + " generic/380 generic/381 generic/382 generic/383 generic/384 generic/385 generic/386"
        + " generic/400 generic/566 generic/587 generic/594 generic/600 generic/601 generic/603"
        + " generic/097 generic/403 generic/103 generic/110 generic/111 generic/115 generic/116"
        + " generic/134 generic/137 generic/138 generic/139 generic/140 generic/142 generic/143"
        + " generic/144 generic/145 generic/146 generic/147 generic/148 generic/149 generic/150"
        + " generic/151 generic/152 generic/153 generic/154 generic/155 generic/156 generic/157"
        + " generic/161 generic/162 generic/163 generic/164 generic/165 generic/166 generic/167"
        + " generic/168 generic/170 generic/171 generic/172 generic/173 generic/174 generic/175"
        + " generic/176 generic/178 generic/179 generic/180 generic/181 generic/183 generic/185"
        + " generic/186 generic/187 generic/188 generic/189 generic/190 generic/191 generic/194"
        + " generic/195 generic/196 generic/197 generic/199 generic/200 generic/201 generic/202"
        + " generic/203 generic/205 generic/206 generic/216 generic/217 generic/220 generic/222"
        + " generic/227 generic/229 generic/238 generic/242 generic/243 generic/253 generic/254"
        + " generic/259 generic/261 generic/262 generic/264 generic/265 generic/266 generic/267"
        + " generic/268 generic/271 generic/272 generic/276 generic/278 generic/279 generic/281"
        + " generic/282 generic/283 generic/284 generic/287 generic/289 generic/290 generic/291"
        + " generic/292 generic/293 generic/295 generic/296 generic/297 generic/298 generic/301"
        + " generic/302 generic/303 generic/305 generic/326 generic/327 generic/328 generic/329"
        + " generic/330 generic/331 generic/332 generic/333 generic/334 generic/352 generic/353"
        + " generic/356 generic/357 generic/358 generic/359 generic/373 generic/387 generic/404"
        + " generic/407 generic/414 generic/415 generic/447 generic/457 generic/458 generic/463"
        + " generic/485 generic/497 generic/499 generic/501 generic/513 generic/514 generic/515"
        + " generic/518 generic/540 generic/541 generic/542 generic/543 generic/544 generic/546"
        + " generic/578 generic/588 generic/612 generic/641 generic/648 generic/649 generic/121"
        + " generic/136 generic/158 generic/162 generic/163 generic/182 generic/304 generic/374"
        + " generic/408 generic/493 generic/516 generic/517 generic/630 generic/131 generic/571"
        + " generic/159 generic/160 generic/545 generic/553 generic/184 generic/277 generic/294"
        + " generic/362 generic/363 generic/364 generic/365 generic/366 generic/367 generic/368"
        + " generic/369 generic/370 generic/372 generic/395 generic/396 generic/397 generic/398"
        + " generic/399 generic/419 generic/421 generic/429 generic/435 generic/440 generic/548"
        + " generic/549 generic/550 generic/580 generic/581 generic/582 generic/583 generic/584"
        + " generic/592 generic/593 generic/595 generic/602 generic/613 generic/621 generic/402"
        + " generic/413 generic/462 generic/605 generic/606 generic/608 generic/426 generic/467"
        + " generic/477 generic/471 generic/472 generic/492 generic/496 generic/506 generic/522"
        + " generic/525 generic/554 generic/556 generic/563 generic/569 generic/572 generic/573"
        + " generic/574 generic/575 generic/576 generic/577 generic/579 generic/624 generic/625"
        + " generic/627 generic/628 generic/629 generic/585 generic/596 generic/607 generic/614"
        + " generic/616 generic/617 generic/622 generic/631 generic/633 generic/644 generic/645"
        + " generic/636 generic/020 generic/337 generic/377 generic/417 generic/474 generic/486"
        + " generic/508 generic/523 generic/533 generic/611 generic/618 generic/087 generic/088"
        + " generic/089 generic/097 generic/125 generic/092 generic/093 generic/094"
    )

    excluded_smb2_tests = ()
    _fs_sku: str = ""
    _fs_kind: str = ""
    _smb_version: str = ""
    _enable_private_endpoint: bool = False
    _remove_storage_account: bool = True

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
        self._enable_private_endpoint = variables.get("enable_private_endpoint", False)

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
        log.info(f"Using mount options for info.config : {mount_opts}")
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
                enable_private_endpoint=self._enable_private_endpoint,
            )
            test_folders_share_dict = {
                _test_folder: fs_url_dict[file_share_name],
                _scratch_folder: fs_url_dict[scratch_name],
            }
            log.info(
                f"Creating folders in storage account: {file_share_name} and {scratch_name}"
            )
            azure_file_share.create_fileshare_folders(test_folders_share_dict)

            log.info(f"Running xfstesting with test_type: cifs")
            self._execute_xfstests(
                log_path,
                xfstests,
                result,
                test_type="cifs",
                test_dev=fs_url_dict[file_share_name],
                scratch_dev=fs_url_dict[scratch_name],
                excluded_tests=self.excluded_tests + self.excluded_smb3_tests,
                mount_opts=mount_opts,
            )
        except LisaException:
            log.info("exception raised when running xfstesting")
            if environment.platform.runbook.keep_environment in ["always","failed"]:
                log.info("Environment will be preserved.")
                public_ip = node.public_address
                log.info(f"Machine name: {node.name}, Public IP: {public_ip}")
                log.info(f"Virtual machine name: {node.name}")
                log.info(f"Storage account name: {fs_url_dict}")
                for share_name, fqdn in fs_url_dict.items():
                    log.info(f"File share name: {share_name}, FQDN: {fqdn}")
            else:
                log.info("Environment will be cleaned based on keep_environment parameter")
        # finally:
        #     if self._remove_storage_account is True:
        #         log.info("Removing Storage account and file shares")
        #         azure_file_share.delete_azure_fileshare([file_share_name, scratch_name])
        #     else:
        #         log.info("Preserving storage account and file shares")

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
