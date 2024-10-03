# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from datetime import datetime
from pathlib import PurePath
from typing import Any, Callable, Dict, List, Optional, Sequence, Type, Union

from assertpy import assert_that
from semver import VersionInfo
from urllib3.util.url import parse_url

from lisa import Node
from lisa.executable import Tool
from lisa.operating_system import Debian, Fedora, Oracle, Posix, Redhat, Suse, Ubuntu
from lisa.tools import Git, Tar, Wget
from lisa.util import UnsupportedDistroException

DPDK_STABLE_GIT_REPO = "https://dpdk.org/git/dpdk-stable"

# azure routing table magic subnet prefix
# signals 'route all traffic on this subnet'
AZ_ROUTE_ALL_TRAFFIC = "0.0.0.0/0"


# Attempt to clean up the DPDK package dependency mess
# Make a Installer class that implements the common steps
# for installing DPDK/rdma-core, either from source or the package manager.
# This generic class will get implemented in DpdkTestpmd and RdmaCore.
# This should help us cover the various installation cases in a nice way,
# and allow us to only re-implement the bits we need for each project.
class OsPackageDependencies:
    # A class to reduce the isinstance() trees that are
    # sprinkled everywhere.
    # Caller provides a function to match an OS and
    # the packages to install on that OS.
    def __init__(
        self,
        matcher: Callable[[Posix], bool],
        packages: Optional[Sequence[Union[str, Tool, Type[Tool]]]] = None,
        stop_on_match: bool = False,
    ) -> None:
        self.matcher = matcher
        self.packages = packages
        self.stop_on_match = stop_on_match


class DependencyInstaller:
    # provide a list of OsPackageDependencies for a project
    def __init__(self, requirements: List[OsPackageDependencies]) -> None:
        self.requirements = requirements

    # evaluate the list of package dependencies,
    def install_required_packages(
        self, node: Node, extra_args: Union[List[str], None]
    ) -> None:
        os = node.os
        assert isinstance(os, Posix), (
            "DependencyInstaller is not compatible with this OS: "
            f"{os.information.vendor} {os.information.release}"
        )
        # find the match for an OS, install the packages.
        # stop on list end or if exclusive_match parameter is true.
        for requirement in self.requirements:
            if requirement.matcher(os) and requirement.packages:
                os.install_packages(requirement.packages, extra_args=extra_args)
                if requirement.stop_on_match:
                    return
        # NOTE: It is up to the caller to raise an exception on an invalid OS


class Downloader:
    def __init__(self, node: Node) -> None:
        self._node = node

    def download(self) -> PurePath:
        raise NotImplementedError("Downloader not implemented.")


class GitDownloader(Downloader):
    _git_repo: str = ""
    _git_ref: str = ""

    def __init__(
        self,
        node: Node,
        git_repo: str,
        git_ref: str,
    ) -> None:
        super().__init__(node)
        self._git_repo = git_repo
        self._git_ref = git_ref

    # checkout the git repository into the working path
    def download(self) -> PurePath:
        # NOTE: fail on exists is set to True.
        # The expectation is that the parent Installer class should
        # remove any lingering installations
        self._asset_path = self._node.tools[Git].clone(
            self._git_repo,
            cwd=self._node.get_working_path(),
            ref=self._git_ref,
            fail_on_exists=True,
        )
        return self._asset_path


# parent class for tarball source installations
class TarDownloader(Downloader):
    def __init__(
        self,
        node: Node,
        tar_url: str,
    ) -> None:
        super().__init__(node)
        self._tar_url = tar_url
        self._is_remote_tarball = tar_url.startswith("https://")

    # fetch the tarball (or copy it to the node)
    # then extract it
    def download(self) -> PurePath:
        node = self._node
        work_path = self._node.get_working_path()
        is_tarball = False
        for suffix in [".tar.gz", ".tar.bz2", ".tar"]:
            if self._tar_url.endswith(suffix):
                is_tarball = True
                tarfile_suffix = suffix
                break
        assert_that(is_tarball).described_as(
            (
                "Source path is not a .tar[.gz|.bz2] file. "
                f"Tar url was set to: {self._tar_url} "
            )
        ).is_true()
        if self._is_remote_tarball:
            tarfile = node.tools[Wget].get(
                self._tar_url,
                file_path=str(work_path),
                overwrite=False,
                force_run=True,
            )
            remote_path = node.get_pure_path(tarfile)
            self.tar_filename = remote_path.name
        else:
            self.tar_filename = PurePath(self._tar_url).name
            remote_path = work_path.joinpath(self.tar_filename)
            node.shell.copy(
                local_path=PurePath(self._tar_url),
                node_path=remote_path,
            )
        # create tarfile dest dir
        self._asset_path = work_path.joinpath(
            self.tar_filename[: -(len(tarfile_suffix))]
        )
        # unpack into the dest dir
        # force name as tarfile name
        node.tools[Tar].extract(
            file=str(remote_path),
            dest_dir=str(work_path),
            gzip=True,
        )
        return self._asset_path


class Installer:
    # Generic 'Installer' parent class for DpdkTestpmd/rdma-core
    # NOTE: This should not be instantiated directly.
    _err_msg = "not implemented for this installation type."

    # setup the node before starting
    # ex: updating the kernel, enabling features, checking drivers, etc.
    def _setup_node(self) -> None:
        raise NotImplementedError(f"_setup_node {self._err_msg}")

    # check if the package is already installed:
    # Is the package installed from source? Or from the package manager?
    # Does the version match the one we want if we need a specific one?
    def _check_if_installed(self) -> bool:
        raise NotImplementedError(f"_check_if_installed {self._err_msg}")

    # setup the installation (install Ninja, Meson, etc)
    def _download_assets(self) -> None:
        if self._downloader:
            self._asset_path = self._downloader.download()
        else:
            self._node.log.debug("No downloader assigned to installer.")

    # do the build and installation
    def _install(self) -> None:
        self._download_assets()

    # remove an installation
    def _uninstall(self) -> None:
        raise NotImplementedError(f"_clean_previous_installation {self._err_msg}")

    # install the dependencies
    def _install_dependencies(self) -> None:
        if self._os_dependencies is not None:
            self._os_dependencies.install_required_packages(
                self._node, extra_args=self._package_manager_extra_args
            )

    # define how to check the installed version
    def get_installed_version(self) -> VersionInfo:
        raise NotImplementedError(f"get_installed_version {self._err_msg}")

    def _should_install(self, required_version: Optional[VersionInfo] = None) -> bool:
        return (not self._check_if_installed()) or (
            required_version is None and required_version > self.get_installed_version()
        )

    # run the defined setup and installation steps.
    def do_installation(self, required_version: Optional[VersionInfo] = None) -> None:
        self._setup_node()
        if self._should_install():
            self._uninstall()
            self._install_dependencies()
            self._install()

    def __init__(
        self,
        node: Node,
        os_dependencies: Optional[DependencyInstaller] = None,
        downloader: Optional[Downloader] = None,
    ) -> None:
        self._node = node
        if not isinstance(self._node.os, Posix):
            raise UnsupportedDistroException(
                self._node.os, "Installer parent class requires Posix OS."
            )
        self._os: Posix = self._node.os
        self._package_manager_extra_args: List[str] = []
        self._os_dependencies = os_dependencies
        self._downloader = downloader


# Base class for package manager installation
class PackageManagerInstall(Installer):
    def __init__(self, node: Node, os_dependencies: DependencyInstaller) -> None:
        super().__init__(node, os_dependencies)

    # uninstall from the package manager
    def _uninstall(self) -> None:
        if not (isinstance(self._os, Posix) and self._check_if_installed()):
            return
        if self._os_dependencies is not None:
            for os_package_check in self._os_dependencies.requirements:
                if os_package_check.matcher(self._os) and os_package_check.packages:
                    self._os.uninstall_packages(os_package_check.packages)

    # verify packages on the node have been installed by
    # the package manager
    def _check_if_installed(self) -> bool:
        # WARNING: Don't use this for long lists of packages.
        # For dpdk, pkg-manager install is only for 'dpdk' and 'dpdk-dev'
        # This will take too long if it's more than a few packages.
        if self._os_dependencies is not None:
            for os_package_check in self._os_dependencies.requirements:
                if os_package_check.matcher(self._os) and os_package_check.packages:
                    for pkg in os_package_check.packages:
                        if not self._os.package_exists(pkg):
                            return False
        return True

    # installing dependencies is the installation in this case, so just return
    def _install(self) -> None:
        return


def force_dpdk_default_source(variables: Dict[str, Any]) -> None:
    if not variables.get("dpdk_source", None):
        variables["dpdk_source"] = DPDK_STABLE_GIT_REPO


# rough check for ubuntu supported versions.
# assumes:
# - canonical convention of YEAR.MONTH for major versions
# - canoical release cycle of EVEN_YEAR.04 for lts versions.
# - 4 year support cycle. 6 year for ESM
# get the age of the distro, if negative or 0, release is new.
# if > 6, distro is out of support
def is_ubuntu_lts_version(distro: Ubuntu) -> bool:
    # asserts if not ubuntu OS object
    version_info = distro.information.version
    distro_age = _get_ubuntu_distro_age(distro)
    is_even_year = (version_info.major % 2) == 0
    is_april_release = version_info.minor == 4
    is_within_support_window = distro_age <= 6
    return is_even_year and is_april_release and is_within_support_window


def is_ubuntu_latest_or_prerelease(distro: Ubuntu) -> bool:
    distro_age = _get_ubuntu_distro_age(distro)
    return distro_age <= 2


def _get_ubuntu_distro_age(distro: Ubuntu) -> int:
    version_info = distro.information.version
    # check release is within esm window
    year_string = str(datetime.today().year)
    assert_that(len(year_string)).described_as(
        "Package bug: The year received from datetime module is an "
        "unexpected size. This indicates a broken package or incorrect "
        "date in this computer."
    ).is_greater_than_or_equal_to(4)
    # TODO: handle the century rollover edge case in 2099
    current_year = int(year_string[-2:])
    release_year = int(version_info.major)
    # 23-18 == 5
    # long term support and extended security updates for ~6 years
    return current_year - release_year


def check_dpdk_support(node: Node) -> None:
    # check requirements according to:
    # https://docs.microsoft.com/en-us/azure/virtual-network/setup-dpdk
    supported = False
    if isinstance(node.os, Debian):
        if isinstance(node.os, Ubuntu):
            node.log.debug(
                "Checking Ubuntu release: "
                f"is_latest_or_prerelease? ({is_ubuntu_latest_or_prerelease(node.os)})"
                f" is_lts_version? ({is_ubuntu_lts_version(node.os)})"
            )
            # TODO: undo special casing for 18.04 when it's usage is less common
            supported = (
                node.os.information.version == "18.4.0"
                or is_ubuntu_latest_or_prerelease(node.os)
                or is_ubuntu_lts_version(node.os)
            )
        else:
            supported = node.os.information.version >= "11.0.0"
    elif isinstance(node.os, Redhat) and not isinstance(node.os, Oracle):
        supported = node.os.information.version >= "7.5.0"
    elif isinstance(node.os, Suse):
        supported = node.os.information.version >= "15.0.0"
    else:
        # this OS is not supported
        raise UnsupportedDistroException(
            node.os, "This OS is not supported by the DPDK test suite for Azure."
        )
    # verify MANA driver is available for the kernel version
    if (
        isinstance(node.os, (Debian, Fedora, Suse, Fedora))
        and node.nics.is_mana_device_present()
    ):
        # NOTE: Kernel backport examples are available for lower kernels.
        # HOWEVER: these are not suitable for general testing and should be installed
        # in the image _before_ starting the test.
        # ex: make a SIG image first using the kernel build transformer.
        if node.os.get_kernel_information().version < "5.15.0":
            raise UnsupportedDistroException(
                node.os, "MANA driver is not available for kernel < 5.15"
            )
    if not supported:
        raise UnsupportedDistroException(
            node.os, "This OS version is EOL and is not supported for DPDK on Azure"
        )


def is_url_for_tarball(url: str) -> bool:
    return ".tar" in PurePath(url).suffixes


def is_url_for_git_repo(url: str) -> bool:
    parsed_url = parse_url(url)
    scheme = parsed_url.scheme
    path = parsed_url.path
    if not (scheme and path):
        return False
    # investigate the rest of the URL as a path
    path_check = PurePath(path)
    check_for_git_https = scheme in ["http", "https"] and (
        path_check.suffixes == [".git"]
        or any([x in path_check.parts for x in ["git", "_git"]])
    )
    return scheme == "git" or check_for_git_https


def unsupported_os_thrower(os: Posix) -> bool:
    raise UnsupportedDistroException(
        os,
        message=("Installer did not define dependencies for this os."),
    )
