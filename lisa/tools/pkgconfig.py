# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from typing import Dict, Optional

from assertpy import assert_that
from semver import VersionInfo

from lisa.executable import Tool
from lisa.operating_system import Posix
from lisa.util import parse_version


class Pkgconfig(Tool):
    @property
    def command(self) -> str:
        return "pkg-config"

    @property
    def can_install(self) -> bool:
        return True

    def install(self) -> bool:
        assert isinstance(self.node.os, Posix)
        self.node.os.install_packages("pkg-config")
        return True

    def package_info_exists(
        self, package_name: str, update_envs: Optional[Dict[str, str]] = None
    ) -> bool:
        package_info_result = self.run(
            f"--modversion {package_name}", force_run=True, update_envs=update_envs
        )
        return package_info_result.exit_code == 0

    def get_package_info(
        self,
        package_name: str,
        update_cached: bool = False,
        update_envs: Optional[Dict[str, str]] = None,
    ) -> str:
        info_exists = self.package_info_exists(
            package_name=package_name, update_envs=update_envs
        )
        assert_that(info_exists).described_as(
            (
                f"pkg-config information was not available for {package_name}. "
                "This indicates an installation or package detection bug. "
                f"ensure .pc file is available for {package_name} on this OS."
            )
        ).is_true()
        return self.run(f"--modversion {package_name}", force_run=update_cached).stdout

    def get_package_version(
        self,
        package_name: str,
        update_cached: bool = False,
        update_envs: Optional[Dict[str, str]] = None,
    ) -> VersionInfo:
        version_info = self.get_package_info(
            package_name, update_cached=update_cached, update_envs=update_envs
        )
        return parse_version(version_info)
