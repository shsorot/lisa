# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import re
from dataclasses import dataclass, field
from functools import partial
from typing import Any, List, Type

from dataclasses_json import dataclass_json

from lisa import schema, search_space
from lisa.feature import Feature
from lisa.features import Disk
from lisa.operating_system import BSD
from lisa.schema import FeatureSettings
from lisa.tools import Ls, Lspci, Nvmecli
from lisa.tools.lspci import PciDevice
from lisa.util import field_metadata, get_matched_str
from lisa.util.constants import DEVICE_TYPE_NVME


class Nvme(Feature):
    # crw------- 1 root root 251, 0 Jun 21 03:08 /dev/nvme0
    # crw------- 1 root root 251, 0 Jun 21 03:08 /dev/nvme10
    _device_pattern = re.compile(r".*(?P<device_name>/dev/nvme[0-9]+$)", re.MULTILINE)
    # brw-rw---- 1 root disk 259, 0 Jun 21 03:08 /dev/nvme0n1
    # brw-rw---- 1 root disk 259, 0 Jun 21 03:08 /dev/nvme11n1
    _namespace_pattern = re.compile(
        r".*(?P<namespace>/dev/nvme[0-9]+n[0-9]+$)", re.MULTILINE
    )
    # '/dev/nvme0n1         351f1f720e5a00000001 Microsoft NVMe Direct Disk               1           0.00   B /   1.92  TB    512   B +  0 B   NVMDV001' # noqa: E501
    # '/dev/nvme11n1        351f1f720e5a00000001 Microsoft NVMe Direct Disk               1           0.00   B /   1.92  TB    512   B +  0 B   NVMDV001' # noqa: E501
    _namespace_cli_pattern = re.compile(
        r"(?P<namespace>/dev/nvme[0-9]+n[0-9])", re.MULTILINE
    )

    # crw-------  1 root  wheel  0x4e Jul 27 21:16 /dev/nvme0ns1
    # crw-------  1 root  wheel  0x4e Jul 27 21:16 /dev/nvme12ns1
    _namespace_pattern_bsd = re.compile(
        r".*(?P<namespace>/dev/nvme[0-9]+ns[0-9]+$)", re.MULTILINE
    )

    # /dev/nvme0n1p15 -> /dev/nvme0n1
    NVME_NAMESPACE_PATTERN = re.compile(r"/dev/nvme[0-9]+n[0-9]+", re.M)

    # /dev/nvme0n1p15 -> /dev/nvme0n1
    NVME_DEVICE_PATTERN = re.compile(r"/dev/nvme[0-9]+", re.M)

    _pci_device_name = "Non-Volatile memory controller"
    _ls_devices: str = ""

    @classmethod
    def settings_type(cls) -> Type[schema.FeatureSettings]:
        return NvmeSettings

    @classmethod
    def can_disable(cls) -> bool:
        return True

    def enabled(self) -> bool:
        return True

    def get_devices(self) -> List[str]:
        devices_list = []
        self._get_device_from_ls()
        for row in self._ls_devices.splitlines():
            matched_result = self._device_pattern.match(row)
            if matched_result:
                devices_list.append(matched_result.group("device_name"))
        return self._remove_nvme_os_disk(devices_list)

    def get_namespaces(self) -> List[str]:
        namespaces = []
        self._get_device_from_ls()
        for row in self._ls_devices.splitlines():
            if isinstance(self._node.os, BSD):
                matched_result = self._namespace_pattern_bsd.match(row)
            else:
                matched_result = self._namespace_pattern.match(row)
            if matched_result:
                namespaces.append(matched_result.group("namespace"))
        return self._remove_nvme_os_disk(namespaces)

    # With disk controller type NVMe, OS disk along with all remote iSCSI devices
    # appears as NVMe.
    # Removing OS disk from the list of NVMe devices will remove all the
    # remote non-NVME disks.
    def _remove_nvme_os_disk(self, disk_list: List[str]) -> List[str]:
        if (
            self._node.features[Disk].get_os_disk_controller_type()
            == schema.DiskControllerType.NVME
        ):
            os_disk_nvme_device = self._get_os_disk_nvme_device()
            # Removing OS disk/device from the list.
            for disk in disk_list:
                if os_disk_nvme_device in disk:
                    disk_list.remove(disk)
                    break
        return disk_list

    def get_namespaces_from_cli(self) -> List[str]:
        namespaces_list = self._node.tools[Nvmecli].get_namespaces()
        return self._remove_nvme_os_disk(namespaces_list)

    def get_os_disk_nvme_namespace(self) -> str:
        node_disk = self._node.features[Disk]
        os_partition_namespace = ""
        os_boot_partition = node_disk.get_os_boot_partition()
        # Sample os_boot_partition when disc controller type is NVMe:
        # name: /dev/nvme0n1p15, disk: nvme, mount_point: /boot/efi, type: vfat
        if os_boot_partition:
            os_partition_namespace = get_matched_str(
                os_boot_partition.name,
                self.NVME_NAMESPACE_PATTERN,
            )
        return os_partition_namespace

    # This method returns NVMe device name of the OS disk.
    def _get_os_disk_nvme_device(self) -> str:
        os_disk_nvme_device = ""
        os_disk_nvme_namespace = self.get_os_disk_nvme_namespace()
        # Sample os_boot_partition when disc controller type is NVMe:
        # name: /dev/nvme0n1p15, disk: nvme, mount_point: /boot/efi, type: vfat
        if os_disk_nvme_namespace:
            os_disk_nvme_device = get_matched_str(
                os_disk_nvme_namespace,
                self.NVME_DEVICE_PATTERN,
            )
        return os_disk_nvme_device

    def get_devices_from_lspci(self) -> List[PciDevice]:
        devices_from_lspci = []
        lspci_tool = self._node.tools[Lspci]
        device_list = lspci_tool.get_devices_by_type(DEVICE_TYPE_NVME)
        devices_from_lspci = [
            x for x in device_list if self._pci_device_name == x.device_class
        ]
        return devices_from_lspci

    def get_raw_data_disks(self) -> List[str]:
        return self.get_namespaces()

    def get_raw_nvme_disks(self) -> List[str]:
        # This routine returns Local NVMe devices as a list.
        nvme_namespaces = self.get_namespaces()
        return nvme_namespaces

    def _get_device_from_ls(self, force_run: bool = False) -> None:
        if (not self._ls_devices) or force_run:
            execute_results = self._node.tools[Ls].run(
                "-l /dev/nvme*", shell=True, sudo=True
            )
            self._ls_devices = execute_results.stdout


@dataclass_json()
@dataclass()
class NvmeSettings(FeatureSettings):
    type: str = "Nvme"
    disk_count: search_space.CountSpace = field(
        default_factory=partial(search_space.IntRange, min=0),
        metadata=field_metadata(decoder=search_space.decode_count_space),
    )

    def __eq__(self, o: object) -> bool:
        if not super().__eq__(o):
            return False

        assert isinstance(o, NvmeSettings), f"actual: {type(o)}"
        return self.type == o.type and self.disk_count == o.disk_count

    def __repr__(self) -> str:
        return f"disk_count:{self.disk_count}"

    def __str__(self) -> str:
        return self.__repr__()

    def __hash__(self) -> int:
        return super().__hash__()

    def _get_key(self) -> str:
        return f"{super()._get_key()}/{self.disk_count}"

    def check(self, capability: Any) -> search_space.ResultReason:
        assert isinstance(capability, NvmeSettings), f"actual: {type(capability)}"
        result = super().check(capability)

        result.merge(
            search_space.check_countspace(self.disk_count, capability.disk_count),
            "disk_count",
        )

        return result

    def _generate_min_capability(self, capability: Any) -> Any:
        assert isinstance(capability, NvmeSettings), f"actual: {type(capability)}"
        min_value = NvmeSettings()

        if self.disk_count or capability.disk_count:
            min_value.disk_count = search_space.generate_min_capability_countspace(
                self.disk_count, capability.disk_count
            )

        return min_value
