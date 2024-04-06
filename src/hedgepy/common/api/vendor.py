from importlib import import_module

from hedgepy.common.utils.config import SOURCE_ROOT, PROJECT_NAME
from hedgepy.common.api.bases import API


def load_vendors() -> tuple[str]:
    return tuple(
        vendor.stem for vendor in (SOURCE_ROOT / "common" / "vendors").iterdir()
        )


def load_vendor(vendor: str) -> API.VendorSpec:
    return import_module(f"{PROJECT_NAME}.common.vendors.{vendor}").spec


def init_vendor(vendor: API.VendorSpec) -> API.Vendor:
    return API.Vendor(vendor)


async def start_vendor(vendor: API.Vendor) -> None:
    await vendor.start()
    