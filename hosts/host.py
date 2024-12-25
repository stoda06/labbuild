from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class Host:
    """
    Represents a host with a name and location.

    Attributes:
        name (str): The name of the host.
        fqdn (str): The fqdn of the host.
        vcenter (str): The fqdn of the host's vcenter.
    """
    name: str
    fqdn: str
    vcenter: str
    resource_pool: str
    folder: str

# Define individual hosts
cliffjumper = Host(name="cliffjumper", fqdn="cliffjumper.rededucation.com", vcenter="vcenter-appliance-1.rededucation.com", resource_pool="cp-cliffjumper", folder="cp")
hydra = Host(name="hydra", fqdn="hydra.rededucation.com", vcenter="vcenter-appliance-1.rededucation.com", resource_pool="cp-hydra", folder="cp")
optimus = Host(name="optimus", fqdn="optimus.rededucation.com", vcenter="vcenter-appliance-1.rededucation.com", resource_pool="cp-optimus", folder="cp")
apollo = Host(name="apollo", fqdn="apollo.rededucation.com", vcenter="vcenter-appliance-2.rededucation.com", resource_pool="cp-apollo", folder="cp")
nightbird = Host(name="nightbird", fqdn="nightbird.rededucation.com", vcenter="vcenter-appliance-2.rededucation.com", resource_pool="cp-nightbird", folder="cp")
ultramagnus = Host(name="ultramagnus", fqdn="ultramagnus.rededucation.com", vcenter="vcenter-appliance-2.rededucation.com", resource_pool="cp-ultramagnus", folder="cp")
hotshot = Host(name="hotshot", fqdn="hotshot.rededucation.com", vcenter="vcenter-appliance-3.rededucation.com", resource_pool="cp-hotshot", folder="cp")
unicron = Host(name="unicron", fqdn="unicron.rededucation.com", vcenter="vcenter-appliance-1.rededucation.com", resource_pool="cp-unicron", folder="cp")
ps01 = Host(name="ps01", fqdn="ps01.rededucation.com", vcenter="vcenter-appliance-4.rededucation.com", resource_pool="cp-ps01", folder="cp")
ps02 = Host(name="ps02", fqdn="ps02.rededucation.com", vcenter="vcenter-appliance-4.rededucation.com", resource_pool="cp-ps02", folder="cp")
ps03 = Host(name="ps03", fqdn="ps03.rededucation.com", vcenter="vcenter-appliance-4.rededucation.com", resource_pool="cp-ps03", folder="cp")
shockwave = Host(name="shockwave", fqdn="shockwave.rededucation.com", vcenter="vcenter-appliance-5.rededucation.com", resource_pool="cp-shockwave", folder="cp")

# Dictionary for host lookup
hosts_dict = {
    cliffjumper.name: cliffjumper,
    hydra.name: hydra,
    optimus.name: optimus,
    apollo.name: apollo,
    nightbird.name: nightbird,
    ultramagnus.name: ultramagnus,
    hotshot.name: hotshot,
    unicron.name: unicron,
    ps01.name: ps01,
    ps02.name: ps02,
    ps03.name: ps03,
    shockwave.name: shockwave
}

def get_host_by_name(host_name: str) -> Optional[Host]:
    """
    Retrieves a Host object by its name.

    Args:
        host_name (str): The name of the host to retrieve.

    Returns:
        Optional[Host]: The Host object if found, None otherwise.
    """
    return hosts_dict.get(host_name)
