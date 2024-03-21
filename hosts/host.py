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

# Define individual hosts
cliffjumper = Host(name="cliffjumper", fqdn="cliffjumper.rededucation.com", vcenter="vcenter-appliance-1.rededucation.com")
hydra = Host(name="hydra", fqdn="hydra.rededucation.com", vcenter="vcenter-appliance-1.rededucation.com")
optimus = Host(name="optimus", fqdn="optimus.rededucation.com", vcenter="vcenter-appliance-1.rededucation.com")
apollo = Host(name="apollo", fqdn="apollo.rededucation.com", vcenter="vcenter-appliance-2.rededucation.com")
nightbird = Host(name="nightbird", fqdn="nightbird.rededucation.com", vcenter="vcenter-appliance-2.rededucation.com")
ultramagnus = Host(name="ultramagnus", fqdn="ultramagnus.rededucation.com", vcenter="vcenter-appliance-2.rededucation.com")
hotshot = Host(name="hotshot", fqdn="hotshot.rededucation.com", vcenter="vcenter-appliance-3.rededucation.com")
ps01 = Host(name="ps01", fqdn="ps01.rededucation.com", vcenter="vcenter-appliance-4.rededucation.com")
ps02 = Host(name="ps02", fqdn="ps02.rededucation.com", vcenter="vcenter-appliance-4.rededucation.com")
ps03 = Host(name="ps03", fqdn="ps03.rededucation.com", vcenter="vcenter-appliance-4.rededucation.com")
shockwave = Host(name="shockwave", fqdn="shockwave.rededucation.com", vcenter="vcenter-appliance-5.rededucation.com")

# Dictionary for host lookup
hosts_dict = {
    cliffjumper.name: cliffjumper,
    hydra.name: hydra,
    optimus.name: optimus,
    apollo.name: apollo,
    nightbird.name: nightbird,
    ultramagnus.name: ultramagnus,
    hotshot.name: hotshot,
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
