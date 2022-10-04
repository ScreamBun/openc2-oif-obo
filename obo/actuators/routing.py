from obo.openc2_types import OpenC2CmdFields, OpenC2RspFields, OpenC2Headers, OpenC2Msg

from dataclasses import dataclass
from obo.utils import get_config_data, setLogLevel, startLogging
import toml
from obo.utils.datafiles import TomlDataFile
from typing import Any, Callable, Dict, List, Union, Optional
import time
import json

from actuator import Actuator
routing = Actuator(nsid='routing')

Config = get_config_data()
downstream_devices = Config.devices
print(downstream_devices)


@routing.pair('query', 'pac')
def query_pac(input_cmd: OpenC2CmdFields) -> OpenC2RspFields:

    subtarget = input_cmd.target.get("pac", None)
    deriv_cmd_fields = OpenC2CmdFields(action="query", target=subtarget, args=input_cmd.args,
                                       profile=input_cmd.profile, command_id=input_cmd.command_id)

    if subtarget is None:
        status_text = f'No PAC target Specified'
        return OpenC2RspFields(status=500, status_text=status_text)
    else:

        r = "Devices Queried"

    if c := subtarget.pop("sbom", None):
        if find_targets("sbom") == []:
            status_text = f'No Devices implement sbom Profile'
            return OpenC2RspFields(status=404, results=status_text)
        downstream_targets = find_targets("sbom")

        for device in downstream_targets:
            deriv_header = target_headers(device, input_cmd.command_id)
            d_msg = construct_msg(deriv_header, deriv_cmd_fields)

            # create connection with downstream device
            # send d_msg
            # receive response from downstream device
            # collect responses




        r = "Device Allowed" + c
    if c := subtarget.pop("database", None):
        if find_targets("database") == []:
            status_text = f'No Devices implement database Profile'
            return OpenC2RspFields(status=404, results=status_text)
        downstream_targets = find_targets("database")

        for device in downstream_targets:
            deriv_header = target_headers(device, input_cmd.command_id)
            d_msg = construct_msg(deriv_header, deriv_cmd_fields)


            # create connection with downstream device
            # send d_msg
            # receive response from downstream device
            # collect responses

        r = "Device Allowed" + c

    return OpenC2RspFields(status=200, results=r)


@routing.pair('query', 'er')
def query_er(input_cmd: OpenC2CmdFields) -> OpenC2RspFields:

    deriv_cmd_fields = ("query", input_cmd.target, input_cmd.args, input_cmd.profile, input_cmd.command_id)
    r = "Devices Queried"

    if find_targets("er") == []:
        status_text = f'No Devices implement er Profile'
        return OpenC2RspFields(status=404, results=status_text)
    downstream_targets = find_targets("er")

    for device in downstream_targets:
        downstream_header = target_headers(device.__getattribute__("name"), input_cmd.command_id)
        construct_msg(downstream_header, input_cmd)

        print("Device Queried" + device.__getattribute__("name"))

        return OpenC2RspFields(status=200, results=r)


def find_targets(profile: str) -> list[str]:
    # check downstream list for actuators that match
    targeted_devices = []
    for d in downstream_devices:
        if d.__getattribute__("profiles").contains("er"):
            targeted_devices.append(d.__getattribute__("name"))
            print("targets found: "+str(targeted_devices))
    # if no actuators match, tough luck, itll get figured out upstairs
    return targeted_devices


def relayed_command(body: OpenC2CmdFields, encode: str) -> OpenC2CmdFields:

    rc = body
    return rc


def derived_command(body: OpenC2CmdFields, encode: str) -> OpenC2CmdFields:

    dc = body
    return dc


def construct_msg(headers: OpenC2Headers, body: OpenC2CmdFields) -> OpenC2Msg:

    # collect headers and cmd fields into one message
    msg = OpenC2Msg(headers, body)

    return msg


def target_headers(device, c_id) -> OpenC2Headers:

    request_id = c_id
    created = time.time()
    from_ = "Bridge Orchestrator 1"  # This can pull from the config toml with minimal pain and suffering but is HC now
    to = device.__getattribute__("Name")
    return_headers = OpenC2Headers(request_id, created, from_, to)

    return return_headers


