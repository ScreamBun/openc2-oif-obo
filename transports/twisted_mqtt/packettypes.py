
class PacketTypes:
    """
    Packet types class.  Includes the AUTH packet for MQTT v5.0.

    Holds constants for each packet type such as PacketTypes.PUBLISH
    and packet name strings: PacketTypes.Names[PacketTypes.PUBLISH].
    """
    # Packet types
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14
    AUTH = 15
    # Dummy packet type for properties use - will delay only applies to will
    WILLMESSAGE = 99

    Names = [
        "reserved", "Connect", "Connack", "Publish", "Puback", "Pubrec", "Pubrel",
        "Pubcomp", "Subscribe", "Suback", "Unsubscribe", "Unsuback",
        "Pingreq", "Pingresp", "Disconnect", "Auth"
    ]
