from enum import Enum

class Hermes_Event(Enum):
    """Event types for general Hermes monitoring."""
    UNKNOWN          = "UNKNOWN"
    BOOT             = "BOOT"
    SHUTDOWN         = "SHUTDOWN"
    SHUTDOWN_REQUEST = "SHUTDOWN_REQUEST"
    CONFIG_UPDATE    = "CONFIG_UPDATE"
    PROCESSING       = "PROCESSING"


class WebGui_Event(Enum):
    """Event types for monitoring the webgui activity."""
    UNKNOWN          = "UNKNOWN"
    LOGIN            = "LOGIN"
    LOGIN_FAIL       = "LOGIN_FAIL"
    LOGOUT           = "LOGOUT"
    USER_CREATE      = "USER_CREATE"
    USER_DELETE      = "USER_DELETE"
    USER_EDIT        = "USER_EDIT"
    RULE_CREATE      = "RULE_CREATE"
    RULE_DELETE      = "RULE_DELETE"
    RULE_EDIT        = "RULE_EDIT"
    TARGET_CREATE    = "TARGET_CREATE"
    TARGET_DELETE    = "TARGET_DELETE"
    TARGET_EDIT      = "TARGET_EDIT"
    SERVICE_CONTROL  = "SERVICE_CONTROL"
    CONFIG_EDIT      = "CONFIG_EDIT"


class Series_Event(Enum):
    """Event types for monitoring everything related to one specific series."""
    UNKNOWN          = "UNKNOWN"
    REGISTERED       = "REGISTERED"
    ROUTE            = "ROUTE"
    DISCARD          = "DISCARD"
    DISPATCH         = "DISPATCH"
    CLEAN            = "CLEAN"
    ERROR            = "ERROR"
    MOVE             = "MOVE"
    SUSPEND          = "SUSPEND"


class Severity(Enum):
    """Severity level associated to the Hermes events."""
    INFO             = 0
    WARNING          = 1
    ERROR            = 2
    CRITICAL         = 3