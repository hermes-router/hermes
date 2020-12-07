import requests
import daiquiri

from common.events import Severity

logger = daiquiri.getLogger("config")



def configure(module,instance,address):
    """Configures the connection to the bookkeeper module. If not called, events
       will not be transmitted to the bookkeeper."""
    monitor = Monitor(module, instance, address)
    return monitor


class Monitor():
    def __init__(self, module_name, instance, address) -> None:
        self.sender_name = module_name + "." + instance
        self.bookkeeper_address = "http://" + address


    def send_event(self, event, severity = Severity.INFO, description = ""):
        """Sends information about general Hermes events to the bookkeeper (e.g., during module start)."""
        if not self.bookkeeper_address:
            return
        try:
            payload = {'sender': self.sender_name, 'event': event, 'severity': severity, 'description': description }
            requests.post(self.bookkeeper_address+"/hermes-event", data=payload, timeout=1)
        except requests.exceptions.RequestException as e:
            logger.error("Failed request to bookkeeper")
            logger.error(e)


    def send_webgui_event(self, event, user, description = ""):
        """Sends information about an event on the webgui to the bookkeeper."""
        if not self.bookkeeper_address:
            return
        try:
            payload = {'sender': self.sender_name, 'event': event, 'user': user, 'description': description }
            requests.post(self.bookkeeper_address+"/webgui-event", data=payload, timeout=1)
        except requests.exceptions.RequestException as e:
            logger.error("Failed request to bookkeeper")
            logger.error(e)


    def send_register_seriese(self, tags):
        """Registers a received series on the bookkeeper. This should be called when a series has been 
        fully received and the DICOM tags have been parsed."""
        if not self.bookkeeper_address:
            return
        try:
            requests.post(self.bookkeeper_address+"/register-series", data=tags, timeout=1)
        except requests.exceptions.RequestException as e:
            logger.error("Failed request to bookkeeper")
            logger.error(e)


    def send_series_event(self, event, series_uid, file_count, target, info):
        """Send an event related to a specific series to the bookkeeper."""
        if not self.bookkeeper_address:
            return
        try:
            payload = {'sender': self.sender_name, 'event': event, 'series_uid': series_uid,
                    'file_count': file_count, 'target': target, 'info': info }
            requests.post(self.bookkeeper_address+"/series-event", data=payload, timeout=1)
        except requests.exceptions.RequestException as e:
            logger.error("Failed request to bookkeeper")
            logger.error(e)
