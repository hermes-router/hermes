"""
dispatcher.py
=============
The dispatcher service of Hermes that executes the DICOM transfer to the different targets.
"""
from common.events import Hermes_Event, Series_Event, Severity
import logging
import os
import signal
import sys
import time
from pathlib import Path

import daiquiri
import graphyte
from redis import Redis
from rq import Queue

import common.config as config
import common.helper as helper
import common.monitor as monitor
import common.version as version

from dispatch.send import _move_sent_directory, execute
from dispatch.status import (has_been_send, is_ready_for_sending,
                             is_target_json_valid)

daiquiri.setup(
    level=logging.INFO,
    outputs=(
        daiquiri.output.Stream(
            formatter=daiquiri.formatter.ColorFormatter(
                fmt="%(color)s%(levelname)-8.8s " "%(name)s: %(message)s%(color_stop)s"
            )
        ),
    ),
)
logger = daiquiri.getLogger("dispatcher")


_monitor = None
_q = None

def receiveSignal(signalNumber, frame):
    """Function for testing purpose only. Should be removed."""
    logger.info("Received:", signalNumber)
    return


def terminateProcess(signalNumber, frame):
    """Triggers the shutdown of the service."""
    helper.g_log('events.shutdown', 1)
    logger.info("Shutdown requested")
    _monitor.send_event(Hermes_Event.SHUTDOWN_REQUEST, Severity.INFO)
    # Note: main_loop can be read here because it has been declared as global variable
    if 'main_loop' in globals() and main_loop.is_running:
        main_loop.stop()
    helper.triggerTerminate()


def dispatch(args):
    """ Main entry function. """
    if helper.isTerminated():
        return

    helper.g_log('events.run', 1)

    try:
        config.read_config()
    except Exception:
        logger.exception("Unable to read configuration. Skipping processing.")
        _monitor.send_event(
            Hermes_Event.CONFIG_UPDATE,
            Severity.WARNING,
            "Unable to read configuration (possibly locked)",
        )
        return

    success_folder = Path(config.hermes["success_folder"])
    error_folder = Path(config.hermes["error_folder"])
    retry_max = config.hermes["retry_max"]
    retry_delay = config.hermes["retry_delay"]

    with os.scandir(config.hermes["outgoing_folder"]) as it:
        for entry in it:
            target_info = is_ready_for_sending(entry.path)
            if (
                entry.is_dir()
                and not has_been_send(entry.path)
                and target_info 
            ):

                delay = target_info.get("next_retry_at", 0)
                if target_info and time.time() >= delay:
                    series_uid=target_info.get("series_uid", "series_uid-missing") 
                    target_name=target_info.get("target_name", "target_name-missing")

                    if (series_uid=="series_uid-missing") or (target_name=="target_name-missing"):
                        _monitor.send_event(Hermes_Event.PROCESSING, Severity.WARNING, f"Missing information for folder {entry.path}")    

                    # Create a .sending file to indicate that this folder is being sent,
                    # otherwise the dispatcher would pick it up again if the transfer is
                    # still going on
                    lock_file = Path(entry.path) / ".sending"
                    lock_file.touch()
                
                    # global queue. dcm send are ofloaded to rq jobs. Workers are started from the cli.
                    logger.info(f"Folder {entry.path} is put to queue")
                    _q.enqueue(execute, target_info, Path(entry.path), success_folder, error_folder, retry_max, retry_delay, _monitor)
            elif entry.is_dir and has_been_send(entry.path):
                logger.info(f"Folder {entry.path} has been sent")
                series_uid=is_target_json_valid(entry.path).get("series_uid", "series_uid-missing")
                _move_sent_directory(Path(entry.path), success_folder, _monitor)
                _monitor.send_series_event(Series_Event.MOVE, series_uid, 0, success_folder, "")
                logger.info(f"Folder {entry.path} has been moved to success folder")
            # If termination is requested, stop processing series after the
            # active one has been completed
            if helper.isTerminated():
                break


def exit_dispatcher(args):
    """ Stop the asyncio event loop. """
    helper.loop.call_soon_threadsafe(helper.loop.stop)


if __name__ == "__main__":
    logger.info("")
    logger.info(f"Hermes DICOM Dispatcher ver {version.hermes_version}")
    logger.info("----------------------------")
    logger.info("")

    # Register system signals to be caught
    signal.signal(signal.SIGINT, terminateProcess)
    signal.signal(signal.SIGQUIT, receiveSignal)
    signal.signal(signal.SIGILL, receiveSignal)
    signal.signal(signal.SIGTRAP, receiveSignal)
    signal.signal(signal.SIGABRT, receiveSignal)
    signal.signal(signal.SIGBUS, receiveSignal)
    signal.signal(signal.SIGFPE, receiveSignal)
    signal.signal(signal.SIGUSR1, receiveSignal)
    signal.signal(signal.SIGSEGV, receiveSignal)
    signal.signal(signal.SIGUSR2, receiveSignal)
    signal.signal(signal.SIGPIPE, receiveSignal)
    signal.signal(signal.SIGALRM, receiveSignal)
    signal.signal(signal.SIGTERM, terminateProcess)

    instance_name = "main"
    if len(sys.argv) > 1:
        instance_name = sys.argv[1]

    logger.info(sys.version)
    logger.info(f"Instance name = {instance_name}")
    logger.info(f"Dispatcher PID is: {os.getpid()}")

    try:
        config.read_config()
        _monitor = monitor.configure("dispatcher", instance_name, config.hermes["bookkeeper"])
    except Exception:
        logger.exception("Cannot start service. Going down.")
        sys.exit(1)

    
    _monitor.send_event(Hermes_Event.BOOT, Severity.INFO, f"PID = {os.getpid()}")
    graphite_prefix = "hermes.dispatcher." + instance_name

    if len(config.hermes["graphite_ip"]) > 0:
        logging.info(f'Sending events to graphite server: {config.hermes["graphite_ip"]}')
        graphyte.init(
            config.hermes["graphite_ip"],
            config.hermes["graphite_port"],
            prefix=graphite_prefix,
        )

    logger.info(f"Dispatching folder: {config.hermes['outgoing_folder']}")

    _q = Queue(connection=Redis())

    # probably not needed to be a global variable, [joshy, 7.12.2020]
    global main_loop
    main_loop = helper.RepeatedTimer(
        config.hermes["dispatcher_scan_interval"], dispatch, exit_dispatcher, {}
    )
    main_loop.start()

    helper.g_log('events.boot', 1)

    # Start the asyncio event loop for asynchronous function calls
    helper.loop.run_forever()

    _monitor.send_event(Hermes_Event.SHUTDOWN, Severity.INFO)
    logging.info("Going down now")
