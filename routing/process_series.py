import json
import os
import shutil
import uuid
from pathlib import Path

import common.config as config
import common.rule_evaluation as rule_evaluation
import daiquiri
from common.events import Hermes_Event, Series_Event, Severity

logger = daiquiri.getLogger("process_series")


class FileLock:
    """Helper class that implements a file lock. The lock file will be removed also from the destructor so that
       no spurious lock files remain if exceptions are raised."""
    def __init__(self, path_for_lockfile):
        self.lockCreated=True
        self.lockfile=path_for_lockfile
        self.lockfile.touch()

    # Destructor to ensure that the lock file gets deleted
    # if the calling function is left somewhere as result
    # of an unhandled exception
    def __del__(self):
        self.free()

    def free(self):
        if self.lockCreated:
            self.lockfile.unlink()
            self.lockCreated=False


def process_series(monitor, series_UID):
    """Processes the series with the given series UID from the incoming folder."""
    lock_file=Path(config.hermes['incoming_folder'] + '/' + str(series_UID) + '.lock')

    if lock_file.exists():
        # Series is locked, so another instance might be working on it
        return

    try:
        lock=FileLock(lock_file)
    except:
        # Can't create lock file, so something must be seriously wrong
        logger.error(f'Unable to create lock file {lock_file}')
        monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Unable to create lock file {lock_file}')
        return

    logger.info(f'Processing series {series_UID}')

    fileList = []
    seriesPrefix=series_UID+"#"

    # Collect all files belonging to the series
    for entry in os.scandir(config.hermes['incoming_folder']):
            if entry.name.endswith(".tags") and entry.name.startswith(seriesPrefix) and not entry.is_dir():
                stemName=entry.name[:-5]
                fileList.append(stemName)

    logger.info("DICOM files found: "+str(len(fileList)))

    # Use the tags file from the first slice for evaluating the routing rules
    tagsMasterFile=Path(config.hermes['incoming_folder'] + '/' + fileList[0] + ".tags")
    if not tagsMasterFile.exists():
        logger.error(f'Missing file! {tagsMasterFile.name}')
        monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Missing file {tagsMasterFile.name}')
        return

    try:
        with open(tagsMasterFile, "r") as json_file:
            tagsList=json.load(json_file)
    except Exception:
        logger.exception(f"Invalid tag information of series {series_UID}")
        monitor.send_series_event(Series_Event.ERROR, entry, 0, "", "Invalid tag information")
        monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f"Invalid tag for series {series_UID}")        
        return

    monitor.send_register_series(tagsList)
    monitor.send_series_event(Series_Event.REGISTERED, series_UID, len(fileList), "", "")

    # Now test the routing rules and decide to which targets the series should be sent to
    transfer_targets = get_routing_targets(monitor, tagsList)

    if len(transfer_targets)==0:
        # If no routing rule has triggered, discard the series
        push_series_discard(monitor, fileList,series_UID)
    else:
        # Otherwise, push the series to a different outgoing folder for every target
        push_series_outgoing(monitor, fileList,series_UID,transfer_targets)

    try:
        lock.free()
    except:
        # Can't delete lock file, so something must be seriously wrong
        logger.error(f'Unable to remove lock file {lock_file}')
        monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Unable to remove lock file {lock_file}')
        return


def get_routing_targets(monitor, tagList):
    """Evaluates the routing rules and returns a list with the desired targets."""
    selected_targets = {}

    for current_rule in config.hermes["rules"]:
        try:
            if config.hermes["rules"][current_rule].get("disabled","False")=="True":
                continue
            if current_rule in selected_targets:
                continue
            if rule_evaluation.parse_rule(config.hermes["rules"][current_rule].get("rule","False"),tagList):
                target=config.hermes["rules"][current_rule].get("target","")
                if target:
                    selected_targets[target]=current_rule
        except Exception as e:
            logger.error(e)
            logger.error(f"Invalid rule found: {current_rule}")
            monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f"Invalid rule: {current_rule}")
            continue

    logger.info("Selected routing:")
    logger.info(selected_targets)
    return selected_targets


def push_series_discard(monitor, fileList,series_UID):
    """Discards the series by moving all files into the "discard" folder, which is periodically cleared."""
    # Define the source and target folder. Use UUID as name for the target folder in the 
    # discard directory to avoid collisions
    discard_path  =config.hermes['discard_folder']  + '/' + str(uuid.uuid1())
    discard_folder=discard_path + '/'
    source_folder =config.hermes['incoming_folder'] + '/'

    # Create subfolder in the discard directory and validate that is has been created
    try:
        os.mkdir(discard_path)
    except Exception:
        logger.exception(f'Unable to create outgoing folder {discard_path}')
        monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Unable to create discard folder {discard_path}')
        return
    if not Path(discard_path).exists():
        logger.error(f'Creating discard folder not possible {discard_path}')
        monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Creating discard folder not possible {discard_path}')
        return

    # Create lock file in destination folder (to prevent the cleaner module to work on the folder). Note that 
    # the DICOM series in the incoming folder has already been locked in the parent function.
    try:
        lock_file=Path(discard_path + '/.lock')
        lock=FileLock(lock_file)
    except:
        # Can't create lock file, so something must be seriously wrong
        logger.error(f'Unable to create lock file {Path(discard_path + "/.lock")}')
        monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Unable to create lock file in discard folder {Path(discard_path + "/.lock")}')
        return

    monitor.send_series_event(Series_Event.DISCARD, series_UID, len(fileList), "", "")

    for entry in fileList:
        try:
            shutil.move(source_folder+entry+'.dcm',discard_folder+entry+'.dcm')
            shutil.move(source_folder+entry+'.tags',discard_folder+entry+'.tags')
        except Exception:
            logger.exception(f'Problem while discarding file {entry}')
            logger.exception(f'Source folder {source_folder}')
            logger.exception(f'Target folder {discard_folder}')
            monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Problem during discarding file {entry}')

    monitor.send_series_event(Series_Event.MOVE, series_UID, len(fileList), discard_path, "")

    try:
        lock.free()
    except:
        # Can't delete lock file, so something must be seriously wrong
        logger.error(f'Unable to remove lock file {lock_file}')
        monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Unable to remove lock file {lock_file}')
        return


def push_series_outgoing(monitor, fileList,series_UID,transfer_targets):
    """Move the DICOM files of the series to a separate subfolder for each target in the outgoing folder."""
    source_folder=config.hermes['incoming_folder'] + '/'

    total_targets=len(transfer_targets)
    current_target=0

    for target in transfer_targets:

        current_target=current_target+1

        if not target in config.hermes["targets"]:
            logger.error(f"Invalid target selected {target}")
            monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f"Invalid target selected {target}")
            continue

        # Determine if the files should be copied or moved. For the last
        # target, the files should be moved to reduce IO overhead
        move_operation=False
        if current_target==total_targets:
            move_operation=True

        folder_name=config.hermes['outgoing_folder'] + '/' + str(uuid.uuid1())
        target_folder=folder_name+"/"

        try:
            os.mkdir(folder_name)
        except Exception:
            logger.exception(f'Unable to create outgoing folder {folder_name}')
            monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Unable to create outgoing folder {folder_name}')
            return

        if not Path(folder_name).exists():
            logger.error(f'Creating folder not possible {folder_name}')
            monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Creating folder not possible {folder_name}')
            return

        try:
            lock_file=Path(folder_name + '/.lock')
            lock=FileLock(lock_file)
        except:
            # Can't create lock file, so something must be seriously wrong
            logger.error(f'Unable to create lock file {Path(folder_name + "/.lock")}')
            monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Unable to create lock file {Path(folder_name + "/.lock")}')
            return

        # Generate target file target.json
        target_filename = target_folder + "target.json"
        target_json = {}
        target_json["target_ip"]        =config.hermes["targets"][target]["ip"]
        target_json["target_port"]      =config.hermes["targets"][target]["port"]
        target_json["target_aet_target"]=config.hermes["targets"][target].get("aet_target","ANY-SCP")
        target_json["target_aet_source"]=config.hermes["targets"][target].get("aet_source","HERMES")
        target_json["target_name"]      =target
        target_json["applied_rule"]     =transfer_targets[target]
        target_json["series_uid"]       =series_UID

        try:
            with open(target_filename, 'w') as target_file:
                json.dump(target_json, target_file)
        except:
            logger.error(f"Unable to create target file {target_filename}")
            monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f"Unable to create target file {target_filename}")
            continue

        monitor.send_series_event(Series_Event.ROUTE, series_UID, len(fileList), target, transfer_targets[target])

        if move_operation:
            operation=shutil.move
        else:
            operation=shutil.copy

        for entry in fileList:
            try:
                operation(source_folder+entry+'.dcm', target_folder+entry+'.dcm')
                operation(source_folder+entry+'.tags',target_folder+entry+'.tags')
            except Exception:
                logger.exception(f'Problem while pushing file to outgoing {entry}')
                logger.exception(f'Source folder {source_folder}')
                logger.exception(f'Target folder {target_folder}')
                monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Problem while pushing file to outgoing {entry}')

        monitor.send_series_event(Series_Event.MOVE, series_UID, len(fileList), folder_name, "")

        try:
            lock.free()
        except:
            # Can't delete lock file, so something must be seriously wrong
            logger.error(f'Unable to remove lock file {lock_file}')
            monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Unable to remove lock file {lock_file}')
            return


def process_error_files(monitor):
    """
    Looks for error files, moves these files and the corresponding DICOM files to the error folder, 
    and sends an alert to the bookkeeper instance.
    """
    error_files_found = 0

    for entry in os.scandir(config.hermes['incoming_folder']):
        if entry.name.endswith(".error") and not entry.is_dir():
            # Check if a lock file exists. If not, create one.
            lock_file=Path(config.hermes['incoming_folder'] + '/' + entry.name + '.lock')
            if lock_file.exists():
                continue
            try:
                lock=FileLock(lock_file)
            except:
                continue

            logger.error(f'Found incoming error file {entry.name}')
            error_files_found += 1

            shutil.move(config.hermes['incoming_folder'] + '/' + entry.name,
                        config.hermes['error_folder'] + '/' + entry.name)

            dicom_filename = entry.name[:-6]
            dicom_file = Path(config.hermes['incoming_folder'] + '/' + dicom_filename)
            if dicom_file.exists():
                shutil.move(config.hermes['incoming_folder'] + '/' + dicom_filename,
                            config.hermes['error_folder'] + '/' + dicom_filename)

            lock.free()

    if error_files_found > 0:
        monitor.send_event(Hermes_Event.PROCESSING, Severity.ERROR, f'Error parsing {error_files_found} incoming files')
    return
