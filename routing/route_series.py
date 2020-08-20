import os
from pathlib import Path
import uuid
import json
import shutil
import daiquiri

# App-specific includes
import common.config as config
import common.rule_evaluation as rule_evaluation
import common.monitor as monitor
import common.helper as helper
import common.notification as notification
from common.constants import mercure_defs, mercure_names, mercure_actions, mercure_rule, mercure_config, mercure_options, mercure_folders, mercure_events
from routing.generate_taskfile import generate_taskfile_route, generate_taskfile_process


logger = daiquiri.getLogger("route_series")


def route_series(series_UID):
    """Processes the series with the given series UID from the incoming folder."""
    lock_file=Path(config.mercure['incoming_folder'] + '/' + str(series_UID) + mercure_names.LOCK)

    if lock_file.exists():
        # Series is locked, so another instance might be working on it
        return

    # Create lock file in the incoming folder and prevent other instances from working on this series
    try:
        lock=helper.FileLock(lock_file)
    except:
        # Can't create lock file, so something must be seriously wrong
        logger.error(f'Unable to create lock file {lock_file}')
        monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to create lock file {lock_file}')
        return

    logger.info(f'Processing series {series_UID}')
    fileList = []
    seriesPrefix=series_UID+"#"

    # Collect all files belonging to the series
    for entry in os.scandir(config.mercure['incoming_folder']):
            if entry.name.endswith(mercure_names.TAGS) and entry.name.startswith(seriesPrefix) and not entry.is_dir():
                stemName=entry.name[:-5]
                fileList.append(stemName)

    logger.info("DICOM files found: "+str(len(fileList)))

    # Use the tags file from the first slice for evaluating the routing rules
    tagsMasterFile=Path(config.mercure['incoming_folder'] + '/' + fileList[0] + mercure_names.TAGS)
    if not tagsMasterFile.exists():
        logger.error(f'Missing file! {tagsMasterFile.name}')
        monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Missing file {tagsMasterFile.name}')
        return

    try:
        with open(tagsMasterFile, "r") as json_file:
            tagsList=json.load(json_file)
    except Exception:
        logger.exception(f"Invalid tag information of series {series_UID}")
        monitor.send_series_event(monitor.s_events.ERROR, entry, 0, "", "Invalid tag information")
        monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f"Invalid tag for series {series_UID}")        
        return

    monitor.send_register_series(tagsList)
    monitor.send_series_event(monitor.s_events.REGISTERED, series_UID, len(fileList), "", "")

    # Now test the routing rules and evaluate which rules have been triggered. If one of the triggered
    # rules enforces discarding, discard_series will be True.
    discard_series = ""
    triggered_rules, discard_series = get_triggered_rules(tagsList)

    if (len(triggered_rules)==0) or (discard_series):
        # If no routing rule has triggered or discarding has been enforced, discard the series
        push_series_discard(fileList,series_UID,discard_series)        
    else:
        # Strategy: If only one triggered rule, move files. If multiple, copy files
        push_series_studylevel(triggered_rules,fileList,series_UID,tagsList)
        push_series_serieslevel(triggered_rules,fileList,series_UID,tagsList)
        
        if (len(triggered_rules)>1):
            remove_series(fileList)

    try:
        lock.free()
    except:
        # Can't delete lock file, so something must be seriously wrong
        logger.error(f'Unable to remove lock file {lock_file}')
        monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to remove lock file {lock_file}')
        return


def get_triggered_rules(tagList):
    """Evaluates the routing rules and returns a list with trigger rules."""
    triggered_rules = {}
    discard_rule = ""

    for current_rule in config.mercure["rules"]:
        try:
            if config.mercure["rules"][current_rule].get(mercure_rule.DISABLED,"False")=="True":
                continue
            if rule_evaluation.parse_rule(config.mercure["rules"][current_rule].get(mercure_rule.RULE,"False"),tagList):
                triggered_rules[current_rule]=current_rule
                if config.mercure["rules"][current_rule].get(mercure_rule.ACTION,"")==mercure_actions.DISCARD:
                    discard_rule=current_rule
                    break

        except Exception as e:
            logger.error(e)
            logger.error(f"Invalid rule found: {current_rule}")
            monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f"Invalid rule: {current_rule}")
            continue

    logger.info("Triggered rules:")
    logger.info(triggered_rules)
    return triggered_rules, discard_rule


def push_series_discard(fileList,series_UID,discard_series):
    """Discards the series by moving all files into the "discard" folder, which is periodically cleared."""
    # Define the source and target folder. Use UUID as name for the target folder in the 
    # discard directory to avoid collisions
    discard_path  =config.mercure['discard_folder']  + '/' + str(uuid.uuid1())
    discard_folder=discard_path + '/'
    source_folder =config.mercure['incoming_folder'] + '/'

    # Create subfolder in the discard directory and validate that is has been created
    try:
        os.mkdir(discard_path)
    except Exception:
        logger.exception(f'Unable to create outgoing folder {discard_path}')
        monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to create discard folder {discard_path}')
        return
    if not Path(discard_path).exists():
        logger.error(f'Creating discard folder not possible {discard_path}')
        monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Creating discard folder not possible {discard_path}')
        return

    # Create lock file in destination folder (to prevent the cleaner module to work on the folder). Note that 
    # the DICOM series in the incoming folder has already been locked in the parent function.
    try:
        lock_file=Path(discard_path / mercure_names.LOCK)
        lock=helper.FileLock(lock_file)
    except:
        # Can't create lock file, so something must be seriously wrong
        logger.error(f'Unable to create lock file {lock_file}')
        monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to create lock file in discard folder {lock_file}')
        return

    info_text = ""
    if discard_series:
        info_text = "Discard by rule " + discard_series
    monitor.send_series_event(monitor.s_events.DISCARD, series_UID, len(fileList), "", info_text)

    for entry in fileList:
        try:
            shutil.move(source_folder+entry+mercure_names.DCM,discard_folder+entry+mercure_names.DCM)
            shutil.move(source_folder+entry+mercure_names.TAGS,discard_folder+entry+mercure_names.TAGS)
        except Exception:
            logger.exception(f'Problem while discarding file {entry}')
            logger.exception(f'Source folder {source_folder}')
            logger.exception(f'Target folder {discard_folder}')
            monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Problem during discarding file {entry}')

    monitor.send_series_event(monitor.s_events.MOVE, series_UID, len(fileList), discard_path, "")

    try:
        lock.free()
    except:
        # Can't delete lock file, so something must be seriously wrong
        logger.error(f'Unable to remove lock file {lock_file}')
        monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to remove lock file {lock_file}')
        return


def push_series_studylevel(triggered_rules,file_list,series_UID,tags_list):
    """Prepeares study-level routing for the current series."""
    # Move series into individual study-level folder for every rule
    for current_rule in triggered_rules:
        if config.mercure[mercure_config.RULES][current_rule].get(mercure_rule.ACTION_TRIGGER,mercure_options.SERIES)==mercure_options.STUDY:

            folder_name=series_UID+mercure_defs.SEPARATOR+current_rule
            if (not os.path.exists(folder_name)):
                try:
                    os.mkdir(folder_name)
                except:
                    logger.error(f'Unable to create folder {folder_name}')
                    monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to create folder {folder_name}')
                    continue

            try:
                lock_file=Path(folder_name / mercure_names.LOCK)
                lock=helper.FileLock(lock_file)
            except:
                # Can't create lock file, so something must be seriously wrong
                logger.error(f'Unable to create lock file {lock_file}')
                monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to create lock file {lock_file}')
                return

            push_files(file_list, folder_name, (len(triggered_rules)>1))
            lock.free()


def push_series_serieslevel(triggered_rules,file_list,series_UID,tags_list):
    """Prepeares all series-level routings for the current series."""
    push_serieslevel_routing(triggered_rules,file_list,series_UID,tags_list)
    push_serieslevel_processing(triggered_rules,file_list,series_UID,tags_list)
    push_serieslevel_notification(triggered_rules,file_list,series_UID,tags_list)


def trigger_serieslevel_notification_reception(current_rule,tags_list):
    notification.send_webhook(config.mercure[mercure_config.RULES][current_rule].get(mercure_rule.NOTIFICATION_WEBHOOK,""),
                              config.mercure[mercure_config.RULES][current_rule].get(mercure_rule.NOTIFICATION_PAYLOAD,""),
                              mercure_events.RECEPTION)


def push_serieslevel_routing(triggered_rules,file_list,series_UID,tags_list):
    selected_targets = {}
    # Collect the dispatch-only targets to avoid that a series is sent twice to the
    # same target due to multiple targets triggered (note: this only makes sense for
    # routing-only tasks as study-level rules might have different completion criteria)
    for current_rule in triggered_rules:
        if config.mercure[mercure_config.RULES][current_rule].get(mercure_rule.ACTION_TRIGGER,mercure_options.SERIES)==mercure_options.SERIES:
            if config.mercure[mercure_config.RULES][current_rule].get(mercure_rule.ACTION,"")==mercure_actions.ROUTE:
                target=config.mercure[mercure_config.RULES][current_rule].get(mercure_rule.TARGET,"")
                if target:
                    selected_targets[target]=current_rule
                trigger_serieslevel_notification_reception(current_rule,tags_list)
    push_serieslevel_outgoing(triggered_rules,file_list,series_UID,tags_list,selected_targets)


def push_serieslevel_processing(triggered_rules,file_list,series_UID,tags_list):
    for current_rule in triggered_rules:
        if config.mercure[mercure_config.RULES][current_rule].get(mercure_rule.ACTION_TRIGGER,mercure_options.SERIES)==mercure_options.SERIES:
            if ((config.mercure[mercure_config.RULES][current_rule].get(mercure_rule.ACTION,"")==mercure_actions.PROCESS) or
                (config.mercure[mercure_config.RULES][current_rule].get(mercure_rule.ACTION,"")==mercure_actions.BOTH)):
                # Determine if the files should be copied or moved. If only one rule triggered, files can
                # safely be moved, otherwise files will be moved and removed in the end
                copy_files=True
                if len(triggered_rules)==1:
                    copy_files=False

                folder_name=config.mercure[mercure_folders.PROCESSING] + '/' + str(uuid.uuid1())
                target_folder=folder_name+"/"

                try:
                    os.mkdir(folder_name)
                except Exception:
                    logger.exception(f'Unable to create outgoing folder {folder_name}')
                    monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to create processing folder {folder_name}')
                    return

                if not Path(folder_name).exists():
                    logger.error(f'Creating folder not possible {folder_name}')
                    monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Creating folder not possible {folder_name}')
                    return

                try:
                    lock_file=Path(folder_name / mercure_names.LOCK)
                    lock=helper.FileLock(lock_file)
                except:
                    # Can't create lock file, so something must be seriously wrong
                    logger.error(f'Unable to create lock file {lock_file}')
                    monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to create lock file {lock_file}')
                    return

                # Generate task file with dispatch information
                task_filename = target_folder + mercure_names.TASKFILE
                task_json = generate_taskfile_process(series_UID, mercure_options.SERIES, current_rule, tags_list)

                try:
                    with open(task_filename, 'w') as task_file:
                        json.dump(task_json, task_file)
                except:
                    logger.error(f"Unable to create task file {task_filename}")
                    monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f"Unable to create task file {task_filename}")
                    continue

                if (not push_files(file_list, target_folder, copy_files)):
                    logger.error(f'Unable to push files into processing folder {target_folder}')
                    monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to push files into processing folder {target_folder}')
                    return

                try:
                    lock.free()
                except:
                    # Can't delete lock file, so something must be seriously wrong
                    logger.error(f'Unable to remove lock file {lock_file}')
                    monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to remove lock file {lock_file}')
                    return

                trigger_serieslevel_notification_reception(current_rule,tags_list)


def push_serieslevel_notification(triggered_rules,file_list,series_UID,tags_list):
    for current_rule in triggered_rules:
        if config.mercure[mercure_config.RULES][current_rule].get(mercure_rule.ACTION_TRIGGER,mercure_options.SERIES)==mercure_options.SERIES:
            if config.mercure[mercure_config.RULES][current_rule].get(mercure_rule.ACTION,"")==mercure_actions.NOTIFICATION:
                trigger_serieslevel_notification_reception(current_rule,tags_list)
                # If the current rule is "notification-only" and this is the only rule that 
                # has been triggered, then remove the files (if more than one rule has been
                # triggered, the parent function will take care of it)
                if len(triggered_rules==1):
                    remove_series(file_list)


def push_serieslevel_outgoing(triggered_rules,file_list,series_UID,tags_list,selected_targets):
    """Move the DICOM files of the series to a separate subfolder for each target in the outgoing folder."""
    source_folder=config.mercure['incoming_folder'] + '/'

    # Determine if the files should be copied or moved. If only one rule triggered, files can
    # safely be moved, otherwise files will be moved and removed in the end
    move_operation=False
    if len(triggered_rules)==1:
        move_operation=True

    for target in selected_targets:
        if not target in config.mercure["targets"]:
            logger.error(f"Invalid target selected {target}")
            monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f"Invalid target selected {target}")
            continue

        folder_name=config.mercure['outgoing_folder'] + '/' + str(uuid.uuid1())
        target_folder=folder_name+"/"

        try:
            os.mkdir(folder_name)
        except Exception:
            logger.exception(f'Unable to create outgoing folder {folder_name}')
            monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to create outgoing folder {folder_name}')
            return

        if not Path(folder_name).exists():
            logger.error(f'Creating folder not possible {folder_name}')
            monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Creating folder not possible {folder_name}')
            return

        try:
            lock_file=Path(folder_name + "/" + mercure_names.LOCK)
            lock=helper.FileLock(lock_file)
        except:
            # Can't create lock file, so something must be seriously wrong
            logger.error(f'Unable to create lock file {lock_file}')
            monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to create lock file {lock_file}')
            return

        # Generate task file with dispatch information
        task_filename = target_folder + mercure_names.TASKFILE
        task_json = generate_taskfile_route(series_UID, mercure_options.SERIES, selected_targets[target], tags_list, target)

        try:
            with open(task_filename, 'w') as task_file:
                json.dump(task_json, task_file)
        except:
            logger.error(f"Unable to create task file {task_filename}")
            monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f"Unable to create task file {task_filename}")
            continue

        monitor.send_series_event(monitor.s_events.ROUTE, series_UID, len(file_list), target, selected_targets[target])

        if move_operation:
            operation=shutil.move
        else:
            operation=shutil.copy

        for entry in file_list:
            try:
                operation(source_folder+entry+mercure_names.DCM, target_folder+entry+mercure_names.DCM)
                operation(source_folder+entry+mercure_names.TAGS,target_folder+entry+mercure_names.TAGS)
            except Exception:
                logger.exception(f'Problem while pushing file to outgoing {entry}')
                logger.exception(f'Source folder {source_folder}')
                logger.exception(f'Target folder {target_folder}')
                monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Problem while pushing file to outgoing {entry}')

        monitor.send_series_event(monitor.s_events.MOVE, series_UID, len(file_list), folder_name, "")

        try:
            lock.free()
        except:
            # Can't delete lock file, so something must be seriously wrong
            logger.error(f'Unable to remove lock file {lock_file}')
            monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Unable to remove lock file {lock_file}')
            return


def push_files(file_list, target_path, copy_files):
    """
    Copies or moves the given files to the target path. If copy_files is True, files are copied, otherwise moved.
    Note that this function does not create a lock file (this needs to be done by the calling function).
    """
    if (copy_files==False):
        operation=shutil.move
    else:
        operation=shutil.copy

    source_folder=config.mercure[mercure_folders.INCOMING] + '/'
    target_folder=target_path + '/'  

    for entry in file_list:
        try:
            operation(source_folder+entry+mercure_names.DCM, target_folder+entry+mercure_names.DCM)
            operation(source_folder+entry+mercure_names.TAGS,target_folder+entry+mercure_names.TAGS)
        except Exception:
            logger.exception(f'Problem while pushing file to outgoing {entry}')
            logger.exception(f'Source folder {source_folder}')
            logger.exception(f'Target folder {target_folder}')
            monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Problem while pushing file to outgoing {entry}')
            return False

    return True


def remove_series(file_list):
    """Deletes the given files from the incoming folder."""
    source_folder=config.mercure[mercure_folders.INCOMING] + '/'
    for entry in file_list:
        try:
            os.remove(source_folder+entry+mercure_names.TAGS)
            os.remove(source_folder+entry+mercure_names.DCM)
        except Exception:
            logger.exception(f'Error while removing file {entry}')
            monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Error while removing file {entry}')


def route_error_files():
    """
    Looks for error files, moves these files and the corresponding DICOM files to the error folder, 
    and sends an alert to the bookkeeper instance.
    """
    error_files_found = 0

    for entry in os.scandir(config.mercure['incoming_folder']):
        if entry.name.endswith(".error") and not entry.is_dir():
            # Check if a lock file exists. If not, create one.
            lock_file=Path(config.mercure['incoming_folder'] / entry.name + mercure_names.LOCK)
            if lock_file.exists():
                continue
            try:
                lock=helper.FileLock(lock_file)
            except:
                continue

            logger.error(f'Found incoming error file {entry.name}')
            error_files_found += 1

            shutil.move(config.mercure['incoming_folder'] + '/' + entry.name,
                        config.mercure['error_folder'] + '/' + entry.name)

            dicom_filename = entry.name[:-6]
            dicom_file = Path(config.mercure['incoming_folder'] + '/' + dicom_filename)
            if dicom_file.exists():
                shutil.move(config.mercure['incoming_folder'] + '/' + dicom_filename,
                            config.mercure['error_folder'] + '/' + dicom_filename)

            lock.free()

    if error_files_found > 0:
        monitor.send_event(monitor.h_events.PROCESSING, monitor.severity.ERROR, f'Error parsing {error_files_found} incoming files')
    return
