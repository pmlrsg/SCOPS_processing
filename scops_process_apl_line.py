#! /usr/bin/env python
###########################################################
# This file has been created by NERC-ARF Data Analysis Node and
# is licensed under the GPL v3 Licence. A copy of this
# licence is available to download with this file.
###########################################################

"""
Main processor script for the arsf web processor, will normally be invoked by
web qsub but can also be used at the command line. This will take a level1b file
through masking to geocorrection dependant on options set in the config file.

web qsub needs to have produced a folder structure and DEM before this script
will work properly.

Author: Stephen Goult

Available functions
email_error: will send an email to the set address on failure of processing
email_PI: will email the PI on completion of processing and zipping with a download_link
status_update: updates status file with current stage
process_web_hyper_line: main function, take a config, line name and output folder to run apl in and zip finished files.
"""

import argparse
import os
import sys
if sys.version_info[0] < 3:
    import ConfigParser
else:
    import configparser as ConfigParser
import glob
import zipfile
import pipes
import logging
import re
import smtplib
from email.mime.text import MIMEText
import platform
import tempfile
import shutil
import atexit

import threading
import time

import status_db

import scops_bandmath
from scops import scops_common

from arsf_dem import dem_common_functions
import importlib

#set up logging
logger = logging.getLogger()

def sensor_folder_lookup(sensor_letter):
    """
    Give a sensor letter prefix will return a folder descriptor for delivery folder lookup

    :param sensor_letter: string
    :return: folder_key
    :rtype: string
    """
    if sensor_letter in "f":
        sensor = "fenix"
        folder_key = "hyperspectral"
    elif sensor_letter in "h":
        sensor = "hawk"
        folder_key = "hyperspectral"
    elif sensor_letter in "e":
        sensor = "eagle"
        folder_key = "hyperspectral"
    elif sensor_letter in "o":
        sensor = "owl"
        folder_key = "owl"
    else:
        raise Exception("no compatible sensors found, check the input files naming convention beings with f, e, o or h.")
    return folder_key

class line_proc_details:
    """
    A holder class for line/file details. Also handles writeback from temporary processing on success/failure
    """

    def __init__(self, processing_location, output_location, output_line_name, projection, is_tmp=False):
        self.is_tmp = is_tmp
        self.processing_location = processing_location
        self.output_location = output_location
        self.output_line_name = output_line_name
        self.projection = projection
        self.masked_file = os.path.join(processing_location, output_line_name + "_masked.bil")
        self.masked_file_hdr = os.path.join(processing_location, output_line_name + "_masked.bil.hdr")
        self.igm_file = os.path.join(processing_location, output_line_name + ".igm")
        self.igm_file_hdr = os.path.join(processing_location, output_line_name + ".igm.hdr")
        self.mapname = os.path.join(processing_location, output_line_name + "3b_mapped.bil")
        self.mapname_hdr = os.path.join(processing_location, output_line_name + "3b_mapped.bil.hdr")
        self.igm_file_transformed = self.igm_file.replace(".igm", "_{}.igm").format(projection.replace(' ', '_'))
        self.igm_file_transformed_hdr = self.igm_file.replace(".igm.hdr", "_{}.igm.hdr").format(projection.replace(' ', '_'))
        self.final_masked_file = os.path.join(self.output_location, scops_common.WEB_MASK_OUTPUT, output_line_name + "_masked.bil")
        self.final_masked_file_hdr = os.path.join(self.output_location, scops_common.WEB_MASK_OUTPUT, output_line_name + "_masked.bil.hdr")
        self.final_igm_file = os.path.join(self.output_location, scops_common.WEB_IGM_OUTPUT, output_line_name + ".igm")
        self.final_igm_file_hdr = os.path.join(self.output_location, scops_common.WEB_IGM_OUTPUT, output_line_name + ".igm.hdr")
        self.final_igm_file_transformed = self.final_igm_file.replace(".igm", "_{}.igm").format(projection.replace(' ', '_'))
        self.final_igm_file_transformed_hdr = self.final_igm_file.replace(".igm.hdr", "_{}.igm.hdr").format(projection.replace(' ', '_'))
        self.final_mapname = os.path.join(self.output_location, scops_common.WEB_MAPPED_OUTPUT, output_line_name + "3b_mapped.bil")
        self.final_mapname = os.path.join(self.output_location, scops_common.WEB_MAPPED_OUTPUT, output_line_name + "3b_mapped.bil.hdr")
        self.zipname = self.mapname + ".zip"
        self.final_zipname = self.final_mapname + ".zip"

def status_to_number(status):
    return scops_common.STAGES.index(status)

def writeback(processing_details):
    """
    Tries to shift all our produced files to the output folder, quietly fails.

    :param processing_details: line_proc_details
    :return: None
    """
    logger.info("Writeback called, sticking in {}".format(processing_details.output_location))
    for key in processing_details.__dict__.keys():
        logger.info([key , processing_details.__dict__[key]])
    outputs = [f.replace("final_", "") for f in processing_details.__dict__.keys() if "final_" in f]
    for output in outputs:
        final_output = "final_" + output
        if os.path.isfile(processing_details.__dict__[output]):
            shutil.move(processing_details.__dict__[output], processing_details.__dict__[final_output])
    if processing_details.is_tmp:
        shutil.rmtree(processing_details.processing_location)


def send_email(message, receive, subject, sender, no_bcc=False, no_error=True):
    """
    Sends an email using smtplib

    :param message: string
    :param receive: string (email)
    :param subject: string
    :param sender: string (email)
    :param no_bcc: bool
    :param no_error: bool
    :return: None
    """
    msg = MIMEText(message)
    msg['From'] = sender
    msg['To'] = receive
    msg['Subject'] = subject
    recipients = []
    recipients.extend([receive])

    if not no_bcc:
        recipients.extend([scops_common.BCC_EMAIL])

        if not no_error and (scops_common.ERROR_EMAIL != scops_common.BCC_EMAIL):
            recipients.extend([scops_common.ERROR_EMAIL])

    try:
        for recipient in recipients:
            mailer = smtplib.SMTP('localhost')
            response = mailer.sendmail(sender, recipient, msg.as_string())
            mailer.close()
    except Exception as e:
        logger.error(str(e))
        raise Exception(e)

def progress_detail_updater_spinner(processing_id, output_folder, logfile, line):
    """
    Updates the database with the current status/progress of our processing by interpretting the logs every second.
    Should be run in a seperate thread.

    :param processing_id: string
    :param output_folder: string
    :param logfile: string
    :param line: string
    """
    complete = False
    iter = 0
    while not complete:
        #find out our current status
        status = status_db.get_line_status_from_db(processing_id, line)
        try:
            #try to update the status database with progress
            progress_detail_updater(processing_id, output_folder, logfile, line, status)
        except Exception as e:
            #If we fail we shouldn't panic as the system will still work, but should say something about it
            logger.error(e)
        if status == "complete":
            complete = True
        if "ERROR" in status:
            #time for this to stop running
            complete = True
        iter += 1
        #give it some time to see if anything changes
        time.sleep(1)

def progress_detail_updater(processing_id, output_folder, logfile, line, status):
    """
    A big switch to work out what the log means at any point, vital for the updating of the database.

    :param processing_id: string
    :param output_folder: string
    :param logfile: string
    :param line: string
    :param status: string
    :return: None
    """
    zipfile_name = os.path.join(output_folder, 'mapped', os.path.basename(line) + "3b_mapped.bil.zip")

    zipbyte="MB"
    bytesize="MB"
    progress = 0
    zipsize=0
    try:
        approx_percents = list(open(logfile, 'r'))[-6:]
    except IndexError as e:
        approx_percent = "0"
        progress = 0

    for approx_percent in approx_percents:
        if "Approximate percent complete:" in approx_percent:
            progress = [int(s) for s in approx_percent[-10:].split() if s.isdigit()][0]
            #if it's greater than 100 we picked up the wrong bit of the message
            if progress > 100:
                progress = [int(s) for s in approx_percent[-16:].split() if s.isdigit()][0]
        else:
            progress = 0

    if os.path.exists(zipfile_name):
        #take it up to megabytes
        zipsize = float(os.path.getsize(zipfile_name)/1024/1024)
        if zipsize > 500:
            zipsize = zipsize / 1024
            zipbyte="GB"
        zipsize = round(zipsize, 2)
        progress = 0

    filesize=0

    for l in open(logfile):
        if "megabytes" in l:
            logline = l.split()
            filesize = float(logline[logline.index("megabytes") - 1])
            if filesize > 500:
                filesize = round((filesize / 1024), 2)
                bytesize="GB"

    weight = 0
    stageprogress=0
    stage="Waiting to process"

    if "complete" in status:
        weight = 0
        stage = "Complete"
        stageprogress = 100
    elif ("waiting to zip" in status) or ("zipping" in status):
        weight = 5
        stage = "Zipping"
        stageprogress = 95
    elif "aplmap" in status:
        weight = 50
        stage = "Mapping"
        stageprogress = 45
    elif "apltran" in status:
        weight = 15
        stage = "Translating"
        stageprogress = 30
    elif "aplcorr" in status:
        weight = 15
        stage = "Geo-correction"
        stageprogress = 15
    elif "aplmask" in status:
        weight = 15
        stage = "Masking"
        stageprogress = 0
    flag=False
    if "ERROR" in status:
        flag = True

    weight = float(weight)
    stageprogress = float(stageprogress)

    if not status == "complete":
        total_progress = stageprogress + ((progress / 100.0) * weight)
    else:
        total_progress = 100

    status_db.update_progress_details(processing_id, line, total_progress, filesize, bytesize, zipsize, zipbyte)


def masklookup(mask_string):
    """
    Compares a string of letters to a dict of mask options, outputs a string of
    mask numerics and CCD letters to input to aplmask
    :param mask_string: string
    :return: mask_list
    :rtype: list
    :return: ccd_list
    :rtype: list
    """
    mask_list = []
    ccd_list = []
    mask_dict = {
       "a": "A",
       "b": "B",
       "c": "C",
       "d": "D",
       "e": "E",
       "f": "F",
       "u": "1",
       "o": "2",
       "m": "8",
       "n": "16",
       "r": "32",
       "q": "64",
    }
    for char in mask_string:
        if char in "abcdef":
            if "4" not in mask_list:
                mask_list.append("4")
            ccd_list.append(mask_dict[char])
        else:
            mask_list.append(mask_dict[char])
    return mask_list, ccd_list

def email_error(stage, line, error, processing_folder):
    """
    Send an email to the set email telling us what went wrong
    :param stage:
    :param line:
    :param error:
    :param processing_folder:
    :return:
    """
    message = "Processing has failed on line {} at stage {} due to:\n\n" \
              "{}\n\n" \
              "The processing is contained in folder {}\n\n" \
              "Once the issue has been resolved update the relevant status file to state either 'complete' or 'waiting to zip' (dependant on what you've done)"

    message = message.format(line, stage, error, processing_folder)
    send_email(message, scops_common.ERROR_EMAIL, processing_folder + " ERROR", scops_common.SEND_EMAIL)


def email_PI(pi_email, output_location, project):
    """
    Send an email to the PI telling them where they can download their data

    :param pi_email:
    :param output_location:
    :param project:
    :return:
    """
    folder_name = os.path.basename(os.path.normpath(output_location))
    download_link = scops_common.DOWNLOAD_LINK.format(folder_name, project)

    message = 'Processing is complete for your order request {}, you can now download the data from the following location:\n\n' \
              '{}\n\n' \
              'The data will be available for a total of two weeks, however this may be extended if requested. If you identify any problems with your data or have issues downloading the data please contact NERC-ARF-DAN staff at arsf-processing@pml.ac.uk.\n\n' \
              'Regards,\n' \
              'NERC-ARF-DAN'

    message = message.format(folder_name, download_link)
    send_email(message, pi_email, folder_name + " order complete", scops_common.SEND_EMAIL)

def email_status(pi_email, output_location, project):
    """
    Sends an email to update the user that their data has begun processing. This
    includes a link to the status page where they can watch progress bars and
    download files on completion.

    :param pi_email:
    :param output_location:
    :param project:
    """
    output_location = os.path.basename(os.path.normpath(output_location))
    status_link = scops_common.STATUS_LINK.format(output_location, project)
    message = "This is to notify you that your NERC-ARF data order has begun processing. You can track its progress at the following URL:\n\n" \
              "{}\n\n" \
              "You will receive a final email once all data has completed processing.\n"\
              "Regards,\n"\
              "NERC-ARF-DAN"

    message=message.format(status_link)
    send_email(message, pi_email, output_location + " order processing", scops_common.SEND_EMAIL)

def email_preprocessing_error(pi_email, output_location, project, reason):
    """
    Sends an email to update the user that their data has begun processing but
    the DEM is not compatible. This problem will require input from both the user
    and an operator.

    :param pi_email:
    :param output_location:
    :param project:
    :param reason:
    """
    output_location = os.path.basename(os.path.normpath(output_location))
    if reason is 'dem_coverage':
        message="This is to notify you that your NERC-ARF data order has encountered an error. " \
                "The dem you uploaded does not cover enough of the project area. "\
                "You can upload a new DEM file or choose to generate one at the link below.\n\n" \
                "{}\n\n" \
                "Once a new file has been uploaded your processing will progress.\n" \
                "Regards,\n" \
                "NERC-ARF-DAN"
        error_link = scops_common.SERVER_BASE + '/dem_error/' + output_location + '?project=' + project
        message=message.format(error_link)

    send_email(message, pi_email, output_location + ' processing error', scops_common.SEND_EMAIL)


def status_update(processing_folder, status_file, newstage, line):
    """
    Updates the status files with a new stage or completion.
    Uses the processing folder as a processing id in the status databse if enabled.
    :param processing_folder:
    :param status_file:
    :param newstage:
    :param line:
    :return:
    """
    if scops_common.USE_DB:
        status_db.update_status(processing_folder, line, newstage)
    open(status_file, 'w').write("{} = {}".format(line, newstage))


def line_handler(config_file, line_name, output_location, process_main_line, process_band_ratio, resume=False):
    """
    The main handler function. This grabs all lines that need to be processed,
    performs band math if requested then dispatches a series of APL requests to
    mask, translate and map the data. Finally it will zip all files together to
    make downloading easier.

    :param config_file:
    :param line_name:
    :param output_location:
    :param process_main_line:
    :param process_band_ratio:
    """
    if not os.path.isfile(config_file):
        raise IOError("Config file not found. Check {} is a valid file".format(config_file))
    #read the config
    config = ConfigParser.SafeConfigParser()
    config.read(config_file)
    #set processing in tmp space to True
    tmp_process = scops_common.TEMP_PROCESSING
    line_details = dict(config.items(line_name))
    if output_location is None:
        output_location = line_details["output_folder"]

    processing_id = os.path.basename(line_details["output_folder"])
    #set up folders
    jday = "{0:03d}".format(int(line_details["julianday"]))

    folder_key = sensor_folder_lookup(line_name[:1])

    delivery_folder = scops_common.HYPER_DELIVERY_FOLDER.format(folder_key)

    sortie = line_details["sortie"]
    if sortie == "None":
        sortie = ''
    folder = line_details['sourcefolder']
    try:
        hyper_delivery = glob.glob(os.path.join(folder, delivery_folder))[0]
    except IndexError:
        raise Exception("Could not find hyperspectral delivery folder. Tried "
                        "'{}'".format(folder + delivery_folder))

    #wildcard in the middle to make sure line number doesn't muck things up
    lev1file=glob.glob(hyper_delivery + '/' + scops_common.LEV1_FOLDER + '/' + line_name +'1b.bil')[0]
    maskfile = lev1file.replace(".bil", "_mask.bil")
    badpix_mask =  lev1file.replace(".bil", "_mask-badpixelmethod.bil")
    band_list = config.get(line_name, 'band_range')
    last_process=True
    if process_main_line:
        if process_band_ratio:
            last_process = False
        process_web_hyper_line(config, line_name, os.path.basename(lev1file), band_list, output_location, lev1file, hyper_delivery, input_lev1_file=None, data_type="uint16", last_process=last_process, tmp=tmp_process, resume=resume)

    if process_band_ratio:
        equations = [x for x in dict(config.items(line_name)) if "eq_" in x]
        plugins = [x for x in dict(config.items(line_name)) if "plugin_" in x]
        #process the equations from the band math
        for enum, eq_name in enumerate(equations):
            last_process=False
            if config.get(line_name, eq_name) in "True":
                equation = config.get('DEFAULT', eq_name)
                band_numbers = re.findall(r'band(\d{1,3})', equation)
                output_location_updated = output_location + "/level1b"
                bm_file, bands = scops_bandmath.bandmath(lev1file, equation, output_location_updated, band_numbers, eqname=eq_name.replace("eq_", ""), maskfile=maskfile, badpix_mask=badpix_mask)
                if bands > 1:
                    band_list = config.get(line_name, 'band_range')
                else:
                    band_list = "1"
                bandmath_maskfile = bm_file.replace(".bil", "_mask.bil")
                polite_eq_name = eq_name.replace("eq_", "")
                if enum == len(equations)-1:
                    last_process = True
                process_web_hyper_line(config, line_name, os.path.basename(bm_file), band_list, output_location, lev1file, hyper_delivery, input_lev1_file=bm_file, maskfile=bandmath_maskfile, eq_name=polite_eq_name, last_process=last_process, tmp=tmp_process, resume=resume)

        #process the plugins - these are all for level1b running
        sys.path.append(config.get('DEFAULT','plugin_directory'))
        for enum, plugin_name in enumerate(plugins):
            last_process=False
            if config.get(line_name, plugin_name) in "True":
                equation = config.get('DEFAULT', plugin_name)
                output_location_updated = output_location + "/level1b"
                #import plugin_name
                polite_plugin_name = plugin_name.replace("plugin_", "")
                plugin_module_name=polite_plugin_name.replace(".py","")
                plugin_module=importlib.import_module(plugin_module_name)
                #run plugin
                plugin_args={'output_folder' : output_location_updated,
                             'hsi_filename' : lev1file,
                             }
                processed_file=plugin_module.run(**plugin_args)
                #always do all bands
                band_list="ALL"
                #do not do masking as the mask does not match this file anymore. Potentially should apply mask first before running the plugin
                skip_stages=['masking']
                if enum == len(plugins)-1:
                    last_process = True
                process_web_hyper_line(config, line_name, os.path.basename(processed_file), band_list, output_location, lev1file, hyper_delivery, input_lev1_file=processed_file, skip_stages=skip_stages,maskfile=None, eq_name=polite_plugin_name, last_process=last_process, tmp=tmp_process)



def process_web_hyper_line(config, base_line_name, output_line_name, band_list, output_location, lev1file, hyper_delivery, input_lev1_file=None, skip_stages=[], maskfile=None, data_type="float32", eq_name=None, last_process=False, tmp=False, resume=True):
    """
    Main function, takes a line and processes it through APL, generates a log file for each line with the output from APL

    This will stop if a file is not produced by APL for whatever reason.

    :param config_file:
    :param base_line_name:
    :param output_line_name:
    :param output_location:
    :return:
    """

    #if output_location is missing a trailing slash - add it on
    output_location=os.path.join(output_location, '')

    output_line_name, _, _ = output_line_name.replace(".","").replace("bil", "").rpartition("1b")
    logstat_name = output_line_name.replace("1b.bil","")
    if eq_name:
        logstat_name = logstat_name +"_"+ eq_name
    logfile = os.path.join(output_location, scops_common.LOG_DIR, logstat_name + "_log.txt")
    if os.path.isfile(logfile):
        os.remove(logfile)
    file_handler = logging.FileHandler(logfile, mode='a')
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.handlers = []
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    #ouput host details - may be useful for debugging
    nameinfo=platform.uname()
    distinfo=platform.dist()
    platformstring=" ".join(nameinfo)+"\n"+" ".join(distinfo)
    logger.info(platformstring)

    #get the line section we want
    line_details = dict(config.items(base_line_name))
    status_file = scops_common.STATUS_FILE.format(output_location, logstat_name)
    #set processing id for database things
    processing_id = os.path.basename(line_details["output_folder"])

    #set our first status
    open(status_file, 'w+').write("{} = {}".format(logstat_name,  scops_common.INITIAL_STATUS))
    output_line_name = logstat_name
    
    if not resume:
        link = scops_common.LINE_LINK.format(processing_id, output_line_name, line_details["project_code"])
        try:
            status_db.insert_line_into_db(processing_id, output_line_name, "Waiting to process", 0, 0, 0, 0, link, 0, 0)
        except:
            pass
        #in case we've already run it once
        status_update(processing_id, status_file, "Waiting to process", output_line_name)
    try:
        #if we fail then the webpage won't be able to update - this is less than ideal but we can always resubmit the job
        progress_thread = threading.Thread(target=progress_detail_updater_spinner, args=(processing_id, output_location, logfile, output_line_name))
        progress_thread.daemon = True
        progress_thread.start()
    except Exception as e:
        logger.error(e)
    
    if resume:
        try:
            resume_stage = status_db.get_line_status_from_db(processing_id, output_line_name)
        except:
            link = scops_common.LINE_LINK.format(processing_id, output_line_name, line_details["project_code"])
            status_db.insert_line_into_db(processing_id, output_line_name, "Waiting to process", 0, 0, 0, 0, link, 0, 0)
        start_stage = status_to_number(resume_stage)
    else:
        start_stage = 0

    jday = "{0:03d}".format(int(line_details["julianday"]))

    if base_line_name[:1] in "f":
        sensor = "fenix"
    elif base_line_name[:1] in "h":
        sensor = "hawk"
    elif base_line_name[:1] in "e":
        sensor = "eagle"
    elif base_line_name[:1] in "o":
        sensor = "owl"
    else:
        raise TypeError("Sensor type has not been implemented yet")

    if maskfile is None:
        maskfile = lev1file.replace(".bil", "_mask.bil")
    dem = line_details["dem_name"]

    if input_lev1_file is None:
        input_lev1_file = lev1file

    #check if we want to ignore free disk space when running aplmap
    #(for filesystems which don't report free space correctly)
    try:
        aplmap_ignore_freespace = config.getboolean(base_line_name,
                                                 "aplmap_ignore_freespace")
    except ConfigParser.NoOptionError:
        aplmap_ignore_freespace = False

    projection = line_details["projection"]

    #set up projection strings
    if "UTM" in line_details["projection"]:
        zone = line_details["projection"].split(" ")[2]
        hemisphere=zone[2:]
        zone = zone[:-1]

        projection = pipes.quote("utm_wgs84{}_{}".format(hemisphere, zone))
    elif "UKBNG" in line_details["projection"]:
        projection = "osng"
    else:
        logger.error("Couldn't find the projection from input string")
        status_update(processing_id, status_file, "ERROR - projection not identified", output_line_name)
        raise Exception("Unable to identify projection")

    #set up file locations and tmp folder if we need it
    if tmp:
        tempdir = tempfile.mkdtemp(prefix="ARF_WEB_", dir=scops_common.TEMP_PROCESSING_DIR)
        masked_file = os.path.join(tempdir, output_line_name.replace(".bil","") + "_masked.bil")
        igm_file = os.path.join(tempdir, base_line_name + ".igm")
        mapname = os.path.join(tempdir, output_line_name + "3b_mapped.bil")
        igm_file_transformed = igm_file.replace(".igm", "_{}.igm").format(projection.replace(' ', '_'))
        final_masked_file = os.path.join(output_location, scops_common.WEB_MASK_OUTPUT, output_line_name.replace(".bil","") + "_masked.bil")
        final_igm_file = os.path.join(output_location, scops_common.WEB_IGM_OUTPUT, base_line_name + ".igm")
        final_igm_file_transformed = final_igm_file.replace(".igm", "_{}.igm").format(projection.replace(' ', '_'))
        final_mapname = os.path.join(output_location, scops_common.WEB_MAPPED_OUTPUT, output_line_name + "3b_mapped.bil")
    else:
        masked_file = os.path.join(output_location, scops_common.WEB_MASK_OUTPUT, output_line_name.replace(".bil","") + "_masked.bil")
        igm_file = os.path.join(output_location, scops_common.WEB_IGM_OUTPUT, base_line_name + ".igm")
        mapname = os.path.join(output_location, scops_common.WEB_MAPPED_OUTPUT, output_line_name + "3b_mapped.bil")

    line_processing_details = line_proc_details(tempdir,output_location,output_line_name,projection,is_tmp=tmp)

    atexit.register(writeback, line_processing_details)
    
    if not "masking" in skip_stages or start_stage <= 1:
        #set new status to masking
        status_update(processing_id, status_file, "aplmask", output_line_name)
        if not 'none' in line_details['masking']:
            #generate masking command
            aplmask_cmd = ["aplmask"]
            aplmask_cmd.extend(["-lev1", input_lev1_file])
            if not 'all' in line_details['masking']:
                mask_list, ccd_list = masklookup(line_details['masking'])
                aplmask_cmd.extend(["-flags"])
                aplmask_cmd.extend(mask_list)
                if len(ccd_list) > 0:
                    if os.path.isfile(maskfile.replace('mask.bil', 'mask-badpixelmethod.bil')):
                        aplmask_cmd.extend(["-onlymaskmethods", maskfile.replace('mask.bil', 'mask-badpixelmethod.bil')])
                        aplmask_cmd.extend(ccd_list)
            aplmask_cmd.extend(["-mask", maskfile])
            aplmask_cmd.extend(["-output", masked_file])

            #try running the command and except on failure
            try:
                dem_common_functions.CallSubprocessOn(aplmask_cmd, redirect=False, logger=logger)
                if not os.path.exists(masked_file):
                    raise Exception("masked file not output")
            except Exception as e:
                status_update(processing_id, status_file, "ERROR - aplmask", output_line_name)
                logger.error([e, output_line_name])
                raise Exception(e)
        else:
            masked_file = input_lev1_file
    else:
        masked_file = input_lev1_file

    #aplcorr command
    if not os.path.exists(igm_file) or start_stage <= 2:
        status_update(processing_id, status_file, "aplcorr", output_line_name)

        #get the navfile
        nav_file = glob.glob(hyper_delivery + scops_common.NAVIGATION_FOLDER + base_line_name + "*_nav_post_processed.bil")[0]

        aplcorr_cmd = ["aplcorr"]
        aplcorr_cmd.extend(["-lev1file", lev1file])
        aplcorr_cmd.extend(["-navfile", nav_file])
        aplcorr_cmd.extend(["-vvfile", hyper_delivery + scops_common.VIEW_VECTOR_FILE.format(sensor)])
        aplcorr_cmd.extend(["-dem", dem])
        aplcorr_cmd.extend(["-igmfile", igm_file])

        try:
            dem_common_functions.CallSubprocessOn(aplcorr_cmd, redirect=False, logger=logger)
            if not os.path.exists(igm_file):
                raise Exception("igm file not output by aplcorr!")
        except Exception as e:
            status_update(processing_id, status_file, "ERROR - aplcorr", output_line_name)
            logger.error([e, output_line_name])
            raise Exception(e)

    igm_file_transformed = igm_file.replace(".igm", "_{}.igm").format(projection.replace(' ', '_'))

    if projection in "osng":
        projection = projection + " " + scops_common.OSNG_SEPERATION_FILE

    if start_stage <= 3:
        status_update(processing_id, status_file, "apltran", output_line_name)

        #build the transformation command, its worth running this just in case
        apltran_cmd = ["apltran"]
        apltran_cmd.extend(["-inproj", "latlong", "WGS84"])
        apltran_cmd.extend(["-igm", igm_file])
        apltran_cmd.extend(["-output", igm_file_transformed])
        if "utm" in projection:
            apltran_cmd.extend(["-outproj", "utm_wgs84{}".format(hemisphere), zone])
        elif "osng" in projection:
            apltran_cmd.extend(["-outproj", "osng", scops_common.OSNG_SEPERATION_FILE])

        try:
            dem_common_functions.CallSubprocessOn(apltran_cmd, redirect=False, logger=logger)
            if not os.path.exists(igm_file_transformed):
                raise Exception("igm file not output by apltran!")
        except Exception as e:
            status_update(processing_id, status_file, "ERROR - apltran", output_line_name)
            logger.error([e,output_line_name])
            raise Exception(e)

    if start_stage <= 4:
        status_update(processing_id, status_file, "aplmap", output_line_name)

        #set pixel size and map name
        pixelx, pixely = line_details["pixelsize"].split(" ")


        aplmap_cmd = ["aplmap"]
        aplmap_cmd.extend(["-igm", igm_file_transformed])
        aplmap_cmd.extend(["-lev1", masked_file])
        aplmap_cmd.extend(["-pixelsize", pixelx, pixely])
        aplmap_cmd.extend(["-bandlist", band_list])
        aplmap_cmd.extend(["-interpolation", line_details["interpolation"]])
        aplmap_cmd.extend(["-mapname", mapname])
        aplmap_cmd.extend(["-buffersize", str(4096)])
        aplmap_cmd.extend(["-outputlevel", "verbose"])
        aplmap_cmd.extend(["-outputdatatype", data_type])
        if aplmap_ignore_freespace:
            aplmap_cmd.extend(["-ignorediskspace"])

        try:
            log = dem_common_functions.CallSubprocessOn(aplmap_cmd, redirect=False, logger=logger)
            if not os.path.exists(mapname):
                raise Exception("mapped file not output by aplmap!")
        except Exception as e:
            status_update(processing_id, status_file, "ERROR - aplmap", output_line_name)
            logger.error([e,output_line_name])
            raise Exception(e)

    status_update(processing_id, status_file, "waiting to zip", output_line_name)

    waiting = True

    while waiting:
        stillwaiting = False
        for file in os.listdir(output_location + scops_common.STATUS_DIR):
            f = open(output_location + scops_common.STATUS_DIR + file, 'r')
            for line in f:
                if "zipping" in line:
                    stillwaiting = True
        if not stillwaiting:
            waiting = False

    status_update(processing_id, status_file, "zipping", output_line_name)

    zip_created=False
    try:
        with zipfile.ZipFile(mapname + ".zip", 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zip:
            #compress the mapped file
            zip.write(mapname, os.path.basename(mapname))
            zip.write(mapname + ".hdr", os.path.basename(mapname + ".hdr"))
            zip.close()
            zip_created = True
    except Exception as e:
        logger.error(e)
        zip_created = False

    if zip_created:
        #we need to delete the resultant file and hdr to save space
        os.remove(mapname)
        os.remove(mapname + ".hdr")

    logger.info("Beginning final zipfile copy")

    if tmp:
        if scops_common.DEBUG_FILE_WRITEBACK:
            logger.info("debug writeback requested, copy time will be increased!")
            writeback(line_processing_details)
        shutil.move(mapname + ".zip", final_mapname + ".zip")
        shutil.rmtree(tempdir)


    logger.info(str("zipped " + output_line_name + " to " + mapname + ".zip" + " at " + output_location))

    status_update(processing_id, status_file, "complete", output_line_name)

    #if all the files are complete its time to zip them together
    if last_process:
        all_check = True
        for status in os.listdir(output_location + scops_common.STATUS_DIR):
            for l in open(output_location + scops_common.STATUS_DIR + status):
                if "complete" not in l:
                    if "not processing" not in l:
                        all_check = False

        if all_check:
            #if all are finished we'll use this process to zip all the zipped mapped files into one for download
            zip_mapped_folder = glob.glob(output_location + scops_common.WEB_MAPPED_OUTPUT + "*.bil.zip")
            zip_contents_file = open(output_location + scops_common.WEB_MAPPED_OUTPUT + "zip_contents.txt", 'a')
            for zip_mapped in zip_mapped_folder:
                zip_contents_file.write(zip_mapped + "\n")
            zip_contents_file.close()
            logger.info("outputting master zip")
            with zipfile.ZipFile(output_location + scops_common.WEB_MAPPED_OUTPUT + line_details["project_code"] + '_' + line_details[
               "year"] + jday + '.zip', 'a', zipfile.ZIP_STORED, allowZip64=True) as zip:
                for zip_mapped in zip_mapped_folder:
                    logger.info("zipping " + zip_mapped)
                    zip.write(zip_mapped, line_details["project_code"] + '_' + line_details["year"] + jday + "/" + os.path.basename(zip_mapped))
                #must close the file or it won't have final bits
                zip.close()
            #this *shouldn't* trigger until the zip file finishes
            email_PI(line_details["email"], output_location, line_details["project_code"])


if __name__ == '__main__':
    # Get the input arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--config',
                        '-c',
                        help='web config file',
                        default=None,
                        required=True,
                        metavar="<configfile>")
    parser.add_argument('--line',
                        '-l',
                        help='line to process',
                        default=None,
                        required=True,
                        metavar="<line>")
    parser.add_argument('--sensor',
                        '-s',
                        help='sensor type',
                        default=None,
                        metavar="<sensor>")
    parser.add_argument('--output',
                        '-o',
                        help='base output folder, should reflect the structure built by web_structure() in web_qsub.py',
                        default=None,
                        metavar="<folder>")
    parser.add_argument('--main',
                        '-m',
                        help='process main line',
                        action="store_true",
                        dest="main")
    parser.add_argument('--bandmath',
                        '-b',
                        help='process main line',
                        action="store_true",
                        dest="bandmath")
    parser.add_argument('--resume',
                        '-r',
                        help="Try to pick up where we left off",
                        action="store_true",
                        dest="noresume")
    args = parser.parse_args()
    line_handler(args.config, args.line, args.output, args.main, args.bandmath, resume=args.resume)
