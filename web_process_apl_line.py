#! /usr/bin/env python
###########################################################
# This file has been created by ARSF Data Analysis Node and
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
import bandmath
import web_common
import smtplib
from email.mime.text import MIMEText
import platform
import tempfile
import shutil

from arsf_dem import dem_common_functions

def send_email(message, receive, subject, sender, no_bcc=False):
   """
   Sends an email using smtplib
   """
   msg = MIMEText(message)
   msg['From'] = sender
   msg['To'] = receive
   msg['Subject'] = subject
   recipients = []
   recipients.extend([receive])
   if not no_bcc:
      recipients.extend([web_common.BCC_EMAIL, web_common.ERROR_EMAIL])

   try:
      for recipient in recipients:
         mailer = smtplib.SMTP('localhost')
         response = mailer.sendmail(sender, recipient, msg.as_string())
         mailer.close()
   except Exception as e:
      print str(e)
      exit(1)


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
   send_email(message, web_common.ERROR_EMAIL, processing_folder + " ERROR", web_common.SEND_EMAIL)


def email_PI(pi_email, output_location, project):
   """
   Send an email to the PI telling them where they can download their data

   :param pi_email:
   :param output_location:
   :param project:
   :return:
   """
   folder_name = os.path.basename(os.path.normpath(output_location))
   download_link = web_common.DOWNLOAD_LINK.format(folder_name, project)

   message = 'Processing is complete for your order request {}, you can now download the data from the following location:\n\n' \
             '{}\n\n' \
             'The data will be available for a total of two weeks, however this may be extended if requested. If you identify any problems with your data or have issues downloading the data please contact ARSF-DAN staff at arsf-processing@pml.ac.uk.\n\n' \
             'Regards,\n' \
             'ARSF'

   message = message.format(folder_name, download_link)
   send_email(message, pi_email, folder_name + " order complete", web_common.SEND_EMAIL)

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
   status_link = web_common.STATUS_LINK.format(output_location, project)
   message = "This is to notify you that your ARSF data order has begun processing. You can track its progress at the following URL:\n\n" \
             "{}\n\n" \
             "You will receive a final email once all data has completed processing.\n"\
             "Regards,\n"\
             "ARSF"

   message=message.format(status_link)
   send_email(message, pi_email, output_location + " order processing", web_common.SEND_EMAIL)

def email_preprocessing_error(pi_email, output_location, project, reason):
   output_location = os.path.basename(os.path.normpath(output_location))
   if reason is 'dem_coverage':
      message="This is to notify you that your ARSF data order has encountered an error. The dem you uploaded does not cover enough of the project area. You can upload a new DEM file or choose to generate one at the link below.\n\n" \
              "{}\n\n" \
              "Once a new file has been uploaded your processing will progress.\n" \
              "Regards,\n" \
              "ARSF"
      error_link = web_common.SERVER_BASE + '/dem_error/' + output_location + '?project=' + project
      message=message.format(error_link)

   send_email(message, pi_email, output_location + ' processing error', web_common.SEND_EMAIL)


def status_update(status_file, newstage, line):
   """
   updates the status files with a new stage or completion
   :param status_file:
   :param newstage:
   :param line:
   :return:
   """
   open(status_file, 'w').write("{} = {}".format(line, newstage))


def line_handler(config_file, line_name, output_location, process_main_line, process_band_ratio):
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
   tmp_process = web_common.TEMP_PROCESSING
   line_details = dict(config.items(line_name))
   if output_location is None:
      output_location = line_details["output_folder"]
   #set up folders
   jday = "{0:03d}".format(int(line_details["julianday"]))

   if line_name[:1] in "f":
      sensor = "fenix"
   elif line_name[:1] in "h":
      sensor = "hawk"
   elif line_name[:1] in "e":
      sensor = "eagle"
   elif line_name[:1] in "o":
      sensor = "owl"
      raise NotImplementedError("owl not yet compatible")
   else:
      raise Exception("no compatible sensors found, check the input files naming convention beings with f, e, o or h.")

   sortie = line_details["sortie"]
   if sortie == "None":
      sortie = ''
   folder = line_details['sourcefolder']
   try:
      hyper_delivery = glob.glob(folder + web_common.HYPER_DELIVERY_FOLDER)[0]
   except IndexError:
      raise Exception("Could not find hyperspectral delivery folder. Tried "
                      "'{}'".format(folder + web_common.HYPER_DELIVERY_FOLDER))

   #wildcard in the middle to make sure line number doesn't muck things up
   lev1file=glob.glob(hyper_delivery + '/' + web_common.LEV1_FOLDER + '/' + line_name +'1b.bil')[0]
   maskfile = lev1file.replace(".bil", "_mask.bil")
   badpix_mask =  lev1file.replace(".bil", "_mask-badpixelmethod.bil")
   band_list = config.get(line_name, 'band_range')
   last_process=True
   if process_main_line:
      if process_band_ratio:
         last_process = False
      process_web_hyper_line(config, line_name, os.path.basename(lev1file), band_list, output_location, lev1file, hyper_delivery, input_lev1_file=None, data_type="uint16", last_process=last_process, tmp=tmp_process)

   if process_band_ratio:
      equations = [x for x in dict(config.items(line_name)) if "eq_" in x]
      for enum, eq_name in enumerate(equations):
         last_process=False
         if config.get(line_name, eq_name) in "True":
            equation = config.get('DEFAULT', eq_name)
            band_numbers = re.findall(r'band(\d{1,3})', equation)
            print(band_numbers)
            output_location_updated = output_location + "/level1b"
            bm_file, bands = bandmath.bandmath(lev1file, equation, output_location_updated, band_numbers, eqname=eq_name.replace("eq_", ""), maskfile=maskfile, badpix_mask=badpix_mask)
            if bands > 1:
               band_list = config.get(line_name, 'band_range')
            else:
               band_list = "1"
            bandmath_maskfile = bm_file.replace(".bil", "_mask.bil")
            polite_eq_name = eq_name.replace("eq_", "")
            if enum == len(equations)-1:
               last_process = True
            process_web_hyper_line(config, line_name, os.path.basename(bm_file), band_list, output_location, lev1file, hyper_delivery, input_lev1_file=bm_file, maskfile=bandmath_maskfile, eq_name=polite_eq_name, last_process=last_process, tmp=tmp_process)


def process_web_hyper_line(config, base_line_name, output_line_name, band_list, output_location, lev1file, hyper_delivery, input_lev1_file=None, skip_stages=[], maskfile=None, data_type="float32", eq_name=None, last_process=False, tmp=False):
   """
   Main function, takes a line and processes it through APL, generates a log file for each line with the output from APL

   This will stop if a file is not produced by APL for whatever reason.

   :param config_file:
   :param base_line_name:
   :param output_line_name:
   :param output_location:
   :return:
   """
   #set up logging
   logger = logging.getLogger()
   output_line_name, _, _ = output_line_name.replace(".","").replace("bil", "").rpartition("1b")
   logstat_name = output_line_name.replace("1b.bil","")
   if eq_name:
      logstat_name = logstat_name +"_"+ eq_name
   file_handler = logging.FileHandler(output_location + web_common.LOG_DIR + logstat_name + "_log.txt", mode='a')
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
   status_file = web_common.STATUS_FILE.format(output_location, logstat_name)

   #set our first status
   open(status_file, 'w+').write("{} = {}".format(logstat_name,  web_common.INITIAL_STATUS))
   output_line_name = logstat_name

   jday = "{0:03d}".format(int(line_details["julianday"]))

   if base_line_name[:1] in "f":
      sensor = "fenix"
   elif base_line_name[:1] in "h":
      sensor = "hawk"
   elif base_line_name[:1] in "e":
      sensor = "eagle"

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
       status_update(status_file, "ERROR - projection not identified", output_line_name)
       raise Exception("Unable to identify projection")

   #set up file locations and tmp folder if we need it
   if tmp:
      tempdir = tempfile.mkdtemp(prefix="ARF_WEB_", dir=web_common.TEMP_PROCESSING_DIR)
      masked_file = os.path.join(tempdir, output_line_name.replace(".bil","") + "_masked.bil")
      igm_file = os.path.join(tempdir, base_line_name + ".igm")
      mapname = os.path.join(tempdir, output_line_name + "3b_mapped.bil")
      igm_file_transformed = igm_file.replace(".igm", "_{}.igm").format(projection.replace(' ', '_'))
      final_masked_file = output_location + web_common.WEB_MASK_OUTPUT + output_line_name.replace(".bil","") + "_masked.bil"
      final_igm_file = output_location + web_common.WEB_IGM_OUTPUT + base_line_name + ".igm"
      final_igm_file_transformed = final_igm_file.replace(".igm", "_{}.igm").format(projection.replace(' ', '_'))
      final_mapname = output_location + web_common.WEB_MAPPED_OUTPUT +output_line_name + "3b_mapped.bil"
   else:
      masked_file = output_location + web_common.WEB_MASK_OUTPUT + output_line_name.replace(".bil","") + "_masked.bil"
      igm_file = output_location + web_common.WEB_IGM_OUTPUT + base_line_name + ".igm"
      mapname = output_location + web_common.WEB_MAPPED_OUTPUT +output_line_name + "3b_mapped.bil"


   #set new status to masking
   status_update(status_file, "aplmask", output_line_name)

   if not "masking" in skip_stages:
      if not 'none' in line_details['masking']:
         #generate masking command
         aplmask_cmd = ["aplmask"]
         aplmask_cmd.extend(["-lev1", input_lev1_file])
         if not 'all' in line_details['masking']:
            mask_list, ccd_list = masklookup(line_details['masking'])
            aplmask_cmd.extend(["-flags"])
            aplmask_cmd.extend(mask_list)
            if len(ccd_list) > 0:
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
             status_update(status_file, "ERROR - aplmask", output_line_name)
             logger.error([e, output_line_name])
             raise Exception(e)
      else:
         masked_file = input_lev1_file

   status_update(status_file, "aplcorr", output_line_name)

   #get the navfile
   nav_file = glob.glob(hyper_delivery + web_common.NAVIGATION_FOLDER + base_line_name + "*_nav_post_processed.bil")[0]

   #aplcorr command
   if not os.path.exists(igm_file):
      aplcorr_cmd = ["aplcorr"]
      aplcorr_cmd.extend(["-lev1file", lev1file])
      aplcorr_cmd.extend(["-navfile", nav_file])
      aplcorr_cmd.extend(["-vvfile", hyper_delivery + web_common.VIEW_VECTOR_FILE.format(sensor)])
      aplcorr_cmd.extend(["-dem", dem])
      aplcorr_cmd.extend(["-igmfile", igm_file])

      try:
          dem_common_functions.CallSubprocessOn(aplcorr_cmd, redirect=False, logger=logger)
          if not os.path.exists(igm_file):
              raise Exception("igm file not output by aplcorr!")
      except Exception as e:
          status_update(status_file, "ERROR - aplcorr", output_line_name)
          logger.error([e, output_line_name])
          raise Exception(e)

   igm_file_transformed = igm_file.replace(".igm", "_{}.igm").format(projection.replace(' ', '_'))

   if projection in "osng":
      projection = projection + " " + web_common.OSNG_SEPERATION_FILE

   status_update(status_file, "apltran", output_line_name)

   #build the transformation command, its worth running this just in case
   apltran_cmd = ["apltran"]
   apltran_cmd.extend(["-inproj", "latlong", "WGS84"])
   apltran_cmd.extend(["-igm", igm_file])
   apltran_cmd.extend(["-output", igm_file_transformed])
   if "utm" in projection:
      apltran_cmd.extend(["-outproj", "utm_wgs84{}".format(hemisphere), zone])
   elif "osng" in projection:
      apltran_cmd.extend(["-outproj", "osng", web_common.OSNG_SEPERATION_FILE])

   try:
      dem_common_functions.CallSubprocessOn(apltran_cmd, redirect=False, logger=logger)
      if not os.path.exists(igm_file_transformed):
         raise Exception("igm file not output by apltran!")
   except Exception as e:
      status_update(status_file, "ERROR - apltran", output_line_name)
      logger.error([e,output_line_name])
      raise Exception(e)

   status_update(status_file, "aplmap", output_line_name)

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
      status_update(status_file, "ERROR - aplmap", output_line_name)
      logger.error([e,output_line_name])
      raise Exception(e)

   status_update(status_file, "waiting to zip", output_line_name)

   waiting = True

   while waiting:
      stillwaiting = False
      for file in os.listdir(output_location + web_common.STATUS_DIR):
         f = open(output_location + web_common.STATUS_DIR + file, 'r')
         for line in f:
            if "zipping" in line:
               stillwaiting = True
      if not stillwaiting:
         waiting = False

   status_update(status_file, "zipping", output_line_name)

   zip_created=False
   try:
      with zipfile.ZipFile(mapname + ".zip", 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zip:
         #compress the mapped file
         zip.write(mapname, os.path.basename(mapname))
         zip.write(mapname + ".hdr", os.path.basename(mapname + ".hdr"))
         zip.close()
         zip_created = True
   except Exception as e:
      zip_created = False

   if zip_created:
      #we need to delete the resultant file and hdr to save space
      os.remove(mapname)
      os.remove(mapname + ".hdr")

   logger.info("Beginning final zipfile copy")

   if tmp:
      if web_common.DEBUG_FILE_WRITEBACK:
         logger.info("debug writeback requested, copy time will be increased!")
         shutil.move(masked_file, final_masked_file)
         shutil.move(masked_file + ".hdr", final_masked_file + ".hdr")
         shutil.move(igm_file, final_igm_file)
         shutil.move(igm_file+ ".hdr", final_igm_file + ".hdr")
         shutil.move(igm_file_transformed, final_igm_file_transformed)
         shutil.move(igm_file_transformed+ ".hdr", final_igm_file_transformed + ".hdr")
      shutil.move(mapname + ".zip", final_mapname + ".zip")


   logger.info(str("zipped " + output_line_name + " to " + mapname + ".zip" + " at " + output_location))

   status_update(status_file, "complete", output_line_name)

   #if all the files are complete its time to zip them together
   if last_process:
      all_check = True
      for status in os.listdir(output_location + web_common.STATUS_DIR):
         for l in open(output_location + web_common.STATUS_DIR + status):
            if "complete" not in l:
               if "not processing" not in l:
                  all_check = False

      if all_check:
         #if all are finished we'll use this process to zip all the zipped mapped files into one for download
         zip_mapped_folder = glob.glob(output_location + web_common.WEB_MAPPED_OUTPUT + "*.bil.zip")
         zip_contents_file = open(output_location + web_common.WEB_MAPPED_OUTPUT + "zip_contents.txt", 'a')
         for zip_mapped in zip_mapped_folder:
            zip_contents_file.write(zip_mapped + "\n")
         zip_contents_file.close()
         logger.info("outputting master zip")
         with zipfile.ZipFile(output_location + web_common.WEB_MAPPED_OUTPUT + line_details["project_code"] + '_' + line_details[
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
   args = parser.parse_args()

   line_handler(args.config, args.line, args.output, args.main, args.bandmath)
