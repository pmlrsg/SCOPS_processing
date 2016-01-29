#! /usr/bin/env python
"""
Main processor script for the arsf web processor, will normally be invoked by
web qsub but can also be used at the command line. This will take a level1b file
through masking to geocorrection dependant on options set in the config file.

web qsub needs to have produced a folder structure and DEM before this script
will work properly.

Author: Stephen Goult

Available functions
email_error: will send an email to arsf-code on failure of processing
email_PI: will email the PI on completion of processing and zipping with a download_link
status_update: updates status file with current stage
process_web_hyper_line: main function, take a config, line name and output folder to run apl in and zip finished files.
"""

import argparse
import os
import ConfigParser
import common_functions
import folder_structure
import glob
import zipfile
import pipes
import smtplib
import logging
import re
import bandmath
from common_arsf.web_functions import send_email

WEB_MASK_OUTPUT = "/level1b/"
WEB_IGM_OUTPUT = "/igm/"
WEB_MAPPED_OUTPUT = "/mapped/"
VIEW_VECTOR_FILE = "/sensor_FOV_vectors/{}_fov_fullccd_vectors.bil"
OSNG_SEPERATION_FILE = "/users/rsg/arsf/dems/ostn02/OSTN02_NTv2.gsb"
STATUS_DIR = "/status/"
ERROR_DIR = "/status/"
LOG_DIR = "/logs/"
STATUS_FILE = "{}/status/{}_status.txt"
INITIAL_STATUS = "initialising"
NAVIGATION_FOLDER = "/flightlines/navigation/"
SEND_EMAIL = "arsf-processing@pml.ac.uk"
ERROR_EMAIL = "stgo@pml.ac.uk" #TODO change from stgo!
DOWNLOAD_LINK = 'https://arsf-dandev.nerc.ac.uk/processor/downloads/{}?&project={}'

def email_error(stage, line, error, processing_folder):
   """
   Send an email to arsf-code telling us what went wrong
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
   common_arsf.web_functions.send_email(message, ERROR_EMAIL, processing_folder + " ERROR", SEND_EMAIL)


def email_PI(pi_email, output_location, project):
   """
   Send an email to the PI telling them where they can download their data

   :param pi_email:
   :param output_location:
   :param project:
   :return:
   """
   folder_name = os.path.basename(os.path.normpath(output_location))
   download_link = DOWNLOAD_LINK.format(folder_name, project)

   message = 'Processing is complete for your order request {}, you can now download the data from the following location:\n\n' \
             '{}\n\n' \
             'The data will be available for a total of two weeks, however this may be extended if requested. If you identify any problems with your data or have issues downloading the data please contact ARSF staff at arsf-processing@pml.ac.uk.\n\n' \
             'Regards,\n' \
             'ARSF'

   message = message.format(folder_name, download_link)
   common_arsf.web_functions.send_email(message, pi_email, folder_name + " order complete", SEND_EMAIL)


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
   #read the config
   config = ConfigParser.SafeConfigParser()
   config.read(config_file)
   line_details = dict(config.items(line_name))
   #set up folders
   jday = "{0:03d}".format(int(line_details["julianday"]))
   line_number = str(line_name[-2:])

   if line_name[:1] in "f":
      sensor = "fenix"
   elif line_name[:1] in "h":
      sensor = "hawk"
   elif line_name[:1] in "e":
      sensor = "eagle"
   folder = folder_structure.FolderStructure(year=line_details["year"],
                                             jday=jday,
                                             projectCode=line_details["project_code"],
                                             fletter=line_details["sortie"],
                                             lineId=line_number,
                                             lineNumber=line_number,
                                             sct=1,
                                             sensor=sensor)

   hyper_delivery = glob.glob(folder.projPath + "/delivery/*hyperspectral*/")[0]

   folder = folder_structure.FolderStructure(year=line_details["year"],
                                             jday=jday,
                                             projectCode=line_details["project_code"],
                                             fletter=line_details["sortie"],
                                             lineId=line_number,
                                             lineNumber=line_number,
                                             sct=1,
                                             delPath=hyper_delivery,
                                             sensor=sensor,
                                             absolute=True)

   lev1file = folder.getLev1File(delivery=True)
   maskfile = lev1file.replace(".bil", "_mask.bil")
   band_list = config.get(line_name, 'band_range')
   if process_main_line:
      process_web_hyper_line(config, line_name, os.path.basename(lev1file), band_list, output_location, lev1file, hyper_delivery, input_lev1_file=None)

   if process_band_ratio:
      equations = [x for x in dict(config.items(line_name)) if "eq_" in x]
      for eq_name in equations:
         if config.get(line_name, eq_name) in "True":
            print "basooning"
            equation = config.get('DEFAULT', eq_name)
            band_numbers = re.findall(r'band(\d{3})', equation)
            output_location_updated = output_location + "/level1b"
            bm_file, bands = bandmath.bandmath(lev1file, equation, output_location_updated, band_numbers, eqname=eq_name.replace("eq_", "_"), maskfile=maskfile)
            if bands > 1:
               band_list = config.get(line_name, 'band_range')
            else:
               band_list = "1"
            bandmath_maskfile = bm_file.replace(".bil", "_mask.bil")
            process_web_hyper_line(config, line_name, os.path.basename(bm_file), band_list, output_location, lev1file, hyper_delivery, input_lev1_file=bm_file, maskfile=bandmath_maskfile)


def process_web_hyper_line(config, base_line_name, output_line_name, band_list, output_location, lev1file, hyper_delivery, input_lev1_file=None, skip_stages=[], maskfile=None):
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
   output_line_name = output_line_name.replace(".","").replace("bil", "").replace("1b","")
   file_handler = logging.FileHandler(output_location + LOG_DIR + output_line_name.replace("1b.bil","") + "_log.txt", mode='a')
   formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
   file_handler.setFormatter(formatter)
   logger.addHandler(file_handler)
   logger.setLevel(logging.DEBUG)

   #get the line section we want
   line_details = dict(config.items(base_line_name))
   line_number = str(base_line_name[-2:])
   status_file = STATUS_FILE.format(output_location, output_line_name.replace("1b.bil", ""))

   #set our first status
   open(status_file, 'w+').write("{} = {}".format(output_line_name, INITIAL_STATUS))

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

   #set new status to masking
   status_update(status_file, "aplmask", output_line_name)

   #create output file name
   masked_file = output_location + WEB_MASK_OUTPUT + output_line_name.replace(".bil","") + "_masked.bil"

   if not "masking" in skip_stages:
      #generate masking command
      aplmask_cmd = ["aplmask"]
      aplmask_cmd.extend(["-lev1", input_lev1_file])
      aplmask_cmd.extend(["-mask", maskfile])
      aplmask_cmd.extend(["-output", masked_file])

      #try running the command and except on failure
      try:
          common_functions.CallSubprocessOn(aplmask_cmd, redirect=False, logger=logger)
          if not os.path.exists(masked_file):
              raise Exception, "masked file not output"
      except Exception as e:
          status_update(status_file, "ERROR - aplmask", output_line_name)
          logger.error([e, output_line_name])
          exit(1)

   status_update(status_file, "aplcorr", output_line_name)

   #create filenames
   igm_file = output_location + WEB_IGM_OUTPUT + base_line_name + ".igm"
   nav_file = glob.glob(hyper_delivery + NAVIGATION_FOLDER + base_line_name + "*_nav_post_processed.bil")[0]

   #aplcorr command
   if not os.path.exists(igm_file):
      aplcorr_cmd = ["aplcorr"]
      aplcorr_cmd.extend(["-lev1file", lev1file])
      aplcorr_cmd.extend(["-navfile", nav_file])
      aplcorr_cmd.extend(["-vvfile", hyper_delivery + VIEW_VECTOR_FILE.format(sensor)])
      aplcorr_cmd.extend(["-dem", dem])
      aplcorr_cmd.extend(["-igmfile", igm_file])

      try:
          common_functions.CallSubprocessOn(aplcorr_cmd, redirect=False, logger=logger)
          if not os.path.exists(igm_file):
              raise Exception, "igm file not output by aplcorr!"
      except Exception as e:
          status_update(status_file, "ERROR - aplcorr", output_line_name)
          logger.error([e, output_line_name])
          #error_write(output_location, e,output_line_name)
          exit(1)

   igm_file_transformed = igm_file.replace(".igm", "_{}.igm").format(projection.replace(' ', '_'))

   if projection in "osng":
      projection = projection + " " + OSNG_SEPERATION_FILE

   status_update(status_file, "apltran", output_line_name)

   #build the transformation command, its worth running this just in case
   apltran_cmd = ["apltran"]
   apltran_cmd.extend(["-inproj", "latlong", "WGS84"])
   apltran_cmd.extend(["-igm", igm_file])
   apltran_cmd.extend(["-output", igm_file_transformed])
   if "utm" in projection:
      apltran_cmd.extend(["-outproj", "utm_wgs84{}".format(hemisphere, zone)])
   elif "osng" in projection:
      apltran_cmd.extend(["-outproj", "osng", OSNG_SEPERATION_FILE])

   try:
      common_functions.CallSubprocessOn(apltran_cmd, redirect=False, logger=logger)
      if not os.path.exists(igm_file_transformed):
         raise Exception, "igm file not output by apltran!"
   except Exception as e:
      status_update(status_file, "ERROR - apltran", output_line_name)
      logger.error([e,output_line_name])
      #error_write(output_location, e,output_line_name)
      exit(1)

   status_update(status_file, "aplmap", output_line_name)

   #set pixel size and map name
   pixelx, pixely = line_details["pixelsize"].split(" ")

   mapname = output_location + WEB_MAPPED_OUTPUT +output_line_name + "3b_mapped.bil"

   aplmap_cmd = ["aplmap"]
   aplmap_cmd.extend(["-igm", igm_file_transformed])
   aplmap_cmd.extend(["-lev1", masked_file])
   aplmap_cmd.extend(["-pixelsize", pixelx, pixely])
   aplmap_cmd.extend(["-bandlist", band_list])
   aplmap_cmd.extend(["-interpolation", line_details["interpolation"]])
   aplmap_cmd.extend(["-mapname", mapname])

   try:
      log = common_functions.CallSubprocessOn(aplmap_cmd, redirect=False, logger=logger)
      if not os.path.exists(mapname):
         raise Exception, "mapped file not output by aplmap!"
   except Exception as e:
      status_update(status_file, "ERROR - aplmap", output_line_name)
      logger.error([e,output_line_name])
      ##error_write(output_location, e,output_line_name)
      exit(1)

   status_update(status_file, "waiting to zip", output_line_name)

   waiting = True

   while waiting:
      stillwaiting = False
      for file in os.listdir(output_location + STATUS_DIR):
         f = open(output_location + STATUS_DIR + file, 'r')
         for line in f:
            if "zipping" in line:
               stillwaiting = True
      if not stillwaiting:
         waiting = False

   status_update(status_file, "zipping", output_line_name)

   with zipfile.ZipFile(mapname + ".zip", 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zip:
      #compress the mapped file
      zip.write(mapname, os.path.basename(mapname))
      zip.write(mapname + ".hdr", os.path.basename(mapname + ".hdr"))
      zip.close()

   logger.info(str("zipped " + output_line_name + " to " + mapname + ".zip" + " at " + output_location))
   #logger("zipped to " + mapname + ".zip", line_name, output_location)

   status_update(status_file, "complete", output_line_name)

   #if all the files are complete its time to zip them together
   all_check = True
   for status in os.listdir(output_location + STATUS_DIR):
      for l in open(output_location + STATUS_DIR + status):
         if "complete" not in l:
            if "not processing" not in l:
               all_check = False

   if all_check:
      #if all are finished we'll use this process to zip all the zipped mapped files into one for download
      zip_mapped_folder = glob.glob(output_location + WEB_MAPPED_OUTPUT + "*.bil.zip")
      zip_contents_file = open(output_location + WEB_MAPPED_OUTPUT + "zip_contents.txt", 'a')
      for zip_mapped in zip_mapped_folder:
         zip_contents_file.write(zip_mapped + "\n")
      zip_contents_file.close()
      logger.info("outputting master zip")
      with zipfile.ZipFile(output_location + WEB_MAPPED_OUTPUT + line_details["project_code"] + '_' + line_details[
         "year"] + jday + '.zip', 'a', zipfile.ZIP_DEFLATED, allowZip64=True) as zip:
         for zip_mapped in zip_mapped_folder:
            logger.info("zipping " + zip_mapped)
            zip.write(zip_mapped, os.path.basename(zip_mapped))
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
                       metavar="<configfile>")
   parser.add_argument('--line',
                       '-l',
                       help='line to process',
                       default=None,
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
   args = parser.parse_args()

   line_handler(args.config, args.line, args.output)
