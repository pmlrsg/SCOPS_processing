#! /usr/bin/env python
"""
qsub script to be called by web_processing_cron, receives config files from web process cron and generates a processing
folder tree/dem before either submitting to the grid or processing locally.

Author: Stephen Goult

Available functions
web_structure(project_code, jday, year, sortie=None, output_name=None): takes project code, jday and year and optionally
a sortie or a set folder to generate the folder tree inside
web_qsub(config, local=False, local_threaded=False, output=None): takes a config file and transforms it to a folder tree
with a dem file included (unless specified in the config already) will then either process files locally or submit to
the grid. Uses web_process_apl_line.py
"""

import os
import folder_structure
import common_functions
import datetime
import ConfigParser
import web_process_apl_line
import argparse
import arsf_dem
import glob
import logging
import subprocess
import sys
import web_process_apl_line

WEB_OUTPUT = "/users/rsg/arsf/web_processing/processing/"
WEB_MASK_OUTPUT = "/level1b/"
WEB_IGM_OUTPUT = "/igm/"
WEB_MAPPED_OUTPUT = "/mapped/"
WEB_DEM_FOLDER = "/dem/"
VIEW_VECTOR_FILE = "/sensor_FOV_vectors/{}_view_vector_list.txt"
OSNG_SEPERATION_FILE = "/users/rsg/arsf/dems/ostn02/OSTN02_NTv2.gsb"
WEB_STATUS_OUTPUT = "/status"
STATUS_FILE = "{}/status/{}_status.txt"
INITIAL_STATUS = "initialising"
LOG_DIR = "/logs/"
QUEUE = "lowpriority.q"


def web_structure(project_code, jday, year, sortie=None, output_name=None):
   """
   Builds the structure for a web job to output to

   :param project_code:
   :param jday:
   :param year:
   :param sortie:
   :param output_name:
   :return: folder location
   """
   #if there isnt an output name generate one from the time and day/year/project
   if output_name is not None:
      folder_base = output_name
   else:
      if sortie is not None:
         folder_base = WEB_OUTPUT + project_code + '_' + year + '_' + jday + sortie + datetime.datetime.now().strftime(
            '%Y%m%d%H%M%S')
      else:
         folder_base = WEB_OUTPUT + project_code + '_' + year + '_' + jday + datetime.datetime.now().strftime(
            '%Y%m%d%H%M%S')
   #make the folders
   os.mkdir(folder_base)
   os.mkdir(folder_base + WEB_MASK_OUTPUT)
   os.mkdir(folder_base + WEB_IGM_OUTPUT)
   os.mkdir(folder_base + WEB_MAPPED_OUTPUT)
   os.mkdir(folder_base + WEB_DEM_FOLDER)
   os.mkdir(folder_base + WEB_STATUS_OUTPUT)
   os.mkdir(folder_base + LOG_DIR)
   #return the location
   return folder_base


def web_qsub(config, local=False, local_threaded=False, output=None):
   """
   Submits the job (or processes locally in its current form)

   :param config:
   :param local:
   :param local_threaded:
   :param output:
   :return:
   """
   config_file = ConfigParser.RawConfigParser()
   config_file.read(config)
   lines = config_file.sections()
   defaults = config_file.defaults()

   #if the output location doesn't exist yet we should create one
   if output is None or output == '':
      try:
         output_location = config_file.get('DEFAULT', 'output_folder')
         if not os.path.exists(output_location):
            raise Exception("specified output location does not exist!")
      except Exception as e:
         common_functions.ERROR(e)
         logging.error(e)
         output_location = web_structure(defaults["project_code"], defaults["julianday"], defaults["year"],
                                         defaults["sortie"])
         config_file.set('DEFAULT', 'output_folder', output_location)
   else:
      output_location = output

   #symlink the config file into the processing folder so that we know the source of any problems that arise
   os.symlink(config, output_location + '/' + os.path.basename(config))

   #find out where the files are for the day we are trying to process
   folder = folder_structure.FolderStructure(year=defaults["year"],
                                             jday=defaults["julianday"],
                                             projectCode=defaults["project_code"],
                                             fletter=defaults["sortie"],
                                             absolute=True)

   #locate delivery and navigation files
   hyper_delivery = glob.glob(folder.getProjPath() + "/delivery/*hyperspectral*/")[0]
   nav_folder = glob.glob(hyper_delivery + "flightlines/navigation/")[0]

   #if the dem doesn't exist generate one
   try:
      dem_name = config_file.get('DEFAULT', 'dem_name')
      if not os.path.exists(dem_name):
         raise Exception("The DEM specified does not exist!")
   except Exception as e:
      common_functions.ERROR(e)
      logging.error(e)
      dem_name = (output_location + WEB_DEM_FOLDER + defaults["project_code"] + '_' + defaults["year"] + '_' + defaults[
         "julianday"] + '_' + defaults["projection"] + ".dem").replace(' ', '_')
      arsf_dem.dem_nav_utilities.create_apl_dem_from_mosaic(dem_name,
                                                   dem_source=defaults["dem"],
                                                   bil_navigation=nav_folder)
   #update config with the dem name then submit the file to the processor, we don't want the script to run twice so set
   # submitted to true
   config_file.set('DEFAULT', "dem_name", dem_name)
   config_file.set('DEFAULT', "submitted", True)
   config_file.write(open(config, 'w'))

   #Generate a status file for each line to be processed, these are important later!
   for line in lines:
      status_file = STATUS_FILE.format(output_location, line)
      if "true" in dict(config_file.items(line))["process"]:
         open(status_file, 'w+').write("{} = {}".format(line, "waiting"))
      else:
         open(status_file, 'w+').write("{} = {}".format(line, "not processing"))

   for line in lines:
      if dict(config_file.items(line))["process"] in "true":
         if local:
            if local_threaded:
               # do threaded processing TODO
               raise Exception("This still needs to be coded")
               pass
            else:
               try:
                  web_process_apl_line.process_web_hyper_line(config, line, output_location)
              except Exception as e:
                  logging.error("Could not process job for {}, Reason: {}".format(line, e))
                  continue
         else:
            qsub_args = ["qsub"]
            qsub_args.extend(["-N", "WEB_" + defaults["project_code"] + "_" + line])
            qsub_args.extend(["-q", QUEUE])
            qsub_args.extend(["-P", "arsfdan"])
            qsub_args.extend(["-wd", os.getcwd()])
            qsub_args.extend(["-e", LOG_DIR])
            qsub_args.extend(["-o", LOG_DIR])
            qsub_args.extend(["-m", "n"]) # Don't send mail
            qsub_args.extend(["-p", "-100"])
            qsub_args.extend(["-b", "y"])
            try:
                qsub = subprocess.Popen(qsub_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            except:
                logging.error("Could not submit qsub job. Reason: " + str(sys.exc_info()[1]))
                continue
            logging.info("line submitted: " + line)

   logging.info("all lines complete")


if __name__ == '__main__':
   # Get the input arguments
   parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
   parser.add_argument('--config',
                       '-c',
                       help='web config file',
                       default=None,
                       required=True,
                       metavar="<configfile>")
   parser.add_argument('--local',
                       '-l',
                       help='local processing vs grid',
                       action='store_true',
                       default=False)
   parser.add_argument('--threaded',
                       '-t',
                       help='multithreaded local processing',
                       default=False,
                       metavar="<sensor>")
   parser.add_argument('--output',
                       '-o',
                       help='Force output path and name',
                       default=None,
                       metavar="<folder_name>")
   args = parser.parse_args()

   web_qsub(args.config, local=args.local, local_threaded=args.threaded, output=args.output)
