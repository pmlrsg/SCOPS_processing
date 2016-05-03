#! /usr/bin/env python
###########################################################
# This file has been created by ARSF Data Analysis Node and
# is licensed under the GPL v3 Licence. A copy of this
# licence is available to download with this file.
###########################################################

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
import datetime
import ConfigParser
import argparse
import arsf_dem
import glob
import logging
import subprocess
import sys
import web_process_apl_line
import web_common

from arsf_dem import dem_common_functions

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
      if sortie is not "None":
         folder_base = web_common.WEB_OUTPUT + project_code + '_' + year + '_' + jday + sortie + datetime.datetime.now().strftime(
            '%Y%m%d%H%M%S')
      else:
         folder_base = web_common.WEB_OUTPUT + project_code + '_' + year + '_' + jday + datetime.datetime.now().strftime(
            '%Y%m%d%H%M%S')
   #make the folders
   if os.access(web_common.WEB_OUTPUT, os.W_OK):
      os.mkdir(folder_base)
      os.mkdir(folder_base + web_common.WEB_MASK_OUTPUT)
      os.mkdir(folder_base + web_common.WEB_IGM_OUTPUT)
      os.mkdir(folder_base + web_common.WEB_MAPPED_OUTPUT)
      os.mkdir(folder_base + web_common.WEB_DEM_FOLDER)
      os.mkdir(folder_base + web_common.WEB_STATUS_OUTPUT)
      os.mkdir(folder_base + web_common.LOG_DIR)
   else:
      raise IOError("no write permissions at {}".format(web_common.WEB_OUTPUT))
   #return the location
   return folder_base


def web_qsub(config, local=False, output=None):
   """
   Submits the job (or processes locally in its current form)

   :param config:
   :param local:
   :param local_threaded:
   :param output:
   :return:
   """
   logger = logging.getLogger()
   file_handler = logging.FileHandler(web_common.QSUB_LOG_DIR + os.path.basename(config).replace(".cfg","") + "_log.txt", mode='a')
   formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
   file_handler.setFormatter(formatter)
   logger.handlers = []
   logger.addHandler(file_handler)
   logger.setLevel(logging.DEBUG)

   logger.info(config)

   config_file = ConfigParser.SafeConfigParser()
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
         logger.error(e)
         sortie = defaults["sortie"]
         if sortie == "None":
            sortie=''
         output_location = web_structure(defaults["project_code"], defaults["julianday"], defaults["year"],
                           sortie)
         config_file.set('DEFAULT', 'output_folder', output_location)
   else:
      output_location = output

   #symlink the config file into the processing folder so that we know the source of any problems that arise
   if not os.path.exists(output_location + '/' + os.path.basename(config)):
      os.symlink(os.path.abspath(config), output_location + '/' + os.path.basename(config))

   sortie=defaults["sortie"]
   if sortie == "None":
      sortie=''
   #find out where the files are for the day we are trying to process
   folder = folder_structure.FolderStructure(year=defaults["year"],
                                             jday=defaults["julianday"],
                                             projectCode=defaults["project_code"],
                                             fletter=sortie,
                                             absolute=True)

   #locate delivery and navigation files
   hyper_delivery = glob.glob(folder.getProjPath() + "/delivery/*hyperspectral*/")[0]
   nav_folder = glob.glob(hyper_delivery + "flightlines/navigation/")[0]

   #if the dem doesn't exist generate one
   try:
      logger.info("checking dem")
      dem_name = config_file.get('DEFAULT', 'dem_name')
      logger.info(dem_name)
      if not os.path.exists(dem_name):
         raise Exception("The DEM specified does not exist!")
   except Exception as e:
      dem_common_functions.ERROR(e)
      logger.error(e)
      dem_name = (output_location + web_common.WEB_DEM_FOLDER + defaults["project_code"] + '_' + defaults["year"] + '_' + defaults[
         "julianday"] + '_' + defaults["projection"] + ".dem").replace(' ', '_')
      arsf_dem.dem_nav_utilities.create_apl_dem_from_mosaic(dem_name,
                                                   dem_source=defaults["dem"],
                                                   bil_navigation=nav_folder)
   #update config with the dem name then submit the file to the processor, we don't want the script to run twice so set
   # submitted to true
   config_file.set('DEFAULT', "dem_name", dem_name)
   config_file.set('DEFAULT', "submitted", "True")
   config_file.write(open(config, 'w'))

   #Generate a status file for each line to be processed, these are important later!
   for line in lines:
      status_file = web_common.STATUS_FILE.format(output_location, line)
      log_file = web_common.LOG_FILE.format(output_location, line)
      if "true" in dict(config_file.items(line))["process"]:
         open(status_file, 'w+').write("{} = {}".format(line, "waiting"))
         open(log_file, mode="a").close()
      else:
         open(status_file, 'w+').write("{} = {}".format(line, "not processing"))
      equations = [x for x in dict(config_file.items('DEFAULT')) if "eq_" in x]
      #if equations exist we should do something with them
      if len(equations) > 0:
         for equation in equations:
            if config_file.has_option(line, equation):
               if config_file.get(line, equation) in "True":
                  #build a load of band math status amd log files
                  bm_status_file = web_common.STATUS_FILE.format(output_location, line + equation.replace("eq_", "_"))
                  bm_log_file =  web_common.LOG_FILE.format(output_location, line + equation.replace("eq_", "_"))

                  #open status and log files
                  open(bm_status_file, 'w+').write("{} = {}".format((line + equation.replace("eq_", "_")), "waiting"))
                  open(bm_log_file, mode="a").close()


   if not config_file.get('DEFAULT', 'status_email_sent'):
      web_process_apl_line.email_status(defaults["email"], output_location, defaults["project_code"])
      config_file.set('DEFAULT', "status_email_sent", "True")
      config_file.write(open(config, 'w'))

   for line in lines:
      band_ratio = False
      main_line = False
      if dict(config_file.items(line))["process"] in "true":
         #if they want the main line processed we should submit it
         main_line = True

      if len([x for x in dict(config_file.items(line)) if "eq_" in x]) > 0:
         # if they want the band ratiod file we should submit it
         band_ratio = True

      if main_line or band_ratio:
         if local:
            try:
               logger.info("processing line {}".format(line))
               web_process_apl_line.line_handler(config, line, output_location, main_line, band_ratio)
            except Exception as e:
               logger.error("Could not process job for {}, Reason: {}".format(line, e))
         else:
            #this will need to be updated to whatever parallel jobs system is
            #being used in the intended use environemnt
            qsub_args = ["qsub"]
            qsub_args.extend(["-N", "WEB_" + defaults["project_code"] + "_" + line])
            qsub_args.extend(["-q", web_common.QUEUE])
            qsub_args.extend(["-P", "arsfdan"])
            qsub_args.extend(["-wd", os.getcwd()])
            qsub_args.extend(["-e", web_common.LOG_DIR])
            qsub_args.extend(["-o", web_common.LOG_DIR])
            qsub_args.extend(["-m", "n"]) # Don't send mail
            qsub_args.extend(["-p", "-100"])
            qsub_args.extend(["-b", "y"])
            script_args = [web_common.PROCESS_COMMAND]
            script_args.extend(["-l", line])
            script_args.extend(["-c", config])
            script_args.extend(["-s","fenix"])
            script_args.extend(["-o", output_location])
            if main_line:
               script_args.extend(["-m"])
            if band_ratio:
               script_args.extend(["-b"])

            qsub_args.extend(script_args)
            try:
               logger.info("submitting line {}".format(line))
               qsub = subprocess.Popen(qsub_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            except:
               logger.error("Could not submit qsub job. Reason: " + str(sys.exc_info()[1]))
               continue
            logger.info("line submitted: " + line)

   logger.info("all lines complete")


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
   parser.add_argument('--output',
                       '-o',
                       help='Force output path and name',
                       default=None,
                       metavar="<folder_name>")
   args = parser.parse_args()

   web_qsub(args.config, local=args.local, output=args.output)
