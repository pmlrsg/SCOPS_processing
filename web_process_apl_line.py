#! /usr/bin/env python
__author__ = 'stgo'

import argparse
import os
import ConfigParser
import common_functions
import folder_structure
import glob
import zipfile
import pipes
import smtplib
from email.mime.text import MIMEText

WEB_MASK_OUTPUT = "/level1b/"
WEB_IGM_OUTPUT = "/igm/"
WEB_MAPPED_OUTPUT = "/mapped/"
VIEW_VECTOR_FILE = "/sensor_FOV_vectors/%s_fov_fullccd_vectors.bil"
OSNG_SEPERATION_FILE = "/users/rsg/arsf/dems/ostn02/OSTN02_NTv2.gsb"
STATUS_DIR = "/status/"
ERROR_DIR = "/status/"
LOG_DIR = "/logs/"
STATUS_FILE = "%s/status/%s_status.txt"
INITIAL_STATUS = "initialising"
NAVIGATION_FOLDER = "/flightlines/navigation/"
SEND_EMAIL = "arsf-processing@pml.ac.uk"
ERROR_EMAIL = "arsf-code@pml.ac.uk"


def send_email(message, receive, subject, send=SEND_EMAIL):
   msg = MIMEText(message)
   msg['From'] = send
   msg['To'] = receive
   msg['Subject'] = subject

   sender = smtplib.SMTP('localhost')
   sender.sendmail(send, [receive], msg.as_string())


def email_error(stage, line, error, processing_folder):
   message = "Processing has failed on line %s at stage %s due to:\n\n" \
             "%s\n\n" \
             "The processing is contained in folder %s\n\n" \
             "Once the issue has been resolved update the relevant status file to state either 'complete' or 'waiting to zip' (dependant on what you've done)"

   message = message % (line, stage, error, processing_folder)
   send_email(message, SEND_EMAIL, ERROR_EMAIL, processing_folder + " ERROR")


def email_PI(pi_email, output_zip, output_location):
   message = "Processing is complete for your order request %s, you can now download the data from the following location:" \
             "" \
             "%s" \
             "" \
             "The data will be available for a total of two weeks, however this may be extended if requested. If you identify any problems with your data or have issues downloading the data please contact ARSF staff at arsf-processing@pml.ac.uk." \
             "" \
             "Regards," \
             "ARSF" % output_zip

   send_email(message, pi_email, SEND_EMAIL, output_location + " order complete")


def status_update(status_file, newstage, line):
   open(status_file, 'w').write("%s = %s" % (line, newstage))


def error_write(output_location, error, line_name):
   error_file = open(output_location + ERROR_DIR + line_name + "_error.txt", 'w')
   error_file.write(str(error))


def logger(log, line_name, output_location):
   logfile = open(output_location + LOG_DIR + line_name + "_log.txt", 'a')
   logfile.write(str(log))


def process_web_hyper_line(config_file, line_name, output_location):
   config = ConfigParser.RawConfigParser()
   config.read(config_file)
   line_details = dict(config.items(line_name))
   line_number = str(line_name[-2:])
   status_file = STATUS_FILE % (output_location, line_name)
   open(status_file, 'w+').write("%s = %s" % (line_name, INITIAL_STATUS))

   if len(line_details["julianday"]) == 2:
      jday = str(0) + str(line_details["julianday"])
   elif len(str(line_details["julianday"])) == 1:
      jday = str(0) + str(line_details["julianday"])
   else:
      jday = line_details["julianday"]

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
   dem = line_details["dem_name"]

   projection = line_details["projection"]

   status_update(status_file, "aplmask", line_name)

   masked_file = output_location + WEB_MASK_OUTPUT + line_name + "_masked.bil"

   aplmask_cmd = ["aplmask"]
   aplmask_cmd.extend(["-lev1", lev1file])
   aplmask_cmd.extend(["-mask", maskfile])
   aplmask_cmd.extend(["-output", masked_file])

   try:
      log = common_functions.CallSubprocessOn(aplmask_cmd, redirect=True)
      logger(log, line_name, output_location)
      if not os.path.exists(masked_file):
         raise Exception, "masked file not output"
   except Exception, e:
      status_update(status_file, "ERROR - aplmask", line_name)
      error_write(output_location, e, line_name)
      exit(1)

   status_update(status_file, "aplcorr", line_name)

   igm_file = output_location + WEB_IGM_OUTPUT + line_name + ".igm"
   nav_file = glob.glob(hyper_delivery + NAVIGATION_FOLDER + line_name + "*_nav_post_processed.bil")[0]

   aplcorr_cmd = ["aplcorr"]
   aplcorr_cmd.extend(["-lev1file", lev1file])
   aplcorr_cmd.extend(["-navfile", nav_file])
   aplcorr_cmd.extend(["-vvfile", hyper_delivery + VIEW_VECTOR_FILE % sensor])
   aplcorr_cmd.extend(["-dem", dem])
   aplcorr_cmd.extend(["-igmfile", igm_file])

   try:
      log = common_functions.CallSubprocessOn(aplcorr_cmd, redirect=True)
      logger(log, line_name, output_location)
      if not os.path.exists(igm_file):
         raise Exception, "igm file not output by aplcorr!"
   except Exception, e:
      status_update(status_file, "ERROR - aplcorr", line_name)
      error_write(output_location, e, line_name)
      exit(1)

   if "UTM" in line_details["projection"]:
      zone = line_details["projection"].split(" ")[2]
      if zone[-1:] >= "N":
         hemisphere = "N"
      elif zone[-1:] <= "M":
         hemisphere = "S"

      zone = zone[:-1]

      projection = pipes.quote("utm_wgs84%s_%s" % (hemisphere, zone))
   elif "UKBNG" in line_details["projection"]:
      projection = "osng"
   else:
      print "dunno what the projection is :/"

   igm_file_transformed = igm_file.replace(".igm", "_%s.igm") % projection.replace(' ', '_')

   if projection in "osng":
      projection = projection + " " + OSNG_SEPERATION_FILE

   status_update(status_file, "apltran", line_name)

   apltran_cmd = ["apltran"]
   apltran_cmd.extend(["-inproj", "latlong", "WGS84"])
   apltran_cmd.extend(["-igm", igm_file])
   apltran_cmd.extend(["-output", igm_file_transformed])
   if "utm" in projection:
      apltran_cmd.extend(["-outproj", "utm_wgs84%s" % hemisphere, zone])
   elif "osng" in projection:
      apltran_cmd.extend(["-outproj", "osng", OSNG_SEPERATION_FILE])

   try:
      log = common_functions.CallSubprocessOn(apltran_cmd, redirect=True)
      logger(log, line_name, output_location)
      if not os.path.exists(igm_file_transformed):
         raise Exception, "igm file not output by apltran!"
   except Exception, e:
      status_update(status_file, "ERROR - apltran", line_name)
      error_write(output_location, e, line_name)
      exit(1)

   status_update(status_file, "aplmap", line_name)

   pixelx, pixely = line_details["pixelsize"].split(" ")

   mapname = output_location + WEB_MAPPED_OUTPUT + line_name + "3b_mapped.bil"

   aplmap_cmd = ["aplmap"]
   aplmap_cmd.extend(["-igm", igm_file_transformed])
   aplmap_cmd.extend(["-lev1", masked_file])
   aplmap_cmd.extend(["-pixelsize", pixelx, pixely])
   aplmap_cmd.extend(["-bandlist", line_details["band_range"]])
   aplmap_cmd.extend(["-interpolation", line_details["interpolation"]])
   aplmap_cmd.extend(["-mapname", mapname])

   try:
      log = common_functions.CallSubprocessOn(aplmap_cmd, redirect=True)
      logger(log, line_name, output_location)
      if not os.path.exists(mapname):
         raise Exception, "mapped file not output by aplmap!"
   except Exception, e:
      status_update(status_file, "ERROR", line_name)
      error_write(output_location, e, line_name)
      exit(1)

   status_update(status_file, "waiting to zip", line_name)

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

   status_update(status_file, "zipping", line_name)

   with zipfile.ZipFile(mapname + ".zip", 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zip:
      zip.write(mapname, os.path.basename(mapname))
      zip.write(mapname + ".hdr", os.path.basename(mapname + ".hdr"))

   logger("zipping to " + mapname + ".zip", line_name, output_location)

   status_update(status_file, "complete", line_name)

   all_check = True
   for status in os.listdir(output_location + STATUS_DIR):
      for l in open(output_location + STATUS_DIR + status):
         if "complete" not in l:
            if "not processing" not in l:
               all_check = False

   if all_check:
      zip_mapped_folder = glob.glob(output_location + WEB_MAPPED_OUTPUT + "*.zip")
      with zipfile.ZipFile(output_location + WEB_MAPPED_OUTPUT + line_details["project_code"] + '_' + line_details[
         "year"] + jday + '.zip', 'a', zipfile.ZIP_DEFLATED, allowZip64=True) as zip:
         for zip_mapped in zip_mapped_folder:
            zip.write(zip_mapped, os.path.basename(zip_mapped))
      email_PI(config["email"], output_location + WEB_MAPPED_OUTPUT + line_details["project_code"] + '_' + line_details[
         "year"] + jday + '.zip', output_location)


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

   process_web_hyper_line(args.config, args.line, args.output)
