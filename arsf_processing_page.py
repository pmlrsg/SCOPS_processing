#! /usr/bin/env python
from flask import Flask, send_from_directory
from flask import render_template
from flask import request, Response
from functools import wraps
from numpy import arange
import ConfigParser
import glob
import xml.etree.ElementTree as etree
import os
import hdr_files
import datetime
from arsf_dem import dem_nav_utilities
import random
import math
import projection
import web_process_apl_line

CONFIG_OUTPUT = "/users/rsg/arsf/web_processing/configs/"
UPLOAD_FOLDER = "/users/rsg/arsf/web_processing/dem_upload/"
WEB_PROCESSING_FOLDER = "/users/rsg/arsf/web_processing/"
KMLPASS = "/users/rsg/arsf/usr/share/kmlpasswords.csv"

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

bands = [1, 2, 3, 4, 5]
PIXEL_SIZES = arange(0.5, 7.5, 0.5)

bounds = {
   'n': 40000,
   's': 20000,
   'e': 60000,
   'w': 40000
}

app.debug = False

import logging
from logging import FileHandler

file_handler = FileHandler("/local1/data/backup/rsgadmin/arsf-dan.nerc.ac.uk/logs/logger.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)


def confirm_email(config_name, project, email):
   confirmation_link = "http://arsf-dandev.nerc.ac.uk/processor/confirm/%s?project=%s" % (config_name, project)

   message = "You've received this email because your address was used to request procesing from the ARSF web processor. If you did not do this please ignore this email.\n\n" \
             "Please confirm your email with the link below:\n" \
             "\n" \
             "%s\n\n" \
             "If you have any questions or issues accessing the above link please email arsf-processing@pml.ac.uk quoting the reference %s" % (confirmation_link, config_name)

   web_process_apl_line.send_email(message, email, "ARSF confirm email")


def check_auth(username, password, projcode):
   """This function is called to check if a username /
   password combination is valid.
   """
   auth = False
   for pair in open(KMLPASS):
      username_auth, password_auth = pair.strip("\n").split(",")
      if username == username_auth and password == password_auth and projcode == username_auth:
         auth = True
      elif username == "arsf_admin" and password == "supers3cr3t":
         auth = True
   return auth


def authenticate():
   """Sends a 401 response that enables basic auth"""
   return Response(
      'Could not verify your access level for that URL.\n'
      'You have to login with proper credentials', 401,
      {'WWW-Authenticate': 'Basic realm="processor"'})


def requires_auth(f):
   @wraps(f)
   def decorated(*args, **kwargs):
      auth = request.authorization
      try:
         project = request.args["project"]
      except:
         project = request.form["project"]
      if not auth or not check_auth(auth.username, auth.password, project):
         return authenticate()
      return f(*args, **kwargs)
   return decorated


def validation(request):
   """
   Takes a dictionary of terms for config output and validates that the options are correct/don't pose a risk to our systems
   :param request: Config options dictionary
   :type request: dict

   :return: validation state either true or false
   :rtype: bool
   """
   # TODO make more checks, maybe come up with a brief black list, should focus whitelisting though
   validated = True
   for key in request:
      if "band" in key or "pixel_size" in key or "bound" in key or "year" in key or "julianday" in key:
         if math.isnan(float(request[key])):
            validated = False
      if "check" in key:
         if "on" not in request[key]:
            validated = False

      if "proj_string" in key:
         wktstring = projection.proj4_to_wkt(request[key])
         projstring = projection.wkt_to_proj4(wktstring)

         if request[key] not in projstring:
            validated = False

      if ";" in request[key]:
         validated = False

   return validated, request


@app.route('/downloads/<path:projfolder>', methods=['GET', 'POST'])
@requires_auth
def download(projfolder):
   """
   Takes a http request with the project folder and provides a download instance, has no authentication currently

   :param projfolder:
   :return: http download
   """
   # TODO make this safer
   projfolder = WEB_PROCESSING_FOLDER + "processing/" + projfolder
   if not os.path.exists(projfolder):
      return "not gonna work"
   download_file = [x for x in glob.glob(projfolder + "/mapped/*.zip") if "bil" not in x][0]
   return send_from_directory(directory=os.path.dirname(download_file), filename=os.path.basename(download_file), mimetype='application/zip')


@app.route('/confirm/<path:configname>', methods=['GET', 'POST'])
@requires_auth
def confirm_request(configname):
   """
   Receives request from user email which will then confirm the email address used

   :param configname: the project name/config file name that needs to be updated
   :type configname: str

   :return: string
   """
   app.logger.warning("confirm req")
   configpath = CONFIG_OUTPUT + configname + ".cfg"
   # TODO make this update the config and return a better message
   config_file = ConfigParser.RawConfigParser()
   config_file.read(configpath)
   config_file.set('DEFAULT', "confirmed", True)
   config_file.write(open(configpath, 'w'))
   return "confirmation.html"


@app.route('/theme')
def theme():
   # TODO take this out
   return render_template('index.html', name=None)


@app.route('/kmlpage')
def kml_page(name=None):
   # TODO make kml pages link to jobrequest and remove this
   return render_template('kml.html')


@app.route('/')
@app.route('/jobrequest', methods=['GET', 'POST'])
@requires_auth
def job_request(name=None):
   """
   Receives a request from html with the day, year and required project code then returns a request page based on the
   data it finds in the proj dir

   :param name: placeholder
   :type name: str

   :return: job request html page
   :rtype: html
   """
   try:
      # input validation, test if these are numbers
      if not math.isnan(float(request.args["day"])):
         day = request.args["day"]
      else:
         raise
      if not math.isnan(float(request.args["year"])):
         year = request.args["year"]
      else:
         raise

      # input validation, get rid of any potential paths the user may have used (should probably reject on this)
      proj_code = request.args["project"].replace("..", "_").replace("/", "_")

      # check if theres a sortie associated with the day
      try:
         sortie = request.args["sortie"]
      except:
         sortie = ''

      # Need to add a 0 or two to day if it isn't long enough
      day = str(day)

      # need to convert day to 00# or 0## for string stuff
      if len(day) == 1:
         day = "00" + day
      elif len(day) == 2:
         day = "0" + day

      # check if the symlink for this day/year/proj code combo exists
      symlink_name = proj_code + '-' + year + '_' + day

      # this should (should) be where the kml is on web server, makes it annoying to test though
      path_to_symlink = "/local1/data/backup/rsgadmin/arsf-dan.nerc.ac.uk/html/kml/" + year + "/" + symlink_name + sortie
      if os.path.exists(path_to_symlink):
         folder = "/" + os.path.realpath(path_to_symlink).strip("/processing/kml_overview")
      else:
         raise
   except Exception, e:
      # TODO make this a html page response
      app.logger.error(str(e))
      return "404.html"

   hyper_delivery = glob.glob(folder + '/delivery/*hyperspectral*')

   # using the xml find the project bounds
   projxml = etree.parse(glob.glob(hyper_delivery[0] + '/project_information/*project.xml')[0]).getroot()
   bounds = {
      'n': projxml.find('.//{http://www.isotc211.org/2005/gmd}northBoundLatitude').find(
         '{http://www.isotc211.org/2005/gco}Decimal').text,
      's': projxml.find('.//{http://www.isotc211.org/2005/gmd}southBoundLatitude').find(
         '{http://www.isotc211.org/2005/gco}Decimal').text,
      'e': projxml.find('.//{http://www.isotc211.org/2005/gmd}eastBoundLongitude').find(
         '{http://www.isotc211.org/2005/gco}Decimal').text,
      'w': projxml.find('.//{http://www.isotc211.org/2005/gmd}westBoundLongitude').find(
         '{http://www.isotc211.org/2005/gco}Decimal').text
   }

   # get the utm zone
   utmzone = projection.ll2utm(float(bounds["n"]), float(bounds["e"]))[:2]

   # if it's britain we should offer UKBNG on the web page
   if utmzone[0] in [29, 30, 31] and utmzone[1] in 'N':
      britain = True
   else:
      britain = False

   # begin building the lines for output
   line_hdrs = [f for f in glob.glob(hyper_delivery[0] + '/flightlines/level1b/*.bil.hdr') if "mask" not in f]
   lines = []
   for line in line_hdrs:
      linehdr = hdr_files.Header(line)
      linedict = {
         "name": os.path.basename(line)[:-10],
         "bandsmax": int(linehdr.bands),
         "bands": range(1, int(linehdr.bands) + 1, 1),
      }
      lines.append(linedict)

   # grab 2 random flightlines for sampling of altitude, any more is going to cause problems with speed
   sampled_nav = random.sample(glob.glob(hyper_delivery[0] + "/flightlines/navigation/*_nav_post_processed.bil"), 2)

   # we should base pixel size off the minimum
   altitude = dem_nav_utilities.get_min_max_from_bil_nav_files(sampled_nav)["altitude"]["min"]

   # for the moment just using fenix
   sensor = "fenix"

   # calculate pixelsize
   pixel = pixelsize(altitude, sensor)

   # round it to .5 since we don't need greater resolution than this
   pixel = round(pixel * 2) / 2

   # sort the lines so they look good on the web page
   lines = sorted(lines, key=lambda line: line["name"])

   # creates the webpage by handing vars into the template engine
   return render_template('requestform.html',
                          flightlines=lines,
                          uk=britain,
                          pixel_sizes=PIXEL_SIZES,
                          optimal_pixel=pixel,
                          bounds=bounds,
                          name=name,
                          julian_day=day,
                          year=year,
                          project_code=proj_code,
                          utmzone="UTM zone " + str(utmzone[0]) + str(utmzone[1]))


@app.route('/progress', methods=['POST'])
@requires_auth
def progress():
   """
   receives a post request from the jobrequest page and validates the input

   :return: html page
   :rtype: html
   """
   requestdict = request.form
   validated = validation(requestdict)
   if validated:
      lines = []
      for key in requestdict:
         if "_line_check" in key:
            lines.append(key.strip("_line_check"))
      lines = sorted(lines)
      filename = requestdict["project"] + '_' + requestdict["year"] + '_' + requestdict[
         "julianday"] + '_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
      config_output(requestdict, lines=lines, filename=filename)
      return render_template('submitted.html')
   else:
      # TODO make this rejection better
      return render_template("reject.html")


def config_output(requestdict, lines, filename):
   """
   Writes a config to the web processing configs folder, this will then be picked up by web_qsub

   :param requestdict: A request converted to immutable dict from the job request page
   :type requestdict: immutable dict

   :param lines: list of flightlines to be processed
   :type lines: list

   :param filename: config filename to write to
   :type filename: str

   :return: 1 on success
   :rtype: int
   """
   config = ConfigParser.RawConfigParser()
   config.set('DEFAULT', "julianday", requestdict["julianday"])
   config.set('DEFAULT', "year", requestdict["year"])
   config.set('DEFAULT', "sortie", requestdict["sortie"])
   config.set('DEFAULT', "project_code", requestdict["project"])
   config.set('DEFAULT', "projection", requestdict["projectionRadios"])
   try:
      config.set('DEFAULT', "projstring", requestdict["projString"])
   except:
      config.set('DEFAULT', "projstring", '')
   config.set('DEFAULT', "dem", requestdict["optionsDemRadios"])
   config.set('DEFAULT', "bounds",
              requestdict["bound_n"] + ' ' + requestdict["bound_e"] + ' ' + requestdict["bound_s"] + ' ' + requestdict[
                 "bound_w"])
   config.set('DEFAULT', "email", requestdict["email"])
   config.set('DEFAULT', "interpolation", requestdict["optionsIntRadios"])
   config.set('DEFAULT', "pixelsize", requestdict["pixel_size_x"] + ' ' + requestdict["pixel_size_y"])
   config.set('DEFAULT', "submitted", False)
   config.set('DEFAULT', "confirmed", False)
   print requestdict
   try:
      if requestdict["mask_all_check"] in "on":
         masking = "all"
      else:
         masking = "none"
   except:
      masking = "none"
   config.set('DEFAULT', "masking", masking)
   for line in lines:
      config.add_section(str(line))
      if requestdict['%s_line_check' % line] in "on" or requestdict['process_all_lines'] in "on":
         config.set(str(line), 'process', 'true')
      else:
         config.set(str(line), 'process', 'false')
      config.set(str(line), 'band_range',
                 requestdict["%s_band_start" % line] + '-' + requestdict["%s_band_stop" % line])
   configfile = open(CONFIG_OUTPUT + filename + '.cfg', 'a')
   config.write(configfile)
   os.chmod(CONFIG_OUTPUT + filename + '.cfg', 0666)
   confirm_email(filename, requestdict["project"], requestdict["email"])
   return 1


@app.route('/processing', methods=['GET', 'POST'])
def processingpage(name=None):
   """
   Function to show the processing page, for the moment doesn't do anything

   :param name: placeholder
   :type name: str

   :return: template rendered html file
   :rtype: html
   """
   folder = request.args["id"]
   lines = []
   for line in glob.glob(WEB_PROCESSING_FOLDER + folder + "/status/*"):
      for l in open(line):
         status = l.split(' ')[2]
      line_details = {
         "name": os.basepath(line),
         "status": status
      }
      lines.append(line_details)

   return render_template('processingpage.html',
                          lines=lines)


def getifov(sensor):
   """
   Function for sensor ifov grabbing

   :param sensor: sensor name, fenix eagle or hawk
   :type sensor: str

   :return: ifov
   :rtype: float
   """
   if "fenix" in sensor:
      ifov = 0.001448623
   if "eagle" in sensor:
      ifov = 0.000645771823
   if "hawk" in sensor:
      ifov = 0.0019362246375
   return ifov


def pixelsize(altitude, sensor):
   return 2 * altitude * math.tan(getifov(sensor) / 2)


if __name__ == '__main__':
   app.run(debug=True,
           host='0.0.0.0',
           threaded=True,
           port=5001)
