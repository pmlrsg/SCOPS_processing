#! /usr/bin/env python
from flask import Flask
from flask import render_template
from flask import request
from numpy import arange
import folder_structure
import ConfigParser
import glob
import xml.etree.ElementTree as etree
import os
import utm
import hdr_files
import datetime
import math
from arsf_dem import dem_nav_utilities
import random

CONFIG_OUTPUT = "/users/rsg/arsf/web_processing/configs/"
UPLOAD_FOLDER = "/users/rsg/arsf/web_processing/dem_upload/"
WEB_PROCESSING_FOLDER = "/users/rsg/arsf/web_processing/"

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

bands = [1,2,3,4,5]
PIXEL_SIZES = arange(0.5, 7.5, 0.5)

bounds={
    'n':40000,
    's':20000,
    'e':60000,
    'w':40000
}

@app.route('/theme')
def theme():
    return render_template('index.html', name=None)

@app.route('/kmlpage')
def kml_page(name=None):
    return render_template('kml.html')

@app.route('/')
@app.route('/jobrequest', methods=['GET', 'POST'])
def job_request(name=None):
    day=request.args["day"]
    year=request.args["year"]
    proj_code=request.args["project"]
    try:
        sortie = request.args["sortie"]
    except:
        sortie = ''
    folder = folder_structure.FolderStructure(year=year, jday=str(day), projectCode=proj_code, fletter=sortie)
    hyper_delivery = glob.glob(folder.projPath + '/delivery/*hyperspectral*')
    projxml = etree.parse(glob.glob(hyper_delivery[0] + '/project_information/*project.xml')[0]).getroot()
    bounds={
        'n' : projxml.find('.//{http://www.isotc211.org/2005/gmd}northBoundLatitude').find('{http://www.isotc211.org/2005/gco}Decimal').text,
        's' : projxml.find('.//{http://www.isotc211.org/2005/gmd}southBoundLatitude').find('{http://www.isotc211.org/2005/gco}Decimal').text,
        'e' : projxml.find('.//{http://www.isotc211.org/2005/gmd}eastBoundLongitude').find('{http://www.isotc211.org/2005/gco}Decimal').text,
        'w' : projxml.find('.//{http://www.isotc211.org/2005/gmd}westBoundLongitude').find('{http://www.isotc211.org/2005/gco}Decimal').text
    }
    utmzone = utm.from_latlon(float(bounds["n"]), float(bounds["e"]))[2:]
    if utmzone[0] in [29, 30, 31] and utmzone[1] in ['U', 'V']:
        britain = True
    else:
        britain=False
    line_hdrs = [f for f in glob.glob(hyper_delivery[0] + '/flightlines/level1b/*.bil.hdr') if "mask" not in f]
    lines = []
    for line in line_hdrs:
        linehdr = hdr_files.Header(line)
        linedict = {
            "name" : os.path.basename(line)[:-10],
            "bandsmax" : int(linehdr.bands),
            "bands" : range(1, int(linehdr.bands)+1, 1),
        }
        lines.append(linedict)

    sampled_nav = random.sample(glob.glob(hyper_delivery[0] + "/flightlines/navigation/*_nav_post_processed.bil"), 2)
    altitude = dem_nav_utilities.get_min_max_from_bil_nav_files(sampled_nav)["altitude"]["min"]
    sensor = "fenix"
    pixel = pixelsize(altitude, sensor)
    pixel = round(pixel * 2) / 2
    lines = sorted(lines, key=lambda line: line["name"])
    return render_template('requestform.html',
                           flightlines=lines,
                           uk = britain,
                           pixel_sizes=PIXEL_SIZES,
                           optimal_pixel=pixel,
                           bounds=bounds,
                           name=name,
                           julian_day=day,
                           year=year,
                           project_code=proj_code,
                           utmzone="UTM zone "+str(utmzone[0])+str(utmzone[1]))

@app.route('/progress', methods=['POST'])
def post():
    requestdict = request.form
    lines = []
    for key in requestdict:
        if "_line_check" in key:
            lines.append(key.strip("_line_check"))
    lines = sorted(lines)
    filename = requestdict["project_code"] + '_' + requestdict["year"] + '_' + requestdict["julianday"] + '_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    config_output(requestdict, lines=lines, filename=filename)
    return render_template('submitted.html')


def config_output(requestdict, lines, filename):
    config = ConfigParser.RawConfigParser()
    config.set('DEFAULT', "julianday", requestdict["julianday"])
    config.set('DEFAULT', "year", requestdict["year"])
    config.set('DEFAULT', "sortie", requestdict["sortie"])
    config.set('DEFAULT', "project_code", requestdict["project_code"])
    config.set('DEFAULT', "projection", requestdict["projectionRadios"])
    try:
        config.set('DEFAULT', "projstring", requestdict["projString"])
    except:
        config.set('DEFAULT', "projstring", '')
    config.set('DEFAULT', "dem", requestdict["optionsDemRadios"])
    config.set('DEFAULT', "bounds", requestdict["bound_n"] + ' ' + requestdict["bound_e"] + ' ' + requestdict["bound_s"] + ' ' + requestdict["bound_w"])
    config.set('DEFAULT', "email", requestdict["email"])
    config.set('DEFAULT', "interpolation", requestdict["optionsIntRadios"])
    config.set('DEFAULT', "pixelsize", requestdict["pixel_size_x"] + ' ' + requestdict["pixel_size_y"])
    config.set('DEFAULT', "submitted", False)
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
        config.set(str(line), 'band_range', requestdict["%s_band_start" % line] + '-' + requestdict["%s_band_stop" % line])
    configfile = open(CONFIG_OUTPUT + filename +'.cfg', 'a')
    config.write(configfile)
    os.chmod(CONFIG_OUTPUT + filename +'.cfg', 0664)
    return 1

@app.route('/processing', methods=['GET', 'POST'])
def processingpage(name=None):
    folder = request.args["id"]
    lines = []
    for line in glob.glob(WEB_PROCESSING_FOLDER + folder + "/status/*"):
        for l in open(line):
            status = l.split(' ')[2]
        line_details = {
            "name" : os.basepath(line),
            "status" : status
        }
        lines.append(line_details)

    return render_template('processingpage.html',
                           lines=lines)


def getifov(sensor):
    if "fenix" in sensor:
        ifov = 0.001448623
    if "eagle" in sensor:
        ifov = 0.000645771823
    if "hawk" in sensor:
        ifov = 0.0019362246375
    return ifov

def pixelsize(altitude, sensor):
    return 2 * altitude * math.tan(getifov(sensor)/2)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
