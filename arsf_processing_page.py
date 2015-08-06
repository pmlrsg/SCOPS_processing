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

CONFIG_OUTPUT = "/data/turkana1/scratch/stgo/web_testing/configs/"
UPLOAD_FOLDER = "/data/turkana1/scratch/stgo/web_testing/dem_upload/"

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = "/users/rsg/stgo/upload_test/"

bands = [1,2,3,4,5]
PIXEL_SIZES = arange(0.5, 1.6, 0.1)
OPTIMAL_PIXEL = float(1.1)

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
def jobr_request(name=None):
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
    lines = sorted(lines, key=lambda line: line["name"])
    return render_template('requestform.html',
                           flightlines=lines,
                           uk = britain,
                           pixel_sizes=PIXEL_SIZES,
                           optimal_pixel=OPTIMAL_PIXEL,
                           bounds=bounds,
                           name=name,
                           julian_day=day,
                           year=year,
                           project_code=proj_code,
                           utmzone="UTM zone "+str(utmzone[0])+str(utmzone[1]))

@app.route('/progress', methods=['POST'])
def post():
    requestdict = request.form
    filename = "test.cfg"
    lines = []
    for key in requestdict:
        if "_line_check" in key:
            lines.append(key.strip("_line_check"))
    lines = sorted(lines)
    filename = requestdict["project_code"] + '_' + requestdict["year"] + '_' + requestdict["julianday"] + datetime.datetime.now()
    config_output(requestdict, lines=lines, filename=filename)
    return render_template('submitted.html')


def config_output(requestdict, lines, filename):
    config = ConfigParser.RawConfigParser()
    config.set('DEFAULT', "projection", requestdict["projectionRadios"])
    config.set('DEFAULT', "projstring", requestdict["projString"])
    config.set('DEFAULT', "dem", requestdict["optionsDemRadios"])
    config.set('DEFAULT', "bounds", requestdict["bound_n"] + ' ' +
               requestdict["bound_e"] + ' ' +
               requestdict["bound_s"] + ' ' +
               requestdict["bound_w"])
    config.set('DEFAULT', "email", requestdict["email"])
    config.set('DEFAULT', "interpolation", requestdict["optionsIntRadios"])
    config.set('DEFAULT', "pixelsize", requestdict["pixel_size_x"] + ' ' + requestdict["pixel_size_y"])
    if requestdict["mask_all_check"] is "on":
        masking = "all"
    else:
        masking = "some"
    config.set('DEFAULT', "masking", masking)
    for line in lines:
        config.add_section(str(line))
        if requestdict['%s_line_check' % line] is "on" or requestdict['process_all_lines'] is "on":
            config.set(str(line), 'process', 'true')
        else:
            config.set(str(line), 'process', 'false')
        config.set(str(line), 'band_range', requestdict["%s_band_start" % line] + ' - ' + requestdict["%s_band_stop" % line])
    configfile = open(CONFIG_OUTPUT + filename, 'a')
    config.write(configfile)
    return 1

# def JobRequestPage(name=None):
#     utmzone = 12
#     flightlines = [1,2,3,4,5,6]
#     uk = True
#     bands = [1,2,3,4,5,6]
#     return render_template('requestform.html',
#                            flightlines=flightlines,
#                            bands=bands,
#                            utmzone=utmzone,
#                            uk=uk,
#                            name=name)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
