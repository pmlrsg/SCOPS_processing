from flask import Flask
from flask import render_template
from flask import request
from numpy import arange
import ConfigParser

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

@app.route('/')
@app.route('/jobrequest')
def test(name=None):
    return render_template('requestform.html',
                           flightlines=[1,2,3,4,5],
                           uk = True,
                           bands=bands,
                           band_max=max(bands),
                           pixel_sizes=PIXEL_SIZES,
                           optimal_pixel=OPTIMAL_PIXEL,
                           bounds=bounds,
                           name=name,
                           julian_day=200,
                           year=2014,
                           project_code="T35T")

@app.route('/jobrequestpost/', methods=['POST'])
def post():
    print "in"
    requestdict = request.form
    print "requested"
    filename = "test.cfg"
    config_output(requestdict, lines=[1, 2, 3, 4, 5], filename=filename)
    print "working"
    return "working"


def config_output(requestdict, lines, filename):
    print requestdict
    print "configging"
    config = ConfigParser.RawConfigParser()
    config.add_section('global')
    print "section"
    config.set('global', "projection", requestdict["projectionRadios"])
    config.set('global', "projstring", requestdict["projString"])
    print "proj"
    config.set('global', "dem", requestdict["optionsDemRadios"])
    print "dem"
    config.set('global', "bounds", requestdict["bound_n"] + ' ' +
               requestdict["bound_e"] + ' ' +
               requestdict["bound_s"] + ' ' +
               requestdict["bound_w"])
    print "dem and bounds"
    config.set('global', "email", requestdict["email"])
    print "email"
    config.set('global', "interpolation", requestdict["optionsIntRadios"])
    print "interp"
    config.set('global', "pixelsize", requestdict["pixel_size_x"] + ' ' + requestdict["pixel_size_y"])
    print "config set"
    if requestdict["mask_all_check"] is "on":
        masking = "all"
    else:
        masking = "some"
    print "masked"
    config.set('global', "masking", masking)
    for line in lines:
        print line
        try:
            config.add_section(str(line))
        except Exception, e:
            print e
        print "section made"
        if requestdict['%s_line_check' % line] is "on" or requestdict['process_all_lines'] is "on":
            config.set(str(line), 'process', 'true')
        else:
            config.set(str(line), 'process', 'false')
        config.set(str(line), 'band_range', requestdict["%s_band_start" % line] + ' - ' + requestdict["%s_band_stop" % line])
    configfile = open(CONFIG_OUTPUT + filename, 'a')
    config.write(configfile)
    print config
    return "working"

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
    app.run(debug=True)
