from flask import Flask
from flask import render_template
from flask import request
from numpy import arange

app = Flask(__name__)

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
                           name=name)

@app.route('/jobrequestpost/', methods=['POST'])
def post():
    print request.form
    print request.values
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
