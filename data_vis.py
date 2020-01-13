import numpy as np
import folium
from folium.plugins import HeatMap
import csv
import json
from bson import json_util
from pymongo import MongoClient
import pandas as pd
from pymongo import MongoClient
import folium
import webbrowser

client = MongoClient('mongodb://localhost:27017')

resultat = client.POC1.Resultat
cursors = resultat.find()
entries = list(cursors)
results = pd.DataFrame(entries)

del results["_id"]

latlon=[]
listmax=[]
for i in range(12):
    listlonlat=[results['startlat'][i],results['startlon'][i]]
    if results['tip'][i]==results.tip.max():
        listmax=listlonlat
    else:
        latlon.append(listlonlat)
latlon
listmax

latitude = 40.751961372056
longitude = -73.97865989544881
traffic_map = folium.Map(location=[latitude, longitude], zoom_start=15 ,title='Stamen Terrain')
folium.Marker(
      location =[latitude, longitude], 
      icon = folium.Icon(color='red', icon='info-sign')
     ).add_to(traffic_map)

for list in latlon:
    folium.CircleMarker(location = list, radius=20,
                      color='#0000FF',
                      fill_color='#0000FF').add_to(traffic_map)
#     if list ==listmax:
folium.CircleMarker(location = listmax, radius=30,
                      color='green',
                      fill_color='green').add_to(traffic_map)




# this won't work:
traffic_map
traffic_map.render()



# ------------------------------------------------------------------------------------------------
# so let's write a custom temporary-HTML renderer
# pretty much copy-paste of this answer: https://stackoverflow.com/a/38945907/3494126
import subprocess
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer


PORT = 7000
HOST = '127.0.0.1'
SERVER_ADDRESS = '{host}:{port}'.format(host=HOST, port=PORT)
FULL_SERVER_ADDRESS = 'http://' + SERVER_ADDRESS


def TemproraryHttpServer(page_content_type, raw_data):
    """
    A simpe, temprorary http web server on the pure Python 3.
    It has features for processing pages with a XML or HTML content.
    """

    class HTTPServerRequestHandler(BaseHTTPRequestHandler):
        """
        An handler of request for the server, hosting XML-pages.
        """

        def do_GET(self):
            """Handle GET requests"""

            # response from page
            self.send_response(200)

            # set up headers for pages
            content_type = 'text/{0}'.format(page_content_type)
            self.send_header('Content-type', content_type)
            self.end_headers()

            # writing data on a page
            self.wfile.write(bytes(raw_data, encoding='utf'))

            return

    if page_content_type not in ['html', 'xml']:
        raise ValueError('This server can serve only HTML or XML pages.')

    page_content_type = page_content_type

    # kill a process, hosted on a localhost:PORT
    subprocess.call(['fuser', '-k', '{0}/tcp'.format(PORT)])

    # Started creating a temprorary http server.
    httpd = HTTPServer((HOST, PORT), HTTPServerRequestHandler)

    # run a temprorary http server
    httpd.serve_forever()


def run_html_server(html_data=None):

    if html_data is None:
        html_data = """
        <!DOCTYPE html>
        <html>
        <head>
        <title>Page Title</title>
        </head>
        <body>
        <h1>This is a Heading</h1>
        <p>This is a paragraph.</p>
        </body>
        </html>
        """

    # open in a browser URL and see a result
    webbrowser.open(FULL_SERVER_ADDRESS)

    # run server
    TemproraryHttpServer('html', html_data)

# ------------------------------------------------------------------------------------------------


# now let's save the visualization into the temp file and render it
from tempfile import NamedTemporaryFile
tmp = NamedTemporaryFile()
traffic_map.save(tmp.name)
with open(tmp.name) as f:
    traffic_map_html = f.read()

run_html_server(traffic_map_html)
