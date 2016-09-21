#!/usr/bin/env python3
import os
import sys
import json
import math
import arrow
import urllib.request
import urllib.parse
from collections import defaultdict
from xml.etree import ElementTree
from flask.ext.api import FlaskAPI
from flask import request
from flask import url_for
from flask import Response

TZ = 'Europe/Brussels'
TIMEFMT = 'YYYY-MM-DDTHH:mm:ssZZ'
HTTPDATEFMT = 'ddd, D MMM YYYY HH:mm:ss'

log = lambda *x, **y: print(*x, **y, file=sys.stderr)

NETWORK_URL = 'https://raw.githubusercontent.com/aureooms/stib-mivb-network/master/data.json'
GEOJSON_URL = 'https://gist.githubusercontent.com/C4ptainCrunch/feff3569bc9a677932e61bca7bea5e4c/raw/9a51fdc4487b3827bf7b6fc6d3a199b333ca9c4f/stops.geojson'

_network = {}
_geojson = {}
_stops = {}
_stops_index = defaultdict(list)
_belongs_index = defaultdict(lambda : defaultdict(lambda : defaultdict(lambda :defaultdict( list ))))
_last_updated = 'never'
_network_headers = { }

def _update_network ( ) :

    global _network, _geojson, _stops, _stops_index, _last_updated
    global _belongs_index, _network_headers
    # retrieve network file
    req = urllib.request.Request(NETWORK_URL)
    req.add_header('Cache-Control', 'max-age=0')
    _network = json.loads( urllib.request.urlopen( req ).read().decode() )
    # build stops index
    _stops_index.clear()
    for stop in _network['stops'].values() :
        _stops_index[stop['name'].lower()].append(stop)
    _belongs_index.clear()
    for line , directions in _network['itineraries'].items() :
        for direction , stops in directions.items() :
            for i , stop in enumerate( stops ) :
                _belongs_index[stop][line][direction]['positions'].append(i)
    # retrieve geojson file
    req = urllib.request.Request(GEOJSON_URL)
    req.add_header('Cache-Control', 'max-age=0')
    _geojson = json.loads( urllib.request.urlopen( req ).read().decode() )
    _stops = { f['properties']['stop_id'] : f for f in _geojson['features'] }
    # update default headers
    _last_updated = arrow.now(TZ).format(TIMEFMT)
    creation = arrow.get(_network['creation'])
    _network_headers = {
        'Cache-Control' :  'public, max-age=60, s-maxage=60' ,
        'Last-Modified' :  httpdatefmt(creation)
    }

def httpdatefmt ( t ) :
    return t.to('GMT').format(HTTPDATEFMT) + ' GMT'

def postprocess ( output , code = 200 , headers = None ) :

    if headers is None : headers = { }

    date = arrow.now(TZ)
    headers['Date'] = httpdatefmt(date)

    if 'Cache-Control' in headers :
        if 'Last-Modified' not in headers :
            headers['Last-Modified'] = headers['Date']
        headers['Age'] = int( ( date - arrow.get(headers['Last-Modified'][5:-4]
            , HTTPDATEFMT[5:] ) ).total_seconds())

    headers['X-RateLimit-Limit'] = '256'
    headers['X-RateLimit-Remaining'] = '255'
    headers['X-RateLimit-Reset'] = '0'
    headers['X-Poll-Interval'] = '0'

    headers['X-Frame-Options'] = 'deny'
    headers['X-Content-Type-Options'] = 'nosniff'
    headers['X-Xss-Protection'] = '1; mode=block'
    headers['Content-Security-Policy'] = "default-src 'self'"
    headers['access-control-allow-origin'] = '*'
    headers['strict-transport-security'] = 'max-age=31536000; includeSubdomains; preload'

    headers['access-control-expose-headers'] = 'X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset, X-Poll-Interval'

    return output , code , headers


app = FlaskAPI(__name__)

app.config['DEFAULT_RENDERERS'] = [
    'flask.ext.api.renderers.JSONRenderer',
    'flask.ext.api.renderers.BrowsableAPIRenderer',
]

@app.route("/")
def app_route_root():
    root = request.host_url.rstrip('/')
    return postprocess( {
        'url' : root + url_for( 'app_route_root' ) ,
        'links' : {
            'network' : root + url_for( 'app_route_network' ) ,
        } ,
    } , headers = _network_headers )

@app.route('/network/', methods=['GET', 'PUT'])
def app_route_network():

    if request.method == 'PUT' :

        _update_network()

    root = request.host_url.rstrip('/')

    return postprocess( {
        'url' : root + url_for( 'app_route_network' ) ,
        'links' : {
            'lines' : root + url_for( 'app_route_network_lines' ) ,
            'stops' : root + url_for( 'app_route_network_stops' ) ,
        } ,
        'last-updated' : _last_updated ,
    } , headers = _network_headers )


@app.route("/network/lines/")
def app_route_network_lines():

    lines = { }

    for id , data in _network['lines'].items() :
        url = request.host_url.rstrip('/') + url_for('app_route_network_line', id=id)
        line = {}
        line['id'] = id
        line['url'] = url
        line['name'] = data['destination1'] + ' - ' + data['destination2']
        line['mode'] = data['mode']
        lines[id] = line

    root = request.host_url.rstrip('/')
    return postprocess( {
        'url' : root + url_for( 'app_route_network_lines' ) ,
        'lines' : lines ,
    } , headers = _network_headers )

@app.route("/network/line/<id>")
def app_route_network_line(id):
    if id in _network['lines'] :
        root = request.host_url.rstrip('/')
        data = _network['lines'][id]
        url = root + url_for('app_route_network_line', id=id)
        line = { }
        line['id'] = id
        line['url'] = url
        line['name'] = data['destination1'] + ' - ' + data['destination2']
        line['mode'] = data['mode']
        line['directions'] = {
            data['destination1'] : root + url_for('app_route_network_direction', id=id, direction=1 ) ,
            data['destination2'] : root + url_for('app_route_network_direction', id=id, direction=2 ) ,
        }
        return postprocess( line , headers = _network_headers )
    else :
        return postprocess( { 'message' : 'line does not exist' } , 404 )

@app.route("/network/line/<id>/<direction>")
def app_route_network_direction(id,direction):
    if id not in _network['itineraries'] or direction not in _network['itineraries'][id] :
        return postprocess( { 'message' : 'itinerary does not exist' } , 404 )

    stops = [ ]

    for stopid in _network['itineraries'][id][direction] :

        data = _network['stops'][stopid]

        root = request.host_url.rstrip('/')

        stop = {
            'id' : stopid ,
            'name' : data['name'] ,
            'latitude' : data['latitude'] ,
            'longitude' : data['longitude'] ,
            'url' : root + url_for('app_route_network_stop', id = stopid) ,
        }

        stops.append(stop)

    output = {
        'url' :  root + url_for('app_route_network_direction', id=id,
            direction=direction) ,
        'line' : {
            'url' : root + url_for('app_route_network_line', id=id )
        } ,
        'stops' : stops
    }

    return postprocess( output , headers = _network_headers )

@app.route('/network/stops/', defaults={'page': 1})
@app.route('/network/stops/page/<int:page>')
def app_route_network_stops(page):
    return { 'message' : 'not implemented yet' } , 404

@app.route("/network/stop/<id>")
def app_route_network_stop(id):

    if id not in _network['stops'] :
        return postprocess( { 'message' : 'stop does not exist' } , 404 )

    data = _network['stops'][id]

    root = request.host_url.rstrip('/')

    stop = {
        'id' : id ,
        'name' : data['name'] ,
        'latitude' : data['latitude'] ,
        'longitude' : data['longitude'] ,
        'url' : root + url_for('app_route_network_stop', id = id) ,
        'realtime' : {
            'url' : root + url_for('app_route_realtime_stop', id = id) ,
        } ,
        'geojson' : {
            'url' : root + url_for('app_route_geojson_stop', id = id) ,
        } ,
        'belongs' : {
            l : {
                d : {
                    'url' : root + url_for('app_route_network_direction', id = l , direction=d ),
                    'positions' : x['positions']
                } for d , x in direction.items()
            } for l , direction in _belongs_index[id].items()
        }
    }

    if id in _stops :
        if stop['latitude'] is None :
            stop['latitude'] = _stop[id]['geometry']['coordinates'][1]
        if stop['longitude'] is None :
            stop['longitude'] = _stop[id]['geometry']['coordinates'][0]

    return postprocess( stop , headers = _network_headers )

@app.route("/search/stop/")
def app_route_search_stop():

    q = request.args.get('query',None)

    if q is None :
        output = { 'message' : 'missing query argument' }
        return postprocess( output , code = 400 )

    root = request.host_url.rstrip('/')

    args = { 'query' : q }

    url = root + url_for('app_route_search_stop') + '?' + urllib.parse.urlencode(args)

    results = []

    for data in _stops_index[q.lower()] :

        stop = {
            'id' : data['id'] ,
            'name' : data['name'] ,
            'latitude' : data['latitude'] ,
            'longitude' : data['longitude'] ,
            'url' : root + url_for('app_route_network_stop', id = data['id'])
        }

        if id in _stops :
            if stop['latitude'] is None :
                stop['latitude'] = _stop[id]['geometry']['coordinates'][1]
            if stop['longitude'] is None :
                stop['longitude'] = _stop[id]['geometry']['coordinates'][0]

        results.append( stop )

    output = {
        'url' : url ,
        'query' : q ,
        'results' : results ,
    }

    return postprocess( output , headers = _network_headers )

@app.route("/geojson/stop/<id>")
def app_route_geojson_stop(id):

    if id not in _network['stops'] :
        return postprocess( { 'message' : 'stop does not exist' } , 404 )

    if id not in _stops :
        return postprocess( { 'message' : 'no geojson data for this stop' } , 404 )

    return postprocess( _stops[id] , headers = _network_headers )


@app.route("/realtime/stop/<id>")
def app_route_realtime_stop(id = None):

    if id not in _network['stops'] :
        output = { 'message' : 'incorrect id parameter' }
        return postprocess( output , code = 400 )

    _max_requests = request.args.get('max_requests','1')

    if any( map( lambda x : x < '0' or x > '9' , _max_requests ) ) :
        output = { 'message' : 'incorrect max_requests parameter' }
        return postprocess( output , code = 400 )

    max_requests = int(_max_requests)

    return get_realtime_stop(id, max_requests, [])


def get_realtime_stop(id, max_requests, requests):

    REQUEST = 'http://m.stib.be/api/getwaitingtimes.php?halt={}'

    output = {
        'requests' : requests ,
    }

    results = [ ]

    halts = _network['waiting'][id]

    sources = { halt : REQUEST.format(halt) for halt in halts }

    for halt in halts :

        for i in range( 1 , max_requests + 1 ) :

            url = REQUEST.format(halt)

            req = {
                'url' : url ,
                'date' : arrow.now(TZ).format( TIMEFMT )
            }

            requests.append(req)

            try :

                _response = urllib.request.urlopen(url)
                req['code'] = _response.getcode()

                W = ElementTree.parse(_response).getroot()

                now = arrow.now(TZ)

                for waitingtime in W.iter('waitingtime') :

                    w = {tag.tag: tag.text for tag in waitingtime}

                    minutes=int(w['minutes'])
                    when = now.replace(minutes=+minutes)

                    _when = when.format(TIMEFMT)

                    results.append({
                        'stop' : id ,
                        'line' : w['line'] ,
                        'mode' : w['mode'] ,
                        'when' : _when ,
                        'destination' : w['destination'] ,
                        'message' : w['message'] ,
                        'minutes' : minutes
                    })

                break

            except urllib.error.HTTPError as e :

                req['code'] = e.code

                if i == max_requests :
                    output = {
                        'message' : 'failed to download ' + url ,
                        'sources' : sources ,
                        'requests' : requests
                    }
                    return postprocess( output , code = 503)

    output['results'] = results
    output['sources'] = sources
    root = request.host_url.rstrip('/')
    output['url'] = root + url_for('app_route_realtime_stop', id = id)

    headers = { 'Cache-Control' :  'no-cache' }

    return postprocess( output , headers = headers )

def dist ( lat1 , lon1 , lat2 , lon2 , sqrt = math.sqrt, rad = math.radians, atan = math.atan2 , sin = math.sin , cos = math.cos ) :

    """

        Should be numerically stable according to wikipedia.
        https://en.wikipedia.org/wiki/Great-circle_distance#Computational_formulas

    """

    a , b , x , y = map( rad, [lat1, lat2, lon1, lon2] )
    dl = abs(x-y)
    cds = sin(a) * sin(b) + cos(a) * cos(b) * cos(dl)
    cs = cos( b ) * sin( dl )
    csscc = cos( a ) * sin( b ) - sin( a ) * cos( b ) * cos( dl )
    return atan(sqrt(cs**2 + csscc**2) , cds)

@app.route("/realtime/closest/<lat>/<lon>")
def app_route_realtime_closest(lat = None, lon = None):

    try:
        lat = float(lat)
    except:
        output = { 'message' : 'incorrect lat parameter' }
        return postprocess( output , code = 400 )

    try:
        lon = float(lon)
    except:
        output = { 'message' : 'incorrect lon parameter' }
        return postprocess( output , code = 400 )

    _max_requests = request.args.get('max_requests','1')

    if any( map( lambda x : x < '0' or x > '9' , _max_requests ) ) :
        output = { 'message' : 'incorrect max_requests parameter' }
        return postprocess( output , code = 400 )

    max_requests = int(_max_requests)

    # SLOW AND STUPID
    id = min(_network['stops'].values(),key=lambda x : dist(lat,lon,x['latitude'],x['longitude']))['id']

    return get_realtime_stop(id,max_requests,[])



if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    _update_network()
    host = '0.0.0.0'
    port = int(os.environ.get('PORT', 5000))
    # debug = os.environ.get('DEBUG', 'False') == 'True'
    debug = True
    app.run(host=host, port=port, debug=debug)
