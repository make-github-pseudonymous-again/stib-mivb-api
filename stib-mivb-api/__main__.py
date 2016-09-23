#!/usr/bin/env python3
import os
import sys
import json
import math
import heapq
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
DEFAULT_MAX_REQUESTS = '10'

log = lambda *x, **y: print(*x, **y, file=sys.stderr)

NETWORK_URL = 'https://raw.githubusercontent.com/aureooms/stib-mivb-network/master/data.json'
GEOJSON_URL = 'https://gist.githubusercontent.com/C4ptainCrunch/feff3569bc9a677932e61bca7bea5e4c/raw/9a51fdc4487b3827bf7b6fc6d3a199b333ca9c4f/stops.geojson'

_network = {}
_geojson = {}
_stops = {}
_stops_index = defaultdict(list)
_belongs_index = defaultdict(lambda : defaultdict(lambda : defaultdict(lambda :defaultdict( list ))))
_last_updated = 'never'

HDYNAMIC = { 'Cache-Control' :  'no-cache' }
HSTATIC = { }

class Error ( Exception ) :

    def __init__ ( self , message , code = 520 , details = None ) :
        self.message = message
        self.code = code
        self.details = { } if details is None else details

    def postprocess ( self ) :
        output = self.json()
        return postprocess( output  , code = self.code )

    def json ( self ) :
        return {
            'error' : self.code ,
            'message' : self.message ,
            'details' : self.details ,
        }

class MaxRequestsError ( Error ) :
    pass

def _update_network ( ) :

    global _network, _geojson, _stops, _stops_index, _last_updated
    global _belongs_index, HSTATIC
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

    # patch coordinates
    for id , stop in _network['stops'].items() :
        if id in _stops :
            if stop['latitude'] is None :
                stop['latitude'] = _stops[id]['geometry']['coordinates'][1]
            if stop['longitude'] is None :
                stop['longitude'] = _stops[id]['geometry']['coordinates'][0]

        if stop['latitude'] is not None :
            stop['latitude'] = float(stop['latitude'])

        if stop['longitude'] is not None :
            stop['longitude'] = float(stop['longitude'])

    # update default headers
    _last_updated = arrow.now(TZ).format(TIMEFMT)
    creation = arrow.get(_network['creation'])
    HSTATIC = {
        'Cache-Control' :  'public, max-age=60, s-maxage=60' ,
        'Last-Modified' :  httpdatefmt(creation)
    }

def get_line ( lineid ) :

    if lineid[0] == 'N' :
        # Noctis
        lineid = "2" + lineid[1:]

    if lineid in _network["lines"] :
        return _network["lines"][lineid]

    return None

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
    } , headers = HSTATIC )

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
    } , headers = HSTATIC )


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
    } , headers = HSTATIC )

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
        line['fgcolor'] = data['fgcolor']
        line['bgcolor'] = data['bgcolor']
        return postprocess( line , headers = HSTATIC )
    else :
        return Error('line does not exist', code = 404 ).postprocess()

@app.route("/network/line/<id>/<direction>")
def app_route_network_direction(id,direction):
    if id not in _network['itineraries'] or direction not in _network['itineraries'][id] :
        return Error( 'itinerary does not exist' , code = 404 ).postprocess()

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

    return postprocess( output , headers = HSTATIC )

@app.route('/network/stops/', defaults={'page': 1})
@app.route('/network/stops/page/<int:page>')
def app_route_network_stops(page):
    return { 'message' : 'not implemented yet' } , 404

@app.route("/network/stop/<id>")
def app_route_network_stop(id):

    if id not in _network['stops'] :
        return Error( 'stop does not exist' , code = 404 ).postprocess()

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

    return postprocess( stop , headers = HSTATIC )

@app.route("/search/stop/")
def app_route_search_stop():

    q = request.args.get('query',None)

    if q is None :
        return Error( 'missing query argument' , code = 400 ).postprocess()

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

        results.append( stop )

    output = {
        'url' : url ,
        'query' : q ,
        'results' : results ,
    }

    return postprocess( output , headers = HSTATIC )

@app.route("/geojson/stop/<id>")
def app_route_geojson_stop(id):

    if id not in _network['stops'] :
        return Error( 'stop does not exist' , code = 404 ).postprocess()

    if id not in _stops :
        return Error( 'no geojson data for this stop' , code = 404 ).postprocess()

    return postprocess( _stops[id] , headers = HSTATIC )


@app.route("/realtime/stop/<id>")
def app_route_realtime_stop(id = None):

    if id not in _network['stops'] :
        return Error( 'incorrect id parameter' , code = 400 ).postprocess()

    _max_requests = request.args.get('max_requests',DEFAULT_MAX_REQUESTS)

    try :
        max_requests = int(_max_requests)
    except:
        return Error( 'incorrect max_requests parameter' , code = 400 ).postprocess()

    if max_requests < 1 :
        return Error( 'max_requests must be > 1' , code = 400 ).postprocess()

    try:
        output = get_realtime_stop(id, max_requests, [])
    except Error as e :
        return e.postprocess()

    return postprocess( output , headers = HDYNAMIC )

def get_realtime_stop(id, max_requests, requests):

    REQUEST = 'http://m.stib.be/api/getwaitingtimes.php?halt={}'

    output = {
        'requests' : requests ,
    }

    results = [ ]

    halts = _network['waiting'][id]

    sources = { halt : REQUEST.format(halt) for halt in halts }

    for halt in halts :

        url = REQUEST.format(halt)

        for i in range( 0 , max_requests + 1 ) :

            if i == max_requests :
                msg = 'failed to download ' + url
                details = { 'sources' : sources , 'requests' : requests }
                raise MaxRequestsError( msg , code = 503 , details = details )

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

                    lineid = w['line']
                    line = get_line( lineid )
                    if line is not None :
                        fgcolor = line['fgcolor']
                        bgcolor = line['bgcolor']
                    else:
                        bgcolor = "#000000"
                        fgcolor = "#FFFFFF"

                    results.append({
                        'stop' : id ,
                        'line' : lineid ,
                        'mode' : w['mode'] ,
                        'when' : _when ,
                        'destination' : w['destination'] ,
                        'message' : w['message'] ,
                        'minutes' : minutes ,
                        'fgcolor' : fgcolor ,
                        'bgcolor' : bgcolor
                    })

                break

            except urllib.error.HTTPError as e :

                req['code'] = e.code


    output['results'] = results
    output['sources'] = sources
    root = request.host_url.rstrip('/')
    output['url'] = root + url_for('app_route_realtime_stop', id = id)

    return output

def _dist ( lat1 , lon1 , lat2 , lon2 , sqrt = math.sqrt, rad = math.radians, atan = math.atan2 , sin = math.sin , cos = math.cos ) :

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

def dist ( a , b , c , d ) :
    if c is None or d is None :
        return 2 # > 1
    else:
        return _dist(a,b,c,d)


@app.route("/realtime/closest/<lat>/<lon>")
def app_route_realtime_closest(lat = None, lon = None):

    try:
        stops = get_realtime_nclosest(lat, lon, n = 1)
    except Error as e:
        return e.postprocess()

    root = request.host_url.rstrip('/')
    url = root + url_for('app_route_realtime_closest', lat = lat , lon = lon )
    output = { 'stop' : stops[0] , 'url' : url }

    return postprocess( output , headers = HDYNAMIC )

@app.route("/realtime/nclosest/<n>/<lat>/<lon>")
def app_route_realtime_nclosest(n = None , lat = None, lon = None):

    try:
        n = int(n)
    except:
        return Error( 'incorrect n parameter' , code = 400 ).postprocess()

    try:
        stops = get_realtime_nclosest(lat, lon, n = n)
    except Error as e:
        return e.postprocess()

    root = request.host_url.rstrip('/')
    url = root + url_for('app_route_realtime_nclosest', n = n  , lat = lat , lon = lon )
    output = { 'stops' : stops , 'url' : url }

    return postprocess( output , headers = HDYNAMIC )


def get_realtime_nclosest(lat, lon, n = 1):

    try:
        _lat = float(lat)
    except:
        raise Error( 'incorrect lat parameter' , code = 400 )

    try:
        _lon = float(lon)
    except:
        raise Error( 'incorrect lon parameter' , code = 400 )

    _max_requests = request.args.get('max_requests',DEFAULT_MAX_REQUESTS)

    try :
        max_requests = int(_max_requests)
    except:
        raise Error( 'incorrect max_requests parameter' , code = 400 )

    if max_requests < 1 :
        raise Error( 'max_requests must be > 1' , code = 400 )

    # SLOW AND STUPID
    closeness = lambda x : dist(_lat,_lon,x['latitude'],x['longitude'])
    nclosest = heapq.nsmallest(n,_network['stops'].values(),key=closeness)

    stops = []
    root = request.host_url.rstrip('/')

    for data in nclosest :

        id = data['id']
        try:
            realtime = get_realtime_stop(id, max_requests, [])
            max_requests -= len(realtime['requests'])
        except MaxRequestsError as e :
            realtime = e.json()
            max_requests = 0


        root + url_for('app_route_realtime_closest', lat = lat , lon = lon )

        stop = {
            'id' : data['id'] ,
            'name' : data['name'] ,
            'latitude' : data['latitude'] ,
            'longitude' : data['longitude'] ,
            'url' : root + url_for('app_route_network_stop', id = data['id']) ,
            'realtime' : realtime ,
        }

        stops.append(stop)

    return stops



if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    _update_network()
    host = '0.0.0.0'
    port = int(os.environ.get('PORT', 5000))
    # debug = os.environ.get('DEBUG', 'False') == 'True'
    debug = True
    app.run(host=host, port=port, debug=debug)
