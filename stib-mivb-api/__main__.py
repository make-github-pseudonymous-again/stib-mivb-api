#!/usr/bin/env python3
import os
import sys
import json
import math
import heapq
import arrow
import concurrent.futures
import urllib.request
import urllib.parse
from collections import defaultdict
from xml.etree import ElementTree
from flask_api import FlaskAPI
from flask import request
from flask import url_for
from flask import Response

TZ = 'Europe/Brussels'
TIMEFMT = 'YYYY-MM-DDTHH:mm:ssZZ'
HTTPDATEFMT = 'ddd, D MMM YYYY HH:mm:ss'
DEFAULT_MAX_REQUESTS = '10'
MAX_MAX_REQUESTS = 10
MAX_NCLOSEST = 30
TIMEOUT = 5

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

class APIError ( Exception ) :

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

class MaxRequestsError ( APIError ) :
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

    if lineid is None :
        return None

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

        if headers['Cache-Control'] == 'no-cache' or 'Last-Modified' not in headers :
            headers['Last-Modified'] = headers['Date']

        last_modified = arrow.get(headers['Last-Modified'][5:-4] , HTTPDATEFMT[5:] )

        headers['Age'] = int( ( date - last_modified ).total_seconds( ) )

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
    'flask_api.renderers.JSONRenderer',
    'flask_api.renderers.BrowsableAPIRenderer',
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
        return APIError('line does not exist', code = 404 ).postprocess()

@app.route("/network/line/<id>/<direction>")
def app_route_network_direction(id,direction):
    if id not in _network['itineraries'] or direction not in _network['itineraries'][id] :
        return APIError( 'itinerary does not exist' , code = 404 ).postprocess()

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
        return APIError( 'stop does not exist' , code = 404 ).postprocess()

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
        return APIError( 'missing query argument' , code = 400 ).postprocess()

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
        return APIError( 'stop does not exist' , code = 404 ).postprocess()

    if id not in _stops :
        return APIError( 'no geojson data for this stop' , code = 404 ).postprocess()

    return postprocess( _stops[id] , headers = HSTATIC )

def get_max_requests ( request ) :

    _max_requests = request.args.get('max_requests',DEFAULT_MAX_REQUESTS)

    try :
        max_requests = int(_max_requests)
    except:
        raise APIError( 'incorrect max_requests parameter' , code = 400 )

    if max_requests < 1 :
        raise APIError( 'max_requests must be >= 1' , code = 400 )

    if max_requests > MAX_MAX_REQUESTS :
        raise APIError( 'max_requests must be <= {}'.format(MAX_MAX_REQUESTS) , code = 400 )

    return max_requests


@app.route("/realtime/stop/<id>")
def app_route_realtime_stop(id = None):

    if id not in _network['stops'] :
        return APIError( 'incorrect id parameter' , code = 400 ).postprocess()

    try:
        max_requests = get_max_requests( request )
        _ , realtime = next(get_realtime_stops([id], max_requests))
    except APIError as e :
        return e.postprocess()

    return postprocess( realtime , headers = HDYNAMIC )

class LoadUrlResult ( object ) :

    def __init__ ( self , data , date , requests ) :

        self.data = data
        self.date = date
        self.requests = requests

class LoadUrlException ( Exception ) :

    def __init__ ( self , date , requests ) :

        self.date = date
        self.requests = requests

def load_url(url, max_requests = 1, timeout = 60) :

    requests = []

    for i in range( max_requests ) :

        req = {
            'url' : url ,
            'date' : arrow.now(TZ).format( TIMEFMT )
        }

        requests.append( req )

        try:

            with urllib.request.urlopen(url, timeout=timeout) as conn:

                req['code'] = conn.getcode()

                now = arrow.now(TZ)

                tree = ElementTree.parse(conn).getroot()

                return LoadUrlResult( tree , now , requests )

        except urllib.error.HTTPError as e :

            req['code'] = e.code

    now = arrow.now(TZ)
    raise LoadUrlException( now , requests )

def query_realtime_stops(ids, max_requests):

    REQUEST = 'http://m.stib.be/api/getwaitingtimes.php?halt={}'

    jobs = []

    for id in ids :

        halts = _network['waiting'][id]

        for halt in halts :

            url = REQUEST.format(halt)

            key = ( id , halt , url )
            fn = load_url
            args = [ url ]
            kwargs = {
                'max_requests' : max_requests ,
                'timeout' : TIMEOUT
            }

            jobs.append(( key , fn , args , kwargs ))

    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        # Start the load operations and mark each future with its URL
        batch = {
            executor.submit(fn,*args,**kwargs) : key
            for ( key , fn , args , kwargs ) in jobs
        }

        for future in concurrent.futures.as_completed(batch):

            key = batch[future]

            yield key , future

def get_realtime_stops(ids, max_requests):

    results = defaultdict(list)
    sources = defaultdict(dict)
    ok = { id : False for id in ids }

    for key , future in query_realtime_stops( ids , max_requests ) :

        id , halt , url = key

        try:
            result = future.result()

        except LoadUrlException as e :
            sources[id][halt] = {
                'error' : True ,
                'url' : url ,
                'date' : e.date ,
                'requests' : e.requests
            }

        else:

            sources[id][halt] = {
                'error' : False ,
                'url' : url ,
                'date' : result.date.format(TIMEFMT) ,
                'requests' : result.requests
            }

            ok[id] = True


            for waitingtime in result.data.iter('waitingtime') :

                try:

                    w = {tag.tag: tag.text for tag in waitingtime}

                    minutes = int(w['minutes'])
                    mode = w['mode']
                    destination = w['destination']
                    message = w['message']
                    lineid = w['line']

                except Exception as e:

                    app.logger.warning( e )
                    app.logger.warning( "couldn't parse waitingtime" )

                else:

                    when = result.date.replace(minutes=+minutes)

                    _when = when.format(TIMEFMT)

                    line = get_line( lineid )
                    if line is not None :
                        fgcolor = line['fgcolor']
                        bgcolor = line['bgcolor']
                    else:
                        bgcolor = "#000000"
                        fgcolor = "#FFFFFF"

                    results[id].append({
                        'stop' : id ,
                        'line' : lineid ,
                        'mode' : mode ,
                        'when' : _when ,
                        'destination' : destination ,
                        'message' : message ,
                        'minutes' : minutes ,
                        'fgcolor' : fgcolor ,
                        'bgcolor' : bgcolor
                    })

    if not any(ok.values()) :
        msg = 'failed to fetch realtime'
        raise MaxRequestError( msg , code = 503 , details = sources )

    root = request.host_url.rstrip('/')

    for id in ids :
        if not ok[id] :
            msg = 'failed to fetch realtime for {}'.format(id)
            yield id , MaxRequestError( msg , code = 503 , details = sources[id] ).json()

        else:
            yield id , {
                'url' : root + url_for('app_route_realtime_stop', id = id) ,
                'sources' : sources[id] ,
                'results' : results[id]
            }


def _dist ( lat1 , lon1 , lat2 , lon2 , sqrt = math.sqrt, rad = math.radians, atan = math.atan2 , sin = math.sin , cos = math.cos ) :

    """

        Numerically stable according to wikipedia.
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
    except APIError as e:
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
        return APIError( 'incorrect n parameter' , code = 400 ).postprocess()

    if n > MAX_NCLOSEST :
        return APIError( 'n must be <= {}'.format( MAX_NCLOSEST ) , code = 400 ).postprocess()

    try:
        stops = get_realtime_nclosest(lat, lon, n = n)
    except APIError as e:
        return e.postprocess()

    root = request.host_url.rstrip('/')
    url = root + url_for('app_route_realtime_nclosest', n = n  , lat = lat , lon = lon )
    output = { 'stops' : stops , 'url' : url }

    return postprocess( output , headers = HDYNAMIC )


def get_realtime_nclosest(lat, lon, n = 1):

    try:
        _lat = float(lat)
    except:
        raise APIError( 'incorrect lat parameter' , code = 400 )

    try:
        _lon = float(lon)
    except:
        raise APIError( 'incorrect lon parameter' , code = 400 )

    max_requests = get_max_requests( request )

    # SLOW AND STUPID
    closeness = lambda x : dist(_lat,_lon,x['latitude'],x['longitude'])
    nclosest = heapq.nsmallest(n,_network['stops'].values(),key=closeness)

    stops = []
    root = request.host_url.rstrip('/')

    ids = [ stop['id'] for stop in nclosest ]

    for id , realtime in get_realtime_stops(ids, max_requests):

        data = _network['stops'][id]

        root + url_for('app_route_realtime_closest', lat = lat , lon = lon )

        stop = {
            'id' : id ,
            'name' : data['name'] ,
            'latitude' : data['latitude'] ,
            'longitude' : data['longitude'] ,
            'url' : root + url_for('app_route_network_stop', id = id) ,
            'realtime' : realtime ,
        }

        stops.append(stop)

    return stops


if __name__ == "__main__":

    # debug = os.environ.get('DEBUG', 'False') == 'True'
    debug = True

    # Bind to PORT if defined, otherwise default to 5000.
    _update_network()
    host = '0.0.0.0'
    port = int(os.environ.get('PORT', 5000))
    app.run(host=host, port=port, debug=debug)
