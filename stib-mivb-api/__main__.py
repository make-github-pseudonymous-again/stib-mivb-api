#!/usr/bin/env python3
import os
import sys
import json
import arrow
import urllib.request

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

_network = {}
_last_updated = 'never'
_network_headers = { }

def _update_network ( ) :

    global _network, _last_updated , _network_headers
    req = urllib.request.Request(NETWORK_URL)
    req.add_header('Cache-Control', 'max-age=0')
    _network = json.loads( urllib.request.urlopen( req ).read().decode() )
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
        headers['Age'] = int( ( date - arrow.get(headers['Last-Modified'][:-4] , HTTPDATEFMT ) ).total_seconds())

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
    request_time = arrow.now(TZ)
    root = request.host_url.rstrip('/')
    return postprocess( {
        'url' : root + url_for( 'app_route_root' ) ,
        'links' : {
            'network' : root + url_for( 'app_route_network' ) ,
        } ,
    } , headers = _network_headers )

@app.route('/network/', methods=['GET', 'PUT'])
def app_route_network():
    request_time = arrow.now(TZ)

    if request.method == 'PUT' :

        _update_network()

    root = request.host_url.rstrip('/')

    return postprocess( {
        'url' : root + url_for( 'app_route_network' ) ,
        'links' : {
            'lines' : root + url_for( 'app_route_network_lines' ) ,
        } ,
        'last-updated' : _last_updated ,
    } , headers = _network_headers )


@app.route("/network/lines/")
def app_route_network_lines():
    request_time = arrow.now(TZ)

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
    request_time = arrow.now(TZ)
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
    request_time = arrow.now(TZ)
    if id not in _network['itineraries'] or direction not in _network['itineraries'][id] :
        return postprocess( { 'message' : 'itinerary does not exist' } , 404 )

    itinerary = [ ]

    for stopid in _network['itineraries'][id][direction] :

        data = _network['stops'][stopid]

        root = request.host_url.rstrip('/')

        stop = {
            'id' : stopid ,
            'name' : data['name'] ,
            'latitude' : data['latitude'] ,
            'longitude' : data['longitude'] ,
            'url' : root + url_for('app_route_network_stop', id = stopid)
        }

        itinerary.append(stop)

    return postprocess( itinerary , headers = _network_headers )

@app.route("/network/stop/<id>")
def app_route_network_stop(id):
    request_time = arrow.now(TZ)

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
    }

    return postprocess( stop , headers = _network_headers )


@app.route("/realtime/stop/<id>")
def app_route_realtime_stop(id = None):

    requests = [ ]

    REQUEST = 'http://m.stib.be/api/getwaitingtimes.php?halt={}'

    if id not in _network['stops'] :
        output = { 'message' : 'incorrect id parameter' }
        return postprocess( output , code = 400 )

    _max_requests = request.args.get('max_requests','1')

    if any( map( lambda x : x < '0' or x > '9' , _max_requests ) ) :
        output = { 'message' : 'incorrect max_requests parameter' }
        return postprocess( output , code = 400 )

    output = {
        'requests' : requests ,
    }

    max_requests = int(_max_requests)

    for i in range( 1 , max_requests + 1 ) :

        url = REQUEST.format(id)

        req = {
            'url' : url ,
            'date' : arrow.now(TZ).format( TIMEFMT )
        }

        requests.append(req)

        try :

            _response = urllib.request.urlopen(url)
            W = ElementTree.parse(_response).getroot()

            now = arrow.now(TZ)

            results = [ ]

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

            output['results'] = results

            req['code'] = _response.getcode()

            headers = {
                'Cache-Control' :  'no-cache' ,
            }

            return postprocess( output , headers = headers )

        except urllib.error.HTTPError as e :

            req['code'] = e.code

            if i == max_requests :
                output = { 'message' : 'failed to download ' + url }
                return postprocess( output , code = 503)

if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    _update_network()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
