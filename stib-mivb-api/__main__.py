#!/usr/bin/env python3
import os
import sys
import json
import arrow
import urllib.request

from xml.etree import ElementTree
from flask import Flask
from flask import request
from flask import Response

TZ = 'Europe/Brussels'
TIMEFMT = 'YYYY-MM-DDTHH:mm:ssZZ'

log = lambda *x, **y: print(*x, **y, file=sys.stderr)

app = Flask(__name__)

def httpdatefmt ( t ) :
    return t.to('GMT').format('ddd, D MMM YYYY HH:mm:ss') + ' GMT'

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/getwaitingtimes")
def getwaitingtimes():

    requests = [ ]

    REQUEST = 'http://m.stib.be/api/getwaitingtimes.php?halt={}'

    halt = request.args.get('halt')

    url = REQUEST.format(halt)

    log(url)
    requests.append(url)

    output = {
        'time-of-request' : arrow.now(TZ).format( TIMEFMT ) ,
        'requests' : requests
    }

    try:

        W = ElementTree.parse(urllib.request.urlopen(url)).getroot()

        now = arrow.now(TZ)

        results = [ ]

        for waitingtime in W.iter('waitingtime') :

            w = {tag.tag: tag.text for tag in waitingtime}

            when = now.replace(minutes=+int(w['minutes']))

            _when = when.format(TIMEFMT)

            results.append({
                'halt' : halt ,
                'line' : w['line'] ,
                'mode' : w['mode'] ,
                'when' : _when ,
                'destination' : w['destination'] ,
                'message' : w['message'] ,
                'minutes' : int(w['minutes'])
            })

        output['results'] = results

        response = Response(json.dumps( output ))

    except urllib.error.HTTPError:
        log('failed to download', url)
        response = Response(json.dumps(output))

    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Age'] = 0
    response.headers['Date'] = httpdatefmt(now)
    response.headers['Last-Modified'] = httpdatefmt(now)

    response.headers['Server'] = 'stib-mivb-api.herokuapp.com'
    response.headers['Status'] = '200 OK'

    response.headers['X-RateLimit-Limit'] = '256'
    response.headers['X-RateLimit-Remaining'] = '255'
    response.headers['X-RateLimit-Reset'] = '0'
    response.headers['X-Poll-Interval'] = '0'

    response.headers['X-Frame-Options'] = 'deny'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Xss-Protection'] = '1; mode=block'
    response.headers['Content-Security-Policy'] = "default-src 'none'"
    response.headers['access-control-allow-origin'] = '*'
    response.headers['strict-transport-security'] = 'max-age=31536000; includeSubdomains; preload'

    response.headers['access-control-expose-headers'] = 'X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset, X-Poll-Interval'

    return response

if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
