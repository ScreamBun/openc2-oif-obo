from datetime import datetime

import httplib2
import simplejson

TESTDATA = {
    'woggle': {
        'version': 1234,
        'updated': str(datetime.now()),
    }
}
URL = 'http://localhost:8080/postCmd'

if __name__ == "__main__":
    jsondata = simplejson.dumps(TESTDATA)
    h = httplib2.Http()
    resp, content = h.request(URL, 'POST', jsondata, headers={'Content-Type': 'application/json'})
    print(resp)
    print(content)
