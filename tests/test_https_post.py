from datetime import datetime

import httplib2
import simplejson

FORM_TEST_DATA = {
    'field1': 'testData1',
    'field2': 1234,
    'field3': str(datetime.now())
}

JSON_TEST_DATA = {
    'woggle': {
        'version': 1234,
        'updated': str(datetime.now()),
    }
}
URL = 'http://localhost:8080/postOc2Cmd'
CONTENT_TYPE_JSON = {'Content-Type': 'application/json'}
CONTENT_TYPE_FORM = {'Content-Type': 'application/x-www-form-urlencoded'}


def test_post(json_data: any, content_type: any):
    h = httplib2.Http()
    resp, content = h.request(URL, 'POST', json_data, headers=content_type)
    print(resp)
    print(content)


if __name__ == "__main__":
    converted_json_data = simplejson.dumps(JSON_TEST_DATA)
    test_post(converted_json_data, CONTENT_TYPE_JSON)

    converted_form_data = simplejson.dumps(FORM_TEST_DATA)
    test_post(converted_form_data, CONTENT_TYPE_FORM)


