from datetime import datetime
from enum import Enum

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


class Urls(Enum):
    GET = 'http://localhost:8080/getOc2Cmd'
    POST = 'http://localhost:8080/postOc2Cmd'


class ContentType(Enum):
    JSON = {'Content-Type': 'application/json'}
    FORM = {'Content-Type': 'application/x-www-form-urlencoded'}


class RequestType(Enum):
    GET = "GET"
    POST = "POST"


def test_post(json_data: any, url: any, request_type: any, content_type: any):
    h = httplib2.Http()
    resp, content = h.request(url, request_type, json_data, headers=content_type)
    print(resp)
    print(content)


if __name__ == "__main__":
    test_post("", Urls.GET.value, RequestType.GET.value, ContentType.FORM.value)

    converted_json_data = simplejson.dumps(JSON_TEST_DATA)
    test_post(converted_json_data, Urls.POST.value, RequestType.POST.value, ContentType.JSON.value)

    converted_form_data = simplejson.dumps(FORM_TEST_DATA)
    test_post(converted_form_data, Urls.POST.value, RequestType.POST.value, ContentType.FORM.value)
