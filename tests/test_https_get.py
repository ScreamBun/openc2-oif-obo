import httplib2

URL = 'http://localhost:8080/getOc2Cmd'

if __name__ == "__main__":
    h = httplib2.Http()
    resp, content = h.request(URL, 'GET', None, headers={'Content-Type': 'application/json'})
    print(resp)
    print(content)
