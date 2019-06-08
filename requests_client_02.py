import json
import requests
import re

prefix = 'http://localhost:5000'

def print_response(response, body=None):
    print('Request:')
    print('\tUrl: {}'.format(response.url))
    print('\tMethod'.format(re.search("<PreparedRequest \[(\w+)\]>",
                                      str(response.request)).group(1)))
    print('\tBody: {}'.format(body))

    print('Response:')
    print('\tCode: {}'.format(response.status_code))
    content = response.content.decode("utf-8")
    if len(content) > 200:
        content = content[:160] + "..." + content[-38:]
    print('\tContent: {}'.format(content), end="")
    print('\tHeaders: {}'.format(response.headers))
    print('-'*30)


def send_get(message, url):
    print(message)
    url = prefix+url
    response = requests.get(url)
    print_response(response)


def send_post(message, url, body=None):
    print(message)
    url = prefix+url
    response = requests.post(url,
                             data=body,
                             headers={"Content-Type":"application/json"})
    print_response(response, body)


def send_put(message, url, body=None):
    print(message)
    url = prefix+url
    if body is None:
        response = requests.put(url)
        print_response(response, body)
    else:
        response = requests.put(url,
                                data=body,
                                headers={"Content-Type":"application/json"})
        print_response(response, body)


def send_delete(message, url):
    print(message)
    url = prefix+url
    response = requests.delete(url)
    print_response(response)


if __name__ == '__main__':
    # GET document
    send_get('Document for user with ID = 75', '/user/document/75')
    send_get('Non existing user document', '/user/document/0')
    send_get('Document for movie with ID = 3', '/movie/document/3')

    # GET preselection
    send_get('Preselction for user 75', '/user/preselection/75')

    # PUT document

    send_put('Add new user document ID = 5555 that likes no movies',
             url='/user/document/5555',
             body='[]')
    send_get('Get user document of ID = 5555', '/user/document/5555')
    send_post('Update user 5555 with movies [1,2]',
              url='/user/document/5555',
              body='[1,2]')
    send_get('Get user document of ID = 5555', '/user/document/5555')
    send_post('Update user 5555 with movie 5',
              url='/user/document/5555',
              body='5')
    send_get('Get user document of ID = 5555', '/user/document/5555')
    send_get('Get movie document of ID = 5', '/movie/document/5')
    send_delete('Delete user of ID 5555', '/user/document/5555')
