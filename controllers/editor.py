# -*- coding: utf-8 -*-

@auth.requires_login()
def index():
    return dict(uri=URL('auth'))

@auth.requires_login()
def auth():
    try:
        r = get_BF_token()
        r.raise_for_status()
    except RequestException (e):
        return e.message
    return response.json(r.json())
