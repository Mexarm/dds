# -*- coding: utf-8 -*-

@auth.requires_login()
def index():
    campaign=get_campaign(int(request.args[0]))
    save_uri = URL('save_bf',vars=dict( campaign_uuid  = campaign.uuid ),scheme='https', hmac_key=URL_KEY)
    return dict(uri=URL('auth_token'),save_uri=save_uri)

@auth.requires_login()
def auth_token():
    try:
        r = get_BF_token()
        r.raise_for_status()
    except RequestException (e):
        return e.message
    return response.json(r.json())

@auth.requires_login()
def save_bf():
    if not URL.verify(request, hmac_key=URL_KEY): raise HTTP(403) #403 Forbidden	The request was a legal request, but the server is refusing to respond to it
    campaign_uuid = request.vars.campaign_uuid
    content = request.vars.content
    r=db(db.campaign.uuid==campaign_uuid).update(BF_json=content)
    if r : return response.json(dict(message = 'saved'))
