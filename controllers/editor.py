# -*- coding: utf-8 -*-

@auth.requires_login()
def index():
    campaign=get_campaign(int(request.args[0]))
    save_uri = URL('save_bf',vars=dict( campaign_uuid  = campaign.uuid ),user_signature=True,scheme=True,host=True)
    return dict(uri=URL('auth_token'),save_uri=save_uri)

@auth.requires_login()
def auth_token():
    try:
        r = get_BF_token()
        r.raise_for_status()
    except RequestException (e):
        return e.message
    return response.json(r.json())

@auth.requires_signature()
def save_bf():
    #s = request.vars._signature
    #c = request.vars.campaign_uuid
    #return str(locals())
    #if not URL.verify(request, hmac_key=URL_KEY): raise HTTP(403) #403 Forbidden	The request was a legal request, but the server is refusing to respond to it
    campaign_uuid = request.vars.campaign_uuid
    content_json = request.vars.content_json
    content_html = request.vars.content_html
    r=db(db.campaign.uuid==campaign_uuid).update(html_body=content_html, BF_json=content_json)
    if r : return response.json(dict(message = 'saved'))

@auth.requires_login()
def ckeditor():
    campaign=get_campaign(int(request.args[0]))
    for f in db.campaign.fields:
        if f != 'html_body':
            db.campaign[f].readable = False
            db.campaign[f].writable = False
    form=SQLFORM(db.campaign,campaign,upload=URL('download'))
    if form.process().accepted:
        response.flash ='Guardado'
       # redirect(URL('list_campaign'))
    elif form.errors:
        response.flash='Errores'
    return dict(form=form)
