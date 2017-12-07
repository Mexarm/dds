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

#@auth.requires_signature()
@auth.requires_login()
def storage_proxy():
    """ upload files to cloudfiles
        works with CKEditor uploadimage plugin
    returns the url of the uploaded image.
    for testing curl -i -F upload=@HZD.png http://127.0.0.1:8000/storage_proxy/<campaign_tag>
    """
    if myconf.get('auth.secure'):
        request.requires_https()
    if len(request.args) == 0: raise HTTP(404)
    campaign_tag = request.args[0]
    upload = 'upload' #name of the upload field
    vars_ = request.vars
    response.view = 'default/webhook.json'
    imgfilename = vars_[upload].filename
    filename = '{}/{}.{}'.format(campaign_tag, uuid.uuid4(), imgfilename.split('.')[1])
    #this saves to local server
    #copyfileobj(vars_[upload].file,
    #            open(path.join(request.folder, 'static', 'images', filename), 'wb'))
    container = cf_create_container(myconf.get('cloudfiles.image_storage_container'))
    if not container.cdn_enabled:
        container.make_public()
    objs = container.get_objects(prefix="{}/".format(campaign_tag))
    existing_object = cf_get_object_with_metadata_key_value(objs,
                                                            "X_Object_Meta_Imgfilename",
                                                            imgfilename)
    if existing_object:
        return dict(uploaded=1, filename=existing_object.name,
                    url=cf_get_CDN_url(existing_object, ssl=False),
                    error = dict(message="{}, was previusly uploaded, you are getting the existing image url".format(imgfilename)))
    obj = container.store_object(filename, vars_[upload].file)
    obj.set_metadata({"imgfilename": imgfilename})
    cdn_url = cf_get_CDN_url(obj, ssl=False) #http://
    #when saved to local server :
    #return dict(uploaded=1, fileName=filename, url=URL('static', 'images/{}'.format(filename)))
    return dict(uploaded=1, fileName=filename, url=cdn_url)

@auth.requires_login()
def ckeditor():
    campaign = get_campaign(int(request.args[0]))
    campaign_tag = get_campaign_tag(campaign)
    doc = db(db.doc.campaign == campaign.id).select(limitby=(0, 1)).first()
    fields = []
    if doc:
        rc = get_rcode(doc.rcode, doc.campaign)
        context = get_context(doc, campaign, rc)
        fields = get_context_fields(context)
    for f in db.campaign.fields:
        if not f in ['html_body', 'from_name', 'from_address' ,'email_subject']:
            db.campaign[f].readable = False
            db.campaign[f].writable = False
    form=SQLFORM(db.campaign, campaign, upload=URL('download'))
    if form.process().accepted:
        response.flash ='Guardado'
       # redirect(URL('list_campaign'))
    elif form.errors:
        response.flash='Errores'
    return dict(form=form, campaign_tag=campaign_tag, fields=json.dumps(fields), 
                format = '<span class="cke_placeholder">[[%placeholder%]]</span>')
