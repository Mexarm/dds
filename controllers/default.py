# -*- coding: utf-8 -*-
# this file is released under public domain and you can use without limitations

# -------------------------------------------------------------------------
# This is a sample controller
# - index is the default action of any application
# - user is required for authentication and authorization
# - download is for downloading files uploaded in the db (does streaming)
# -------------------------------------------------------------------------


def __schedule_daemon_tasks():
    for t in DAEMON_TASKS:
        __schedule_daemon_task(t)

def __schedule_daemon_task(task_name):
    tasks = db(db.scheduler_task.function_name == task_name).count()
    if not tasks:
        session.flash = scheduler.queue_task(task_name,
                pvars={},
                period = 30,
                repeats= 0
                )
    db.commit()

__schedule_daemon_tasks()

def index():
    """
    example action using the internationalization operator T and flash
    rendered by views/default/index.html or views/generic.html

    if you need a simple wiki simply replace the two lines below with:
    return auth.wiki()
    """
    #redirect(URL('list_campaign'))
    response.flash = T("Hello World")
    return dict(message=T('Welcome to web2py!'))

@auth.requires_login()
def process_event():
    from automaton import exceptions
    campaign_id=request.vars.campaign_id
    event=request.vars.event
    if not (campaign_id and event): return "Not valid variables"
    campaign = db.campaign(campaign_id)
    if auth.user.id == campaign.created_by:
        try:
            f = FM_process_event(campaign_id,event)
            if f:
                r= f()
            return "sucess!"
        except exceptions.AutomatonException as e:
            return e.message
    else:
         return "Only the owner "+ campaign.created_by +  " can process the event"


@auth.requires_login()
def get_html_body():
    campaign=get_campaign(request.args[0])
    return campaign.html_body

@auth.requires_login()
def edit_campaign():
    campaign=get_campaign(request.args[0])
    set_campaign_fields_writable(campaign.status)
    db.campaign.mg_campaign_name.requires=IS_IN_SET(campaigns_list(mg_get_campaigns(campaign.mg_domain)))
    form=SQLFORM(db.campaign,campaign,upload=URL('download'))
    tasks = [ scheduler.task_status(t,output=True) for t in campaign.tasks]
    doc = db(db.doc.campaign == campaign.id & db.doc.status.belongs(DOC_LOCAL_STATE_OK[2:])).select(limitby=(0,1)).first()
    #return str(doc)+str(campaign)
    context = get_context(doc,campaign,get_rcode(doc.id,doc.campaign)) if doc else None
    if form.process().accepted:
        session.flash ='Guardado'
        redirect(URL('list_campaign'))
    elif form.errors:
        response.flash='Errores'
    return dict(form=form,tasks=tasks,fm_history=campaign.fm_history,campaign=campaign,context=context)

@auth.requires_login()
def get_fm_state():
    campaign_id = request.vars.campaign_id
    return FM_get(campaign_id)[0].current_state


@auth.requires_login()
def select_mg_domain():
    domains = active_domains_list(mg_get_domains())
    dflt_domain = myconf.get('mailgun.default_domain')
    #zero = dftl_domain if dflt_domain  in domains else None
    form=FORM('mailgun domain:',
              SELECT(_name='mg_domain',_class='selectpicker',*domains ),
              INPUT(_type='submit',_label='OK'))
    if form.accepts(request,session):
        session.mg_domain = form.vars.mg_domain
        redirect(URL('create_campaign'))
    elif form.errors:
        response.flash = 'form has errors'
    else:
        response.flash = 'Seleccione Dominio'
    return dict(form=form)


@auth.requires_login()
def list_campaign():
    if len(request.args): page=int(request.args[0])
    else: page=0
    items_per_page=10
		# Notice that this code selects one more item than is needed, 20+1.
		# The extra element tells the view whether there is a next page.
    limitby=(page*items_per_page,(page+1)*items_per_page+1)
    rows = db(db.campaign.created_by==auth.user.id).select(db.campaign.ALL,
            limitby=limitby,orderby=[~db.campaign.modified_on,db.campaign.created_on])
    return dict(rows=rows,page=page,items_per_page=items_per_page,args=request.args)

@auth.requires_login()
def get_fm_buttons():
    campaign_id = int(request.vars.campaign_id)
    return get_fm_action_buttons(campaign_id)

@auth.requires_login()
def create_campaign():
    import types
    if not session.mg_domain:
        redirect(URL('select_mg_domain'))
    domain = session.mg_domain or myconf.get('mailgun.domain')
    db.campaign.mg_campaign_name.requires=IS_IN_SET(campaigns_list(mg_get_campaigns(domain)))
    db.campaign.mg_campaign_id.readable=False
    db.campaign.is_active.readable=False
    db.campaign.status.readable = False
    db.campaign.total_campaign_recipients.readable = False
    db.campaign.total_campaign_bytes.readable = False
    db.campaign.container_objects.readable = False
    db.campaign.container_bytes.readable = False
    db.campaign.tasks.readable = False
    db.campaign.status_progress.readable = False
    db.campaign.current_task.readable = False
    db.campaign.fm_history.readable = False
    db.campaign.mg_domain.default= domain
    db.campaign.mg_domain.writable=False
    db.campaign.test_address.default = auth.user.email
    db.campaign.from_address.default = myconf.get('dds.default_from_address')
    try:
        html_body=open(path.join(request.folder,'views/body_template/default.html'),'r').read()
    except IOError:
        html_body='Please download your document {{=url}}'
    db.campaign.html_body.default = html_body
    form=SQLFORM(db.campaign)
    if form.process(onvalidation=validate_campaign).accepted:
        r=container_object_count_total_bytes(get_container_name(form.vars.cf_container_folder),get_credentials_storage())
        if isinstance(r, types.TupleType):
            count,bytes=r
        else:
            count,bytes=(0,0)
        db(db.campaign.id==form.vars.id).update(container_objects=count, container_bytes=bytes)
        session.flash = DIV('Campaign ID {} created'.format(form.vars.id))
        redirect(URL('list_campaign'))
    elif form.errors:
        response.flash = 'Please verify entered information'
    return dict(form=form)

@cache.action()
def secure():
    #https://github.com/mozilla/pdf.js/blob/master/README.md
    if not URL.verify(request, hmac_key=URL_KEY): raise HTTP(403) #403 Forbidden	The request was a legal request, but the server is refusing to respond to it
    id = request.vars.id
    rcode = request.vars.rcode
    r = db.retrieve_code(id)
    if r.rcode == rcode:
        from datetime import datetime
        if r.available_until > datetime.now():
            redirect(r.temp_url, client_side=True)
        else:
            raise HTTP(410) #410 Gone	The requested page is no longer available
    else:
        raise HTTP(403) #403 Forbidden	The request was a legal request, but the server is refusing to respond to it
    #redirect(..., client_side=True)

def gone():
    raise HTTP(410)

def user():
    """
    exposes:
    http://..../[app]/default/user/login
    http://..../[app]/default/user/logout
    http://..../[app]/default/user/register
    http://..../[app]/default/user/profile
    http://..../[app]/default/user/retrieve_password
    http://..../[app]/default/user/change_password
    http://..../[app]/default/user/bulk_register
    use @auth.requires_login()
        @auth.requires_membership('group name')
        @auth.requires_permission('read','table name',record_id)
    to decorate functions that need access control
    also notice there is http://..../[app]/appadmin/manage/auth to allow administrator to manage users
    """
    return dict(form=auth())


@cache.action()
def download():
    """
    allows downloading of uploaded files
    http://..../[app]/default/download/[filename]
    """
    return response.download(request, db)


def call():
    """
    exposes services. for example:
    http://..../[app]/default/call/jsonrpc
    decorate with @services.jsonrpc the functions to expose
    supports xml, json, xmlrpc, jsonrpc, amfrpc, rss, csv
    """
    return service()
