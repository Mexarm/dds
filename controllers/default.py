# -*- coding: utf-8 -*-
# this file is released under public domain and you can use without limitations

# -------------------------------------------------------------------------
# This is a sample controller
# - index is the default action of any application
# - user is required for authentication and authorization
# - download is for downloading files uploaded in the db (does streaming)
# -------------------------------------------------------------------------



def index():
    """
    example action using the internationalization operator T and flash
    rendered by views/default/index.html or views/generic.html

    if you need a simple wiki simply replace the two lines below with:
    return auth.wiki()
    """
    #redirect(URL('list_campaign'))
    #response.flash = T("Hello World")
    if auth.user:
        redirect(URL('list_campaign'))
    return dict(message=T('Welcome to CDS!@{}'.format(myconf.get('host.server'))))

@auth.requires_login()
def process_event():
    from automaton import exceptions
    campaign_id=request.vars.campaign_id
    event = request.vars.event
    if not (campaign_id and event): return "Not valid variables"
    campaign = db.campaign(campaign_id)
    if auth.user.id == campaign.created_by:
        try:
            f = FM_process_event(campaign_id, event)
            if f:
                r = f()
            return "sucess!"
        except exceptions.AutomatonException as e:
            return e.message
    else:
        return "Only the owner "+ campaign.created_by +  " can process the event"


@auth.requires_login()
def get_html_body():
    campaign=get_campaign(request.args[0])
    if campaign:
        return campaign.html_body
    raise HTML(404)

@auth.requires_login()
def workers():
    from gluon.tools import prettydate
    sdb = db
    sw, st, sr = (sdb.scheduler_worker, sdb.scheduler_task,sdb.scheduler_run)
    workers = sdb.executesql("select substring_index(worker_name,'#',1) node,group_names,count(id) from scheduler_worker group by node,group_names order by node,group_names")
    latest_task_id=get_latest_task_id('task_evt_poll')
    latest_task=scheduler.task_status(latest_task_id) if  latest_task_id else None
    events_horizont_ts = json.loads(latest_task.args)[2] if latest_task else None
    events_horizont_dt = datetime.datetime.fromtimestamp(events_horizont_ts) if events_horizont_ts else None
    events_horizont_ts = json.loads(latest_task.args)[2] if latest_task else None
    events_horizont_dt = datetime.datetime.fromtimestamp(events_horizont_ts) if events_horizont_ts else None
    count = sdb(sdb.scheduler_worker.id>0).count()
    ticker,tasks,completed_tasks = (None,None,None)
    if auth.has_membership(role = 'advanced_scheduler_viewer'):
        ticker = sdb(sw.is_ticker == True).select(sw.worker_stats).first()
        tasks = sdb(st.status != 'COMPLETED').select(st.id,st.application_name,st.task_name,st.status,
                                            st.group_name,st.args,st.vars,st.start_time,
                                            st.times_run,st.times_failed,st.last_run_time,
                                            st.next_run_time,st.assigned_worker_name,
                                            limitby=(0,500),
                                            orderby=(st.status,~st.last_run_time))
        completed_tasks = sdb(st.status=='COMPLETED').select(st.id,st.application_name,st.task_name,st.status,
                                            st.group_name,st.args,st.vars,st.start_time,
                                            st.times_run,st.times_failed,st.last_run_time,
                                            st.next_run_time,st.assigned_worker_name,
                                            limitby=(0,50),
                                            orderby=(~st.last_run_time))
    return dict(workers=workers,ticker = ticker, count=count, tasks = tasks, completed_tasks = completed_tasks,
            events_horizont = dict (dt = events_horizont_dt, 
                                    ts = events_horizont_ts,
                                    pd = prettydate(events_horizont_dt)
                                   )
                )

def webhook():
    if myconf.get('auth.secure'):
        request.requires_https()
    v = request.vars
    t = cache.ram(v.token,lambda:exist_webhook_token(v.token),time_expire=15)
    if t: raise HTTP(400)
    api_key = myconf.get('mailgun.api_key')
    if verify_webhook(api_key,v.token,v.timestamp,v.signature):
        #logger.debug("verified {}".format(request._vars['message-id']))
        if 'message-id' in v:
            eid =  store_mg_event(adjust_webhook_vars(v))
            if eid: return dict(message = 'Stored', id = eid)
        return dict(message = 'Test OK')
    raise HTTP(400)

@auth.requires_login()
def edit_campaign():
    campaign_id=request.args[0]
    #mg_update_local_campaign_stats(campaign_id)
    campaign=get_campaign(campaign_id)
    set_campaign_fields_writable(campaign.status)
    #db.campaign.mg_campaign_name.requires=IS_IN_SET(campaigns_list(mg_get_campaigns(campaign.mg_domain)))
    db.campaign.uncompress_attachment.show_if = (db.campaign.service_type == 'Attachment')
    #db.campaign.html_body.readable=False
    #if not db.campaign.html_body.writable: db.campaign.html_body.readable=False
    db.campaign.BF_json.readable=False
    db.campaign.html_body.widget=advanced_editor
    form=SQLFORM(db.campaign,campaign,upload=URL('download'))
    tasks = [ scheduler.task_status(t,output=True) for t in campaign.tasks]
    doc = db((db.doc.campaign == campaign.id) & db.doc.status.belongs(DOC_LOCAL_STATE_OK[2:])).select(limitby=(0,1)).first()
    context = get_context(doc,campaign,get_rcode(doc.rcode,doc.campaign)) if doc else None
    form.id=campaign.id #pass the id of the campaign in the form to the onvalidation function
    onvalidation= validate_dates if form.vars.available_from else lambda x: None
    if form.process(onvalidation=onvalidation).accepted:
        session.flash ='Guardado'
        redirect(URL('list_campaign'))
    elif form.errors:
        response.flash='Errores'
    return dict(form=form,tasks=tasks,fm_history=campaign.fm_history,campaign=campaign,context=context,load_editor=(db.campaign.html_body.writable))

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
    return dict(rows=rows,page=page,items_per_page=items_per_page,args=request.args,FM_STATES_WITH_PROGRESS_TRACKING=FM_STATES_WITH_PROGRESS_TRACKING)

@auth.requires_login()
def list_docs():
    campaign_id=int(request.args[0])
    cview = request.args[1][:]
    campaign = get_campaign(campaign_id)
    fn=None
    selectable_states = ['defined','validating documents','documents ready','documents error','in approval','approved']
    if campaign.status in selectable_states:
        fn = {'set_as_sample':set_as_sample,'delete_records':delete_records, 'none':None}[cview]
    db.mg_event.is_webhook.readable=True
    constraints={'doc':db.doc.campaign==campaign_id}
    fields=[db.doc.record_id,db.doc.object_name,db.doc.email_address,
            db.doc.deliverytime,
            db.doc.status,db.doc.accepted_on,db.doc.delivered_on,
            db.doc.failed_on,db.doc.opened_on,db.doc.clicked_on,
            db.mg_event.event_timestamp_dt,db.mg_event.event_,db.mg_event.is_webhook,db.mg_event.event_ip,
            db.mg_event.event_log_level,db.mg_event.event_recipient,db.mg_event.event_tags,
            db.mg_event.event_geolocation_country]
    #links = [ (dict(header = 'fecha' , body = lambda row: dir(row)))]
    smartgrid=SQLFORM.smartgrid(db.doc,
            linked_tables=['mg_event'],
            constraints=constraints,
            args=request.args[:2],
            fields=fields,
            deletable=False,
            editable=False,
            create=False,
            selectable=fn,
            maxtextlength=35
            # , links=links
            )
    render_dropdown = False
    if smartgrid.rows and campaign.status in selectable_states:
        if 'doc.record_id' in smartgrid.rows.colnames:
            render_dropdown = True
            if fn:
                submit = smartgrid.element('.web2py_grid').element('.web2py_table').element('form').element('input',_type='submit')
                submit['_value'] = cview.replace('_',' ')
            
                if cview == 'delete_records':
                    submit["_onclick"] = "return confirm('Do you want to delete the selected records?');"
    return locals()

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
    #db.campaign.mg_campaign_name.requires=IS_IN_SET(campaigns_list(mg_get_campaigns(domain)))
    #db.campaign.mg_campaign_id.readable=False
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
    db.campaign.html_body.widget=advanced_editor
    db.campaign.uncompress_attachment.show_if = (db.campaign.service_type == 'Attachment')
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
