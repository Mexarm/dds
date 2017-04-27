# -*- coding: utf-8 -*-
import requests
import time
import datetime
import json
from shutil import copyfileobj
import sys
import inspect
#import traceback
import urllib2
import hashlib

import pyrax
import pyrax.exceptions as exc
import uuid
import html2text   #sudo pip install html2text

from gluon.storage import Storage
from gluon.fileutils import abspath
from gluon.template import render
from os import path,mkdir,system,remove
from email.utils import formatdate

URL_KEY = myconf.get('dds.url_key')
# -------------mailgun--------------

def mg_get_domains():
    return requests.get(
        "https://api.mailgun.net/v3/domains",
        auth=("api", myconf.get('mailgun.api_key'))
        )

def mg_get_campaigns(domain):
    return requests.get(
        "https://api.mailgun.net/v3/{}/campaigns".format(domain),
        auth=('api', myconf.get('mailgun.api_key')))

def mg_update_local_campaigns_stats():
    for d in db().select(db.campaign.mg_domain, distinct=True): #distincts domains in campaigns
        now = datetime.datetime.now()
        for i in mg_get_campaigns(d.mg_domain).json()['items']:
            i.update(dict(stats_timestamp = now))
            db(db.campaign.mg_campaign_id == i['id']).update(mg_stats=i)
     #db.commit() the calling function should commit

def mg_update_local_campaign_stats(campaign_id): #update a campaign with the information retrieved from mailgun
    c = get_campaign(campaign_id)
    r1 = requests.get(
        "https://api.mailgun.net/v3/{}/campaigns/{}/stats".format(c.mg_domain,c.mg_campaign_id),
        auth=('api', myconf.get('mailgun.api_key')))
    r2 = requests.get(
        "https://api.mailgun.net/v3/{}/campaigns/{}".format(c.mg_domain,c.mg_campaign_id), # juntar la info de ambos o ponerla en 2 campos diferentes
        auth=('api', myconf.get('mailgun.api_key')))
    c.mg_stats=r2.json()
    c.mg_stats_unique=r1.json()
    c.update_record()
    db.commit()
        #r1.json() = {u'unique': {u'clicked': {u'recipient': 2, u'link': 4}, u'opened': {u'recipient': 2}}, u'total': {u'complained': 0, u'delivered': 6, u'clicked': 9, u'opened': 14, u'dropped': 0, u'bounced': 0, u'sent': 6, u'unsubscribed': 0}}
        #r2.json() = {u'unsubscribed_count': 0, u'name': u'dds_demo1', u'created_at': u'Wed, 21 Dec 2016 23:59:35 GMT', u'clicked_count': 9, u'opened_count': 14, u'submitted_count': 6, u'delivered_count': 6, u'bounced_count': 0, u'complained_count': 0, u'id': u'xka6g', u'dropped_count': 0}

def get_events(domain, params):
    return requests.get(
        "https://api.mailgun.net/v3/{}/events".format(domain),
        auth=('api', myconf.get('mailgun.api_key')),
        params=params)

def get_events_page(url):
    return requests.get(
            url,
            auth=('api', myconf.get('mailgun.api_key')))

#--------------utilerias---------
def get_container_name(uri):
    return uri.split('/')[0] if '/' in uri else uri

def get_prefix(uri):
    return uri.split('/')[1] if '/' in uri else ''

def split_uri(uri):
    return (get_container_name(uri),get_prefix(uri))

def get_credentials_storage():
    return Storage({'username': myconf.get('rackspace.username'), 'api_key':myconf.get('rackspace.api_key') , 'region': myconf.get('rackspace.region') })

def active_domains_list(res): #takes the mail gun request.response object and built a list of domains.
    l=list()
    res_dict=res.json()
    for d in res_dict['items']:
        if d['state']=='active' and d['type'] != 'sandbox':
            l.append(d['name'])
    return l

def campaigns_list(res): #takes the mail gun request.response object and built a list of domains.
    l=list()
    res_dict=res.json()
    for d in res_dict['items']:
        l.append(d['name'])
    return l

def get_mg_campaign(res,name):
    if 'items' in res.json():
        for c in res.json()['items']:
            if c['name'] == name:
                return c

def get_latest_dt(dt1,dt2):
    if (not dt1 and not dt2): return
    if not dt1: return dt2
    if not dt2: return dt1
    if dt1>dt2: return dt1
    return dt2

def store_mg_event(event_dict): #store an event returned by mailgun example: event_dict = response.json()['items'][0]
    r=db(db.mg_event.event_id == event_dict['id']).select()
    if r: return
    e=Storage(event_dict)
    doc=db(db.doc.mailgun_id ==e['message']['headers']['message-id']).select(limitby=(0,1)).first()
    if not doc: return
    mg_event=dict()
    mg_event['doc']=doc.id
    mg_event['campaign']=doc.campaign
    mg_event['event_id']=e.id
    struct_time=time.gmtime(e.timestamp)
    dt=datetime.datetime.fromtimestamp(time.mktime(struct_time))
    mg_event['event_timestamp_dt']=dt
    mg_event['event_timestamp']=e.timestamp
    mg_event['event_ip']=e.ip
    mg_event['event_']=e.event
    mg_event['event_log_level']=e['log-level']
    mg_event['event_recipient']=e.recipient
    mg_event['event_campaigns']=e.campaigns
    mg_event['event_tags']=e.tags
    mg_event['event_client_type']=e['client-info']['client-type'] if e['client-info'] else None
    mg_event['event_client_os']=e['client-info']['client-os'] if e['client-info'] else None
    mg_event['event_client_device_type']=e['client-info']['device-type'] if e['client-info'] else None
    mg_event['event_client_name']=e['client-info']['client-name'] if e['client-info'] else None
    mg_event['event_client_user_agent']=e['client-info']['user-agent'] if e['client-info'] else None
    mg_event['event_geolocation_country']=e.geolocation['country'] if e.geolocation else None
    mg_event['event_geolocation_region']=e.geolocation['region'] if e.geolocation else None
    mg_event['event_geolocation_city']=e.geolocation['city'] if e.geolocation else None
    mg_event['event_json']=event_dict
    r=db.mg_event.insert(**mg_event)
    if e.event in [ 'accepted', 'rejected', 'delivered', 'failed', 'opened', 'clicked', 'unsubscribed', 'complained', 'stored' ]:
        field=e.event + '_on'
        doc[field]=get_latest_dt(dt,doc[field])
        doc.update_record()
    db.commit()
    return r


def store_mg_events(events_dict):
    for e in events_dict['items']:
        store_mg_event(e)
# to-do:get the next page

def retrieve_events_for_doc(doc_id):
    doc=get_doc(doc_id)
    params=dict()
    params['message-id']=doc.mailgun_id
    res=get_events(doc.campaign.mg_domain,params)
    if res.status_code == 200:
        store_mg_events(res.json())

def daemon_retrieve_events_for_campaigns():
    l=['approved','queueing','live','finished']
    for c in db(db.campaign.status.belongs(l)).select():
        for d in db((db.doc.campaign==c.id) & (db.doc.mailgun_id != '')).select():
            retrieve_events_for_doc(d.id)

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def verify_checksum(cs,filename):
    if cs != md5(filename):
        remove(filename)
        raise ValueError('checksum error for file {}'.format(filename))

def daemon_reclaim_attach_storage(): # looks in the attach_temp dir to reclaim storage
    import os
    import shutil
    attach_temp = path.join(request.folder , 'attach_temp')
    for c_uuid in os.listdir(attach_temp):
        c=get_campaign_by_uuid(c_uuid)
        rmtree=True
        if c:
            if c.status == 'queuing':
                reclaim_attach_storage_campaign(c_uuid)
                rmtree=False
        if rmtree: shutil.rmtree(path.join(attach_temp),c_uuid)

def reclaim_attach_storage_campaign(c_uuid):
    import os
    attach_temp = path.join(request.folder , 'attach_temp')
    c_folder = path.join(attach_temp,c_uuid)
    c=get_campaign_by_uuid(c_uuid)
    for f in os.listdir(cfolder):
        row = db((db.doc.campaign == c.id) & (db.doc.object_name == f) & (db.doc.status =='cf validated') ).select(limitby=(0,1)).first()
        if not row:
            remove(path.join(c_folder,f))

#--------------------------rackspace cloudfiles ------------------
def container_object_count_total_bytes(container_name,credentials):
    """
    credentials is a Storage Object with attributes username, api_key, region
    """
    import pyrax
    import pyrax.exceptions as exc
    import pyrax.utils as utils
    pyrax.set_setting("identity_type", "rackspace")
    pyrax.set_default_region(credentials.region or get_region_id(rackspace_regions[0]))
    try:
        pyrax.set_credentials(credentials.username, credentials.api_key)
    except exc.AuthenticationFailed as e:
        return e
    if pyrax.identity.authenticated:
        cf=pyrax.cloudfiles
        try:
            cont=cf.get_container(container_name)
            object_count=cont.object_count
            total_bytes=cont.total_bytes
        except exc.NoSuchContainer as e:
            return e
        return (object_count,total_bytes)

def exist_object(container_name,object_name,credentials):
    """
    object_name with prefix example folder/example.txt
    credentials is a Storage Object with attributes username, api_key, region
    """
    import pyrax
    import pyrax.exceptions as exc
    import pyrax.utils as utils
    pyrax.set_setting("identity_type", "rackspace")
    pyrax.set_default_region(credentials.region or get_region_id(rackspace_regions[0]))
    try:
        pyrax.set_credentials(credentials.username, credentials.api_key)
    except exc.AuthenticationFailed as e:
        return e
    if pyrax.identity.authenticated:
        cf=pyrax.cloudfiles
        try:
            obj=cf.get_object(container_name,object_name)
        except exc.NoSuchContainer as e:
            return e
        except exc.NoSuchObject as e:
            return e
        return True

def download_object(container_name,object_name,savepath,credentials):
    """
    object_name with prefix example folder/example.txt
    credentials is a Storage Object with attributes username, api_key, region
    save is the absolute path to save the object
    """
    if not path.isabs(savepath): return
    import pyrax
    import pyrax.exceptions as exc
    import pyrax.utils as utils

    chunk_size = 256 * 1024 #256kB

    pyrax.set_setting("identity_type", "rackspace")
    pyrax.set_default_region(credentials.region or get_region_id(rackspace_regions[0]))
    try:
        pyrax.set_credentials(credentials.username, credentials.api_key)
    except exc.AuthenticationFailed as e:
        return e
    if pyrax.identity.authenticated:
        cf=pyrax.cloudfiles
        try:
            obj=cf.get_object(container_name,object_name)
        except exc.NoSuchContainer as e:
            return e
        except exc.NoSuchObject as e:
            return e
        filename=path.join(savepath,utils.to_slug(container_name ) +'_'+ utils.to_slug(obj.name.replace('/','_')))  #filename = /savepath/container_name_folder_example.txt
        data_chunks = obj.fetch(chunk_size=chunk_size)
        with open(filename,'wb') as handle:
            for chunk in data_chunks:
                handle.write(chunk)
        verify_checksum(obj.etag,filename)
        return filename

def prepare_subfolder(subfolder):
    pth=path.join(abspath(request.folder),subfolder)
    if not path.isdir(pth):
        mkdir(pth)
    return pth

def download_file(url,filename):
        res=urllib2.urlopen(url)
        f=open(filename,'wb')
        f.write(res.read())
        f.close()

def save_attachment(doc,campaign,rcode):
    pth=prepare_subfolder('attach_temp/')
    pth=prepare_subfolder('attach_temp/{}'.format(campaign.uuid))
    fullname = path.join(pth, doc.object_name)
    if not path.isfile(fullname):
        download_file(rcode.temp_url,fullname)
    verify_checksum(doc.checksum,fullname)
    return fullname

def register_on_db(campaign_id,update=True):
    #Implementar index ddel archivo original para verificar si no se ha subido ya y soportar retries de esta funcion ---------------
    import pyrax.utils as utils
    from gluon.fileutils import abspath

    sep=',' # ------ support diferent separators--------------

                                                                      # Download file
    pth=prepare_subfolder('index_files/')
    campaign = db(db.campaign.id == campaign_id).select().first()
    credentials=get_credentials_storage()
    container,prefix=split_uri(campaign.cf_container_folder)
    index_file=campaign.index_file
    object_name=path.join(prefix,index_file)
    #print '!clear!Downloading {}/{}...'.format(container,object_name)
    dld_file=download_object(container,object_name,pth,credentials)
    ok=0
    errors=0
    messages = list()
    with open(dld_file,'r') as handle:                                                            # check UNICODE SUPPORT!!!
        hdr=handle.next() # read header (first line) strip \n
        hdr_list=[ f.strip('"').strip().lower() for f in hdr.strip('\n').strip('\r').split(sep)]# make a list of field names
        if not set(REQUIRED_FIELDS) < set(hdr_list):
            #aqui cambiar estado a error "documents error"
            raise ValueError('required fields "{}" are not present in file {}/{}'.format(','.join(REQUIRED_FIELDS),
                                                                                  container,object_name))
        db.doc.campaign.default=campaign_id
        n=0
        for line in handle: # enumeration needed? optimize for millions records
            values = [v.strip('"') for v in line.strip('\n').strip('\r').split(sep)]
            row=Storage(make_doc_row(dict(zip(hdr_list, values))))   ## USE TRY EXCEPT TO CATCH ERRORS IN RECORDS (FEWER OR MORE FIELDS)!!
            q=(db.doc.record_id==row.record_id) & (db.doc.campaign==campaign_id)
            doc=db(q).select(limitby=(0,1)).first()

            if doc:
                if update:
                    ret = db(q).validate_and_update(**row)
                    valid = ret.updated >0

            else:
                ret = db.doc.validate_and_insert(**row) #field values not defined in row should have a default value defined defined in the model
                valid=ret.id >0
            #ret.updated ----------------------------------------------------------------------------
            if not valid:
                messages += [ 'could not insert or update in table "doc": {}'.format(str(row))  ]
                errors+=1
            else:
                ok+=1
            #print '!clear!{} registros leidos (ok={},err={})'.format(n+1,ok,errors)
            n+=1
            db.commit() #commit each row to avoid lock of the db

    remove(dld_file)
    #db.commit() #maybe commit each row to avoid lock of the db

    #ret = scheduler.queue_task(validate_files2,pvars=dict(campaign_id=campaign_id),timeout=15 * ok, sync_output=60 ) # timeout = 15secs per record

    ret = scheduler.queue_task(create_validate_docs_tasks,pvars=dict(campaign_id=campaign_id),timeout=15 * ok) # timeout = 15secs per record

    tasks = db.campaign(campaign_id).tasks
    tasks =  tasks + [ret.id] if tasks else [ret.id]
    db(db.campaign.id==campaign_id).update(tasks=tasks, total_campaign_recipients=n)
    db.commit()
    return dict(ok=ok,errors=errors,messages=messages)

def register_on_dbok(campaign_id,update=True):  #-------------- PENDIENTE TERMINAR
    #Implementar index ddel archivo original para verificar si no se ha subido ya y soportar retries de esta funcion ---------------
    import pyrax.utils as utils
    from gluon.fileutils import abspath

    sep=',' # ------ support diferent separators--------------

                                                                      # Download file
    pth=prepare_subfolder('index_files/')
    campaign = db(db.campaign.id == campaign_id).select().first()
    credentials=get_credentials_storage()
    container,prefix=split_uri(campaign.cf_container_folder)
    index_file=campaign.index_file
    object_name=path.join(prefix,index_file)
    #print '!clear!Downloading {}/{}...'.format(container,object_name)
    dld_file=download_object(container,object_name,pth,credentials)
    ok=0
    errors=0
    messages = list()
    with open(dld_file,'r') as handle:                                                            # check UNICODE SUPPORT!!!
        hdr=handle.next() # read header (first line) strip \n
        hdr_list=[ f.strip('"').strip().lower() for f in hdr.strip('\n').strip('\r').split(sep)]# make a list of field names
        if not set(REQUIRED_FIELDS) < set(hdr_list):
            #aqui cambiar estado a error "documents error"
            raise ValueError('required fields "{}" are not present in file {}/{}'.format(','.join(REQUIRED_FIELDS),
                                                                                  container,object_name))
        db.doc.campaign.default=campaign_id
        n=0
        for line in handle: # enumeration needed? optimize for millions records
            values = [v.strip('"') for v in line.strip('\n').strip('\r').split(sep)]
            row=Storage(make_doc_row(dict(zip(hdr_list, values))))   ## USE TRY EXCEPT TO CATCH ERRORS IN RECORDS (FEWER OR MORE FIELDS)!!
            q=(db.doc.record_id==row.record_id) & (db.doc.campaign==campaign_id)
            doc=db(q).select(limitby=(0,1)).first()

            if doc:
                if update:
                    ret = db(q).validate_and_update(**row)
                    valid = ret.updated >0

            else:
                ret = db.doc.validate_and_insert(**row) #field values not defined in row should have a default value defined defined in the model
                valid=ret.id >0
            #ret.updated ----------------------------------------------------------------------------
            if not valid:
                messages += [ 'could not insert or update in table "doc": {}'.format(str(row))  ]
                errors+=1
            else:
                ok+=1
            #print '!clear!{} registros leidos (ok={},err={})'.format(n+1,ok,errors)
            n+=1
            db.commit() #commit each row to avoid lock of the db

    remove(dld_file)
    #db.commit() #maybe commit each row to avoid lock of the db

    #ret = scheduler.queue_task(validate_files2,pvars=dict(campaign_id=campaign_id),timeout=15 * ok, sync_output=60 ) # timeout = 15secs per record

    ret = scheduler.queue_task(create_validate_docs_tasks,pvars=dict(campaign_id=campaign_id),timeout=15 * ok) # timeout = 15secs per record

    tasks = db.campaign(campaign_id).tasks
    tasks =  tasks + [ret.id] if tasks else [ret.id]
    db(db.campaign.id==campaign_id).update(tasks=tasks, total_campaign_recipients=n)
    db.commit()
    return dict(ok=ok,errors=errors,messages=messages)

def reset_campaign_progress(campaign_id):
    return db(db.campaign.id == campaign_id).update(status_progress = 0.0, current_task='')
#----------------
def create_validate_docs_tasks(campaign_id):   #this function creates a task for validate each document (replaces: validate_files2 -----------------------
    #create a task to check periodically if all the tasks for this campaign were completed and update de campaign accordingly-------------------------
    campaign = db.campaign(campaign_id)
    period = myconf.get('retry.period')
    retry_failed = myconf.get ('retry.retry_failed')
    timeout = myconf.get ('retry.rackspace_timeout')

    query = (db.doc.campaign == int(campaign_id)) & (db.doc.status == DOC_LOCAL_STATE_OK[0])
    dt=datetime.datetime.now()+datetime.timedelta(minutes=5)
    n=0
    for doc in db(query).iterselect():
        validation_task = scheduler.queue_task(cf_validate_doc,  #----------------------------
                                                   pvars=dict(doc_id=doc.id),
                                                   timeout = timeout,
                                                   period = period,
                                                   #start_time=dt,
                                                   #next_run_time= dt,
                                                   retry_failed = retry_failed
                                                  )
        db(db.doc.id==doc.id).update(validation_task=validation_task.id,
                                     status=DOC_LOCAL_STATE_OK[1])

        event_data(campaign=campaign_id,doc=doc.id,category='info',event_type=inspect.currentframe().f_code.co_name,
                   event_data = 'scheduled_task {} cf_validate_doc created'.format(validation_task))
        n+=1
        db.commit()
    return dict(result = '{} tasks created'.format(n))

def parse_datetime(s,dflt_format):
    #s is s string that represents a datetime#format example :01/12/017 09:15:00#%d/%m/%Y %H:%M:%S
    #if no format is specified the default format is used
    t = s.split('#')
    return datetime.datetime.strptime(t[0],t[1] if len(t)>1 else dflt_format)


def cf_validate_doc(doc_id):  #this function is scheduled by create_validate_docs_tasks

    doc = db(db.doc.id==doc_id).select(limitby=(0,1)).first()
    if doc.status != DOC_LOCAL_STATE_OK[1]: return 'doc.status should be {}'.format(DOC_LOCAL_STATE_OK[1])
    campaign= db(db.campaign.id == doc.campaign).select(limitby=(0,1)).first()
    event_data_id=None
    credentials=get_credentials_storage()
    container,prefix=split_uri(campaign.cf_container_folder)
    temp_url_key = myconf.get('rackspace.temp_url_key')   # optimize maybe this should be global variables -----------------------------------------------
    server = myconf.get('host.server')

    pyrax.set_setting("identity_type", "rackspace")
    pyrax.set_default_region(credentials.region)
    pyrax.set_credentials(credentials.username, credentials.api_key)

    if pyrax.identity.authenticated:
        cf=pyrax.cloudfiles

        curr_key = cf.get_temp_url_key()
        if not curr_key == temp_url_key: #throw an exception if not the same key??
            cf.set_temp_url_key(temp_url_key)
        event_type=inspect.currentframe().f_code.co_name #get this function name
        try:
            obj=cf.get_object(container,path.join(prefix,doc.object_name))
            if obj.bytes:
                seconds = (campaign.available_until - datetime.datetime.now()).total_seconds() #seconds from now to campaign.available_until 
                temp_url = obj.get_temp_url(seconds = seconds)
                rcode=uuid.uuid4()
                rc_id = db.retrieve_code.insert(campaign = campaign.id ,
                                             doc = doc.id,
                                             temp_url = temp_url,
                                             rcode =rcode )  #insert  retrieve_code

                dds_url = URL('secure',vars=dict( id = rc_id, rcode = rcode ),scheme='https', host=server,hmac_key=URL_KEY)

                db(db.retrieve_code.id == rc_id).update(dds_url=dds_url)

                    #task= scheduler.queue_task(queue_notification,pvars = dict(doc_id=row.id,retrieve_code_id=rc.id), #separate queue action
                    #                           start_time=campaign.mg_acceptance_time,
                    #                           next_run_time=campaign.mg_acceptance_time,
                    #                           timeout = notification_timeout,
                    #                           period = period,
                    #                           retry_failed = retry_failed)

                doc.status=DOC_LOCAL_STATE_OK[2]
                doc.deliverytime=parse_datetime(doc.json['deliverytime'],campaign.datetime_format) if 'deliverytime' in doc.json else None

                doc.bytes=obj.bytes
                doc.checksum=obj.etag
                event_data_id=event_data(campaign=campaign.id,doc=doc.id,category='info',
                        event_type=event_type,
                        event_data='{}/{} OK'.format(container,  path.join(prefix,doc.object_name)),
                        created_by_task =W2P_TASK.uuid) #event_data
            else:
                doc.status=DOC_LOCAL_STATE_ERR[0]
                event_data_id=event_data(campaign=campaign.id,doc=doc.id,category='error',
                        event_type=event_type, event_data='{}/{} ERROR: 0 BYTES'.format(container.name,  path.join(prefix,doc.object_name)))             #event_data
            doc.update_record()
        except (exc.NoSuchContainer,exc.NoSuchObject,ValueError)  as e:
            event_data_id=event_data(campaign=campaign.id,doc=doc.id,category='error',event_type=event_type, event_data=e.message)             #event_data
            doc.status=DOC_LOCAL_STATE_ERR[0]
            doc.update_record()
            db.commit()
            return 'error please see event_data id={}'.format(event_data_id)
        else:
            db.commit()
    return event_data_id

def event_data(**kwargs):
    # kwargs doc=<doc_id>, category = ..., event_type=... if campaign is not present it is calculated
    if not ('campaign' in kwargs):
        kwargs['campaign'] = get_doc(kwargs['doc']).campaign
    if 'W2P_TASK' in locals():
        kwargs['created_by_task']=W2P_TASK.uuid
    return db.event_data.insert(**kwargs)

def RFC_2822_section_3_3(dt):
    # dt is a datetime
    return formatdate(time.mktime(dt.timetuple())) #email.utils.formatdate

def make_doc_row(row):
    fields=dict()
    for f in REQUIRED_FIELDS:
        fields[f]=row[f]
    fields.update(dict(json=json.dumps(row)))
    return fields

def save_image(campaign_logo):

    (filename_, stream_) = db.campaign.logo.retrieve(campaign_logo)
    pth=path.join(abspath(request.folder),'logos/')
    if not path.isdir(pth):
        mkdir(pth)
    fullname = path.join(abspath(request.folder),'logos/',campaign_logo)
    copyfileobj(stream_, open(fullname, 'wb'))
    return fullname

def send_doc_wrapper(*args,**kwargs):
    #return errors about the rendering of the subject or view, if any
    sd_kwargs = { k : kwargs[k] for k in ['to','mg_campaign_id','ignore_delivery_time','testmode'] if k in kwargs}
    try:
        return process_mg_response(send_doc(*args,**sd_kwargs),*args,**kwargs)
    except (NameError,requests.exceptions.RequestException)  as e:
        event_data(doc=args[0],category='error',
                event_type='send_doc',
                event_data='error:{}'.format(e.message),
                event_json=kwargs)
        db.commit()
        if isinstance(e,requests.exceptions.RequestException):
            raise

def process_mg_response(*args,**kwargs):
    #    Mailgun returns standard HTTP response codes.

    #Code	Description
    #200	Everything worked as expected
    #400	Bad Request - Often missing a required parameter
    #401	Unauthorized - No valid API key provided
    #402	Request Failed - Parameters were valid but request failed
    #404	Not Found - The requested item doesn’t exist
    #500, 502, 503, 504	Server Errors - something is wrong on Mailgun’s end
    res=args[0]
    doc_id=args[1]

    doc=get_doc(doc_id) #response, doc_id
    category='error'
    if res.status_code == 200:
        doc.status=DOC_LOCAL_STATE_OK[4] if 'Queued' in res.json()['message'] else None
        category = 'info'
    else:
        doc.status=DOC_LOCAL_STATE_ERR[1]

    doc.mailgun_id=res.json()['id'].strip('<').strip('>') if 'id' in res.json() else None
    update_doc=True
    if 'update_doc' in kwargs:
        if not kwargs['update_doc']:
            update_doc=False
    if update_doc: doc.update_record()
    ed_id = event_data(doc=doc.id,category=category,
                event_type='send_doc',
                event_data='{}'.format(res.reason),
                event_json=res.json(),
                response_status_code=res.status_code)
    db.commit()
    if res.status_code in [500,502,503,504]:
        raise Exception('Mailgun returned status code = {}'.format(res.status_code))
    return ed_id

def get_context(doc,campaign,rc):
    #rc = retrieve code row
    url_type = { 'Attachment' : None , 'Cloudfiles Temp URL': 'temp_url', 'DDS Server URL': 'dds_url'}[campaign.service_type]
    data = dict(record_id = doc.record_id,
            object_name = doc.object_name,
            email_address = doc.email_address
            )
    if url_type: data['url']=rc[url_type]
    data.update(doc.json)
    campaign_dict = dict( domain = campaign.mg_domain,
            uuid = campaign.uuid,
            mg_id = campaign.mg_campaign_id,
            mg_name = campaign.mg_campaign_name,
            logo = IMG(_src='cid:{}'.format(campaign.logo),_alt='logo'),
            available_from = campaign.available_from,
            available_until = campaign.available_until,
            mg_tags = campaign.mg_tags,
            subject = campaign.email_subject)
    return dict(data=Storage(data),campaign=Storage(campaign_dict))

def send_doc(doc_id,to=None,mg_campaign_id=None,ignore_delivery_time=False,test_mode=False):
    doc = get_doc(doc_id)
    campaign = get_campaign(doc.campaign)
    rc = get_rcode(doc.id,doc.campaign)
    logofile = path.join(abspath(request.folder),'logos/',campaign.logo)
    if not path.isfile(logofile):
        save_image(campaign.logo)
    context=get_context(doc,campaign,rc)
    html_body = render(campaign.html_body,context=context)
    data={'from':'{} <{}>'.format(campaign.from_name,campaign.from_address) if campaign.from_name else campaign.from_address,
          'to':to or doc.email_address,
          'subject':render(campaign.email_subject,context=context),
          'html':html_body,
          'text':html2text.html2text(html_body.decode('utf-8')),
          'o:campaign':mg_campaign_id or campaign.mg_campaign_id}
    if not ignore_delivery_time:
        data['o:deliverytime']=RFC_2822_section_3_3(doc.deliverytime or campaign.available_from)
    if campaign.mg_tags:
        data['o:tag']=campaign.mg_tags[0:3] #maximum 3 tags per message
    if test_mode or campaign.test_mode:
        data['o:testmode']='true'
    #v:myvar
    files=[("inline",open(logofile))]
    if campaign.service_type == 'Attachment':
        files.append( ('attachment', (doc.object_name, open(save_attachment(doc,campaign,rc),'rb').read())))
    return mg_send_message(campaign.mg_domain,  myconf.get('mailgun.api_key'),
            files=files,
            data=data)

def mg_send_message(domain,api_key,**kwargs):
    return requests.post(
        "https://api.mailgun.net/v3/{}/messages".format(domain),
        auth=("api", api_key),
        **kwargs)

def get_rcode(doc_id,campaign_id):
    return db((db.retrieve_code.doc == doc_id) & (db.retrieve_code.campaign == campaign_id)).select(limitby=(0,1),
            orderby=~db.retrieve_code.id).first()

def get_campaign(campaign_id):
    return db(db.campaign.id==campaign_id).select(limitby=(0,1)).first()

def get_campaign_by_uuid(campaign_uuid):
    return db(db.campaign.uuid==campaign_uuid).select(limitby=(0,1)).first()

def get_doc(doc_id):
    return db(db.doc.id==doc_id).select(limitby=(0,1)).first()
#-------------------------------------------------------------------------------------------------------------------

def validate_campaign(form):
    import pyrax.exceptions as exc
    from dateutil.relativedelta import relativedelta
    container_name = get_container_name(form.vars.cf_container_folder)
    object_name=path.join(get_prefix(form.vars.cf_container_folder),form.vars.index_file)
    r = exist_object(container_name,object_name,get_credentials_storage())
    #form.vars.mg_campaign_id = get_mg_campaign(mg_get_campaigns(form.vars.mg_domain or session.mg_domain),form.vars.mg_campaign_name)['id'] #retrieve mg_campaign_id from mailgun
    if not form.vars.available_until:  #default + 1 año
        form.vars.available_until=form.vars.available_from + relativedelta(years=1)
    else:
        if form.vars.available_until < datetime.datetime.now():
            form.errors.available_until = 'available until should be a future date'
        if form.vars.available_from >= form.vars.available_until:
            form.errors.available_from = 'this date should be before than "available until"'
            form.errors.available_until = 'this date should be later than "available from"'

    if r:
        if isinstance(r,exc.AuthenticationFailed):
            form.errors.cf_container_folder = r.message
        if isinstance(r,exc.NoSuchContainer):
            form.errors.cf_container_folder = r.message
        if isinstance(r,exc.NoSuchObject):
            form.errors.index_file = r.message

def validate_dates_cid(form):
    available_until =  get_campaign(form.id).available_until
    if form.vars.available_from >= available_until:
        form.errors.available_from = 'this date should be before "available until"'

# BEGIN Progress tracking and status changer ------------------------------------------------------------------------------------------
def do_function_on_records(query,f):
    rows=db(query).select()
    for row in rows:
        f(row.status)(row.id)
        db.commit()

def daemon_progress_tracking():
    do_function_on_records(db.campaign.status.belongs(FM_STATES_WITH_PROGRESS_TRACKING),do_progress_tracking_for)

def daemon_status_changer():
    do_function_on_records(db.campaign.status.belongs(FM_STATES_TO_UPDATE),do_change_status_for)


def do_progress_tracking_for(campaign_status):
    """
        returns a function that reports the progress of the campaign according to its status
    """
    return {
        'validating documents':lambda campaign_id : validating_documents_progress(campaign_id),
        'queueing': lambda campaign_id: queueing_progress(campaign_id)
        }[campaign_status]

def do_change_status_for(campaign_status):
    return {
            'validating documents':lambda campaign_id :validating_documents_change_status(campaign_id),
            'queueing':lambda campaign_id : queueing_change_status(campaign_id),
            'scheduled':lambda campaign_id : sheduled_change_status(campaign_id),
            'live':lambda campaign_id : live_change_status(campaign_id)
            }[campaign_status]


def validating_documents_progress(campaign_id):
    campaign=get_campaign(campaign_id)
    progress1,progress2 = (0,0)
    for tid in campaign.tasks:
        task_status= scheduler.task_status(tid,output=True)
        if task_status.scheduler_task.function_name == 'register_on_db':
            if task_status.scheduler_task.status in ['RUNNING', 'COMPLETED'] :
                inserted_docs =  db(db.doc.campaign == campaign_id).count()
                progress1 = (inserted_docs / float(campaign.total_campaign_recipients or (campaign.container_objects-1) )) * 50 #inserting on docs table is the 50% of the validate docs process

        if task_status.scheduler_task.function_name == 'create_validate_docs_tasks':
            if task_status.scheduler_task.status in ['RUNNING', 'COMPLETED'] :
                validated_docs =  db((db.event_data.campaign == campaign_id) & (db.event_data.event_type == 'cf_validate_doc')).count()
                progress2 = (validated_docs / float(campaign.total_campaign_recipients) ) * 50 #validate docs is the 50% of the validate docs process

    campaign.status_progress = progress1+progress2
    campaign.update_record()
    db.commit()

def queueing_progress(campaign_id):
    c=get_campaign(campaign_id)
    count = db((db.doc.campaign==c.id) &
            (db.doc.status.belongs([DOC_LOCAL_STATE_OK[4],DOC_LOCAL_STATE_ERR[1]]))).count()
    c.status_progress= (float(count) / c.total_campaign_recipients) * 100.0
    c.update_record()
    db.commit()

def validating_documents_change_status(campaign_id):

    campaign=get_campaign(campaign_id)
    if campaign.status_progress == 100.0 :
        cf_not_valid = db((db.doc.campaign == campaign) & (db.doc.status == DOC_LOCAL_STATE_ERR[0])).count() # DOC_LOCAL_STATE_ERR = [ 'cf not valid','rejected (mailgun)' ]
        #try:
        f = FM_process_event(campaign_id,'_not valid docs' if cf_not_valid else '_valid docs')
        if f:
            f()
        b_sum = db.doc.bytes.sum()
        campaign.total_campaign_bytes = db(db.doc.campaign==campaign_id).select(b_sum).first()[b_sum]
    else:
        register_task=None
        create_validate_task=None
        for t in campaign.tasks:
            task = scheduler.task_status(t)
            if task.function_name == 'register_on_db':
                register_task = task
                create_validate_task=None
            if task.function_name == 'create_validate_docs_tasks':
                create_validate_task = task
        fail1 = False
        fail2 = False
        if register_task:
            fail1= (register_task.status == 'FAILED')

        if create_validate_task:
            fail2 = (create_validate_task.status == 'FAILED')
        if fail1 or fail2:
            f = FM_process_event(campaign_id,'_not valid docs')
            if f:
                f()
    db.commit()
        #except exceptions.AutomatonException as e:
        #    return e.message

def queueing_change_status(campaign_id):
    c=get_campaign(campaign_id)
    if c.status_progress==100.0:
        action_='_go live' if c.available_from < datetime.datetime.now() else '_go scheduled'
        f = FM_process_event(campaign_id,action_)
        if f:
            f()
    db.commit()  #Check if this commit is necessary here

def update_send_tasks_stats(campign_id):
    c = get_campaign(campaign_id)
    q = (db.doc.campaign == campaign_id)
    failed_docs=db((db.doc.status==DOC_LOCAL_STATE_ERR[1]) & q).select()
    completed_docs=db((db.doc.status==DOC_LOCAL_STATE_OK[4]) & q).select()
    if failed_docs:
        send_retry_active=False
        for d in failed_docs:
            tstatus=scheduler.task_status(s.send_task)
            if tstatus.times_failed < tstatus.retry_failed:
                if not d.send_retry_active:
                    d.send_retry_active=True
                    d.update_record()
                    db.commit()
                send_retry_active=True
   ##pendiente continuar escribir en la campaign

def sheduled_change_status(campaign_id):
    campaign=get_campaign(campaign_id)
    if campaign.available_from < datetime.datetime.now():
        f = FM_process_event(campaign_id,'_go live')
        if f:
            f()
    db.commit()  #Check if this commit is necessary here
def live_change_status(campaign_id):
    campaign=get_campaign(campaign_id)
    if campaign.available_until < datetime.datetime.now():
        f = FM_process_event(campaign_id,'_finish')
        if f:
            f()
    db.commit()  #Check if this commit is necessary here
# END Progress tracking and status changer ------------------------------------------------------------------------------------------
def set_campaign_fields_writable(campaign_status):
    l0=['mg_campaign_name', 'test_mode', 'delete_documents_on_expire', 'download_limit',
            'maximum_bandwith', 'mg_tags','available_from', 'datetime_format']
    l1=['from_name', 'from_address', 'test_address', 'email_subject', 'html_body',
            'logo', 'logo_file']
    l2=[ 'cf_container_folder', 'index_file', 'service_type', 'available_until']
    wfields = { 'defined' : l0+l1+l2,
                'documents error': l0+l1+l2,
                'documents ready' :l0+l1,
                'in approval' :l0+l1,
                'approved':l0
              }.get(campaign_status,[])
    for f in db.campaign.fields:
        db.campaign[f].writable = True if f in wfields else False
