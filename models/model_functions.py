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
from urlparse import urlparse,parse_qs
import hashlib, hmac

import pyrax
import pyrax.exceptions as exc
import uuid
import html2text   #sudo pip install html2text

from gluon.storage import Storage
from gluon.fileutils import abspath
from gluon.template import render
from os import path,mkdir,system,remove,walk,listdir
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

def get_events(domain, query_options):
    r =  requests.get(
        "https://api.mailgun.net/v3/{}/events".format(domain),
        auth=('api', myconf.get('mailgun.api_key')),
        params=query_options)
    r.raise_for_status()
    return r

def get_events_page(url):
    r = requests.get(
            url,
            auth=('api', myconf.get('mailgun.api_key')))
    r.raise_for_status()
    return r

def task_evt_poll(domain,begin_ts,end_ts):
    qopt= dict(begin= begin_ts,end=end_ts)
    store_mg_events(get_events(domain,qopt))

def daemon_master_event_poll():
    now_ts = time.time()
    max = db.scheduler_task.id.max()
    latest_task_id=db(db.scheduler_task.task_name== 'task_evt_poll').select(max).first()[max]
    latest_task=scheduler.task_status(latest_task_id) if  latest_task_id else None
    t2 = now_ts - EP_DELAY
    time_slice = t2 - json.loads(latest_task.args)[2] if latest_task else EP_TIME_SLICE
    domains = [ r['mg_domain'] for r in  db().select(db.campaign.mg_domain, distinct=True)] #distincts domains in campaigns
    tsk_t1 = t2 - time_slice
    while (tsk_t1+EP_TASK_TIME_SLICE) <= t2:
        tsk_t2=tsk_t1+EP_TASK_TIME_SLICE
        for d in domains:
            r=scheduler.queue_task(task_evt_poll,
                    pargs =[d, tsk_t1, tsk_t2],
                    period = EP_DAEMON_PERIOD,
                    repeats = EP_TASK_REPEATS,
                    retry_failed = -1,
                    timeout = EP_TASK_TIMEOUT,
                    group_name=WGRP_POLLERS)
            db.commit()
        tsk_t1 = tsk_t2

def daemon_event_poll_remove_old_tasks():
    from dateutil.relativedelta import relativedelta
    limit_dt = datetime.datetime.now() - relativedelta(days = int(myconf.get('eventpoll.delete_tasks_older_than')))
    db((db.scheduler_task.last_run_time < limit_dt) & (db.scheduler_task.task_name == 'task_evt_poll')).delete()
    db.commit()


def verify_webhook(api_key, token, timestamp, signature):
    hmac_digest = hmac.new(key=api_key,
                            msg='{}{}'.format(timestamp, token),
                            digestmod=hashlib.sha256).hexdigest()
    return hmac.compare_digest(unicode(signature), unicode(hmac_digest))

def get_BF_token():
    return requests.post('https://auth.getbee.io/apiauth',timeout=5,
            headers = { 'content-type' : 'application/x-www-form-urlencoded'
                        },
            data = { 'grant_type' : 'password',
                        'client_id' : BEEFREE_CLIENT_ID,
                        'client_secret' : BEEFREE_CLIENT_SECRET
                        })
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

def adjust_webhook_vars(req_vars):
    #req_vars is of type Storage
    v=req_vars
    e = dict()
    if v.country:
        e['geolocation'] = dict(country=v.country,region = v.region, city = v.city)
    if v.ip:
        e['ip'] = v.ip
    e['log-level'] =''
    if v.url:
        e['url'] = v.url
    if 'campaign-name' in v:
        e['campaigns'] = [ dict(name= v['campaign-name'], id = v['campaign-id'])]
    if 'client-name' in v:
        e['client-info'] = dict()
        e['client-info']['client-type']=v['client-type']
        e['client-info']['client-os']=v['client-os']
        e['client-info']['device-type']=v['device-type']
        e['client-info']['client-name']=v['client-name']
        e['client-info']['user-agent']=v['user-agent']
    e['tags']=  v.tag or v['X-Mailgun-Tag']
    if 'domain' in v:
        e['domain'] = v.domain
    e['event']=v.event
    e['timestamp']=float(v.timestamp)
    e['recipient']=v.recipient
    e['message'] = dict(headers = dict())
    e['message']['headers']['message-id']=v['message-id']
    if 'message-headers' in v:
        e['message-headers']=v['message-headers']
    if 'token' in v:
        e['token']=v.token
    return e

def exist_webhook_token(token):
    return db(db.mg_event.webhook_token == token).count()

def store_mg_event(event_dict): #store an event returned by mailgun example: event_dict = response.json()['items'][0]
    if 'id' in event_dict:
        r=db(db.mg_event.event_id == event_dict['id']).select(db.mg_event.id,limitby=(0,1)).first()
        if r: return
    e=Storage(event_dict)
    if not e['message']['headers']['message-id']: return
    doc=db(db.doc.mailgun_id ==e['message']['headers']['message-id']).select(limitby=(0,1)).first()
#http://bin.mailgun.net/62ea548b
    if not doc: return
    mg_event=dict()
    if e.id:
        mg_event['event_id']=e.id
    mg_event['is_webhook']=False if e.id else True
    mg_event['webhook_token']=e.token if 'token' in e else None
    mg_event['doc']=doc.id
    mg_event['campaign']=doc.campaign
#    struct_time=time.gmtime(e.timestamp)
#    dt=datetime.datetime.fromtimestamp(time.mktime(struct_time))
    dt=datetime.datetime.fromtimestamp(e.timestamp)
    mg_event['event_timestamp_dt']=dt
#    mg_event['event_local_dt']=dt
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

def store_mg_events(response):
    if response.status_code != 200: return
    rdict=response.json()
    if not rdict['items']: return
    for e in rdict['items']:
        store_mg_event(e)
    next_page=rdict['paging']['next']
    if next_page:
        store_mg_events(get_events_page(next_page))

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

def recursive_list_files(pth):
    files=list()
    for root, directories, filenames in walk(pth):
        for filename in filenames:
            files.append(path.join(root,filename))
    return files

def daemon_reclaim_attach_storage(): # looks in the attach_temp dir to reclaim storage
    import shutil
    attach_temp = path.join(request.folder , 'attach_temp')
    if not path.isdir(attach_temp):
        return
    for c_uuid in listdir(attach_temp):
        c=get_campaign_by_uuid(c_uuid)
        rmtree=True
        if c:
            if c.status in ['in approval','approved','queuing','live','scheduled']:
                reclaim_attach_storage_campaign(c_uuid)
                rmtree=False
        if rmtree: shutil.rmtree(path.join(attach_temp,c_uuid))

def reclaim_attach_storage_campaign(c_uuid):
    import shutil
    attach_temp = path.join(request.folder , 'attach_temp')
    c_folder = path.join(attach_temp,c_uuid)
    c=get_campaign_by_uuid(c_uuid)
    for f in [entry for entry in listdir(c_folder) if path.isfile(path.join(c_folder,entry))]:
        row = db((db.doc.campaign == c.id) & (db.doc.object_name == f) & (db.doc.status =='validated') ).select(limitby=(0,1)).first()
        if not row:
            pth=path.join(c_folder,f)
            remove(pth)
            if path.isdir(pth+'.unzip'):
                shutil.rmtree(pth+'.unzip')

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
        cf=pyrax.connect_to_cloudfiles(credentials.region)
        #cf=pyrax.cloudfiles
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
    pyrax.set_default_region(credentials.region)
    try:
        pyrax.set_credentials(credentials.username, credentials.api_key)
    except exc.AuthenticationFailed as e:
        return e
    if pyrax.identity.authenticated:
        cf=pyrax.connect_to_cloudfiles(credentials.region)
        #cf=pyrax.cloudfiles
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

    chunk_size = 512 * 1024 #512kB

    pyrax.set_setting("identity_type", "rackspace")
    pyrax.set_default_region(credentials.region)
    #try:
    pyrax.set_credentials(credentials.username, credentials.api_key)
    #except exc.AuthenticationFailed as e:
    #    return e
    if pyrax.identity.authenticated:
        cf=pyrax.connect_to_cloudfiles(credentials.region)
        #cf=pyrax.cloudfiles
        #try:
        obj=cf.get_object(container_name,object_name)
        #except exc.NoSuchContainer as e:
        #    return e
        #except exc.NoSuchObject as e:
        #    return e
        filename=path.join(savepath,utils.to_slug(container_name ) +'_'+ utils.to_slug(obj.name.replace('/','_')))  #filename = /savepath/container_name_folder_example.txt
        data_chunks = obj.fetch(chunk_size=chunk_size)
        with open(filename,'wb') as handle:
            for chunk in data_chunks:
                handle.write(chunk)
        verify_checksum(obj.etag,filename)
        return filename

def delete_files(campaign_id):
    campaign = get_campaign(campaign_id)
    credentials=get_credentials_storage()
    container,prefix=split_uri(campaign.cf_container_folder)
    pyrax.set_setting("identity_type", "rackspace")
    pyrax.set_default_region(credentials.region)
    pyrax.set_credentials(credentials.username, credentials.api_key)
    if pyrax.identity.authenticated:
        cf=pyrax.connect_to_cloudfiles(credentials.region)
    cont = cf.get_container(container)
    #docs = db((db.doc.campaign==campaign_id)&(db.doc.status=='queued (mailgun)')).select()
    q = (db.doc.campaign==campaign_id)
    docs = db(q).select(db.doc.object_name,groupby=db.doc.object_name)
    objs = list()
    result = list()
    for doc in docs:
        objs.append(path.join(prefix,doc.object_name))
        if (len(objs) % 1000) == 0:
            result.append(cf.bulk_delete(cont,objs,async=True))#,async=True
            objs = []
    if objs:
        result.append(cf.bulk_delete(cont,objs,async=True))#,async=True
    while True:
        completed=True
        for r in result:
            if not r.completed: completed = False
        if completed: break
        time.sleep(30)
    return dict(deleted = sum ( [ r.results['deleted'] for r in result ]), errors = [ r.results['errors'] for r in result ])
    # cont.get_object(path.join(prefix,doc.object_name)).delete()
    # Delete all the objects in the container and delete the container
    #cf.delete_container(container,del_objects=True)

def prepare_subfolder(subfolder):
    pth=path.join(abspath(request.folder),subfolder)
    if not path.isdir(pth):
        mkdir(pth)
    return pth

def download_file(url,filename):
        res=urllib2.urlopen(url.replace(' ','%20'))
        f=open(filename,'wb')
        f.write(res.read())
        f.close()

def unzip_file(filename):
    import zipfile
    zip_ref = zipfile.ZipFile(filename, 'r')
    unzip_path = prepare_subfolder(filename+'.unzip')
    zip_ref.extractall(unzip_path)
    zip_ref.close()
    return recursive_list_files(unzip_path)

def save_attachment(doc,campaign,rcode):
    pth=prepare_subfolder('attach_temp/')
    pth=prepare_subfolder('attach_temp/{}'.format(campaign.uuid))
    fullname = path.join(pth, doc.object_name)
    if not path.isfile(fullname):
        download_file(rcode.temp_url,fullname)
    verify_checksum(doc.checksum,fullname)
    if doc.object_name[-4:].lower() == '.zip' and campaign.uncompress_attachment:
        return unzip_file(fullname)
    return [fullname]

def register_on_db(campaign_id):
    import pyrax.utils as utils
    from gluon.fileutils import abspath
    import csv
    t1=time.time()
    sep=',' # ------ support diferent separators--------------
    quotechar='"'
    beg=time.time()
    pth=prepare_subfolder('index_files/')
    campaign = get_campaign(campaign_id)
    db.doc.status.default= 'validated' if campaign.service_type == 'Body Only' else 'initial'
    credentials=get_credentials_storage()
    container,prefix=split_uri(campaign.cf_container_folder)
    index_file=campaign.index_file
    object_name=path.join(prefix,index_file)
    dld_file=download_object(container,object_name,pth,credentials)
    ok=0
    errors=0
    messages = list()
    db(db.doc.campaign==campaign_id).delete()
    db(db.retrieve_code.campaign==campaign_id).delete()
    db.commit()
    with open(dld_file,'rb') as handle:                                                            # check UNICODE SUPPORT!!!
        csv_reader = csv.reader(handle,delimiter=sep,quotechar=quotechar) #
        #hdr=handle.next() # read header (first line) strip \n
        #hdr_list=[ f.strip('"').strip().lower() for f in hdr.strip('\n').strip('\r').split(sep)]# make a list of field names
        hdr_list=csv_reader.next() #
        if not set(REQUIRED_FIELDS) < set(hdr_list):
            raise ValueError('required fields "{}" are not present in file {}/{}'.format(','.join(REQUIRED_FIELDS)))
        db.doc.campaign.default=campaign_id
        n=0
        osequence = 0
        #for line in handle:
        for line in csv_reader:#
            osequence +=1
            #values = [v.strip('"') for v in line.strip('\n').strip('\r').split(sep)]
            values = line #
            rdict = make_doc_row(dict(zip(hdr_list, values)))
            rdict.update(dict(osequence=osequence))
            rd_json=json.loads(rdict['json'])
            if 'deliverytime' in rd_json:
                if rd_json['deliverytime']:
                    rdict.update(dict(deliverytime = parse_datetime(rd_json['deliverytime'],campaign.datetime_format)))
            row=Storage(rdict)
            ret = db.doc.validate_and_insert(**row) #field values not defined in row should have a default value defined defined in the model
            valid=ret.id >0
            if not valid:
                messages.append('error record#: {}'.format(str(osequence)))
                errors+=1
            else:
                ok+=1
            n+=1
            if ok%1000 == 0:
                print '!clear!{}'.format(str(dict(ok=ok,errors=errors, processes=n)))
                db.commit() #commit each row to avoid lock of the db
    remove(dld_file)
    if db.doc.status.default != 'validated':
        ret = scheduler.queue_task(create_validate_docs_tasks2,pvars=dict(campaign_id=campaign_id),timeout=1200) # timeout = 15secs per record
        tasks = db.campaign(campaign_id).tasks
        tasks =  tasks + [ret.id] if tasks else [ret.id]
        db(db.campaign.id==campaign_id).update(tasks=tasks)
    db(db.campaign.id==campaign_id).update(total_campaign_recipients=ok)
    db.commit()
    t2=time.time()
    return dict(ok=ok,errors=errors,total_rows=n,time=t2-t1)

def reset_campaign_progress(campaign_id):
    return db(db.campaign.id == campaign_id).update(status_progress = 0.0, current_task='')
#----------------
def get_ranges(start,end,i):
    if start==end: return [(start,end)]
    return [ (x,x+i-1) if x+i-1 < end else (x,end) for x in range(start,end,i)]

def create_validate_docs_tasks2(campaign_id):
    campaign = db.campaign(campaign_id)
    period = myconf.get('retry.period')
    retry_failed = myconf.get ('retry.retry_failed')
    timeout = myconf.get ('retry.rackspace_timeout')
    i = myconf.get('task.load')
    q = (db.doc.campaign==campaign_id)&(db.doc.status==DOC_LOCAL_STATE_OK[0])
    objs = db(q).select(db.doc.object_name,orderby=db.doc.osequence,groupby=db.doc.object_name)
    n=0
    for beg in range(0,len(objs)+1,i):
        obj_list = [ obj.object_name for obj in objs[beg:beg+i]]
        validation_task = scheduler.queue_task(cf_validate_doc_set2,
                pvars=dict(campaign_id=campaign_id,objs=obj_list),
                timeout = timeout*i, period = period, retry_failed = -1,
                group_name = WGRP_VALIDATORS)
        n+=1
        db.commit()
    return dict(result = '{} create_validate_tasks created'.format(n))

def parse_datetime(s,dflt_format):
    #s is s string that represents a datetime#format example :01/12/017 09:15:00#%d/%m/%Y %H:%M:%S
    #if no format is specified the default format is used
    t = s.split('#')
    return datetime.datetime.strptime(t[0],t[1] if len(t)>1 else dflt_format)

def cf_validate_doc_set2(campaign_id,objs):
    t0=time.time()
    campaign = get_campaign(campaign_id)
    credentials=get_credentials_storage()
    container,prefix=split_uri(campaign.cf_container_folder)
    temp_url_key = myconf.get('rackspace.temp_url_key')   # optimize maybe this should be global variables -----------------------------------------------
    server = myconf.get('host.server')
    pyrax.set_setting("identity_type", "rackspace")
    pyrax.set_default_region(credentials.region)
    pyrax.set_credentials(credentials.username, credentials.api_key)
    if pyrax.identity.authenticated:
        cf=pyrax.connect_to_cloudfiles(credentials.region)
        #cf=pyrax.cloudfiles
        curr_key = cf.get_temp_url_key()
        if not curr_key == temp_url_key: #throw an exception if not the same key??
            cf.set_temp_url_key(temp_url_key)
    doc_values = list()
    rcode_values = list()
    cont = cf.get_container(container)
    t1= time.time()
    for o in objs:
        obj=cont.get_object(path.join(prefix,o))
        if obj.bytes:
            seconds = (campaign.available_until - datetime.datetime.now()).total_seconds() #seconds from now to campaign.available_until
            temp_url = obj.get_temp_url(seconds = seconds)
            rcode=uuid.uuid4()
            dds_url = URL('secure',vars=dict( rcode = rcode ),scheme='https', host=server,hmac_key=URL_KEY)
            rcode_values.append(dict(
                                    object_name = o,
                                    temp_url=temp_url,
                                    dds_url=dds_url,
                                    rcode=rcode
                                    ))
            doc_values.append(dict(
                                        bytes=obj.bytes,
                                        checksum=obj.etag))
        else:
            db(q & (db.doc.object_name == o)).update(status=DOC_LOCAL_STATE_ERR[0])
    t2= time.time()
    db.retrieve_code.campaign.default=campaign.id
    q = (db.doc.campaign==campaign_id)&(db.doc.status==DOC_LOCAL_STATE_OK[0])
    updated=0
    for n,rc in enumerate(rcode_values):
        rc_id = db.retrieve_code.insert(**rc)
        dv= doc_values[n]
        dv.update(dict(rcode=rc_id,status=DOC_LOCAL_STATE_OK[2]))
        updated+=db((q) & (db.doc.object_name==rc['object_name'])).update(**dv)
    db.commit()
    t3= time.time()
    return (dict(updated=updated,connection= t1-t0,loop= t2-t1,records=len(objs),insert=t3-t2))

def send_doc_set(campaign_id,oseq_beg,oseq_end): #called by a task
    t0 = time.time()
    docs = db((db.doc.osequence>=oseq_beg)&(db.doc.osequence<=oseq_end)&
              (db.doc.campaign==campaign_id)&(db.doc.status=='validated')).select()
    if not docs:
        db(db.scheduler_task.id == W2PTASK.id).update(repeats = 1)
        return
    campaign = get_campaign(campaign_id)
    min_datetime = None
    t1= time.time()
    sended = 0
    for d in docs:
        mg_acceptance_time = compute_acceptance_time(d.deliverytime) if d.deliverytime else campaign.mg_acceptance_time
        if mg_acceptance_time <= datetime.datetime.now():
            send_doc_wrapper(d.id)
            sended+=1
        else:
            if min_datetime:
                if min_datetime > mg_acceptance_time:
                    min_datetime = mg_acceptance_time
            else:
                min_datetime = mg_acceptance_time
    t2= time.time()
    r = dict(docs=len(docs),prepare_time= t1-t0,loop= t2-t1,sended=sended)
    if min_datetime:
        raise Exception('{}, Task has pending records to send in the future'.format(r))
        #db(db.scheduler_task.id == W2PTASK.id).update( next_run_time=min_datetime)
    return r

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
    sd_kwargs = { k : kwargs[k] for k in ['to','is_sample','ignore_delivery_time','testmode'] if k in kwargs}
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
        #doc.status=DOC_LOCAL_STATE_OK[4] if 'Queued' in res.json()['message'] else None
        doc.status=DOC_LOCAL_STATE_OK[4]
        category = 'info'
    else:
        doc.status=DOC_LOCAL_STATE_ERR[1]

    doc.mailgun_id=res.json()['id'].strip('<').strip('>') if 'id' in res.json() else None
    update_doc=True
    if 'update_doc' in kwargs:
        if not kwargs['update_doc']:
            update_doc=False
    if update_doc: doc.update_record()
#    ed_id = event_data(doc=doc.id,category=category,
#                event_type='send_doc',
#                event_data='{}'.format(res.reason),
#                event_json=res.json(),
#                response_status_code=res.status_code)
    db.commit()
    if res.status_code in [400,401,402,404,500,502,503,504]:
        raise Exception('Mailgun returned status code = {}'.format(res.status_code))
    return res.ok
#   return ed_id

def get_context(doc,campaign,rc):
    #rc = retrieve code row
    url_type = { 'Body Only': None , 'Attachment' : None , 'Cloudfiles Temp URL': 'temp_url', 'DDS Server URL': 'dds_url'}[campaign.service_type]
    data = dict(record_id = doc.record_id,
            object_name = doc.object_name,
            email_address = doc.email_address
            )
    if url_type: data['url']=rc[url_type]
    data.update(doc.json)
    campaign_dict = dict( domain = campaign.mg_domain,
            uuid = campaign.uuid,
            #mg_id = campaign.mg_campaign_id,
            #mg_name = campaign.mg_campaign_name,
            campaign_name = campaign.campaign_name,
            available_from = campaign.available_from,
            available_until = campaign.available_until,
            mg_tags = campaign.mg_tags,
            subject = campaign.email_subject)
    if campaign.logo:
        campaign_dict.update(dict(logo_src = 'cid:{}'.format(campaign.logo)))
    return dict(data=Storage(data),campaign=Storage(campaign_dict))

def get_campaign_tag(campaign):
    return (campaign.id)+'_' + IS_SLUG()(campaign.campaign_name)[0]

def send_doc(doc_id,to=None,is_sample=False,ignore_delivery_time=False,test_mode=False):
    import ntpath

    doc = get_doc(doc_id)
    campaign = get_campaign(doc.campaign)
    rc = get_rcode(doc.rcode,doc.campaign)
    files=[]
    if campaign.logo:
        logofile = path.join(abspath(request.folder),'logos/',campaign.logo)
        if not path.isfile(logofile):
            save_image(campaign.logo)
        files.append(("inline",open(logofile)))
    context=get_context(doc,campaign,rc)
    html_body = render(campaign.html_body,context=context)
    data={'from':'{} <{}>'.format(campaign.from_name,campaign.from_address) if campaign.from_name else campaign.from_address,
          'to':to or doc.email_address,
          'subject':render(campaign.email_subject,context=context),
          'html':html_body,
          'text':html2text.html2text(html_body.decode('utf-8'))}
    #,'o:campaign':mg_campaign_id or campaign.mg_campaign_id}
    if not ignore_delivery_time:
        data['o:deliverytime']=RFC_2822_section_3_3(doc.deliverytime or campaign.available_from)

    data['o:tag']= [myconf.get('mailgun.tag_for_proofs')] if is_sample else [get_campaign_tag(campaign)]
    if campaign.mg_tags and not is_sample:
        data['o:tag']+= campaign.mg_tags[0:2] #maximum 3 tags per message
    if test_mode or campaign.test_mode:
        data['o:testmode']='true'
    #v:myvar
    if campaign.service_type == 'Attachment':
        #files.append( ('attachment', (doc.object_name, open(save_attachment(doc,campaign,rc),'rb').read())))
        for f in save_attachment(doc,campaign,rc):
            files.append(('attachment',(ntpath.basename(f),open(f,'rb').read())))
    return mg_send_message(campaign.mg_domain,  myconf.get('mailgun.api_key'),
            files=files,
            data=data)

def mg_send_message(domain,api_key,**kwargs):
    return requests.post(
        "https://api.mailgun.net/v3/{}/messages".format(domain),
        auth=("api", api_key),
        **kwargs)

def get_rcode(rcode_id,campaign_id):
    return db((db.retrieve_code.id == rcode_id) & (db.retrieve_code.campaign == campaign_id)).select(limitby=(0,1),
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
    tsk = db((db.scheduler_task.id.belongs(campaign.tasks))&(db.scheduler_task.function_name=='register_on_db')&
            (db.scheduler_task.status=='COMPLETED')).count()
    if tsk:
        progress1 = 50.0
    if campaign.total_campaign_recipients:
        validated_docs =  db((db.doc.campaign == campaign_id) & (db.doc.status == 'validated')).count()
        progress2 = (validated_docs / float(campaign.total_campaign_recipients) ) * 50.0 #validate docs is the 50% of the validate docs process

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
    l0=['campaign_name', 'test_mode', 'delete_documents_on_expire', 'download_limit',
            'maximum_bandwith', 'mg_tags','available_from', 'datetime_format']
    l1=['from_name', 'from_address', 'test_address', 'email_subject', 'html_body',
            'logo', 'logo_file']
    l2=[ 'cf_container_folder', 'index_file', 'service_type', 'uncompress_attachment','available_until']
    wfields = { 'defined' : l0+l1+l2,
                'documents error': l0+l1+l2,
                'documents ready' :l0+l1,
                'in approval' :l0+l1,
                'approved':l0
              }.get(campaign_status,[])
    for f in db.campaign.fields:
        db.campaign[f].writable = True if f in wfields else False
