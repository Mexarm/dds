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
from urlparse import urlparse, parse_qs, urljoin
import hashlib, hmac

import pyrax
import pyrax.exceptions as exc
import uuid
import html2text   #sudo pip install html2text

from gluon.storage import Storage
from gluon.fileutils import abspath
from gluon.template import render
from os import path, mkdir, system, remove, walk, listdir
from email.utils import formatdate

URL_KEY = myconf.get('dds.url_key')

# -------------mailgun--------------
def parse_RFC_3339_date(rfc3339_date): #returns datetime
    import dateutil.parser
    return dateutil.parser.parse(rfc3339_date) # RFC 3339 format ex. "2017-10-21T00:00:00Z"

def UTC_datetime_to_epoch(dt):
    epoch = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
    return (dt-epoch).total_seconds()

def datetime_to_epoch(dt):
    epoch = datetime.datetime.fromtimestamp(0)
    return (dt - epoch).total_seconds()

def timestamp_to_dt(ts):
    return datetime.datetime.fromtimestamp(ts)

def RFC_3339_to_local_dt(rfc3339_date):
    return timestamp_to_dt(UTC_datetime_to_epoch(parse_RFC_3339_date(rfc3339_date)))

def past_timestamp(days):
    return long(UTC_datetime_to_epoch(
        (__midnight(datetime.datetime.utcnow()+datetime.timedelta(days=days)
                   ).replace(tzinfo=pytz.utc))))

def print_dict(d): # print dict human readable
    print json.dumps(d, indent=4, sort_keys=True)

def mg_api_request(endpoint, params=None):
    """ endpoint: api endpoint ex. : /<domain>/tags
        params: dict with params
    """
    return requests.get(
        "https://api.mailgun.net/v3{}".format(endpoint),
        auth=("api", myconf.get('mailgun.api_key')),
        params=params
        )

def get_list_index(s, l):
    if s in l:
        return l.index(s)

def get_ids(l, key):
    return [i[key] for i in l]

def update_stats_list(stats1, stats2): # stats1 & 2 are lists of dicts (mailgun stats)
    lid1 = get_ids(stats1, 'time')
    lid2 = get_ids(stats2, 'time')
    for i, id_ in enumerate(lid2):
        k = get_list_index(id_, lid1)
        if  k != None:
            stats1[k].update(stats2[i])
        else:
            stats1.append(stats2[i])
    return stats1

def mg_update_analitycs(campaign_id):
    c = get_campaign(campaign_id)
    tag = get_campaign_tag(c)
    try:
        r = mg_api_request('/{}/tags/{}'.format(c.mg_domain, tag))
        r.raise_for_status()
        data = r.json()
        c.mg_first_seen = RFC_3339_to_local_dt(data['first-seen'])
        c.mg_last_seen = RFC_3339_to_local_dt(data['last-seen'])
        a = db.analitycs
        for r in MG_ANALITYCS_RESOLUTION:
            analitycs_record = db(((a.tag_ == tag)|(a.campaign == campaign_id))
                                  & (a.resolution == r)
                                 ).select(limitby=(0, 1)).first()
            start_ = ''
            if analitycs_record:
                action = db(a.id == analitycs_record.id).validate_and_update #update
                stats = analitycs_record.stats_
                start_ = stats['start']
            else:
                action = a.insert
                stats = dict()
            #ts_29days_ago = long(time.time() - (29 * 24 * 60 * 60))
            ts_past_limit = None
            if r in ['hour', 'week']:
                #one year for keek 30 days for hour stats
                ts_past_limit = past_timestamp(-364) if r == 'week' else past_timestamp(-29)
            start_ts = long(UTC_datetime_to_epoch(parse_RFC_3339_date(data['first-seen'])))
            if ts_past_limit:
                start_ts = start_ts if start_ts > ts_past_limit else ts_past_limit
            res = mg_api_request("/{}/tags/{}/stats".format(c.mg_domain, tag),
                                 params=dict(start=start_ts,
                                             resolution=r,
                                             event=MG_EVENT_TYPES))
            if (res.status_code == 400) and (r == 'hour'): continue
            res.raise_for_status()
            rd = res.json().copy()
            retrieved_stats = rd['stats'][:]
            del rd['stats']
            stats.update(rd)
            if 'stats' in stats:
                stats['stats'] = update_stats_list(stats['stats'], retrieved_stats)
            else:
                stats['stats'] = retrieved_stats
            if start_:
                stats['start'] = start_
            fields = dict(
                campaign=c.id,
                tag_=rd['tag'],
                start_=RFC_3339_to_local_dt(stats['start']),
                end_=RFC_3339_to_local_dt(stats['end']),
                description=stats['description'],
                resolution=stats['resolution'],
                stats_=json.dumps(stats) if analitycs_record else stats
            )
            result = action(**fields)
            if result.errors:
                db.rollback()
                raise Exception(result.errors.as_json())
            db.commit()
        c.update_record()
        db.commit()
        return True
    except:
        db.rollback()
        raise

def daemon_retrieve_campaign_analitycs():
    campaigns = db((db.campaign.id > 0) & (db.campaign.is_active == True)).select()
    for c in campaigns:
        tag = get_campaign_tag(c)
        r = mg_api_request('/{}/tags/{}'.format(c.mg_domain, tag))
        if r.status_code != 200:
            continue
        data = r.json()
        r_last_seen_ts = UTC_datetime_to_epoch(parse_RFC_3339_date(data['last-seen']))
        c_last_seen_ts = datetime_to_epoch(c.mg_last_seen) if c.mg_last_seen else None
        if (r_last_seen_ts == c_last_seen_ts) and ((time.time() - r_last_seen_ts) > (24 * 60 * 60)):
            continue
        #print tag, c_last_seen_ts, r_last_seen_ts
        #print_dict(data)
        mg_update_analitycs(c.id)

def sumarize_stats(stats_list, stats_keys):
    result = dict((k, 0) for k in stats_keys)
    for d in stats_list:
        for k in stats_keys:
            v = d
            for kk in k.split('.'):
                #if not v: break
                v = v.get(kk, {})
            if isinstance(v, int) or isinstance(v, long):
                result[k] += v
    return result

def mg_get_domains():
    return requests.get(
        "https://api.mailgun.net/v3/domains",
        auth=("api", myconf.get('mailgun.api_key'))
        )

def mg_get_campaigns(domain):
    return requests.get(
        "https://api.mailgun.net/v3/{}/campaigns".format(domain),
        auth=('api', myconf.get('mailgun.api_key')))

def mg_update_local_campaign_stats(campaign_id):
    #update a campaign with the information retrieved from mailgun
    c = get_campaign(campaign_id)
    r1 = requests.get(
        "https://api.mailgun.net/v3/{}/campaigns/{}/stats".format(c.mg_domain, c.mg_campaign_id),
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
            auth = ('api', myconf.get('mailgun.api_key')))
    r.raise_for_status()
    return r

def task_evt_poll(domain, begin_ts, end_ts):
    qopt = dict(begin = begin_ts, end = end_ts)
    store_mg_events(get_events(domain, qopt))

def get_latest_task_id(task_name):
    max = db.scheduler_task.id.max()
    return db(db.scheduler_task.task_name == task_name).select(max).first()[max]

def daemon_master_event_poll():
    now_ts = time.time()
    latest_task_id=get_latest_task_id('task_evt_poll')
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
    st = db.scheduler_task
    db((st.last_run_time < limit_dt) & (st.task_name == 'task_evt_poll') & (st.status == 'COMPLETED')).delete()
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

def dict_getv(d,keys, default=None):
    k = keys.split('.')
    v=d
    for j in k:
        v = v.get(j,None)
    return v

def active_domains_list(res): #takes the mail gun request.response object and built a list of domains.
    l=list()
    res_dict=res.json()
    for d in res_dict['items']:
        r = mg_api_request('/domains/{}/tracking'.format(d['name']),dict(auth = myconf.get('mailgun.api_key'))).json()
        if (d['state']=='active') and d['type'] != 'sandbox'and \
                                  dict_getv(r,'tracking.click.active') and \
                                  dict_getv(r,'tracking.open.active'): # and \
                                  #dict_getv(r,'tracking.unsubscribe.active'):
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
    if 'reason' in event_dict:
        if event_dict['reason'] == 'bounce': return # discard bounce events, this events does not contain a valid message-id 
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
    dt=timestamp_to_dt(e.timestamp)
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
    if e.event in MG_EVENT_TYPES:
        field=e.event + '_on'
        doc_field=get_latest_dt(dt,doc[field])
        if doc_field != doc[field]:
            doc[field] = doc_field
            doc.update_record()
    db.commit()
    return r

def store_mg_events(response):
    if response.status_code != 200: return
    rdict=response.json()
    if not rdict['items']: return
    if len(rdict['items']) == 0: return
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
            if c.status in ['queuing','live','scheduled']:
                reclaim_attach_storage_campaign(c_uuid)
                rmtree=False
        if rmtree: shutil.rmtree(path.join(attach_temp,c_uuid))

def reclaim_attach_storage_campaign(c_uuid):
    import shutil
    attach_temp = path.join(request.folder , 'attach_temp')
    c_folder = path.join(attach_temp,c_uuid)
    c=get_campaign_by_uuid(c_uuid)
    if c.status == 'in approval': return #dont reclaim storage in approval
    queued = DOC_LOCAL_STATE_OK[4]
    for f in [entry for entry in listdir(c_folder) if path.isfile(path.join(c_folder,entry))]:
        row = db((db.doc.campaign == c.id) & (db.doc.object_name == f) & (db.doc.status == queued )).select(limitby=(0,1)).first()
        if row:
            pth=path.join(c_folder,f)
            remove(pth)
            if path.isdir(pth+'.unzip'):
                shutil.rmtree(pth+'.unzip')

def set_as_sample(ids):
    if ids:
        campaign_id = db(db.doc.id == ids[0]).select(limitby=(0,1)).first().campaign
        db((db.doc.campaign == campaign_id) & (db.doc.is_sample == True)).update(is_sample = False)
        session.flash = "%i records were included in samples set" % db(db.doc.id.belongs(ids)).update(is_sample=True)

def update_total_campaign_recipients(campaign_id):
    ok = db(db.doc.campaign == campaign_id).count()
    db(db.campaign.id==campaign_id).update(total_campaign_recipients=ok)

def delete_records(ids):
    if ids:
        campaign_id = db(db.doc.id == ids[0]).select(limitby=(0,1)).first().campaign
        session.flash = "%i records were deleted" % db(db.doc.id.belongs(ids)).delete()
        update_total_campaign_recipients(campaign_id)
        
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

def cf_create_container(container_name):
    """ if containers exists returns that container
    """
    import pyrax
    import pyrax.exceptions as exc
    import pyrax.utils as utils
    credentials = get_credentials_storage()
    pyrax.set_setting("identity_type", "rackspace")
    pyrax.set_default_region(credentials.region or get_region_id(rackspace_regions[0]))
    try:
        pyrax.set_credentials(credentials.username, credentials.api_key)
    except exc.AuthenticationFailed as e:
        return e
    cf=pyrax.connect_to_cloudfiles(credentials.region)
    return cf.create_container(container_name)

def cf_get_CDN_url(obj,ssl=False):
    """ obj is Storage object (pyrax.cloudfiles), it must be inside a 
    public container
    """
    import urllib
    encoded_name = urllib.quote(obj.name)
    uri = obj.container.cdn_ssl_uri if ssl else obj.container.cdn_uri
    return urljoin(uri, encoded_name)

def cf_get_object_with_metadata_key_value(objects,key,value):
    """ looks in the objects list if an returns the 
    object that matchs key, value
    """
    for obj in objects:
        meta = obj.get_metadata()
        if key in meta.keys():
            if meta[key] == value: return obj
    return None
        
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
    sep=',' # ------ IMPLEMENT: support diferent separators--------------
    quotechar='"'
    beg=time.time()
    pth=prepare_subfolder('index_files/')
    campaign = get_campaign(campaign_id)
    db.doc.status.default= 'validated' if campaign.service_type == 'Body Only' else 'initial'
    credentials=get_credentials_storage()
    container,prefix=split_uri(campaign.cf_container_folder)
    index_file=campaign.index_file
    object_name=path.join(prefix,index_file).replace('\\','/')
    dld_file=download_object(container,object_name,pth,credentials)
    ok=0
    errors=0
    messages = list()
    db(db.doc.campaign==campaign_id).delete()
    db(db.retrieve_code.campaign==campaign_id).delete()
    db.commit()
    with open(dld_file,'rb') as handle:                                                            # check UNICODE SUPPORT!!!
        csv_reader = csv.reader(handle,delimiter=sep,quotechar=quotechar) #
        hdr_list=[ IS_SLUG()(fldn)[0].replace('-',"_") for fldn in csv_reader.next() ] #fix 17
        if not set(REQUIRED_FIELDS) <= set(hdr_list):
            raise ValueError('required fields "{}" are not present in file {}/{}/{}'.format(','.join(REQUIRED_FIELDS),container,prefix,index_file))
        db.doc.campaign.default=campaign_id
        n=0
        osequence = 1
        for line in csv_reader:
            values = line #
            rdict = make_doc_row(dict(zip(hdr_list, values)))
            rdict.update(dict(osequence=osequence))
            rd_json=json.loads(rdict['json'])
            if 'deliverytime' in rd_json:
                if rd_json['deliverytime']:
                    rdict.update(dict(deliverytime = parse_datetime(rd_json['deliverytime'],campaign.datetime_format)))
            row=Storage(rdict)
            row.email_address = row.email_address.strip().lower()
            validation = IS_EMAIL(error_message='not valid email address')(row.email_address)
            if validation[1]: #invalid email address
                messages.append(dict(event='error',validation=validation,osequence=osequence,record_id=row.record_id))
                errors+=1
            else:
                db.doc.validate_and_insert(**row) #field values not defined in row should have a default value defined defined in the model
                osequence +=1
                ok+=1
            n+=1
            if ok%1000 == 0:
                print '!clear!{}'.format(str(dict(ok=ok,errors=errors, processes=n)))
                db.commit() #COMMIT
    remove(dld_file)
    if db.doc.status.default != 'validated':
        ret = scheduler.queue_task(create_validate_docs_tasks2,
                                   pvars=dict(campaign_id=campaign_id),
                                   timeout=1200,
                                   immediate=True)
        update_campaign_tasks(campaign_id,[ret.id])
    db(db.campaign.id==campaign_id).update(total_campaign_recipients=ok)
    db.commit()
    t2=time.time()
    print '!clear!'
    return dict(ok=ok,errors=errors,total_rows=n,time=t2-t1,messages=json.dumps(messages))

def reset_campaign_progress(campaign_id):
    return db(db.campaign.id == campaign_id).update(status_progress = 0.0, current_task='')
#----------------
def get_ranges(start,end,i):
    if start==end: return [(start,end)]
    return [ (x,x+i-1) if x+i-1 < end else (x,end) for x in range(start,end,i)]

def update_campaign_tasks(campaign_id, tasks_ids):
    if not isinstance(tasks_ids,list): return
    if not tasks_ids: return
    tasks = db(db.campaign.id==campaign_id).select(db.campaign.tasks,limitby=(0,1)).first().tasks
    tasks =  tasks + tasks_ids if tasks else tasks_ids
    r = db(db.campaign.id==campaign_id).update(tasks=tasks)
    db.commit()
    return r

def create_validate_docs_tasks2(campaign_id):
    campaign = db.campaign(campaign_id)
    period = myconf.get('retry.period')
    retry_failed = myconf.get ('retry.retry_failed')
    timeout = myconf.get ('retry.rackspace_timeout')
    i = myconf.get('task.load')
    q = (db.doc.campaign==campaign_id)&(db.doc.status==DOC_LOCAL_STATE_OK[0])
    objs = db(q).select(db.doc.object_name,orderby=db.doc.osequence,groupby=db.doc.object_name)
    n=0
    validate_tasks = []
    for beg in range(0,len(objs)+1,i):
        obj_list = [ obj.object_name for obj in objs[beg:beg+i]]
        validation_task = scheduler.queue_task(cf_validate_doc_set2,
                pvars=dict(campaign_id=campaign_id,objs=obj_list),
                timeout = timeout*i, period = period, retry_failed = -1,
                group_name = WGRP_VALIDATORS,
                immediate=True)
        n+=1
        validate_tasks.append(validation_task.id)
        db.commit()
    update_campaign_tasks(campaign_id,validate_tasks)
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
    q = (db.doc.campaign==campaign_id)&(db.doc.status==DOC_LOCAL_STATE_OK[0])
    for o in objs:
        try:
            obj=cont.get_object(path.join(prefix,o).replace('\\','/'))
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
        except pyrax.exceptions.NoSuchObject as e:
            db(q & (db.doc.object_name == o)).update(status=DOC_LOCAL_STATE_ERR[0])
    t2= time.time()
    db.retrieve_code.campaign.default=campaign.id
    updated=0
    for n,rc in enumerate(rcode_values):
        rc_id = db.retrieve_code.insert(**rc)
        dv= doc_values[n]
        dv.update(dict(rcode=rc_id,status=DOC_LOCAL_STATE_OK[2]))
        updated+=db((q) & (db.doc.object_name==rc['object_name'])).update(**dv)
    db.commit()
    t3= time.time()
    return (dict(updated=updated,connection= t1-t0,loop= t2-t1,records=len(objs),insert=t3-t2))

def domain_is_active(domain):
    r = mg_api_request('/domains/{}'.format(domain),
                       params = dict(auth=myconf.get('mailgun.api_key')))
    if r.status_code != 200:
        raise Exception('cannot verify if domain: {} is active'.format(d))
    return dict_getv(r.json(),'domain.state') == 'active'

def send_doc_set(campaign_id,oseq_beg,oseq_end):
    import MTRequests
    from Queue import Queue
#    import requests

    q = Queue()

    campaign = get_campaign(campaign_id)
    if not domain_is_active(campaign.mg_domain):
        raise Exception('{} domain is disabled'.format(campaign.mg_domain))
    t0 = time.time()
    docs = db((db.doc.osequence>=oseq_beg)&(db.doc.osequence<=oseq_end)&
              (db.doc.campaign==campaign_id)&(db.doc.status=='validated')
              ).select(orderby=db.doc.deliverytime|db.doc.osequence)
    if not docs: return
    min_datetime = None
    t1= time.time()
    sended = 0
    for d in docs:
        mg_acceptance_time = compute_acceptance_time(d.deliverytime) if d.deliverytime else campaign.mg_acceptance_time
        if mg_acceptance_time <= datetime.datetime.now():
            q.put(d.id)
        else:
            if min_datetime:
                if min_datetime > mg_acceptance_time:
                    min_datetime = mg_acceptance_time
            else:
                min_datetime = mg_acceptance_time
    myworkers = MTRequests.MTRequests(q, send_doc,num_workers=100)
    out = myworkers.run()
    for item in list(out.queue):
        if not item.error:
            send_doc_wrapper(item.output)
            sended +=1 
    t2= time.time()
    r = dict(docs=len(docs),prepare_time= t1-t0,loop= t2-t1,processed=sended)
    if min_datetime:
        raise Exception('{}, Task has pending records to send in the future'.format(r))
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
    sd_kwargs = { k : kwargs[k] for k in ['to','is_sample','ignore_delivery_time','testmode'] if k in kwargs}
    doc_id = args[0]
    update_doc = True
    if 'update_doc' in kwargs:
        update_doc = kwargs['update_doc']
    return process_mg_response(send_doc(*args,**sd_kwargs),doc_id,update_doc=update_doc)

def process_mg_response(res,doc_id,update_doc=True):
    #    Mailgun returns standard HTTP response codes.
    #Code	Description
    #200	Everything worked as expected
    #400	Bad Request - Often missing a required parameter
    #401	Unauthorized - No valid API key provided
    #402	Request Failed - Parameters were valid but request failed
    #404	Not Found - The requested item doesn’t exist
    #500, 502, 503, 504	Server Errors - something is wrong on Mailgun’s end
    queued = DOC_LOCAL_STATE_OK[4]
    rejected = DOC_LOCAL_STATE_ERR[1]
    doc = get_doc(doc_id) # retrieve doc from db
    if not res.status_code in [200, 400]: 
        res.raise_for_status()
        return False
    doc.status = queued if res.status_code == 200 else rejected
    if doc.status == queued:
        doc.mailgun_id=res.json()['id'].strip('<').strip('>') if 'id' in res.json() else None
    if update_doc: 
        doc.update_record()
        db.commit()
    return doc.mailgun_id or res.text

def get_campaign_tag(campaign):
    return str(campaign.id)+'_' + IS_SLUG()(campaign.campaign_name)[0]

def get_doc_view_url(docid):
    #docid is the uuid of the doc
    server = myconf.get('host.server')
    return URL('view',vars=dict( docid = docid ),scheme='https', host=server,hmac_key=URL_KEY)
    
def get_context(doc,campaign,rc):
    url_type = { 'Body Only': None , 'Attachment' : None , 'Cloudfiles Temp URL': 'temp_url', 'DDS Server URL': 'dds_url'}[campaign.service_type]
    data = dict(record_id = doc.record_id,
            object_name = doc.object_name,
            email_address = doc.email_address
            )
    #if url_type: data['url']=rc[url_type]
    if url_type: data['object_url']=rc[url_type]
    if doc.doc_uuid: data['message_view_url'] = get_doc_view_url(doc.doc_uuid)
    data.update(doc.json)
    campaign_dict = dict( domain = campaign.mg_domain,
            uuid = campaign.uuid,
            campaign_name = campaign.campaign_name,
            available_from = campaign.available_from,
            available_until = campaign.available_until,
            mg_tags = [get_campaign_tag(campaign)] + campaign.mg_tags[0:2],
            subject = campaign.email_subject)
    if campaign.logo:
        campaign_dict.update(dict(logo_src = 'cid:{}'.format(campaign.logo)))
    return dict(data=Storage(data),campaign=Storage(campaign_dict))

def get_context_fields(context, parent='', prekey = '.', postkey = ''):
    result = []
    for k,v in context.iteritems():
        if isinstance(v, dict) or isinstance(v,Storage):
            result += get_context_fields(v,parent=k,prekey=prekey,postkey=postkey)
        else:
            pre = prekey if parent else ''
            result.append(parent + pre + k +postkey)
    return result

def send_doc(doc_id,to=None,is_sample=False,ignore_delivery_time=False,test_mode=False):
    import ntpath
    from re import sub
    doc = get_doc(doc_id)
    campaign = get_campaign(doc.campaign)
    rc = get_rcode(doc.rcode,doc.campaign)
    files=[]
    if campaign.logo:
        logofile = path.join(abspath(request.folder),'logos',campaign.logo)
        if not path.isfile(logofile):
            save_image(campaign.logo)
        files.append(("inline",open(logofile,'rb')))
    context=get_context(doc,campaign,rc)
    raw_html_body = sub(r'(\[\[)(\w+\.\w+)(\]\])', r'{{=\2}}', campaign.html_body) # [[xxx.yyy]] -> {{=xxx.yy}}
    raw_subject = sub(r'(\[\[)(\w+\.\w+)(\]\])', r'{{=\2}}', campaign.email_subject) # [[xxx.yyy]] -> {{=xxx.yy}}
    html_body = render(raw_html_body,context=context)
    subject = render(raw_subject,context=context)
    sample_text = "[sample c={},rid={}]".format(campaign.id,doc.record_id) if is_sample else ""
    data={'from':'{} <{}>'.format(campaign.from_name,campaign.from_address) if campaign.from_name else campaign.from_address,
          'to':to or doc.email_address,
          'subject': subject + sample_text,
          'html':html_body,
          'text':html2text.html2text(html_body.decode('utf-8'))}
    if not ignore_delivery_time:
        data['o:deliverytime']=RFC_2822_section_3_3(doc.deliverytime or campaign.available_from)
    data['o:tag']= [myconf.get('mailgun.tag_for_proofs')] if is_sample else [get_campaign_tag(campaign)]
    if campaign.mg_tags and not is_sample:
        data['o:tag']+= campaign.mg_tags[0:2] #maximum 3 tags per message
    if test_mode or campaign.test_mode:
        data['o:testmode']='true'
    if campaign.service_type == 'Attachment':
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
    object_name=path.join(get_prefix(form.vars.cf_container_folder),form.vars.index_file).replace('\\','/')
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
        validated_docs =  db((db.doc.campaign == campaign_id) & (db.doc.status.belongs(['validated', DOC_LOCAL_STATE_ERR[0]]))).count()
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
