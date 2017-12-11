# -*- coding: utf-8 -*-
from os import path
from automaton import machines #http://docs.openstack.org/developer/automaton/examples.html#creating-a-simple-machine
import pickle
import uuid
from dateutil.tz import tzlocal
import pytz

GOOGLE_TRACKING_ID = myconf.get('google.tracking_id')

SERVICE_TYPE=['Body Only','Attachment','Cloudfiles Temp URL', 'DDS Server URL']

DOC_LOCAL_STATE_OK = [ 'initial', 'validating','validated', 'queued (local)', 'queued (mailgun)' ]
DOC_LOCAL_STATE_ERR = [ 'cf not valid','rejected (mailgun)' ]
UUID_LENGTH = 36
REQUIRED_FIELDS = ['record_id','object_name','email_address'] #required fields in the index.csv file
OPTIONAL_FIELDS = ['deliverytime'] #optional fields in the index.csv file

MG_ANALITYCS_RESOLUTION = [ 'hour', 'day', 'month']
MG_EVENT_TYPES = [ 'accepted', 'delivered', 'failed', 'opened', 'clicked', 'unsubscribed', 'complained', 'stored' ] # rejected was removed

#event poll params
EP_TIME_SLICE = int(myconf.get('eventpoll.time_slice'))
EP_DAEMON_PERIOD = int(myconf.get('eventpoll.daemon_period'))
EP_DELAY = int(myconf.get('eventpoll.delay'))
EP_TASK_TIME_SLICE = int(myconf.get('eventpoll.task_time_slice'))
EP_TASK_TIMEOUT = int(myconf.get('eventpoll.task_timeout'))
EP_TASK_REPEATS = int(myconf.get('eventpoll.task_repeats'))

WGRP_DAEMONS = 'daemons'
WGRP_VALIDATORS = 'validators'
WGRP_SENDERS = 'senders'
WGRP_SENDERS1 = 'senders1'
WGRP_POLLERS = 'pollers'
WGRP_FINISHERS = 'finishers'

DAEMON_TASKS = [('daemon_progress_tracking', 20),
                ('daemon_status_changer', 25),
                ('daemon_master_event_poll', EP_DAEMON_PERIOD),
                ('daemon_reclaim_attach_storage', 300),
                ('daemon_event_poll_remove_old_tasks', 86400),
                ('daemon_retrieve_campaign_analitycs', 86400)] # (task_name, period in seconds)

MIDNIGHT_TASKS = ['daemon_event_poll_remove_old_tasks', 'daemon_retrieve_campaign_analitycs']

DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

MAX_ALLOWED_PACKET = db.executesql("SHOW VARIABLES like 'max_allowed_packet';")[0][1]

BEEFREE_CLIENT_ID = myconf.get('befree.client_id')
BEEFREE_CLIENT_SECRET = myconf.get('befree.client_secret')

coment1 = """<small><p>Please include an unsubscribe link in the body, 
ex.: <code>"""
comment2 = '<a href="%unsubscribe_url%"><unsubscribe>unsubscribe</unsubscribe></a>'
comment3 = """</code></p><p>
1) <code>%unsubscribe_url%</code> -- link to unsubscribe recipient from all messages sent by
given domain<br />
2) <code>%tag_unsubscribe_url%</code> -- link to unsubscribe from all tags provided in the message <br />
3) <code>%mailing_list_unsubscribe_url%</code> -- link to unsubscribe from future messages sent to a 
mailing list<br /></p></small>"""
HTML_BODY_COMMENT = XML(coment1 + xmlescape(comment2)+ comment3)

def compute_acceptance_time(dt):
    import datetime
    from dateutil.relativedelta import relativedelta

    if dt > datetime.datetime.now():
        return dt + relativedelta(days=-2)
    else:
        return dt

#def local_dt(utc_dt):
#        return utc_dt.replace(tzinfo=pytz.utc).astimezone(tzlocal()).replace(tzinfo=None)
class IS_EMAIL_LIST(object):
    def __init__(self, error_message="Email %s is invalid", sep=","):
        self.error_message = error_message
        self.sep = sep

    def __call__(self, value):
        emails = value.strip().replace('\n', '').replace('\t', '').split(self.sep)
        emails = [e.strip() for e in emails]
        for email in emails:
            email = email.strip()
            if IS_EMAIL()(email)[1] != None:
                return (email, self.error_message % email)
        return (self.sep.join(emails), None)

class HAS_UNSUBSCRIBE_URL(object):
    def __init__(self, error_message='no valid unsubscribe url found please use: ' \
                                     '<a href="%unsubscribe_url%">...</a> (valid urls: {})',
                 default='<br><p><a href="%unsubscribe_url%">unsubscribe</a></p>',
                 valid_url_list=['%unsubscribe_url%', '%tag_unsubscribe_url%',
                                 '%mailing_list_unsubscribe_url%']):
        self.error_message = error_message
        self.default = default
        self.valid_url_list = valid_url_list

    def __call__(self, value):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(value, 'html.parser')
        for a in soup.find_all('a'):
            if a['href'] in self.valid_url_list:
                return (value, None)
        return (value, self.error_message.format(self.valid_url_list))

def mysql_add_index(table, column):
    if db._uri[:5] == 'mysql':
        params = dict(table=table, column=column, idx_name='{}__idx'.format(column))
        qry = r"SELECT COUNT(1) IndexIsThere " \
              "FROM INFORMATION_SCHEMA.STATISTICS " \
              "WHERE table_schema=DATABASE() " \
              "AND table_name='{table}' " \
              "AND index_name='{idx_name}';"
        result = db.executesql(qry.format(**params))
                                        #returns ((1L,),) if the index exists
        if not result[0][0]:
            return db.executesql(
                "ALTER TABLE {table} ADD INDEX `{idx_name}` (`{column}`);".format(**params))

db.define_table('campaign',
                Field('uuid', 'string',default=uuid.uuid4(), label='Campaign UUID', writable=False, readable=False),
                Field('mg_domain','string',label = 'Mailgun Domain'),
                Field('campaign_name','string',notnull=True,label=T('Campaign Name'),
                     requires=[IS_NOT_EMPTY(),IS_NOT_IN_DB(db, 'campaign.campaign_name')]),
                Field('mg_tags','list:string',label='Mailgun tags (2 maximum)'),
                #Field('mg_campaign_name','string',notnull=True,label=T('Mailgun Campaign Name')),
                #Field('mg_campaign_id','string',label=T('Mailgun Campaign id'),writable=False,
                #    compute= lambda (row): get_mg_campaign(mg_get_campaigns(row.mg_domain),row.mg_campaign_name)['id']), #retrieve mg_campaign_id from mailgun ),
                Field('test_mode','boolean',label='test mode (Mailgun will accept the messages but not deliver)'),
                Field('cf_container_folder','string',notnull=True,label='Cloudfiles Container/Folder',
                     requires=[IS_NOT_EMPTY(),IS_NOT_IN_DB(db, 'campaign.cf_container_folder')]), #unique=True, gives error in mysql <class 'gluon.contrib.pymysql.err.InternalError'> (1071, u'Specified key was too long; max key length is 767 bytes')
                Field('index_file','string',default='index.csv',notnull=True,label='archivo indice (.CSV)'),
                Field('service_type','string',notnull=True,label=T('Service type'),
                      default=SERVICE_TYPE[0], requires = IS_IN_SET(SERVICE_TYPE)),
                Field('uncompress_attachment','boolean',default=False,notnull=True),
                Field('available_from','datetime',notnull=True),
                Field('mg_acceptance_time','datetime', writable=False, label='Mailgun acceptance time', compute=lambda(row): compute_acceptance_time(row.available_from)),
                Field('available_until','datetime'),
                Field('datetime_format', 'string', label='datetime import format',default=DEFAULT_DATETIME_FORMAT,
                    requires=IS_NOT_EMPTY()),
                Field('is_active','boolean',notnull=True,default=True),
                Field('status','string',default=FM.states[0], writable=False), #Created, Verified, Active, Failed
                Field('status_progress','float',default='',writable=False),
                Field('current_task','string',writable=False),
                Field('delete_documents_on_expire','boolean',default=True,label=T('Delete Documents on campaign finish')), #Delete all documents listed on the index file on the date entered on the available_until field.
                Field('total_campaign_recipients','integer',writable=False),
                Field('total_campaign_bytes','integer',writable=False), #total size of the container
                Field('container_objects','integer',writable=False),
                Field('container_bytes','integer',writable=False),
                Field('download_limit','integer',notnull=True,default=0), #maximun times that each file can be downloaded, 0= no limit, only valid when service type is DDS Server URL
                Field('maximum_bandwith', 'integer',notnull=True,default=0), #limit the maximum bandwith to consume in bytes, 0= no limit, only valid when service type is DDS Server URL
                Field('from_name','string',default=T('Your Name'),requires=IS_NOT_EMPTY()),
                Field('from_address','string',requires = IS_EMAIL(), label = 'From email address'),
                Field('test_address','string',requires = IS_EMAIL_LIST(), label = 'email address to sent tests' ),
                Field('email_subject','string',notnull=True, default=T('Default email subject'),label=T('email subject'),
                      requires=IS_NOT_EMPTY()),
                Field('html_body','text', notnull=True, default='', requires=HAS_UNSUBSCRIBE_URL(),
                      comment=HTML_BODY_COMMENT),
                Field('BF_json','json',readable=False,writable=False),
                Field('logo','upload', uploadfield='logo_file'),
                Field('logo_file', 'blob'),
                Field('tasks','list:reference scheduler_task', default =[],writable=False,readable=False),
                Field('fm','text',default=pickle.dumps(FM),writable=False,readable=False), #finite state machine
                Field('fm_history','list:string', default=[],writable=False,readable=False),
                Field('mg_stats','json',default='{}',readable=False,writable=False),
                Field('mg_stats_unique','json',default='{}',readable=False,writable=False),
                Field('mg_first_seen','datetime',readable=False,writable=False),
                Field('mg_last_seen','datetime',readable=False,writable=False),
                Field('send_tasks_stats','json',readable=False,writable=False),
                Field('send_retry_active','boolean',readable=False,writable=False),
                auth.signature)

db.define_table('analitycs',
                Field('campaign', 'reference campaign'),
                Field('tag_'),
                Field('start_','datetime'),
                Field('end_','datetime'),
                Field('description'),
                Field('resolution',requires = IS_IN_SET(MG_ANALITYCS_RESOLUTION)),
                Field('stats_','json', default = '{}'),
                auth.signature)
mysql_add_index('analitycs','campaign')
mysql_add_index('analitycs','tag_')
mysql_add_index('analitycs','resolution')

db.define_table('doc', Field('campaign','reference campaign'),
                Field('osequence','integer',notnull=True,label='original sequence'), #original sequence
                Field('is_sample','boolean',default=False,label='included in samples set'),
                Field('record_id','string',notnull=True),                #required on index.csv fieldname = record_id
                Field('object_name','string',notnull=True), #unique=True?              #required on index.csv fieldname = object_name
                Field('rcode','integer',default = 0,writable=False,readable=False),
                Field('email_address','string',notnull=True),            #required on index.csv fieldname = email_address
                Field('deliverytime','datetime'),
                Field('json','json',default = '{}'),
                Field('checksum','string',default=0),
                Field('bytes','integer',default=0),
                Field('download_limit','integer',default=0),
                Field('download_counter','integer',default=0),
                Field('send_task','integer'),  #'reference scheduler_task' or integer
                Field('validation_task','integer'),
                Field('status','string'),
                Field('send_retry_active','boolean'),
                Field('mailgun_id','string'),
                Field('doc_uuid','string',default=uuid.uuid4(), label='Doc UUID', writable=False,readable=False),
                Field('accepted_on','datetime',writable=False), #analitycs fields
                Field('rejected_on','datetime',writable=False), 
                Field('delivered_on','datetime',writable=False),
                Field('failed_on','datetime',writable=False),
                Field('opened_on','datetime',writable=False),
                Field('clicked_on','datetime',writable=False),
                Field('unsubscribed_on','datetime',writable=False),
                Field('complained_on','datetime',writable=False),
                Field('stored_on','datetime',writable=False))
mysql_add_index('doc','mailgun_id')
mysql_add_index('doc','status')
mysql_add_index('doc','object_name')
mysql_add_index('doc','osequence')
mysql_add_index('doc','doc_uuid')
db.doc.deliverytime.represent = lambda value,row: value.strftime(DEFAULT_DATETIME_FORMAT) if value else ''
db.doc.accepted_on.represent = lambda value,row: value.strftime(DEFAULT_DATETIME_FORMAT) if value else ''
db.doc.rejected_on.represent = lambda value,row: value.strftime(DEFAULT_DATETIME_FORMAT) if value else ''
db.doc.delivered_on.represent = lambda value,row: value.strftime(DEFAULT_DATETIME_FORMAT) if value else ''
db.doc.failed_on.represent = lambda value,row: value.strftime(DEFAULT_DATETIME_FORMAT) if value else ''
db.doc.opened_on.represent = lambda value,row: value.strftime(DEFAULT_DATETIME_FORMAT) if value else ''
db.doc.clicked_on.represent = lambda value,row: value.strftime(DEFAULT_DATETIME_FORMAT) if value else ''
db.doc.unsubscribed_on.represent = lambda value,row: value.strftime(DEFAULT_DATETIME_FORMAT) if value else ''
db.doc.complained_on.represent = lambda value,row: value.strftime(DEFAULT_DATETIME_FORMAT) if value else ''
db.doc.stored_on.represent = lambda value,row: value.strftime(DEFAULT_DATETIME_FORMAT) if value else ''

db.define_table('retrieve_code',
        #Field('doc','reference doc'),
                Field('campaign','reference campaign'),
                Field('object_name','string'),
                Field('temp_url','string'),
                Field('dds_url','string'),
                Field('available_until','datetime', compute = lambda row : db(db.campaign.id == row.campaign).select(db.campaign.available_until,limitby = (0,1)).first().available_until) ,
                Field('rcode','string',unique=True,length=UUID_LENGTH, notnull=True)
                )
mysql_add_index('retrieve_code','rcode')
mysql_add_index('retrieve_code','object_name')

db.define_table('event_data',
                Field('campaign','reference campaign'),
                Field('doc','reference doc'),
                Field('category','string'),
                Field('event_type','string'),
                Field('event_data','string'),
                Field('event_json','json',default='{}'),
                Field('response_status_code','integer'),
                Field('bandwith_consumed','integer'),
                Field('created_by_task','string',length=UUID_LENGTH),
                #Field('from_ip','string'),
                auth.signature)

db.define_table('mg_event',
            Field('campaign','reference campaign',notnull=True),
            Field('doc','reference doc',notnull=True),
            Field('event_id','string',default=uuid.uuid4(),length=UUID_LENGTH,unique=True,notnull=True),
            Field('is_webhook','boolean',default = False,readable=False),
            Field('webhook_token','string',readable=False),
            Field('event_timestamp_dt','datetime',notnull=True), # UTC time
            # Field('event_local_dt','datetime',notnull=True),
            Field('event_timestamp','double',notnull=True), #unix EPOCH
            Field('event_ip','string',length=15),
            Field('event_','string',length=15,notnull=True),
            Field('event_log_level','string',length=10),
            Field('event_recipient','string'),
            Field('event_campaigns','list:string'),
            Field('event_tags','list:string'),
            Field('event_client_type','string'),
            Field('event_client_os','string'),
            Field('event_client_device_type','string'),
            Field('event_client_name','string'),
            Field('event_client_user_agent','string'),
            Field('event_geolocation_country','string'),
            Field('event_geolocation_region','string'),
            Field('event_geolocation_city','string'),
            Field('event_json','json')
            )
mysql_add_index('mg_event','event_id')
mysql_add_index('mg_event','webhook_token')
db.mg_event.event_timestamp_dt.represent = lambda value,row: value.strftime(DEFAULT_DATETIME_FORMAT) if value else ''

if not auth.db(auth.db.auth_group.role == 'advanced_scheduler_viewer').count():
    auth.add_group('advanced_scheduler_viewer', 'Users that can view more scheduler details')