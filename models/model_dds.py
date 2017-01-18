# -*- coding: utf-8 -*-
from os import path
from automaton import machines #http://docs.openstack.org/developer/automaton/examples.html#creating-a-simple-machine
import pickle
import uuid

#
#http://ckeditor.com/ para la edicion del body online
#https://www.tinymce.com/ otro
#ticket error pyrax
#https://172.20.1.105/admin/default/ticket/dds/172.20.1.231.2016-12-09.18-52-56.f0f57a04-acb2-4fce-9f7b-78204db83907
#http://www.mail-tester.com/

SERVICE_TYPE=['Cloudfiles Temp URL', 'DDS Server URL']
#doc_local_states =['registered', 'ready', 'rejected']

DOC_LOCAL_STATE_OK = [ 'initial', 'validating','cf validated', 'queued (local)', 'queued (mailgun)' ]
DOC_LOCAL_STATE_ERR = [ 'cf not valid','rejected (mailgun)' ]
UUID_LENGTH = 36
REQUIRED_FIELDS = ['record_id','object_name','email_address'] #required fields in the index,csv file
DAEMON_TASKS = [ 'progress_tracking', 'status_changer']
DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def compute_acceptance_time(dt):
    import datetime
    from dateutil.relativedelta import relativedelta

    if dt > datetime.datetime.now():
        return dt + relativedelta(days=-2)
    else:
        return dt

db.define_table('campaign',
                Field('uuid','string',default=uuid.uuid4(), label='Campaign UUID', writable=False,readable=False),
                Field('mg_domain','string',label = 'Mailgun Domain'),
                Field('mg_campaign_name','string',notnull=True,label=T('Mailgun Campaign Name')),
                Field('mg_campaign_id','string',label=T('Mailgun Campaign id'),writable=False,
                    compute= lambda (row): get_mg_campaign(mg_get_campaigns(row.mg_domain),row.mg_campaign_name)['id']), #retrieve mg_campaign_id from mailgun ),
                Field('test_mode','boolean',label='test mode (Mailgun will accept the messages but not deliver)'),
                Field('cf_container_folder','string',notnull=True,label='Cloudfiles Container/Folder',
                     requires=IS_NOT_IN_DB(db, 'campaign.cf_container_folder')), #unique=True, gives error in mysql <class 'gluon.contrib.pymysql.err.InternalError'> (1071, u'Specified key was too long; max key length is 767 bytes')
                Field('index_file','string',default='index.csv',notnull=True,label='archivo indice (.CSV)'),
                Field('service_type','string',notnull=True,label=T('Service type'),
                      default=SERVICE_TYPE[0], requires = IS_IN_SET(SERVICE_TYPE)),
                Field('available_from','datetime',notnull=True),
                Field('mg_acceptance_time','datetime', writable=False, label='Mailgun acceptance time', compute=lambda(row): compute_acceptance_time(row.available_from)),
                Field('available_until','datetime'),
                Field('datetime_format', 'string', label='datetime import format',default=DEFAULT_DATETIME_FORMAT),
                Field('is_active','boolean',notnull=True,default=True),
                Field('status','string',default=FM.states[0], writable=False), #Created, Verified, Active, Failed
                Field('status_progress','float',default='',writable=False),
                Field('current_task','string',writable=False),
                Field('delete_documents_on_expire','boolean',default=True), #Delete all documents listed on the index file on the date entered on the available_until field.
                Field('total_campaign_recipients','integer',writable=False),
                Field('total_campaign_bytes','integer',writable=False), #total size of the container
                Field('container_objects','integer',writable=False),
                Field('container_bytes','integer',writable=False),
                Field('download_limit','integer',notnull=True,default=0), #maximun times that each file can be downloaded, 0= no limit, only valid when service type is DDS Server URL
                Field('maximum_bandwith', 'integer',notnull=True,default=0), #limit the maximum bandwith to consume in bytes, 0= no limit, only valid when service type is DDS Server URL
                Field('mg_tags','list:string',label='Mailgun tags (3 maximum)'),
                Field('from_name','string',label='From address name',default='Notifications (No Reply)'),
                Field('from_address','string',requires = IS_EMAIL(), label = 'From email address'),
                Field('test_address','string',requires = IS_EMAIL(), label = 'email address to sent tests' ),
                Field('email_subject','string',notnull=True, default=T('Your Document is Ready'),label=T('email subject')),
                Field('html_body','text',notnull=True, default=''),
                Field('logo','upload', uploadfield='logo_file'),
                Field('logo_file', 'blob'),
                Field('tasks','list:reference scheduler_task', default =[],writable=False,readable=False),
                Field('fm','text',default=pickle.dumps(FM),writable=False,readable=False), #finite state machine
                Field('fm_history','list:string', default=[],writable=False,readable=False),
                Field('mg_stats','json',default='{}',readable=False,writable=False),
                Field('mg_stats_unique','json',default='{}',readable=False,writable=False),
                Field('send_tasks_stats','json',readable=False,writable=False),
                Field('send_retry_active','boolean',readable=False,writable=False),
                auth.signature)

db.define_table('doc', Field('campaign','reference campaign'),
                Field('record_id','string',notnull=True),                #required on index.csv fieldname = record_id
                Field('object_name','string',notnull=True), #unique=True?              #required on index.csv fieldname = object_name
                Field('email_address','string',notnull=True),            #required on index.csv fieldname = email_address
                Field('deliverytime','datetime'), 
                Field('json','json',default = '{}'),
                Field('checksum','string',default=0),
                Field('bytes','integer',default=0),
                Field('download_limit','integer',default=0),
                Field('download_counter','integer',default=0),
                Field('send_task','integer'),  #'reference scheduler_task' or integer
                Field('validation_task','integer'),
                Field('status','string',default=DOC_LOCAL_STATE_OK[0]),
                Field('send_retry_active','boolean'),
                Field('mailgun_id','string')
                )

db.define_table('retrieve_code', Field('doc','reference doc'),
                Field('campaign','reference campaign'),
                Field('temp_url','string'),
                Field('dds_url','string'),
                Field('available_until','datetime', compute = lambda row : db(db.campaign.id == row.campaign).select(db.campaign.available_until,limitby = (0,1)).first().available_until) ,
                Field('rcode','string',unique=True,length=UUID_LENGTH, notnull=True)
                )

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
                Field('from_ip','string'),
                auth.signature)
