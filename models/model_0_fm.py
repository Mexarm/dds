# -*- coding: utf-8 -*-
from automaton import machines #http://docs.openstack.org/developer/automaton/examples.html#creating-a-simple-machine
from automaton import exceptions
import pickle


def validate_documents(campaign_id):
    #delete docs,retrieve codes, and event_data if this is a retry
    db(db.doc.campaign == campaign_id).delete()
    db(db.retrieve_code.campaign == campaign_id).delete()
    db(db.event_data.campaign == campaign_id).delete()
    reset_campaign_progress(campaign_id)
    db.commit()
    campaign = get_campaign(campaign_id)
    ret = scheduler.queue_task(register_on_db,
                               pvars=dict(campaign_id=campaign_id),
                               timeout=86400, # 1 day
                               sync_output=300,
                               immediate=True)
    tasks = campaign.tasks
    tasks = tasks + [ret.id] if tasks else [ret.id]
    db(db.campaign.id == campaign_id).update(tasks=tasks)

def send_test(campaign_id):
    campaign = get_campaign(campaign_id)
    first_doc = db((db.doc.campaign == campaign_id) & (db.doc.status == 'validated')
                  ).select(limitby=(0, 1))
    sample_set = db((db.doc.campaign == campaign_id) & (db.doc.status == 'validated') &
                    (db.doc.is_sample == True)
                   ).select(db.doc.id, limitby=(0, myconf.get('dds.max_samples')))
    tasks = campaign.tasks or []
    for doc in sample_set or first_doc:
        tasks.append(
            scheduler.queue_task(send_doc_wrapper,
                                 pargs=[doc.id],
                                 pvars=dict(to=campaign.test_address.split(','),
                                            is_sample=True,
                                            update_doc=False,
                                            ignore_delivery_time=True),
                                 immediate=True,
                                 group_name=WGRP_SENDERS1).id)
    db(db.campaign.id == campaign_id).update(tasks=tasks)

def launch_campaign(campaign_id):
    campaign = db.campaign(campaign_id)
    period = myconf.get('retry.period')
    retry_failed = myconf.get('retry.retry_failed')
    timeout = myconf.get('retry.mailgun_timeout')
    i = myconf.get('task.load')
    if campaign.mg_acceptance_time > datetime.datetime.now():
        raise ValueError("can not launch campaign before {}".format(c.mg_acceptance_time)) # only execute this on or after campaign.mg_acceptance_time
    max = db.doc.osequence.max()
    e = campaign.total_campaign_recipients or db(db.doc.campaign == campaign_id).select(max).first()[max]
    tasks=[]
    for r in get_ranges(1,e,i):
        send_task = scheduler.queue_task(send_doc_set,
                    pvars=dict(campaign_id=campaign_id,oseq_beg=r[0],oseq_end=r[1]),
                    timeout = timeout*(r[1]-r[0]), period = period, retry_failed = -1,
                    group_name=WGRP_SENDERS)
        tasks.append(send_task.id)
    campaign.tasks = campaign.tasks + tasks if campaign.tasks else tasks
    r = campaign.update_record()
    db.commit()
    return dict(result = '{} send_doc_set created'.format(len(tasks)))

def schedule_launch_campaign(campaign_id):
    c = get_campaign(campaign_id)
    reset_campaign_progress(campaign_id)
    task = scheduler.queue_task(launch_campaign,
                                pargs=[campaign_id],
                                start_time=c.mg_acceptance_time,
                                next_run_time=c.mg_acceptance_time)
    c.tasks = c.tasks + [task.id] if c.tasks else [task.id]
    r = c.update_record()
    return task

def finished_campaign(campaign_id):
    #finish the campaign, delete storage files etc
    c = get_campaign(campaign_id)
    if c.delete_documents_on_expire:
        task = scheduler.queue_task(delete_files,
                                    pargs=[campaign_id],
                                    group_name=WGRP_FINISHERS)
        c.tasks = c.tasks + [task.id] if c.tasks else [task.id]
        r = c.update_record()
        return task

actions = {
    'validating documents' : validate_documents,
    'in approval': send_test,
    'queueing': schedule_launch_campaign,
    'finished': finished_campaign
    }

def fm_on_enter(new_state, triggered_event):
    return actions[new_state] if new_state in actions else None

FM = machines.FiniteMachine()

FM.add_state('defined')
FM.add_state('validating documents')
FM.add_state('documents ready')
FM.add_state('documents error')
FM.add_state('in approval')
FM.add_state('approved')
FM.add_state('queueing')
FM.add_state('live')
FM.add_state('scheduled')
FM.add_state('finished', terminal=True)

FM.add_transition('defined','validating documents','validate documents')
FM.add_transition('validating documents','documents ready','_valid docs')
FM.add_transition('validating documents','documents error','_not valid docs')
FM.add_transition('documents ready','in approval','send test')
FM.add_transition('documents error','validating documents','validate documents')
FM.add_transition('in approval','in approval','send test')
FM.add_transition('in approval','approved','approve')
FM.add_transition('approved','queueing','launch campaign')
FM.add_transition('approved','in approval','revert approval')
FM.add_transition('queueing','live','_go live')
FM.add_transition('queueing','scheduled','_go scheduled')
FM.add_transition('scheduled','live','_go live')
FM.add_transition('live','finished','_finish')

FM.default_start_state = 'defined'
FM.initialize()

FM_STATES_WITH_PROGRESS_TRACKING = ['validating documents','queueing']
FM_STATES_TO_UPDATE = ['validating documents', 'queueing', 'scheduled', 'live']

#print (FM.pformat())
"""
+----------------------+--------------------+----------------------+----------+---------+
|        Start         |       Event        |         End          | On Enter | On Exit |
+----------------------+--------------------+----------------------+----------+---------+
|       approved       |  launch campaign   |       queueing       |    .     |    .    |
|       approved       |  revert approval   |     in approval      |    .     |    .    |
|     @defined[^]      | validate documents | validating documents |    .     |    .    |
|   documents error    | validate documents | validating documents |    .     |    .    |
|   documents ready    |     send test      |     in approval      |    .     |    .    |
|     finished[$]      |         .          |          .           |    .     |    .    |
|     in approval      |      approve       |       approved       |    .     |    .    |
|     in approval      |     send test      |     in approval      |    .     |    .    |
|         live         |      _finish       |       finished       |    .     |    .    |
|       queueing       |      _go live      |         live         |    .     |    .    |
|       queueing       |   _go scheduled    |      scheduled       |    .     |    .    |
|      scheduled       |      _go live      |         live         |    .     |    .    |
| validating documents |  _not valid docs   |   documents error    |    .     |    .    |
| validating documents |    _valid docs     |   documents ready    |    .     |    .    |
+----------------------+--------------------+----------------------+----------+---------+
"""

def FM_get(campaign_id):
    campaign = db(db.campaign.id == campaign_id).select(db.campaign.fm,db.campaign.fm_history).first()
    return (pickle.loads(campaign.fm),campaign.fm_history)

def FM_store(campaign_id,fm,fm_history):
    campaign= get_campaign(campaign_id)
    status=fm.current_state
    campaign.update_record(status = status, fm=pickle.dumps(fm),fm_history = fm_history)
    db.commit()

def FM_get_user_options(fm):
    return [o for o in fm._transitions[fm.current_state].keys() if o[0] != "_" ]

def FM_process_event(campaign_id,fm_event):
    from datetime import datetime
    fm,fm_history = FM_get(campaign_id)
    cstate=fm.current_state
    fm.process_event(fm_event)
    if auth.user:
        by = auth.user.email
    else:
        by = "System" #when it runs inside the scheduler worker
    fm_history += ["{} by {}:{},{},{}".format(str(datetime.now()), by ,cstate,fm_event,fm.current_state) ]
    FM_store(campaign_id,fm,fm_history)
    f = fm_on_enter(fm.current_state,fm_event)
    if f:
        def closure():
            return f(campaign_id) #returns the function or it could be called here ?
        return closure
    else:
        return None

def get_fm_options(campaign_row):
    #import json
    options = FM_get_user_options(FM_get(campaign_row.id)[0])
    result=list()
    for o in options:
        result+=[(o,json.dumps(dict(campaign_id=campaign_row.id,
                                    campaign_name=campaign_row.campaign_name,
                                    event=o,
                                    url=URL('process_event',vars=dict(campaign_id = campaign_row.id, event=o)),
                                    url_fm_state=URL('get_fm_state',vars=dict(campaign_id=campaign_row.id)),
                                    url_fm_buttons=URL('get_fm_buttons',vars=dict(campaign_id=campaign_row.id))
                                   )))]
    return result

def get_bootstrap_status_style(status):
    primary = ['finished']
    success = ['live', 'scheduled']
    info   = ['defined', 'documents ready', 'approved']
    warning = ['validating documents', 'in approval', 'queueing']
    danger  = ['documents error']
    styles = [(primary,'primary'),
              (success,'success'),
              (info,'info'),
              (warning,'warning'),
              (danger,'danger')]
    for l,s in styles:
        if status in l:
            return s
    return 'info'

def get_bootstrap_btn_class(event):
    primary = ['validate documents'] 
    success = ['approve'] 
    info = ['send test'] 
    warning = ['revert approval'] 
    danger = ['launch campaign'] 
    classes = [(primary,'btn-primary'),
               (success,'btn-success'),
               (info,'btn-info'),
               (warning,'btn-warning'),
               (danger,'btn-danger')]
    for l,c in classes:
        if event in l:
            return c
    return 'btn-info'

def get_fm_action_buttons(campaign_id):
    campaign = get_campaign(campaign_id)
    buttons=[]
    for o,j in get_fm_options(campaign):
        buttons+= [ str(BUTTON(o,_type="button", _class="btn {} fm-action".format(get_bootstrap_btn_class(o)),  _data=j))]  #<button type="button" class="btn btn-info fm-action" data="{{=j}}">{{=o}}</button>
    return XML ( ''.join(buttons))