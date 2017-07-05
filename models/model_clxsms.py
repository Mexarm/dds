def get_clx_conf():
    token = myconf.get('clx.token')
    url = myconf.get('clx.url')
    sp_id = myconf.get('clx.service_plan_id')
    return (url,sp_id,token)

def test((url,sp_id,token)):
    headers = dict( Authorization = 'Bearer {}'.format(token))
    return requests.get(url+'/{}/{}'.format(sp_id,'batches'), headers = headers)
#>>> r.json()
#{u'count': 0, u'batches': [], u'page': 0, u'page_size': 0}
def send_SMS_batch(campaign_id,oseq_beg,oseq_end):
    t0 = time.time()
    docs = db((db.doc.osequence>=oseq_beg)&(db.doc.osequence<=oseq_end)&
              (db.doc.campaign==campaign_id)&(db.doc.status=='validated')).select()
    if not docs:
        db(db.scheduler_task.id == W2PTASK.id).update(repeats = 1)
        return
    campaign = get_campaign(campaign_id)
    t1= time.time()
    for d in docs:
        mg_acceptance_time = compute_acceptance_time(d.deliverytime) if d.deliverytime else campaign.mg_acceptance_time
        if mg_acceptance_time <= datetime.datetime.now():
            send_doc_wrapper(d.id)
        else:
            if min_datetime > mg_acceptance_time:
                min_datetime = mg_acceptance_time
    if min_datetime:
        db(db.scheduler_task.id == W2PTASK.id).update( next_run_time=min_datetime)
    t2= time.time()
    return dict(docs=len(docs),prepare_time= t1-t0,loop= t2-t1)
