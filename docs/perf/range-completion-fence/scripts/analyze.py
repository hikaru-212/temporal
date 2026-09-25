import collections, csv, glob, json, math, os, statistics
root='/tmp/temporal-range-pilot-20260925/results'
def stats(xs):
 xs=sorted(xs)
 return {'n':len(xs),'mean':statistics.mean(xs) if xs else 0,'p50':statistics.median(xs) if xs else 0,'p95':xs[math.ceil(.95*len(xs))-1] if xs else 0,'max':max(xs,default=0)}
runs=[]; summaries=[]
for path in sorted(glob.glob(root+'/*-trial*.samples.jsonl')):
 data=[json.loads(s) for s in open(path)]
 if not data: continue
 backend,rev,trial=data[0]['backend'],data[0]['revision'],data[0]['trial']
 prefix=path.removesuffix('.samples.jsonl')
 resource=[json.loads(s) for s in open(prefix+'.resources.jsonl')]
 changes={k:max((r[k] for r in resource),default=0)-min((r[k] for r in resource),default=0) for k in ['pswpin','pswpout','oom_kill']}
 active_swap=sum(x.get('swap_in_pages',0)+x.get('swap_out_pages',0) for x in data)
 log=open(prefix+'.log').read(); success='\nPASS\n' in log
 max_container_swap=max((int(c['memory.swap.current']) for r in resource for c in r.get('containers',{}).values()),default=-1)
 valid=success and changes['pswpout']==0 and changes['oom_kill']==0 and max_container_swap==0
 overrides_path=root+'/validity-overrides.json'
 if os.path.exists(overrides_path):
  overrides=json.load(open(overrides_path))
  valid=overrides.get(f'{backend}-{rev}-trial{trial}',{}).get('valid',valid)
 runs.append({'backend':backend,'revision':rev,'trial':trial,'valid':valid,'success':success,'resource_deltas':changes,'active_swap_pages':active_swap,'max_container_swap_bytes':max_container_swap,'min_available_kib':min(int(r['MemAvailable'].split()[0]) for r in resource),'min_disk_bytes':min(r['disk_free_bytes'] for r in resource)})
 groups=collections.defaultdict(list)
 for x in data:
  if x['operation'] in ['cleanup','workflow_update','shard_update'] and not x['warmup']:
   groups[(x['scenario'],x['rows_expected'],x['operation'],'all')].append(x)
   if x['operation']=='workflow_update' and x['overlap']: groups[(x['scenario'],x['rows_expected'],x['operation'],'overlap')].append(x)
 for (scenario,n,op,subset),samples in groups.items():
  active=sum(x.get('active_ns',0) for x in data if x['operation']=='case_total' and x['scenario']==scenario and x['rows_expected']==n)/1e9
  row={'backend':backend,'revision':rev,'trial':trial,'valid':valid,'scenario':scenario,'rows':n,'operation':op,'subset':subset,**stats([x['duration_ns']/1e6 for x in samples]),'errors':dict(collections.Counter(x['error_type'] for x in samples if x.get('error_type'))),'queries':sum(x['driver_queries'] for x in samples),'batches':sum(x['driver_batches'] for x in samples),'retry_callbacks':sum(x['driver_retry_callbacks'] for x in samples),'driver_errors':sum(x['driver_errors'] for x in samples),'lock_waits':sum(x['lock_wait_observed'] for x in samples),'overlaps':sum(x['overlap'] for x in samples),'active_seconds':active,'writes_per_sec':sum(not x.get('error') for x in samples)/active if active and op=='workflow_update' and subset=='all' else None}
  summaries.append(row)
with open(root+'/trial-validity.json','w') as f: json.dump(runs,f,indent=2)
with open(root+'/summary.json','w') as f: json.dump(summaries,f,indent=2)
if summaries:
 with open(root+'/summary.csv','w') as f:
  w=csv.DictWriter(f,fieldnames=summaries[0].keys());w.writeheader();w.writerows(summaries)
aggregate=[]
for backend in ['PostgreSQL','Cassandra']:
 for rev in ['base','patch']:
  valid_trials={r['trial'] for r in runs if r['backend']==backend and r['revision']==rev and r['valid']}
  data=[]
  for trial in valid_trials:
   data.extend(json.loads(s) for s in open(f'{root}/{backend}-{rev}-trial{trial}.samples.jsonl'))
  for n in [1000,10000]:
   for scenario,op,subset in [('isolated','cleanup','all'),('mixed','workflow_update','all'),('mixed','workflow_update','overlap'),('mixed','cleanup','all'),('contention','cleanup','all'),('contention','shard_update','all')]:
    xs=[x for x in data if x['scenario']==scenario and x['operation']==op and x['rows_expected']==n and not x['warmup'] and (subset=='all' or x['overlap'])]
    if not xs:continue
    active=sum(x.get('active_ns',0) for x in data if x['operation']=='case_total' and x['scenario']==scenario and x['rows_expected']==n)/1e9
    aggregate.append({'backend':backend,'revision':rev,'scenario':scenario,'rows':n,'operation':op,'subset':subset,**stats([x['duration_ns']/1e6 for x in xs]),'writes_per_sec':len(xs)/active if active and op=='workflow_update' and subset=='all' else None,'lock_waits':sum(x['lock_wait_observed'] for x in xs),'errors':sum(bool(x.get('error')) for x in xs),'retry_callbacks':sum(x['driver_retry_callbacks'] for x in xs),'driver_errors':sum(x['driver_errors'] for x in xs)})
with open(root+'/aggregate.json','w') as f:json.dump(aggregate,f,indent=2)
print('VALIDITY')
for r in runs:print(json.dumps(r))
print('AGGREGATES (latencies ms)')
for r in aggregate:print(json.dumps(r))
print('PAIRED MIXED')
for backend in ['PostgreSQL','Cassandra']:
 for n in [1000,10000]:
  for trial in [1,2,3]:
   xs=[r for r in summaries if r['backend']==backend and r['rows']==n and r['trial']==trial and r['scenario']=='mixed' and r['operation']=='workflow_update' and r['subset']=='all']
   if len(xs)!=2:continue
   d={r['revision']:r for r in xs};a,b=d['base'],d['patch']
   print(backend,n,trial,'base/patch ops/s',round(a['writes_per_sec'],2),round(b['writes_per_sec'],2),'delta%',round(100*(b['writes_per_sec']/a['writes_per_sec']-1),2),'base/patch p95',round(a['p95'],4),round(b['p95'],4),'delta%',round(100*(b['p95']/a['p95']-1),2))
