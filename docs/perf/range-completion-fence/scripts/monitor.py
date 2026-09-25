import glob, json, os, pathlib, shutil, signal, sys, time
running=True
def stop(*_):
 global running
 running=False
signal.signal(signal.SIGTERM,stop)
def read(path):
 try:return pathlib.Path(path).read_text().strip()
 except OSError:return ''
with open(sys.argv[1],'w') as f:
 while running:
  vm=dict(line.split() for line in open('/proc/vmstat'))
  mem={line.split(':')[0]:line.split(':')[1].strip() for line in open('/proc/meminfo')}
  obj={'time':time.time(),'pswpin':int(vm['pswpin']),'pswpout':int(vm['pswpout']),'oom_kill':int(vm['oom_kill']),'MemAvailable':mem['MemAvailable'],'SwapFree':mem['SwapFree'],'disk_free_bytes':shutil.disk_usage('/').free,'load':os.getloadavg(),'host_cpu':read('/proc/stat').splitlines()[0],'memory_psi':read('/proc/pressure/memory'),'containers':{}}
  for path in glob.glob('/sys/fs/cgroup/system.slice/docker-*.scope'):
   obj['containers'][path.rsplit('/',1)[1]]={name:read(path+'/'+name) for name in ['cpu.stat','memory.current','memory.swap.current','memory.events','io.stat']}
  f.write(json.dumps(obj)+'\n'); f.flush(); time.sleep(1)
