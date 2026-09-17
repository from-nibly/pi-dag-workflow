import subprocess,time,json,concurrent.futures
from pathlib import Path
root=Path('/tmp/n04-implementation/final')
def run(name):
 start=time.monotonic()
 with (root/(name+'.log')).open('w') as f:
  p=subprocess.run(['timeout','3600','node','scripts/'+name+'.mjs'],stdout=f,stderr=subprocess.STDOUT)
 result={'command':'node scripts/'+name+'.mjs','timeoutSeconds':3600,'exit':p.returncode,'durationSeconds':round(time.monotonic()-start,3),'log':str(root/(name+'.log'))}
 (root/(name+'.json')).write_text(json.dumps(result,indent=2));print(result,flush=True)
 return result
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
 results=list(pool.map(run,['dag-v2-git-acceptance-test','dag-v2-lifecycle-test','dag-v2-context-test','dag-v2-state-test','git-integration-test','dag-runtime-test','dag-v2-git-test']))
(root/'results.json').write_text(json.dumps(results,indent=2))
raise SystemExit(any(r['exit'] for r in results))
