import subprocess,time,json
from pathlib import Path
root=Path('/tmp/n04-implementation/final')
deadline=time.monotonic()+14400
while not (root/'results.json').exists():
 if time.monotonic()>deadline: raise SystemExit('final suites did not finish')
 time.sleep(2)
results=[]
for scenario in ['semanticIntegrationRecovery','semanticIntegrationFailure']:
 command=['node','scripts/dag-dogfood-test.mjs','--scenario',scenario]
 start=time.monotonic()
 with (root/(scenario+'.log')).open('w') as f:
  p=subprocess.run(['timeout','3600',*command],stdout=f,stderr=subprocess.STDOUT)
 result={'command':' '.join(command),'timeoutSeconds':3600,'exit':p.returncode,'durationSeconds':round(time.monotonic()-start,3),'log':str(root/(scenario+'.log'))}
 results.append(result);print(result,flush=True)
 (root/(scenario+'.json')).write_text(json.dumps(result,indent=2))
(root/'dogfood-results.json').write_text(json.dumps(results,indent=2))
raise SystemExit(any(r['exit'] for r in results))
