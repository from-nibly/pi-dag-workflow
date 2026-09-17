import subprocess,time,json
from pathlib import Path
root=Path('/tmp/n04-implementation/final');start=time.monotonic()
source=subprocess.check_output(['git','write-tree'],text=True).strip()
with (root/'native-certified.log').open('w') as f:
 p=subprocess.run(['timeout','3600','node','scripts/dag-v2-git-acceptance-test.mjs'],stdout=f,stderr=subprocess.STDOUT)
result={'command':'node scripts/dag-v2-git-acceptance-test.mjs','timeoutSeconds':3600,'exit':p.returncode,'durationSeconds':round(time.monotonic()-start,3),'log':str(root/'native-certified.log'),'sourceTree':source,'note':'Final full native suite; required built-in driver shadowing fails closed rather than executing repository commands.'}
(root/'native-certified.json').write_text(json.dumps(result,indent=2));print(result)
raise SystemExit(p.returncode)
