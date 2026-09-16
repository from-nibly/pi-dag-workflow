set -euo pipefail
git diff 01947f9 --check
git diff --cached --check
python3 -I -B -c 'import ast; from pathlib import Path; [ast.parse(Path(p).read_text(), filename=p) for p in ["extensions/dag-workflow/runtime-v2/command-supervisor.py", "scripts/fixtures/command-fork-handoff.py", "scripts/fixtures/command-owner-subreaper.py"]]; print("Python syntax OK")'
bash -n .ai/ordinary-legacy-plus/N03/subreaper-01947f9/verify.sh
