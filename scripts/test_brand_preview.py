"""Run modules in separate processes, as required by existing DB test fixtures."""
import os
import subprocess
import sys
from pathlib import Path

root=Path(__file__).resolve().parents[1]
env={**os.environ,'DATABASE_URL':'sqlite:///:memory:','GOOGLE_ANALYTICS_ID':''}
failed=[]
for test in sorted((root/'tests').glob('test_*.py')):
    result=subprocess.run([sys.executable,'-m','unittest','discover','-s','tests','-p',test.name],cwd=root,env=env,capture_output=True,text=True)
    output=result.stdout+result.stderr
    print(test.name, 'PASS' if result.returncode==0 else 'FAIL', next((line for line in output.splitlines() if line.startswith('Ran ')),''),flush=True)
    if result.returncode:
        failed.append(test.name)
        print(output[-12000:],flush=True)
sys.exit(bool(failed))
