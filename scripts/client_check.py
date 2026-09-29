#!/usr/bin/env python3
"""Run owned, isolated real-client checks; never infer a pass from API advertising."""
from __future__ import annotations
import argparse, datetime, hashlib, json, os, platform, re, shutil, socket, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
CLIENTS = {'go': 'franz-go', 'python': 'kafka-python', 'node': 'KafkaJS', 'java': 'Apache Kafka'}
BASE_CHECKS = {'create-topic', 'produce-acks', 'byte-fidelity', 'key-fidelity', 'partition-order'}
LIMITS = ['Single-node, one-partition PLAINTEXT fixture on loopback.', 'Idempotent producers, transactions, replication and exactly-once processing are not implemented.', 'Modern consumer group protocol and Kafka Streams are not implemented; Java uses group.protocol=classic.', 'Go and Python examples consume directly without a group. Node and Java additionally check a classic group and committed offsets.', 'A pass covers the listed checks and pinned client only; it is not complete Kafka conformance, a security test or a production recommendation.', 'Imported reports are self-reported measurements; fingerprints aid reproduction and are not a signature.']
def advertised():
    s = (ROOT/'internal/wire/api_versions.go').read_text()
    block = s.split('var advertisedAPIs = []apiVersionRange{', 1)[1].split('\n}', 1)[0]
    rows = re.findall(r'\{APIKey: int16\(kmsg\.(\w+)\), MinVersion: (\d+), MaxVersion: (\d+)\}', block)
    if not rows or len(rows) != block.count('APIKey:') or len({r[0] for r in rows}) != len(rows):
        raise ValueError('Cannot parse complete advertised API table; update exporter.')
    return [{'name': n, 'min_version': int(lo), 'max_version': int(hi)} for n, lo, hi in rows]
def fingerprint():
    names = [p for d in ('cmd','internal','examples') for p in (ROOT/d).rglob('*') if p.is_file() and not any(x in p.parts for x in ('node_modules','target','__pycache__')) and p.suffix in ('.go','.py','.mjs','.java','.json','.xml','.txt')]
    names += [ROOT/'go.mod',ROOT/'go.sum',ROOT/'VERSION',Path(__file__).resolve()]
    h = hashlib.sha256()
    for p in sorted(set(names)):
        h.update(p.relative_to(ROOT).as_posix().encode()+b'\0'+p.read_bytes()+b'\0')
    try:
        commit = subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True,stderr=subprocess.DEVNULL).strip()
        dirty = bool(subprocess.check_output(['git','status','--porcelain','-uno'],cwd=ROOT,text=True,stderr=subprocess.DEVNULL))
    except (OSError,subprocess.CalledProcessError):
        commit,dirty = None,None
    return {'commit':commit,'modified_checkout':dirty,'content_sha256':h.hexdigest(),'version':(ROOT/'VERSION').read_text().strip()}
def parse_receipt(stdout, language):
    receipts = []
    for line in stdout.splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value,dict) and value.get('schema')=='kafka-wire.client-check':
            receipts.append(value)
    if len(receipts)!=1:
        raise ValueError('Expected exactly one structured client receipt.')
    value = receipts[0]
    if value.get('version')!=1 or value.get('language')!=language or value.get('client')!=CLIENTS[language] or value.get('status')!='passed' or value.get('records')!=5:
        raise ValueError('Client receipt identity, record count or status did not match.')
    checks = value.get('checks',[])
    required = BASE_CHECKS | ({'classic-group','offset-commit-fetch'} if language in ('node','java') else set())
    if not isinstance(checks,list) or not all(isinstance(x,str) for x in checks) or not required.issubset(checks):
        raise ValueError('Client receipt omitted a required real check.')
    if not isinstance(value.get('client_version'),str) or value['client_version'] in ('','unknown','(devel)'):
        raise ValueError('Client library version was not recorded.')
    expected={'idempotence':False,'compression':'none','partitions':1,'security':'PLAINTEXT'}
    if not isinstance(value.get('settings'),dict) or any(value['settings'].get(k)!=v for k,v in expected.items()):
        raise ValueError('Client settings did not match fixture.')
    return value

def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));return sock.getsockname()[1]
def invoke(command,cwd,env,log,timeout=240):
    try:
        p=subprocess.run(command,cwd=cwd,env=env,capture_output=True,text=True,timeout=timeout)
        log.write_text(p.stdout+'\n'+p.stderr)
        if p.returncode:raise RuntimeError('Command exited '+str(p.returncode)+'; see '+log.name)
        return p.stdout
    except subprocess.TimeoutExpired as exc:
        log.write_text('Command timed out.\n')
        raise RuntimeError('Command timed out; see '+log.name) from exc

def run(output,languages):
    output.mkdir(parents=True,exist_ok=False)
    report={'schema':'kafka-wire.compatibility','version':1,'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':fingerprint(),'environment':{'platform':platform.platform(),'python':platform.python_version()},'advertised_apis':advertised(),'clients':[{'language':n,'client':c,'status':'unverified','checks':[]} for n,c in CLIENTS.items()],'limitations':LIMITS}
    # Drop inherited broker configuration. The run has no access to an existing broker or data directory.
    env={k:v for k,v in os.environ.items() if not k.startswith('KAFKA_WIRE_')}
    broker=None
    try:
        with tempfile.TemporaryDirectory(prefix='kafka-wire-check-') as temporary:
            tmp=Path(temporary);binary=tmp/('kafka-wire.exe' if os.name=='nt' else 'kafka-wire')
            invoke(['go','build','-o',str(binary),'./cmd/kafka-wire'],ROOT,env,output/'build.log')
            kp,ap=port(),port()
            while ap==kp:ap=port()
            env.update({'KAFKA_WIRE_LISTENERS_KAFKA':f'127.0.0.1:{kp}','KAFKA_WIRE_LISTENERS_ADMIN':f'127.0.0.1:{ap}','KAFKA_WIRE_STORAGE_DATADIR':str(tmp/'data'),'KAFKA_WIRE_BROKERS':f'127.0.0.1:{kp}'})
            with (output/'broker.log').open('w') as log:
                broker=subprocess.Popen([str(binary),'serve'],cwd=tmp,env=env,stdout=log,stderr=subprocess.STDOUT)
                try:
                    for attempt in range(100):
                        if broker.poll() is not None:raise RuntimeError('Owned broker exited before health check.')
                        try:
                            with urllib.request.urlopen(f'http://127.0.0.1:{ap}/health',timeout=.5) as res:
                                if res.status==200:break
                        except OSError:pass
                        time.sleep(.1)
                    else:raise RuntimeError('Owned broker did not become healthy.')
                    for row in report['clients']:
                        lang=row['language']
                        if lang not in languages:continue
                        start=time.perf_counter()
                        try:
                            cwd=ROOT
                            if lang=='go':command=['go','run','./examples/go']
                            elif lang=='python':
                                venv=tmp/'python-client'
                                invoke([sys.executable,'-m','venv',str(venv)],ROOT,env,output/'python-setup.log')
                                python=venv/('Scripts/python.exe' if os.name=='nt' else 'bin/python')
                                invoke([str(python),'-m','pip','install','--disable-pip-version-check','-r',str(ROOT/'examples/python/requirements.txt')],ROOT,env,output/'python-install.log')
                                command=[str(python),str(ROOT/'examples/python/roundtrip.py')]
                            elif lang=='node':
                                cwd=ROOT/'examples/nodejs'
                                invoke(['npm','ci','--ignore-scripts','--no-audit','--no-fund'],cwd,env,output/'node-install.log')
                                command=['node','roundtrip.mjs']
                            else:
                                cwd=ROOT/'examples/java';command=['mvn','-q','--batch-mode','compile','exec:java']
                            receipt=parse_receipt(invoke(command,cwd,env,output/(lang+'.log'),timeout=180),lang)
                            row.update(receipt);row['duration_seconds']=round(time.perf_counter()-start,3)
                        except (OSError,RuntimeError,ValueError) as exc:
                            row.update({'status':'failed','reason':str(exc),'duration_seconds':round(time.perf_counter()-start,3)})
                        print(lang+': '+row['status'],flush=True)
                finally:
                    broker.terminate()
                    try:broker.wait(timeout=15)
                    except subprocess.TimeoutExpired:broker.kill();broker.wait(timeout=5)
    except (OSError,RuntimeError,ValueError) as exc:
        report['run_error']=str(exc)
    (output/'compatibility.json').write_text(json.dumps(report,indent=2)+'\n')
    for name in ('index.html','dashboard.css','dashboard.js'):
        shutil.copyfile(ROOT/'docs/compatibility'/name,output/name)
    return 1 if report.get('run_error') or any(row['status']!='passed' for row in report['clients'] if row['language'] in languages) else 0

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--clients',default='go,python,node,java')
    args=p.parse_args();languages=args.clients.split(',')
    if not languages or any(x not in CLIENTS for x in languages) or len(set(languages))!=len(languages):p.error('clients must be unique go,python,node,java values')
    if args.output.exists():p.error('output directory already exists; choose a new path')
    return run(args.output.resolve(),languages)
if __name__=='__main__':sys.exit(main())
