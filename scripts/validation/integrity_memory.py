"""Measure integrity's project-size allocation using synthetic pending metadata."""
import argparse,json,sqlite3,subprocess,sys,tempfile,time
from pathlib import Path
from runtime_metrics import peak_rss_mib
p=argparse.ArgumentParser();p.add_argument('--repository',type=Path,required=True);p.add_argument('--manifest',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--one-count',type=int);p.add_argument('--check-project',type=Path)
a=p.parse_args();sys.path.insert(0,str(a.repository.resolve()))
if a.check_project:
    from archive_scout.database.connection import open_database
    from archive_scout.projects.integrity import check_project_integrity
    db=open_database(a.check_project)
    assert [r[0] for r in db.execute('PRAGMA quick_check')]==['ok']
    before=peak_rss_mib();started=time.perf_counter()
    report=check_project_integrity(a.check_project,db)
    out={'metadata_captures':db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],
         'payload_files':0,'seconds':time.perf_counter()-started,
         'before_peak_rss_mib':before,'after_peak_rss_mib':peak_rss_mib(),'report_bytes':report.stat().st_size,
         'measurement_note':'Fresh process after fixture creation; pre-check quick_check included in initial peak.'}
    db.close();print(json.dumps(out))
elif a.one_count:
    from archive_scout.database.connection import open_database
    from archive_scout.projects.integrity import check_project_integrity
    with tempfile.TemporaryDirectory() as temp:
        root=Path(temp);db=open_database(root)
        db.execute('ATTACH DATABASE ? AS fixture',(str(a.manifest.resolve()),))
        with db:
            db.execute("INSERT OR IGNORE INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,state,created_at,updated_at) SELECT original_url,timestamp,'resource-probe',mimetype,statuscode,'pending',created_at,updated_at FROM fixture.captures WHERE id<=?",(a.one_count,))
            assert db.execute('SELECT COUNT(*) FROM captures').fetchone()[0]==a.one_count
        db.execute('DETACH DATABASE fixture');db.close()
        db=open_database(root)
        assert [r[0] for r in db.execute('PRAGMA quick_check')]==['ok']
        db.close()
        result=subprocess.run([sys.executable,__file__,'--repository',str(a.repository.resolve()),
                               '--manifest',str(a.manifest.resolve()),'--output',str(a.output),
                               '--check-project',str(root)],capture_output=True,text=True,check=True)
        print(result.stdout.strip())
else:
    out=[]
    for count in (100000,1000000):
        result=subprocess.run([sys.executable,__file__,'--repository',str(a.repository.resolve()),'--manifest',str(a.manifest.resolve()),'--output',str(a.output),'--one-count',str(count)],capture_output=True,text=True)
        if result.returncode:
            out.append({'metadata_captures':count,'failed':True,'stderr':result.stderr})
            continue
        out.append(json.loads(result.stdout))
    a.output.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out,indent=2))
