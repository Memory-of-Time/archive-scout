"""Standard-library process metrics. Windows branch is supplied but untested here."""
import os,sys,threading
from pathlib import Path
try:
    import resource
except ImportError:
    resource=None

def peak_rss_mib():
    if resource is not None:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1024*1024 if sys.platform=='darwin' else 1024)
    if os.name=='nt':
        import ctypes
        from ctypes import wintypes
        class Counters(ctypes.Structure):
            _fields_=[('cb',wintypes.DWORD),('PageFaultCount',wintypes.DWORD),('PeakWorkingSetSize',ctypes.c_size_t),('WorkingSetSize',ctypes.c_size_t),('QuotaPeakPagedPoolUsage',ctypes.c_size_t),('QuotaPagedPoolUsage',ctypes.c_size_t),('QuotaPeakNonPagedPoolUsage',ctypes.c_size_t),('QuotaNonPagedPoolUsage',ctypes.c_size_t),('PagefileUsage',ctypes.c_size_t),('PeakPagefileUsage',ctypes.c_size_t)]
        k=ctypes.WinDLL('kernel32',use_last_error=True);p=ctypes.WinDLL('psapi',use_last_error=True)
        k.GetCurrentProcess.restype=wintypes.HANDLE
        p.GetProcessMemoryInfo.argtypes=[wintypes.HANDLE,ctypes.POINTER(Counters),wintypes.DWORD]
        p.GetProcessMemoryInfo.restype=wintypes.BOOL
        counters=Counters();counters.cb=ctypes.sizeof(counters)
        if p.GetProcessMemoryInfo(k.GetCurrentProcess(),ctypes.byref(counters),counters.cb):
            return counters.PeakWorkingSetSize/(1024*1024)
    return None

def snapshot():
    times=os.times();out={'cpu_seconds':times.user+times.system,'peak_rss_mib':peak_rss_mib()}
    proc=Path('/proc/self')
    if proc.exists():
        out['fds']=len(list((proc/'fd').iterdir()));out['threads']=len(list((proc/'task').iterdir()))
    else:out['python_threads']=threading.active_count()
    return out
