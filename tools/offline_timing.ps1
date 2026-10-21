# Passive Windows observer for ONE owner-approved canonical F3 run.
# This file is not an execution grant. No retry, setup, CI or arbitrary command.
[CmdletBinding()]
param([Parameter(Mandatory)][string]$ContextFile)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$expectedPowerShell = 'fa2c8bb1e35cdd62ad15cd06a1d8f2c5961e9725a9bfa1e7cfd2c322f49e9619'
$expectedBasePython = '434b361fe0c5960d404974feed493db47f2603a1ace2574312104fbed0182c6f'
$expectedVenvConfig = '92de011aa4ca41e566cfe9edc6fa80b8883a9f169b0e94cbee57c0974fbce437'
$expectedPytest = '95b617849288d07a5858a3ab8d5a9cebe65a33cd4b423235f4f22df68359ea6e'
$clock = [Diagnostics.Stopwatch]::StartNew()
$self = [Diagnostics.Process]::GetCurrentProcess()
$initialCpu = $self.TotalProcessorTime.TotalSeconds
$nativeExit = $null
$diagnostic = 'INVALID'
$reason = 'PREFLIGHT_NOT_COMPLETE'
$custody = $null
$trace = $null
$bytes = 0L
$owned = @{}
$helperCpu = 0.0
$helperPeakRss = 0L
$resourceGap = $false

function Assert-PlainPath([string]$Path, [string]$Boundary) {
    $full = [IO.Path]::GetFullPath($Path)
    if ($full -ne $Boundary -and -not $full.StartsWith($Boundary + '\', [StringComparison]::OrdinalIgnoreCase)) {
        throw 'PATH_BOUNDARY'
    }
    $cursor = $full
    while ($cursor) {
        if (Test-Path -LiteralPath $cursor) {
            $item = Get-Item -LiteralPath $cursor -Force
            if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'PATH_ALIAS' }
            if (-not $item.PSIsContainer -and $null -ne $item.LinkType) { throw 'PATH_ALIAS' }
        }
        $next = [IO.Path]::GetDirectoryName($cursor)
        if ($next -eq $cursor) { break }
        $cursor = $next
    }
    return $full
}

function Assert-Hash([string]$Path, [string]$Expected) {
    if ($Expected -cnotmatch '^[0-9a-f]{64}$' -or
        (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() -cne $Expected) {
        throw 'SOURCE_BINDING'
    }
}

function Assert-Private([string]$Path) {
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $acl = Get-Acl -LiteralPath $Path
    if (-not $acl.AreAccessRulesProtected) { throw 'PRIVATE_ACL_REQUIRED' }
    foreach ($rule in $acl.Access) {
        $identity = $rule.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value
        if ($rule.AccessControlType -eq 'Allow' -and $identity -notin @($sid, 'S-1-5-18')) {
            throw 'PRIVATE_ACL_REQUIRED'
        }
    }
}

function Write-Observation([hashtable]$Record) {
    # Whitelist all public fields. Never serialize context, environment or exceptions.
    $Record.mono_ticks = $clock.ElapsedTicks
    $payload = [Text.Encoding]::UTF8.GetBytes(($Record | ConvertTo-Json -Compress -Depth 4) + "`n")
    if ($script:bytes + $payload.Length -gt 1031168) { throw 'TELEMETRY_BUDGET' }
    $script:trace.Write($payload)
    $script:trace.Flush()
    $script:bytes += $payload.Length
}

function Test-TimingCoverage([object[]]$Tickets, [object[]]$Writers,
        [object[]]$Records, [hashtable]$External, [hashtable]$Starts, [hashtable]$Binding) {
    # Pure post-quiescence checker. Claims in workload records never fill an
    # absent OS observation. This function does not reconcile native outcomes.
    $errors = New-Object 'Collections.Generic.HashSet[string]'
    $ticketMap = @{}; $writerMap = @{}; $covered = 0
    $childEvents = @('ticket_issued','child_launch','popen_begin','popen_end',
        'job_assigned','gate_released','communicate_begin','communicate_end',
        'communicate_timeout','cleanup_end','exit_written','child_reconciled')
    try {
        $resolveRuntime = {
            param($launch)
            if ($launch.image_kind -ceq 'base_runtime' -and
                $launch.image_sha256 -ceq $Binding.base_python_sha256) { return $launch }
            if ($launch.image_kind -cne 'venv' -or $launch.image_sha256 -cne $Binding.runtime_sha256) { return $null }
            $children = @($External.Values | Where-Object {
                $_.parent -eq $launch.pid -and $_.creation -ge $launch.creation -and
                ($launch.exit_creation -eq 0 -or $_.creation -le $launch.exit_creation)
            })
            if ($children.Count -ne 1 -or $children[0].image_kind -cne 'base_runtime' -or
                $children[0].image_sha256 -cne $Binding.base_python_sha256) { return $null }
            return $children[0]
        }
        foreach ($writer in $Writers) {
            if ($writerMap.ContainsKey($writer.ticket_id)) { [void]$errors.Add('DUPLICATE_WRITER'); continue }
            $writerMap[$writer.ticket_id] = $writer
            $key = '{0}:{1}' -f $writer.pid,$writer.creation_100ns
            if (-not $External.ContainsKey($key)) { [void]$errors.Add('MISSING_WRITER_IDENTITY'); continue }
            $observed = $External[$key]
            if ($observed.image_kind -cne 'base_runtime' -or $observed.image_sha256 -cne $Binding.base_python_sha256) {
                [void]$errors.Add('FOREIGN_WRITER_IMAGE')
            }
            if ($writer.role -ceq 'supervisor') {
                $custodyKey = '{0}:{1}' -f $Binding.custody_pid,$Binding.custody_creation_100ns
                if (-not $External.ContainsKey($custodyKey)) { [void]$errors.Add('MISSING_CUSTODY_IDENTITY'); continue }
                $custodyRoot = $External[$custodyKey]
                $supervisor = & $resolveRuntime $custodyRoot
                if ($custodyRoot.parent -ne $Binding.observer_pid -or $null -eq $supervisor -or
                    $writer.pid -ne $supervisor.pid -or $writer.creation_100ns -ne $supervisor.creation) {
                    [void]$errors.Add('FOREIGN_SUPERVISOR')
                }
            }
        }
        foreach ($ticket in $Tickets) {
            $id = $ticket.ticket_id
            if ($id -cnotmatch '^[0-9a-f]{32}$' -or $ticketMap.ContainsKey($id)) {
                [void]$errors.Add('DUPLICATE_OR_CORRUPT_TICKET'); continue
            }
            $ticketMap[$id] = $ticket
            if ($ticket.run_id -cne $Binding.run_id -or $ticket.tested_sha -cne $Binding.tested_sha -or
                $ticket.source_digest -cne $Binding.source_digest -or $ticket.lane -cne 'f3') {
                [void]$errors.Add('FOREIGN_TICKET')
            }
        }
        foreach ($record in $Records) {
            if ($record.event -cnotin $childEvents) { continue }
            if (-not $ticketMap.ContainsKey($record.child_id) -or
                -not $writerMap.ContainsKey($record.ticket_id) -or
                $record.issuer_id -cne $record.ticket_id -or
                $ticketMap[$record.child_id].parent_id -cne $record.issuer_id) {
                [void]$errors.Add('FOREIGN_CHILD_OR_ISSUER')
            }
        }
        foreach ($ticket in $Tickets) {
            $id = $ticket.ticket_id
            $issued = @($Records | Where-Object { $_.event -ceq 'ticket_issued' -and $_.child_id -ceq $id })
            $launched = @($Records | Where-Object { $_.event -ceq 'child_launch' -and $_.child_id -ceq $id })
            if ($issued.Count -ne 1 -or $launched.Count -ne 1) {
                [void]$errors.Add('MISSING_OR_DUPLICATE_BINDING'); continue
            }
            $launch = $launched[0]
            if ($launch.issuer_id -cne $issued[0].issuer_id -or
                $launch.issuer_id -cne $ticket.parent_id -or
                -not $writerMap.ContainsKey($launch.issuer_id)) {
                [void]$errors.Add('FOREIGN_ISSUER'); continue
            }
            $profile = 0
            $argv = $ticket.argv -join [char]31
            if ($ticket.role -ceq 'lane-worker') { $profile=4 }
            elseif ($ticket.role -ceq 'command') {
                if ($argv -ceq (@($Binding.console,'-q') -join [char]31)) { $profile=1 }
                elseif ($argv -ceq (@($Binding.runtime,'-B','-X','utf8','-m','pytest','-q') -join [char]31)) { $profile=2 }
                elseif ($argv -ceq (@($Binding.runtime,'-B','-X','utf8','-m','unittest','discover') -join [char]31)) { $profile=3 }
            }
            if ($issued[0].value -ne $profile -or $launch.value -ne $profile) { [void]$errors.Add('FOREIGN_PROFILE') }
            $issuer = $writerMap[$launch.issuer_id]
            $issuerKey = '{0}:{1}' -f $issuer.pid,$issuer.creation_100ns
            $childKey = '{0}:{1}' -f $launch.child_pid,$launch.child_creation_100ns
            if (-not $External.ContainsKey($issuerKey) -or -not $External.ContainsKey($childKey)) {
                [void]$errors.Add('MISSING_EXTERNAL_IDENTITY'); continue
            }
            $parent = $External[$issuerKey]; $child = $External[$childKey]
            if ($launch.child_parent_pid -ne $child.parent -or $child.parent -ne $parent.pid -or
                $parent.creation -gt $child.creation -or
                ($parent.exit_creation -ne 0 -and $parent.exit_creation -lt $child.creation)) {
                [void]$errors.Add('ACTUAL_PARENT_MISMATCH'); continue
            }
            $console = $ticket.role -ceq 'command' -and $ticket.argv[0] -ieq $Binding.console
            if ($console) {
                # Exactly the existing frozen runner's seven console argv forms.
                # No arbitrary ancestry or executable-name-only exception.
                $tail = ($ticket.argv | Select-Object -Skip 1) -join [char]31
                $tails = @('-q', ('-q','tests.py','main_tests.py' -join [char]31),
                    ('--collect-only','-q','tests.py','main_tests.py' -join [char]31),
                    ('-q','tests.py' -join [char]31), ('--collect-only','-q','tests.py' -join [char]31),
                    ('-q','main_tests.py' -join [char]31), ('--collect-only','-q','main_tests.py' -join [char]31))
                if ($child.image_kind -cne 'console' -or $child.image_sha256 -cne $Binding.pytest_sha256 -or
                    $tail -cnotin $tails) {
                    [void]$errors.Add('FOREIGN_CONSOLE_WRAPPER'); continue
                }
                $interpreters = @($External.Values | Where-Object {
                    $_.parent -eq $child.pid -and
                    $_.creation -ge $child.creation -and
                    ($child.exit_creation -eq 0 -or $_.creation -le $child.exit_creation)
                })
                if ($interpreters.Count -ne 1) { [void]$errors.Add('MISSING_OR_AMBIGUOUS_CONSOLE_CHILD'); continue }
                $workload = & $resolveRuntime $interpreters[0]
                if ($null -eq $workload) { [void]$errors.Add('MISSING_OR_FOREIGN_RUNTIME_EDGE'); continue }
            } else {
                if ($ticket.native -and $ticket.role -ceq 'native-git') {
                    if ($child.image_kind -cne 'git' -or $child.image_sha256 -cne $Binding.git_sha256) {
                        [void]$errors.Add('FOREIGN_CHILD_IMAGE'); continue
                    }
                    $workload = $child
                } else {
                    $workload = & $resolveRuntime $child
                    if ($null -eq $workload) { [void]$errors.Add('MISSING_OR_FOREIGN_RUNTIME_EDGE'); continue }
                }
            }
            if ($writerMap.ContainsKey($id) -and
                ($writerMap[$id].pid -ne $workload.pid -or
                 $writerMap[$id].creation_100ns -ne $workload.creation)) {
                [void]$errors.Add('WRITER_CHILD_MISMATCH'); continue
            }
            if ($Starts.ContainsKey($id)) {
                $start = $Starts[$id]
                if ($start.ticket_id -cne $id -or $start.parent_id -cne $ticket.parent_id -or
                    $start.run_id -cne $Binding.run_id -or $start.tested_sha -cne $Binding.tested_sha -or
                    $start.source_digest -cne $Binding.source_digest -or $start.role -cne $ticket.role -or
                    $start.lane -cne 'f3' -or $start.pid -ne $workload.pid) {
                    [void]$errors.Add('NATIVE_START_CHILD_MISMATCH')
                }
            }
            foreach ($record in $Records) {
                if ($record.event -cnotin $childEvents -or $record.child_id -cne $id) { continue }
                if ($record.PSObject.Properties['child_pid'] -and
                    ($record.child_pid -ne $child.pid -or $record.child_creation_100ns -ne $child.creation -or
                     $record.child_parent_pid -ne $child.parent)) { [void]$errors.Add('FOREIGN_CHILD_IDENTITY') }
            }
            $covered++
        }
        foreach ($writer in $Writers) {
            if ($writer.role -cne 'supervisor' -and -not $ticketMap.ContainsKey($writer.ticket_id)) {
                [void]$errors.Add('FOREIGN_WRITER_TICKET')
            } elseif ($writer.role -cne 'supervisor' -and
                $ticketMap[$writer.ticket_id].role -cne $writer.role) {
                [void]$errors.Add('FOREIGN_WRITER_ROLE')
            }
        }
    } catch {
        [void]$errors.Add('CORRUPT_COVERAGE')
    }
    return @{ valid=($errors.Count -eq 0 -and $covered -eq $Tickets.Count);
        tickets=$Tickets.Count; covered=$covered; errors=@($errors | Sort-Object) }
}

# No additional workload thread, pipe or stdlib replacement. The job is only a
# custody/cleanup boundary, with KILL_ON_JOB_CLOSE and NO workload resource limits.
try {
Add-Type -TypeDefinition @'
using System;
using System.Text;
using System.Runtime.InteropServices;
public static class F3TimingNative {
    [StructLayout(LayoutKind.Sequential)] public struct SA { public int size; public IntPtr sd; public int inherit; }
    [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)] public struct SI {
        public int cb; public string reserved, desktop, title;
        public int x,y,xs,ys,xc,yc,fill,flags; public short show,reserved2;
        public IntPtr reservedBytes,input,output,error;
    }
    [StructLayout(LayoutKind.Sequential)] public struct SX { public SI si; public IntPtr attributes; }
    [StructLayout(LayoutKind.Sequential)] public struct PI { public IntPtr process,thread; public int pid,tid; }
    [StructLayout(LayoutKind.Sequential)] public struct BASIC {
        public long processTime,jobTime; public uint flags; public UIntPtr min,max;
        public uint active; public UIntPtr affinity; public uint priority,scheduling;
    }
    [StructLayout(LayoutKind.Sequential)] public struct IO { public ulong readOps,writeOps,otherOps,readBytes,writeBytes,otherBytes; }
    [StructLayout(LayoutKind.Sequential)] public struct LIMIT {
        public BASIC basic; public IO io; public UIntPtr processMemory,jobMemory,peakProcess,peakJob;
    }
    [StructLayout(LayoutKind.Sequential)] public struct MEMORY {
        public uint cb,faults; public UIntPtr peak,working,pagedPeak,paged,nonPagedPeak,nonPaged,pagePeak,page;
    }
    [StructLayout(LayoutKind.Sequential)] public struct PBI {
        public IntPtr reserved1,peb,reserved2,reserved3,pid,parent;
    }
    public sealed class Sample {
        public int pid,parent; public ulong creation,exitCreation,kernel,user; public long peakRss;
        public IO io; public bool exited; public uint exit;
    }
    public sealed class Custody { public IntPtr job,process; public int pid; public ulong creation; }
    [DllImport("kernel32",SetLastError=true,CharSet=CharSet.Unicode)] static extern IntPtr CreateJobObjectW(IntPtr a,string n);
    [DllImport("kernel32",SetLastError=true)] static extern bool SetInformationJobObject(IntPtr j,int c,ref LIMIT p,int n);
    [DllImport("kernel32",SetLastError=true)] static extern bool AssignProcessToJobObject(IntPtr j,IntPtr p);
    [DllImport("kernel32",SetLastError=true)] static extern bool IsProcessInJob(IntPtr p,IntPtr j,out bool yes);
    [DllImport("kernel32",SetLastError=true)] public static extern bool TerminateJobObject(IntPtr j,uint c);
    [DllImport("kernel32")] public static extern bool CloseHandle(IntPtr h);
    [DllImport("kernel32")] static extern bool TerminateProcess(IntPtr p,uint c);
    [DllImport("kernel32")] static extern uint ResumeThread(IntPtr t);
    [DllImport("kernel32")] public static extern uint WaitForSingleObject(IntPtr p,uint ms);
    [DllImport("kernel32")] static extern bool GetExitCodeProcess(IntPtr p,out uint c);
    [DllImport("kernel32")] static extern bool GetProcessTimes(IntPtr p,out ulong c,out ulong e,out ulong k,out ulong u);
    [DllImport("kernel32")] static extern bool GetProcessIoCounters(IntPtr p,out IO io);
    [DllImport("psapi")] static extern bool GetProcessMemoryInfo(IntPtr p,ref MEMORY m,uint n);
    [DllImport("ntdll")] static extern int NtQueryInformationProcess(IntPtr p,int c,out PBI b,int n,IntPtr r);
    [DllImport("kernel32",SetLastError=true)] static extern IntPtr OpenProcess(uint a,bool i,int p);
    [DllImport("kernel32",SetLastError=true,CharSet=CharSet.Unicode)] static extern bool QueryFullProcessImageNameW(IntPtr p,uint f,StringBuilder image,ref uint size);
    [DllImport("kernel32",SetLastError=true)] static extern bool QueryInformationJobObject(IntPtr j,int c,IntPtr b,int n,out int r);
    [DllImport("kernel32",SetLastError=true,CharSet=CharSet.Unicode)] static extern IntPtr CreateFileW(string p,uint a,uint s,ref SA sa,uint c,uint f,IntPtr t);
    [DllImport("kernel32",SetLastError=true)] static extern bool InitializeProcThreadAttributeList(IntPtr l,int n,int f,ref IntPtr bytes);
    [DllImport("kernel32",SetLastError=true)] static extern bool UpdateProcThreadAttribute(IntPtr l,uint f,IntPtr a,IntPtr v,IntPtr n,IntPtr old,IntPtr r);
    [DllImport("kernel32")] static extern void DeleteProcThreadAttributeList(IntPtr l);
    [DllImport("kernel32",SetLastError=true,CharSet=CharSet.Unicode)] static extern bool CreateProcessW(string a,StringBuilder command,IntPtr pa,IntPtr ta,bool inherit,uint flags,IntPtr env,string cwd,ref SX si,out PI pi);
    static void Check(bool yes) { if(!yes) throw new InvalidOperationException("NATIVE_OBSERVATION_OR_CUSTODY"); }
    public static Sample Read(IntPtr process) {
        var s = new Sample(); ulong end; uint exit; PBI basic;
        Check(GetProcessTimes(process,out s.creation,out end,out s.kernel,out s.user)); s.exitCreation=end;
        Check(NtQueryInformationProcess(process,0,out basic,Marshal.SizeOf<PBI>(),IntPtr.Zero)==0);
        s.pid=basic.pid.ToInt32(); s.parent=basic.parent.ToInt32();
        var memory=new MEMORY(); memory.cb=(uint)Marshal.SizeOf<MEMORY>();
        s.peakRss=GetProcessMemoryInfo(process,ref memory,memory.cb) ? (long)memory.peak.ToUInt64() : -1;
        Check(GetProcessIoCounters(process,out s.io)); s.exited=WaitForSingleObject(process,0)==0;
        Check(GetExitCodeProcess(process,out exit)); s.exit=exit; return s;
    }
    public static string Image(IntPtr process) {
        var image=new StringBuilder(32768); uint size=(uint)image.Capacity;
        Check(QueryFullProcessImageNameW(process,0,image,ref size)); return image.ToString();
    }
    public static IntPtr OpenOwned(Custody c,int pid) {
        IntPtr p=OpenProcess(0x101410,false,pid); Check(p!=IntPtr.Zero);
        bool yes; if(!IsProcessInJob(p,c.job,out yes)||!yes) { CloseHandle(p); throw new InvalidOperationException("FOREIGN_PROCESS"); }
        return p;
    }
    public static int[] Pids(Custody c) {
        IntPtr b=Marshal.AllocHGlobal(65536);
        try { int returned; Check(QueryInformationJobObject(c.job,3,b,65536,out returned));
            int assigned=Marshal.ReadInt32(b), count=Marshal.ReadInt32(b,4);
            Check(count>=0 && count<=4096 && assigned==count);
            int[] result=new int[count]; for(int i=0;i<count;i++) result[i]=(int)Marshal.ReadIntPtr(b,8+i*IntPtr.Size);
            return result;
        } finally { Marshal.FreeHGlobal(b); }
    }
    public static Custody Launch(string runtime,string command,string cwd,string stdout,string stderr) {
        var c=new Custody(); PI pi=new PI(); IntPtr list=IntPtr.Zero,handles=IntPtr.Zero;
        bool listReady=false;
        IntPtr input=IntPtr.Zero,output=IntPtr.Zero,error=IntPtr.Zero;
        try {
            c.job=CreateJobObjectW(IntPtr.Zero,null); Check(c.job!=IntPtr.Zero);
            var limit=new LIMIT(); limit.basic.flags=0x2000;
            Check(SetInformationJobObject(c.job,9,ref limit,Marshal.SizeOf<LIMIT>()));
            var sa=new SA { size=Marshal.SizeOf<SA>(), inherit=1 };
            input=CreateFileW("NUL",0x80000000,3,ref sa,3,0,IntPtr.Zero);
            output=CreateFileW(stdout,0x40000000,1,ref sa,1,0,IntPtr.Zero);
            error=CreateFileW(stderr,0x40000000,1,ref sa,1,0,IntPtr.Zero);
            Check(input!=new IntPtr(-1)&&output!=new IntPtr(-1)&&error!=new IntPtr(-1));
            IntPtr size=IntPtr.Zero; InitializeProcThreadAttributeList(IntPtr.Zero,1,0,ref size);
            Check(size!=IntPtr.Zero); list=Marshal.AllocHGlobal(size);
            listReady=InitializeProcThreadAttributeList(list,1,0,ref size); Check(listReady);
            handles=Marshal.AllocHGlobal(3*IntPtr.Size);
            Marshal.WriteIntPtr(handles,0,input); Marshal.WriteIntPtr(handles,IntPtr.Size,output); Marshal.WriteIntPtr(handles,2*IntPtr.Size,error);
            Check(UpdateProcThreadAttribute(list,0,new IntPtr(0x20002),handles,new IntPtr(3*IntPtr.Size),IntPtr.Zero,IntPtr.Zero));
            var si=new SX(); si.si.cb=Marshal.SizeOf<SX>(); si.si.flags=0x100;
            si.si.input=input; si.si.output=output; si.si.error=error; si.attributes=list;
            Check(CreateProcessW(runtime,new StringBuilder(command),IntPtr.Zero,IntPtr.Zero,true,0x80004,IntPtr.Zero,cwd,ref si,out pi));
            Check(AssignProcessToJobObject(c.job,pi.process)); bool yes;
            Check(IsProcessInJob(pi.process,c.job,out yes)&&yes);
            c.process=pi.process; c.pid=pi.pid; c.creation=Read(pi.process).creation;
            Check(ResumeThread(pi.thread)!=0xffffffff); return c;
        } catch {
            if(pi.process!=IntPtr.Zero) { TerminateProcess(pi.process,125); WaitForSingleObject(pi.process,5000); CloseHandle(pi.process); }
            if(c.job!=IntPtr.Zero) { TerminateJobObject(c.job,125); CloseHandle(c.job); } throw;
        } finally {
            if(pi.thread!=IntPtr.Zero) CloseHandle(pi.thread);
            foreach(var h in new[]{input,output,error}) if(h!=IntPtr.Zero&&h!=new IntPtr(-1)) CloseHandle(h);
            if(list!=IntPtr.Zero) { if(listReady) DeleteProcThreadAttributeList(list); Marshal.FreeHGlobal(list); }
            if(handles!=IntPtr.Zero) Marshal.FreeHGlobal(handles);
        }
    }
}
'@

    $source = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
    $controlBoundary = Join-Path $source '.f3/evidence/u-telemetry'
    $contextPath = Assert-PlainPath $ContextFile $controlBoundary
    if ([IO.Path]::GetFileName($contextPath) -cne 'run-context.json' -or
        [IO.Path]::GetFileName([IO.Path]::GetDirectoryName($contextPath)) -cnotmatch '^[0-9a-f]{32}$') { throw 'CONTEXT_PATH' }
    Assert-Private ([IO.Path]::GetDirectoryName($contextPath))
    if ((Get-Item -LiteralPath $contextPath).Length -gt 16384) { throw 'CONTEXT_BUDGET' }
    $spec = Get-Content -LiteralPath $contextPath -Raw | ConvertFrom-Json
    if ($spec.version -ne 1 -or $spec.root -cne $source -or $spec.source_digest -cnotmatch '^[0-9a-f]{64}$' -or
        $spec.tested_sha -cnotmatch '^[0-9a-f]{40}$' -or $spec.run_id -cnotmatch '^[0-9a-f]{32}$') { throw 'CONTEXT_IDENTITY' }
    Assert-Hash $self.MainModule.FileName $expectedPowerShell
    if ($self.MainModule.FileName -ine 'C:\Program Files\PowerShell\7\pwsh.exe') { throw 'OBSERVER_EXECUTABLE' }
    Assert-Hash $PSCommandPath $spec.observer_sha256
    Assert-Hash (Join-Path $source 'tools/offline_tests.py') $spec.runner_sha256
    Assert-Hash (Join-Path $source 'tools/offline_guard.py') $spec.guard_sha256
    $runtime = Assert-PlainPath $spec.runtime $spec.runtime_root
    $expectedRuntime = Join-Path ([IO.Path]::GetDirectoryName($source)) 'cu/.f1/venv-dev/Scripts/python.exe'
    if ($runtime -ine [IO.Path]::GetFullPath($expectedRuntime) -or
        $spec.runtime_sha256 -cne '16a377d348c3a0c8d3a58d222d96807065dd0dcd9438c6025900c842ec836603') { throw 'RUNTIME_EXECUTABLE' }
    Assert-Hash $runtime $spec.runtime_sha256
    # A console wrapper is admitted only with this separately pinned executable.
    # The new context pin grants no command or runtime permission.
    if ($spec.pytest_sha256 -cne $expectedPytest -or $spec.venv_cfg_sha256 -cne $expectedVenvConfig -or
        $spec.base_python_sha256 -cne $expectedBasePython) { throw 'RUNTIME_EXECUTABLE' }
    $console = Assert-PlainPath (Join-Path ([IO.Path]::GetDirectoryName($runtime)) 'pytest.exe') $spec.runtime_root
    Assert-Hash $console $spec.pytest_sha256
    $venvCfg = Assert-PlainPath (Join-Path ([IO.Path]::GetDirectoryName([IO.Path]::GetDirectoryName($runtime))) 'pyvenv.cfg') $spec.runtime_root
    Assert-Hash $venvCfg $spec.venv_cfg_sha256
    $baseHome = [IO.Path]::GetFullPath((Join-Path ([IO.Path]::GetDirectoryName($source)) 'cu/.f1/python'))
    $basePython = Assert-PlainPath (Join-Path $baseHome 'python.exe') $baseHome
    Assert-Hash $basePython $spec.base_python_sha256
    $venvSettings = @{}
    foreach ($line in [IO.File]::ReadLines($venvCfg)) {
        if ($line -cmatch '^(home|executable) = (.+)$') {
            if ($venvSettings.ContainsKey($matches[1])) { throw 'RUNTIME_EXECUTABLE' }
            $venvSettings[$matches[1]] = [IO.Path]::GetFullPath($matches[2])
        }
    }
    if ($venvSettings.home -ine $baseHome -or $venvSettings.executable -ine $basePython) { throw 'RUNTIME_EXECUTABLE' }
    Assert-Hash (Join-Path $spec.stdlib_root 'subprocess.py') $spec.subprocess_sha256
    if ($spec.subprocess_sha256 -cne '8e33d2fca1666dc959ac1a48609d5ecb055990771a0e9142be0c1c2050109f92') { throw 'STDLIB_NOT_REVIEWED' }
    $parent = [F3TimingNative]::Read($self.Handle)
    $owner = [Diagnostics.Process]::GetProcessById($parent.parent)
    $ownerIdentity = [F3TimingNative]::Read($owner.Handle)
    if ($owner.Id -ne $spec.parent_pid -or $ownerIdentity.creation -ne $spec.parent_creation_100ns) { throw 'PARENT_CUSTODY' }
    Assert-Hash $owner.MainModule.FileName $spec.parent_sha256
    $affinity = [uint64]$self.ProcessorAffinity.ToInt64()
    $capacity = 0
    while ($affinity) { $capacity += [int]($affinity -band 1); $affinity = $affinity -shr 1 }
    if ($capacity -le 0 -or $capacity -ne $spec.cpu_capacity) { throw 'CPU_CAPACITY' }
    $git = (Get-Command git -CommandType Application).Source
    if ($git -ine $spec.git_executable) { throw 'GIT_EXECUTABLE' }
    Assert-Hash $git $spec.git_sha256
    $control = [IO.Path]::GetDirectoryName($contextPath)
    # Three closed, pinned read-only Git metadata helpers. Their CPU and OS peak
    # RSS enter the observer budget; output goes to files, never workload pipes.
    $metadata = @{}
    $queries = @{
        head='-c core.fsmonitor=false rev-parse HEAD'
        status='-c core.fsmonitor=false status --porcelain --untracked-files=all'
        ancestor='merge-base --is-ancestor ef5b9611620712417366f0710743cd892ac0ae53 HEAD'
    }
    $gitEnvironment=@{}
    foreach ($entry in Get-ChildItem Env: | Where-Object {$_.Name.StartsWith('GIT_')}) {
        $gitEnvironment[$entry.Name]=$entry.Value
        [Environment]::SetEnvironmentVariable($entry.Name,$null,'Process')
    }
    $env:GIT_CONFIG_GLOBAL='NUL'; $env:GIT_CONFIG_NOSYSTEM='1'; $env:GIT_TERMINAL_PROMPT='0'
    try {
    foreach ($query in @('head','status','ancestor')) {
        $helper=$null
        try {
            if ($git.Contains('"') -or $source.Contains('"')) { throw 'ARGV_QUOTING' }
            $command='"'+$git+'" -C "'+$source+'" '+$queries[$query]
            $outputPath=Join-Path $control ('metadata-'+$query+'.stdout')
            $helper=[F3TimingNative]::Launch($git,$command,$source,$outputPath,
                (Join-Path $control ('metadata-'+$query+'.stderr')))
            while ([F3TimingNative]::WaitForSingleObject($helper.process,0) -ne 0) {
                if ($clock.Elapsed.TotalSeconds -ge 10) { throw 'PREFLIGHT_BUDGET' }
                [Threading.Thread]::Sleep(5)
            }
            $measurement=[F3TimingNative]::Read($helper.process)
            if ($measurement.exit -ne 0 -or $measurement.peakRss -lt 0) { throw 'METADATA_OR_RESOURCE_GAP' }
            $helperCpu+=($measurement.user+$measurement.kernel)/10000000.0
            $helperPeakRss+=$measurement.peakRss
            $metadata[$query]=[IO.File]::ReadAllText($outputPath).Trim()
        } finally {
            if ($null -ne $helper) {
                [F3TimingNative]::TerminateJobObject($helper.job,125) | Out-Null
                [F3TimingNative]::WaitForSingleObject($helper.process,5000) | Out-Null
                [F3TimingNative]::CloseHandle($helper.process) | Out-Null
                [F3TimingNative]::CloseHandle($helper.job) | Out-Null
            }
        }
    }
    } finally {
        foreach ($entry in @(Get-ChildItem Env: | Where-Object {$_.Name.StartsWith('GIT_')})) {
            [Environment]::SetEnvironmentVariable($entry.Name,$null,'Process')
        }
        foreach ($name in $gitEnvironment.Keys) { [Environment]::SetEnvironmentVariable($name,$gitEnvironment[$name],'Process') }
    }
    if ($metadata.head -cne $spec.tested_sha -or $metadata.status) { throw 'SOURCE_HEAD_OR_DIRTY' }
    $self.Refresh()
    if ($clock.Elapsed.TotalSeconds -ge 10) { throw 'PREFLIGHT_BUDGET' }
    if ($self.PeakWorkingSet64+$helperPeakRss -gt 134217728) { throw 'RESOURCE_BUDGET' }
    $run = Join-Path $source ('.f3/evidence/runs/' + $spec.run_id)
    Assert-PlainPath $run (Join-Path $source '.f3/evidence/runs') | Out-Null
    # Owner preflight must create/lock down this parent; run itself must be fresh.
    Assert-Private ([IO.Path]::GetDirectoryName($run))
    if (Test-Path -LiteralPath $run) { throw 'RUN_ALREADY_EXISTS' }
    $trace = [IO.File]::Open((Join-Path $control 'observer.jsonl'), 'CreateNew', 'Write', 'Read')
    Write-Observation @{ event='observer_start'; frequency=[Diagnostics.Stopwatch]::Frequency; qpc_ticks=[Diagnostics.Stopwatch]::GetTimestamp(); cpu_capacity=$capacity; pid=$PID; creation_100ns=$parent.creation;
        observer_sha256=$spec.observer_sha256; runner_sha256=$spec.runner_sha256; guard_sha256=$spec.guard_sha256; tested_sha=$spec.tested_sha }
    # Process-only variables are restored. Native inventory/deadlines stay in Python.
    $saved = @{}
    foreach ($name in @('MDS_OFFLINE_TIMING_CONTEXT','MDS_OFFLINE_REPORT_ROOT','PYTHONDONTWRITEBYTECODE','PYTEST_DISABLE_PLUGIN_AUTOLOAD','PYTEST_ADDOPTS','PYTEST_PLUGINS')) {
        $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
    }
    try {
        $env:MDS_OFFLINE_TIMING_CONTEXT = $contextPath
        $env:MDS_OFFLINE_REPORT_ROOT = $run
        $env:PYTHONDONTWRITEBYTECODE = '1'
        $env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = '1'
        Remove-Item Env:PYTEST_ADDOPTS,Env:PYTEST_PLUGINS -ErrorAction SilentlyContinue
        # Closed argv, quoted file identities only; no user-supplied command text.
        if ($runtime.Contains('"') -or $source.Contains('"')) { throw 'ARGV_QUOTING' }
        $command = '"' + $runtime + '" -I -B -X utf8 "' + (Join-Path $source 'tools/offline_tests.py') + '" --lane f3'
        $launchTime = $clock.Elapsed.TotalSeconds
        $custody = [F3TimingNative]::Launch($runtime, $command, $source,
            (Join-Path $control 'native.stdout'), (Join-Path $control 'native.stderr'))
    } finally {
        foreach ($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
    }
    Write-Observation @{ event='custody'; pid=$custody.pid; creation_100ns=$custody.creation; parent_pid=$PID }
    $bucketWall = 0.0
    $bucketCpu = $initialCpu
    while ($true) {
        $active = [F3TimingNative]::Pids($custody)
        foreach ($processId in $active) {
            $handle = [F3TimingNative]::OpenOwned($custody, $processId)
            try {
                $row = [F3TimingNative]::Read($handle)
                $key = '{0}:{1}' -f $row.pid,$row.creation
                if (-not $owned.ContainsKey($key)) {
                    $image = [F3TimingNative]::Image($handle)
                    $kind = 'other'
                    $imageHash = 'UNKNOWN'
                    if ($image -ieq $runtime) { Assert-Hash $image $spec.runtime_sha256; $kind='venv'; $imageHash=$spec.runtime_sha256 }
                    elseif ($image -ieq $basePython) { Assert-Hash $image $spec.base_python_sha256; $kind='base_runtime'; $imageHash=$spec.base_python_sha256 }
                    elseif ($image -ieq $console) { Assert-Hash $image $spec.pytest_sha256; $kind='console'; $imageHash=$spec.pytest_sha256 }
                    elseif ($image -ieq $git) { Assert-Hash $image $spec.git_sha256; $kind='git'; $imageHash=$spec.git_sha256 }
                    # Private image paths stay in memory. Only closed kinds enter evidence.
                    $owned[$key] = @{ handle=$handle; pid=$row.pid; parent=$row.parent;
                        creation=$row.creation; exit_creation=$row.exitCreation; ended=$false;
                        image_kind=$kind; image_sha256=$imageHash }
                    $handle=[IntPtr]::Zero
                    Write-Observation @{ event='process_start_seen'; pid=$row.pid; parent_pid=$row.parent;
                        creation_100ns=$row.creation; image_kind=$kind; image_sha256=$imageHash }
                }
            } finally {
                if ($handle -ne [IntPtr]::Zero) { [F3TimingNative]::CloseHandle($handle) | Out-Null }
            }
        }
        foreach ($key in @($owned.Keys)) {
            $entry = $owned[$key]
            if ($entry.ended) { continue }
            $row = [F3TimingNative]::Read($entry.handle)
            if ($row.pid -ne $entry.pid -or $row.creation -ne $entry.creation -or $row.parent -ne $entry.parent) { throw 'PROCESS_IDENTITY' }
            if ($row.peakRss -lt 0) { $resourceGap=$true }
            if ($row.exited) {
                $entry.ended = $true
                $entry.exit_creation = $row.exitCreation
                Write-Observation @{ event='process_exit_seen'; pid=$row.pid; creation_100ns=$row.creation;
                    exit=$row.exit; cpu_100ns=($row.kernel+$row.user); peak_rss_bytes=$row.peakRss;
                    read_bytes=$row.io.readBytes; write_bytes=$row.io.writeBytes }
            }
        }
        $self.Refresh()
        $now = $clock.Elapsed.TotalSeconds
        $cpu = $self.TotalProcessorTime.TotalSeconds + $helperCpu
        $wholeCpu = 100 * ($cpu - $initialCpu) / ($now * $capacity)
        $bucketPercent = 100 * ($cpu - $bucketCpu) / (($now - $bucketWall) * $capacity)
        Write-Observation @{ event='sample'; observer_cpu_seconds=($cpu-$initialCpu); observer_peak_rss_bytes=($self.PeakWorkingSet64+$helperPeakRss); active=$active.Count }
        if ($self.PeakWorkingSet64+$helperPeakRss -gt 134217728 -or
            ($now -ge 10 -and $wholeCpu -gt 1) -or
            ($now-$bucketWall -ge 10 -and $bucketPercent -gt 1)) { throw 'RESOURCE_BUDGET' }
        if ($now-$bucketWall -ge 10) { $bucketWall=$now; $bucketCpu=$cpu }
        if (Test-Path -LiteralPath (Join-Path $run 'timing')) {
            if (Get-ChildItem -LiteralPath (Join-Path $run 'timing') -Filter '*.aborted') { throw 'WRITER_ABORTED' }
        }
        if ([F3TimingNative]::WaitForSingleObject($custody.process,0) -eq 0 -and $active.Count -eq 0) {
            $nativeExit = [F3TimingNative]::Read($custody.process).exit
            break
        }
        if ($now-$launchTime -ge 930) { throw 'OBSERVER_WATCHDOG' }
        [Threading.Thread]::Sleep(500)  # Observer only, not workload sleep.
    }
    Write-Observation @{ event='native_settled'; native_exit=$nativeExit }
    # Post-quiescence only: no additional scans/reads during native reconciliation.
    $invalid = $resourceGap
    $slots = @(Get-ChildItem -LiteralPath (Join-Path $run 'timing') -Filter 'slot-*.jsonl')
    if ($slots.Count -ne 5) { $invalid=$true }
    $writerBytes = 0L
    $identities = @{}
    $writerIdentities = New-Object 'Collections.Generic.List[object]'
    $writerRecords = New-Object 'Collections.Generic.List[object]'
    $roles = @{supervisor=0; 'lane-worker'=0; command=0}
    $settledNs = [decimal]([Diagnostics.Stopwatch]::GetTimestamp()) * 1000000000 / [Diagnostics.Stopwatch]::Frequency
    foreach ($slot in $slots) {
        $writerBytes += $slot.Length
        if ($slot.Length -gt 524280) { throw 'WRITER_BUDGET' }
        $sequence = 0
        $previous = -1L
        $events = @{}
        $identity = $null
        foreach ($line in [IO.File]::ReadLines($slot.FullName)) {
            if ([Text.Encoding]::UTF8.GetByteCount($line) -gt 4096) { throw 'TRACE_RECORD_BUDGET' }
            $row = $line | ConvertFrom-Json
            $writerRecords.Add($row)
            if ($null -eq $identity) { $identity=$row }
            $sequence++
            foreach ($field in @('seq','mono_ns','pid','creation_100ns','value','phase')) {
                if ($row.$field -isnot [int] -and $row.$field -isnot [long]) { $invalid=$true }
            }
            if ($row.pid -le 0 -or $row.creation_100ns -le 0) { $invalid=$true }
            if ($row.seq -ne $sequence -or $row.mono_ns -lt $previous -or $row.mono_ns -gt $settledNs -or
                $row.run_id -cne $spec.run_id -or $row.tested_sha -cne $spec.tested_sha -or
                $row.lane -cne 'f3' -or $row.ticket_id -cne $identity.ticket_id -or
                $row.source_digest -cne $spec.source_digest -or
                $row.role -cne $identity.role -or $row.pid -ne $identity.pid -or
                $row.creation_100ns -ne $identity.creation_100ns) { $invalid=$true }
            $previous = $row.mono_ns
            $events[$row.event] = $true
            if ($row.event -eq 'writer_end' -and $row.value -gt 1000000000) { throw 'SYNC_BUDGET' }
        }
        if ($null -eq $identity -or -not $events.ContainsKey('bootstrap') -or
            -not $events.ContainsKey('writer_end')) { $invalid=$true }
        elseif (-not $owned.ContainsKey(('{0}:{1}' -f $identity.pid,$identity.creation_100ns))) { $invalid=$true }
        if ($null -ne $identity) {
            $writerIdentities.Add($identity)
            if ($identities.ContainsKey($identity.ticket_id) -or -not $roles.ContainsKey($identity.role)) { $invalid=$true }
            else { $identities[$identity.ticket_id]=$true; $roles[$identity.role]++ }
        }
        Write-Observation @{ event='writer_checked'; records=$sequence; bytes=$slot.Length;
            complete=($events.ContainsKey('writer_end')) }
    }
    if ($roles.supervisor -ne 1 -or $roles['lane-worker'] -ne 1 -or $roles.command -ne 3) { $invalid=$true }
    # Completeness includes explicit native coverage/gaps. Missing native reports
    # are facts of the original failure, never synthesized by this observer.
    $missingStart=0; $missingFinal=0; $missingExit=0; $missingBuffers=0
    $authoritativeTickets = New-Object 'Collections.Generic.List[object]'
    $nativeStarts = @{}
    foreach ($ticketPath in Get-ChildItem -LiteralPath (Join-Path $run 'tickets') -Filter '*.json') {
        if ($ticketPath.Name -notmatch '^[0-9a-f]{32}\.json$') { continue }
        $ticket = Get-Content -LiteralPath $ticketPath.FullName -Raw | ConvertFrom-Json
        $id = $ticket.ticket_id
        if ($ticket.run_id -cne $spec.run_id -or $ticket.tested_sha -cne $spec.tested_sha -or
            $id -cnotmatch '^[0-9a-f]{32}$' -or $ticketPath.BaseName -cne $id) { $invalid=$true; continue }
        $authoritativeTickets.Add($ticket)
        if (-not $ticket.native) {
            $startPath = Join-Path $run "processes/$id.start.json"
            if (-not (Test-Path -LiteralPath $startPath)) { $missingStart++ }
            else {
                try {
                    if ((Get-Item -LiteralPath $startPath).Length -gt 32768) { throw 'CORRUPT_NATIVE_START' }
                    $nativeStarts[$id] = Get-Content -LiteralPath $startPath -Raw | ConvertFrom-Json
                } catch { $invalid=$true }
            }
            if (-not (Test-Path -LiteralPath (Join-Path $run "processes/$id.final.json"))) { $missingFinal++ }
        }
        if (-not (Test-Path -LiteralPath (Join-Path $run "tickets/$id.exit.json"))) { $missingExit++ }
        foreach ($suffix in @('stdout','stderr')) {
            if (-not (Test-Path -LiteralPath (Join-Path $run "timing/native/$id.$suffix"))) { $missingBuffers++ }
        }
    }
    if ($missingBuffers) { $invalid=$true }
    $coverage = Test-TimingCoverage -Tickets @($authoritativeTickets.ToArray()) -Writers @($writerIdentities.ToArray()) `
        -Records @($writerRecords.ToArray()) -External $owned -Starts $nativeStarts -Binding @{
            run_id=$spec.run_id; tested_sha=$spec.tested_sha; source_digest=$spec.source_digest;
            custody_pid=$custody.pid; custody_creation_100ns=$custody.creation; observer_pid=$PID;
            runtime=$runtime; console=$console; runtime_sha256=$spec.runtime_sha256;
            base_python_sha256=$spec.base_python_sha256; pytest_sha256=$spec.pytest_sha256; git_sha256=$spec.git_sha256 }
    if (-not $coverage.valid) { $invalid=$true }
    Write-Observation @{ event='identity_coverage'; tickets=$coverage.tickets; covered=$coverage.covered;
        valid=$coverage.valid; errors=$coverage.errors }
    Write-Observation @{ event='native_coverage'; missing_start=$missingStart; missing_final=$missingFinal;
        missing_exit=$missingExit; missing_buffers=$missingBuffers; pipe_eof_timestamp='UNKNOWN'; unittest_return_timestamp='UNKNOWN' }
    # All bytes are retained locally. Only aliases/hashes can enter a public projection.
    $trace.Flush($true)
    $seal = New-Object Collections.Generic.List[object]
    foreach ($store in @(@{alias='control';root=$control}, @{alias='run';root=$run})) {
        foreach ($file in Get-ChildItem -LiteralPath $store.root -Recurse -File -Force) {
            Assert-PlainPath $file.FullName $store.root | Out-Null
            if ($file.FullName -eq (Join-Path $control 'observer.jsonl')) { continue }
            $relative = [IO.Path]::GetRelativePath($store.root,$file.FullName).Replace('\','/')
            $seal.Add(@{ store=$store.alias; name=$relative; bytes=$file.Length;
                sha256=(Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant() })
        }
    }
    $self.Refresh()
    $finalCpu=$self.TotalProcessorTime.TotalSeconds+$helperCpu
    $finalSeconds=$clock.Elapsed.TotalSeconds
    if ($self.PeakWorkingSet64+$helperPeakRss -gt 134217728 -or
        100*($finalCpu-$initialCpu)/($finalSeconds*$capacity) -gt 1 -or
        ($finalSeconds-$bucketWall -ge 10 -and
         100*($finalCpu-$bucketCpu)/(($finalSeconds-$bucketWall)*$capacity) -gt 1)) { throw 'RESOURCE_BUDGET' }
    $diagnostic = $(if ($invalid) {'INVALID'} else {'COMPLETE'})
    $reason = $(if ($invalid) {'COVERAGE_OR_TRACE_GAP'} else {'OBSERVATION_COMPLETE_NOT_NATIVE_ACCEPTANCE'})
    Write-Observation @{ event='diagnostic_ready'; diagnostic='PREPARED'; native_exit=$nativeExit; reason=$reason }
    $trace.Flush($true); $trace.Dispose(); $trace=$null
    $observerPath = Join-Path $control 'observer.jsonl'
    $seal.Add(@{ store='control'; name='observer.jsonl'; bytes=(Get-Item -LiteralPath $observerPath).Length;
        sha256=(Get-FileHash -LiteralPath $observerPath -Algorithm SHA256).Hash.ToLowerInvariant() })
    $manifestBytes = [Text.Encoding]::UTF8.GetBytes(($seal | ConvertTo-Json -Depth 4 -Compress) + "`n")
    $contextBytes = (Get-Item -LiteralPath $contextPath).Length
    $abortBytes = (@(Get-ChildItem -LiteralPath (Join-Path $run 'timing') -Filter '*.aborted') | Measure-Object Length -Sum).Sum
    if ($bytes + $manifestBytes.Length + $contextBytes + 128 -gt 1048576 -or
        $bytes + $manifestBytes.Length + $contextBytes + 128 + $writerBytes + $abortBytes -gt 4194304) { throw 'SEAL_BUDGET' }
    $manifestPath = Join-Path $control 'bundle-manifest.json'
    $stream = [IO.File]::Open($manifestPath,'CreateNew','Write','None')
    try { $stream.Write($manifestBytes); $stream.Flush($true) } finally { $stream.Dispose() }
    $sealPath = Join-Path $control 'SEALED.sha256'
    $stream = [IO.File]::Open($sealPath,'CreateNew','Write','None')
    try {
        $payload=[Text.Encoding]::ASCII.GetBytes((Get-FileHash -LiteralPath $manifestPath -Algorithm SHA256).Hash.ToLowerInvariant()+"`n")
        $stream.Write($payload); $stream.Flush($true)
    } finally { $stream.Dispose() }
    foreach ($store in @($control,$run)) {
        foreach ($file in Get-ChildItem -LiteralPath $store -Recurse -File -Force) { $file.IsReadOnly=$true }
    }
} catch {
    $diagnostic = 'ABORTED'
    $reason = 'OBSERVATION_STORAGE_CUSTODY_OR_BUDGET'
    $known = @('PATH_BOUNDARY','PATH_ALIAS','SOURCE_BINDING','PRIVATE_ACL_REQUIRED','TELEMETRY_BUDGET',
        'NATIVE_OBSERVATION_OR_CUSTODY','FOREIGN_PROCESS','CONTEXT_PATH','CONTEXT_BUDGET','CONTEXT_IDENTITY',
        'OBSERVER_EXECUTABLE','RUNTIME_EXECUTABLE','STDLIB_NOT_REVIEWED','PARENT_CUSTODY','CPU_CAPACITY',
        'GIT_EXECUTABLE','ARGV_QUOTING','PREFLIGHT_BUDGET','METADATA_OR_RESOURCE_GAP','SOURCE_HEAD_OR_DIRTY',
        'RUN_ALREADY_EXISTS','PROCESS_IDENTITY','RESOURCE_BUDGET','WRITER_ABORTED','OBSERVER_WATCHDOG',
        'WRITER_BUDGET','TRACE_RECORD_BUDGET','SYNC_BUDGET','SEAL_BUDGET')
    if ($_.Exception.Message -cin $known) { $reason=$_.Exception.Message }
    # Exception payloads, private paths and raw native output stay out of summaries.
} finally {
    if ($null -ne $custody) {
        if ($null -eq $nativeExit) {
            [F3TimingNative]::TerminateJobObject($custody.job,125) | Out-Null
            $cleanup=[Diagnostics.Stopwatch]::StartNew()
            try {
                while (@([F3TimingNative]::Pids($custody)).Count -ne 0 -and $cleanup.Elapsed.TotalSeconds -lt 5) {
                    [Threading.Thread]::Sleep(5)
                }
                if (@([F3TimingNative]::Pids($custody)).Count -ne 0) { $reason='CLEANUP_INCOMPLETE' }
            } catch { $reason='CLEANUP_OBSERVATION_GAP' }
        }
        foreach ($entry in $owned.Values) { [F3TimingNative]::CloseHandle($entry.handle) | Out-Null }
        [F3TimingNative]::CloseHandle($custody.process) | Out-Null
        [F3TimingNative]::CloseHandle($custody.job) | Out-Null
    }
    if ($null -ne $trace) { $trace.Dispose() }
}
# Return codes belong to the diagnostic controller; native result is separate.
[ordered]@{ NATIVE_VERDICT=$(if ($null -eq $nativeExit) {'NOT_AVAILABLE'} else {$nativeExit});
    DIAGNOSTIC_VERDICT=$diagnostic; REASON=$reason } | ConvertTo-Json -Compress
exit $(if ($diagnostic -eq 'COMPLETE') {0} else {2})
