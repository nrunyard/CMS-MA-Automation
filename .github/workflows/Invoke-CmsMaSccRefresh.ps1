
<#!
.SYNOPSIS
  Build a rolling 24-month CMS MA SCC dataset (Full version) enriched with Parent Organization
  from the MA Plan Directory, filtered to the 48 contiguous U.S. states. Pure PowerShell.
#>

param(
  [int]$RollingMonths = 24,
  [string]$SccListUrl = "https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-ma-enrollment-state/county/contract",
  [string]$PlanDirectoryUrl = "https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/ma-plan-directory",
  [string]$DataRoot = "data"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$rawDir  = Join-Path $DataRoot "raw"
$procDir = Join-Path $DataRoot "processed"
New-Item -ItemType Directory -Force -Path $rawDir, $procDir | Out-Null

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

function Get-Page {
  param([string]$Url)
  Write-Host "GET $Url" -ForegroundColor Cyan
  $headers = @{
    'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36'
    'Accept'     = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
  }
  $resp = Invoke-WebRequest -Uri $Url -Headers $headers -TimeoutSec 180 -MaximumRedirection 5
  return $resp.Content
}

function Get-AbsoluteUrl {
  param([string]$Href)
  if ($Href -like 'http*') { return $Href }
  return ("https://www.cms.gov" + $Href)
}

function Get-AllHrefs {
  param([string]$Html)
  $list = New-Object System.Collections.Generic.List[string]
  $rxDq = [regex]'href="([^"]+)"'
  $rxSq = [regex]"href='([^']+)'"
  foreach ($m in $rxDq.Matches($Html)) { $list.Add($m.Groups[1].Value) }
  foreach ($m in $rxSq.Matches($Html)) { $list.Add($m.Groups[1].Value) }
  return $list
}

function Get-SccMonthLinks {
  param([string]$ListUrl)
  $html = Get-Page $ListUrl
  $hrefs = Get-AllHrefs $html
  $map = @{}
  $rxPer = [regex]'ma-enrollment-scc-(\d{4}-\d{2})'
  foreach ($h in $hrefs) {
    $m = $rxPer.Match($h)
    if ($m.Success) {
      $per = $m.Groups[1].Value
      $map[$per] = Get-AbsoluteUrl $h
    }
  }
  $map.GetEnumerator() | Sort-Object { $_.Key } -Descending
}

function Get-SccFullDownloadUrl {
  param([string]$MonthPageUrl)
  $html = Get-Page $MonthPageUrl

  # Collect file URLs from anchors
  $hrefs = Get-AllHrefs $html
  $fileHrefCandidates = New-Object System.Collections.Generic.List[string]
  foreach ($u in $hrefs) {
    if ($u -match '/files/.*\.(csv|zip|xlsx|xls)($|\?)') {
      $fileHrefCandidates.Add((Get-AbsoluteUrl $u))
    }
  }
  # Also collect JSON-embedded CMS file URLs
  $jsonUrlRx = [regex]'https:\\/\\/www\.cms\.gov\\/files\\/(?:zip|csv|xlsx|xls)\\/[^"\\s<>\']+'
  foreach ($m in $jsonUrlRx.Matches($html)) {
    $clean = $m.Value.Replace('\\/','/')
    $fileHrefCandidates.Add($clean)
  }
  # Deduplicate
  $fileHrefCandidates = [System.Linq.Enumerable]::ToList([System.Linq.Enumerable]::Distinct($fileHrefCandidates))
  if ($fileHrefCandidates.Count -eq 0) { throw "No data download found on $MonthPageUrl" }

  # Prefer ZIP, then CSV, else first
  $preferred = $fileHrefCandidates | Where-Object { $_ -match '\\.zip($|\?)' } | Select-Object -First 1
  if (-not $preferred) { $preferred = $fileHrefCandidates | Where-Object { $_ -match '\\.csv($|\?)' } | Select-Object -First 1 }
  if (-not $preferred) { $preferred = $fileHrefCandidates | Select-Object -First 1 }
  return $preferred
}

function Save-RemoteFile {
  param([string]$Url, [string]$OutPath)
  if (-not (Test-Path $OutPath)) {
    Write-Host "Downloading -> $OutPath" -ForegroundColor Yellow
    Invoke-WebRequest -Uri $Url -OutFile $OutPath -TimeoutSec 600
  }
  return (Get-Item $OutPath)
}

function Import-FromZipFirstCsv {
  param([string]$ZipPath)
  $tmp = Join-Path ([IO.Path]::GetTempPath()) ("cms_" + [IO.Path]::GetFileNameWithoutExtension($ZipPath) + "_" + [Guid]::NewGuid())
  New-Item -ItemType Directory -Force -Path $tmp | Out-Null
  Expand-Archive -LiteralPath $ZipPath -DestinationPath $tmp -Force
  $csv = Get-ChildItem -LiteralPath $tmp -Recurse -File | Where-Object { $_.Extension -ieq ".csv" } | Select-Object -First 1
  if (-not $csv) { throw "No CSV found in $ZipPath" }
  $rows = Import-Csv -LiteralPath $csv.FullName
  Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
  return $rows
}

function Normalize-Enrollment { param([object]$v)
  $s = [string]$v
  if ([string]::IsNullOrWhiteSpace($s)) { return $null }
  if ($s -eq "*") { return $null }
  $s = $s.Replace(",", "").Trim()
  if ($s -match '^[0-9]+(\.[0-9]+)?$') { return [double]$s }
  return $null
}

function Normalize-SccFrame {
  param([object[]]$Rows, [string]$Period)
  $cols = $Rows | Select-Object -First 1 | Get-Member -MemberType NoteProperty | ForEach-Object Name
  $lower = @{}; foreach ($c in $cols) { $lower[$c.ToLower()] = $c }
  function pick { param([string[]]$alts)
    foreach ($a in $alts) { if ($lower.ContainsKey($a)) { return $lower[$a] }
      foreach ($k in $lower.Keys) { if ($k.Replace(' ','') -like "*$( $a.Replace(' ','') )*") { return $lower[$k] } }
    } return $null }
  $cState   = pick @('state','state name','state_desc','bene_state_desc','state description','state_abbrev','state cd')
  $cStateAb = pick @('state_abbrev','state abbreviation','state_cd','state code','state abbr')
  $cCounty  = pick @('county','county name','bene_county_desc','county_desc')
  $cFips    = pick @('fips','bene fips cd','county_fips','fips code','bene_fips_cd')
  $cCtrct   = pick @('contract id','contract','contract number','contract_no','contract_num')
  $cOrg     = pick @('organization name','organization','org name','parent organization','legal entity name')
  $cPlan    = pick @('plan id','pbp','plan_number','plan number')
  $cEnroll  = pick @('enrollment','member count','enrollees','count')
  if (-not $cCtrct -or -not $cEnroll) { throw "Cannot find contract/enrollment columns in SCC month $Period" }
  $out = foreach ($r in $Rows) {
    [pscustomobject]@{
      report_period = [datetime]::ParseExact($Period, 'yyyy-MM', $null)
      state         = $(if ($cState) { $r.$cState } else { $null })
      state_abbr    = $(if ($cStateAb) { $r.$cStateAb } else { $null })
      county        = $(if ($cCounty) { $r.$cCounty } else { $null })
      fips          = $(if ($cFips) { $r.$cFips } else { $null })
      contract_id   = $r.$cCtrct
      org_name      = $(if ($cOrg) { $r.$cOrg } else { $null })
      plan_id       = $(if ($cPlan) { $r.$cPlan } else { $null })
      enrollment    = (Normalize-Enrollment $r.$cEnroll)
    }
  }
  return ,$out
}

function Get-ParentOrgMap {
  param([string]$PlanDirUrl, [string]$PeriodHint)
  $html = Get-Page $PlanDirUrl
  $hrefs = Get-AllHrefs $html
  $zipUrl = $hrefs | Where-Object { $_ -match '\\.zip($|\?)' } | Select-Object -First 1
  if (-not $zipUrl) { throw "Could not find MA Plan Directory ZIP link." }
  $zipUrl = Get-AbsoluteUrl $zipUrl
  $perRx = [regex]'(\d{4}-\d{2})'
  $pm = $perRx.Match($html)
  $per = $(if ($pm.Success) { $pm.Groups[1].Value } else { $PeriodHint })
  $zipPath = Join-Path $rawDir ("ma_plan_directory_" + ($per ?? 'latest') + ".zip")
  Save-RemoteFile -Url $zipUrl -OutPath $zipPath | Out-Null
  $rows = Import-FromZipFirstCsv -ZipPath $zipPath
  $cols = $rows | Select-Object -First 1 | Get-Member -MemberType NoteProperty | ForEach-Object Name
  $lower = @{}; foreach ($c in $cols) { $lower[$c.ToLower()] = $c }
  function pick { param([string[]]$alts)
    foreach ($a in $alts) { if ($lower.ContainsKey($a)) { return $lower[$a] }
      foreach ($k in $lower.Keys) { if ($k.Replace(' ','') -like "*$( $a.Replace(' ','') )*") { return $lower[$k] } }
    } return $null }
  $cCtrct = pick @('contract id','contract','contract number','h number','h#')
  $cParent= pick @('parent organization','parent organization name','parent_org','parent org')
  $cOrg   = pick @('organization name','org name','organization','legal entity name')
  if (-not $cCtrct) { throw "MA Plan Directory CSV missing Contract column." }
  $map = @{}
  foreach ($r in $rows) {
    $cid = [string]$r.$cCtrct
    if ([string]::IsNullOrWhiteSpace($cid)) { continue }
    $parent = $null; if ($cParent) { $parent = [string]$r.$cParent }
    $org    = $null; if ($cOrg)    { $org    = [string]$r.$cOrg }
    if (-not $map.ContainsKey($cid)) {
      $map[$cid] = [pscustomobject]@{ contract_id=$cid; parent_org=$parent; org_name_dir=$org }
    }
  }
  $map.Values
}

function Join-Object { param([Parameter(ValueFromPipeline=$true)]$Left,$Right,[hashtable]$On,[ValidateSet('Inner','LeftOuter','RightOuter','Full')][string]$Kind='Inner',[string[]]$Property)
  begin{ $leftList=New-Object System.Collections.Generic.List[object]; $rightList=@{}; foreach($r in $Right){ $key=($On.Keys|ForEach-Object{ $r.($On[$_])}) -join '||'; if(-not $rightList.ContainsKey($key)){ $rightList[$key]=@()} $rightList[$key]+=,$r } }
  process{ $leftList.Add($Left) }
  end{
    foreach($l in $leftList){ $key=($On.Keys|ForEach-Object{ $l.$_ }) -join '||'; $matched=$rightList[$key]; if($matched){ foreach($r in $matched){ if($Property){ $o=New-Object psobject; foreach($p in $Property){ $o|Add-Member NoteProperty $p $( if($l.PSObject.Properties[$p]){ $l.$p } elseif($r.PSObject.Properties[$p]){ $r.$p } else{ $null }) } $o } else { [pscustomobject]@{Left=$l;Right=$r} } } } elseif($Kind -in 'LeftOuter','Full'){ if($Property){ $o=New-Object psobject; foreach($p in $Property){ $o|Add-Member NoteProperty $p $( if($l.PSObject.Properties[$p]){ $l.$p } else { $null }) } $o } else { [pscustomobject]@{Left=$l;Right=$null} } } }
    if($Kind -in 'RightOuter','Full'){ $leftKeys=$leftList|ForEach-Object{ ($On.Keys|ForEach-Object{ $PSItem.( $_ )}) -join '||' }; foreach($kv in $rightList.GetEnumerator()){ if($leftKeys -notcontains $kv.Key){ foreach($r in $kv.Value){ if($Property){ $o=New-Object psobject; foreach($p in $Property){ $o|Add-Member NoteProperty $p $( if($r.PSObject.Properties[$p]){ $r.$p } else{ $null }) } $o } else { [pscustomobject]@{Left=$null;Right=$r} } } } } }
}

Write-Host "Discovering SCC monthly pages..." -ForegroundColor Cyan
$months = Get-SccMonthLinks -ListUrl $SccListUrl
if (-not $months -or $months.Count -eq 0) { throw "No SCC months discovered from listing." }
$latestPeriod = $months[0].Key
$latestDt = [datetime]::ParseExact($latestPeriod,'yyyy-MM',$null)
$cutoffDt = $latestDt.AddMonths(-($RollingMonths-1))
$target = $months | Where-Object { [datetime]::ParseExact($_.Key,'yyyy-MM',$null) -ge $cutoffDt }

$frames = New-Object System.Collections.Generic.List[object[]]
foreach ($m in $target) {
  $period = $m.Key; $detailUrl = $m.Value
  try {
    $dl = Get-SccFullDownloadUrl -MonthPageUrl $detailUrl
    $ext = [IO.Path]::GetExtension(($dl -split '\\?')[0]).ToLower()
    $rawPath = Join-Path $rawDir ("ma_scc_"+$period+$ext)
    Save-RemoteFile -Url $dl -OutPath $rawPath | Out-Null
    $rows = if($ext -eq '.zip'){ Import-FromZipFirstCsv -ZipPath $rawPath } elseif($ext -eq '.csv'){ Import-Csv -LiteralPath $rawPath } else { Write-Warning "Skipping $period; unsupported file type: $ext"; $null }
    if($rows){ $frame = Normalize-SccFrame -Rows $rows -Period $period; $frames.Add($frame); Write-Host ("Loaded SCC {0}: {1} rows" -f $period, $frame.Count) }
  } catch { Write-Warning "Skipping $period due to error: $($_.Exception.Message)" }
}
if ($frames.Count -eq 0) { throw "No SCC months successfully loaded." }

$scc = @(); foreach($f in $frames){ $scc += $f }
$excludeNames=@('Alaska','Hawaii','District of Columbia','Puerto Rico','Guam','American Samoa','U.S. Virgin Islands','Northern Mariana Islands')
$excludeAbbr =@('AK','HI','DC','PR','GU','AS','VI','MP')
$scc = $scc | Where-Object { ($_.state -and ($excludeNames -notcontains $_.state)) -or -not $_.state } | Where-Object { ($_.state_abbr -and ($excludeAbbr -notcontains $_.state_abbr)) -or -not $_.state_abbr }

Write-Host "Fetching Parent Organization map from MA Plan Directory..." -ForegroundColor Cyan
try{ $parentMap = Get-ParentOrgMap -PlanDirUrl $PlanDirectoryUrl -PeriodHint $latestPeriod; $parentLut=@{}; foreach($r in $parentMap){ $parentLut[$r.contract_id]=$r }
  $scc = $scc | ForEach-Object { $x=$_; $po=$null; $on=$null; if($parentLut.ContainsKey($x.contract_id)){ $po=$parentLut[$x.contract_id].parent_org; $on=$parentLut[$x.contract_id].org_name_dir }
    [pscustomobject]@{ report_period=$x.report_period; state=$x.state; state_abbr=$x.state_abbr; county=$x.county; fips=$x.fips; contract_id=$x.contract_id; org_name=$(if($x.org_name){$x.org_name}elseif($on){$on}else{$null}); parent_org=$(if($po){$po}else{$x.org_name}); plan_id=$x.plan_id; enrollment=$x.enrollment }
  }
}catch{ Write-Warning "Parent Org enrichment failed: $($_.Exception.Message). Continuing with org_name only."; $scc = $scc | ForEach-Object { $_|Add-Member NoteProperty parent_org $_.org_name -Force; $_ } }

$outCsv = Join-Path $procDir 'ma_scc_latest.csv'
$scc | Sort-Object state, county, contract_id, report_period | Export-Csv -NoTypeInformation -Path $outCsv -Encoding UTF8
Write-Host "Saved $outCsv ("$($scc.Count)" rows)" -ForegroundColor Green

try{
  $byMonth = $scc | Group-Object state, county, report_period | ForEach-Object { $state,$county,$rp = $_.Name -split ', '; [pscustomobject]@{ state=$state; county=$county; report_period=[datetime]$rp; enrollment=($_.Group|Measure-Object -Property enrollment -Sum).Sum } }
  $latestByCounty = $byMonth | Group-Object state, county | ForEach-Object {
    $rows=$_.Group|Sort-Object report_period; if($rows.Count -eq 0){return}
    $last=$rows[-1]; $prev=$(if($rows.Count -ge 2){$rows[-2]}else{$null}); $yoy=$rows|Where-Object{ $_.report_period -eq $last.report_period.AddYears(-1) }|Select-Object -First 1
    function fmtDelta($a,$b){ if($null -eq $a -or $null -eq $b){return $null}; $diff=$a-$b; if($b -ne 0){ $pct=(($a-$b)/$b); return ("{0} ({1:+0.0%})" -f $diff,$pct) } else { return ("{0}" -f $diff) } }
    [pscustomobject]@{ state=$last.state; county=$last.county; latest_period=$last.report_period.ToString('yyyy-MM'); latest_enrollment=[int]$last.enrollment; mom_change=$(if($prev){fmtDelta $last.enrollment $prev.enrollment}else{$null}); yoy_change=$(if($yoy){fmtDelta $last.enrollment $yoy.enrollment}else{$null}) }
  }
  $latestMonth = ($scc | Measure-Object -Property report_period -Maximum).Maximum
  $tops = $scc | Where-Object { $_.report_period -eq $latestMonth } | Group-Object state, county, parent_org | ForEach-Object { $state,$county,$parent = $_.Name -split ', '; [pscustomobject]@{ state=$state; county=$county; parent_org=$parent; enrollment=($_.Group|Measure-Object -Property enrollment -Sum).Sum } } |
          Group-Object state, county | ForEach-Object { $_.Group | Sort-Object enrollment -Descending | Select-Object -First 5 }
  $topsAgg = $tops | Group-Object state, county | ForEach-Object { $state,$county=$_.Name -split ', '; $list=($_.Group|Sort-Object enrollment -Descending|ForEach-Object{ "{0} ({1:N0})" -f $_.parent_org,$_.enrollment }) -join '; '; [pscustomobject]@{ state=$state; county=$county; top_parent_orgs=$list } }
  $kpi = $latestByCounty | Join-Object -Right $topsAgg -On @{state='state'; county='county'} -Kind LeftOuter -Property state,county,latest_period,latest_enrollment,mom_change,yoy_change,top_parent_orgs
  $kpiOut = Join-Path $procDir 'ma_scc_kpis_county.csv'
  $kpi | Sort-Object state, county | Export-Csv -NoTypeInformation -Path $kpiOut -Encoding UTF8
  Write-Host "Saved $kpiOut ("$($kpi.Count)" rows)" -ForegroundColor Green
}catch{ Write-Warning "KPI calculation failed: $($_.Exception.Message)" }

Write-Host "Done." -ForegroundColor Green
