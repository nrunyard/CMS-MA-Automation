const DATA_MAIN = '../data/processed/ma_scc_latest.csv';
const DATA_KPI  = '../data/processed/ma_scc_kpis_county.csv';
let mainRows = [], kpiRows = [];
function parseNumber(s){ if(s==null) return null; const x=(""+s).replace(/,/g,'').trim(); const n=parseFloat(x); return isNaN(n)?null:n; }
function fmtNum(n){ return (n==null)?'—': n.toLocaleString('en-US'); }
function fmtDelta(a,b){ if(a==null||b==null) return '—'; const d=a-b; const p=b!==0? d/b: null; return p==null? `${d.toLocaleString('en-US')}` : `${d.toLocaleString('en-US')} (${(p*100).toFixed(1)}%)`; }
function toMonthStr(s){ return (""+s).substring(0,7); }
async function loadCsv(url){ return new Promise((res,rej)=>Papa.parse(url,{download:true,header:true,skipEmptyLines:true,complete:r=>res(r.data),error:rej})); }
function uniqueSorted(a){ return Array.from(new Set(a.filter(x=>x&&x.trim().length>0))).sort((x,y)=>x.localeCompare(y)); }
function populateFilters(){ const s=document.getElementById('stateSel'), c=document.getElementById('countySel'); const p=document.getElementById('parentSel');
  const states=uniqueSorted(mainRows.map(r=>r.state)); s.innerHTML=states.map(v=>`<option>${v}</option>`).join(''); if(states.includes('Arizona')) s.value='Arizona'; updateCountyAndParents();
  s.addEventListener('change', updateCountyAndParents); c.addEventListener('change', updateParentsOnly); p.addEventListener('change', updateDashboard);
}
function updateCountyAndParents(){ const s=document.getElementById('stateSel').value; const c=document.getElementById('countySel'); const counties=uniqueSorted(mainRows.filter(r=>r.state===s).map(r=>r.county)); c.innerHTML=counties.map(v=>`<option>${v}</option>`).join(''); if(counties.length) c.value=counties[0]; updateParentsOnly(); }
function updateParentsOnly(){ const s=document.getElementById('stateSel').value, c=document.getElementById('countySel').value, p=document.getElementById('parentSel'); const parents=uniqueSorted(mainRows.filter(r=>r.state===s&&r.county===c).map(r=>r.parent_org).filter(Boolean)); p.innerHTML=parents.map(v=>`<option>${v}</option>`).join(''); Array.from(p.options).forEach(o=>o.selected=false); updateDashboard(); }
function getSelectedParents(){ return Array.from(document.getElementById('parentSel').selectedOptions).map(o=>o.value); }
function updateDashboard(){ const s=document.getElementById('stateSel').value, c=document.getElementById('countySel').value, parents=getSelectedParents(); let d=mainRows.filter(r=>r.state===s&&r.county===c); if(parents.length) d=d.filter(r=>parents.includes(r.parent_org));
  const map=new Map(); for(const r of d){ const m=toMonthStr(r.report_period); const en=parseNumber(r.enrollment)||0; map.set(m,(map.get(m)||0)+en); }
  const months=Array.from(map.keys()).sort(); const totals=months.map(m=>map.get(m)); const cur=totals.at(-1)||null, prev=totals.length>1?totals.at(-2):null, yoy=totals.length>12?totals.at(-13):null;
  document.getElementById('kpiCur').textContent=fmtNum(cur); document.getElementById('kpiMoM').textContent=fmtDelta(cur,prev); document.getElementById('kpiYoY').textContent=fmtDelta(cur,yoy);
  Plotly.newPlot('trend',[{x:months,y:totals,type:'scatter',mode:'lines+markers',line:{color:'#7c5cff'}}],{title:`Enrollment Trend — ${s}, ${c}`,xaxis:{title:'Month'},yaxis:{title:'Enrollment',rangemode:'tozero'},margin:{t:40}},{displaylogo:false,responsive:true});
  const latest=months.at(-1); const group=new Map(); for(const r of d){ if(toMonthStr(r.report_period)!==latest) continue; const key=r.parent_org||r.org_name||'(Unknown)'; const en=parseNumber(r.enrollment)||0; group.set(key,(group.get(key)||0)+en); }
  const pairs=Array.from(group.entries()).sort((a,b)=>b[1]-a[1]).slice(0,10); Plotly.newPlot('topParents',[{x:pairs.map(p=>p[1]).reverse(),y:pairs.map(p=>p[0]).reverse(),type:'bar',orientation:'h',marker:{color:'#4cc3d9'}}],{title:`Top Parent Organizations — ${s}, ${c} — ${latest||''}`,xaxis:{title:'Enrollment'},yaxis:{title:'Parent Org'},margin:{t:40}},{displaylogo:false,responsive:true});
}
(async function(){ try{ [mainRows,kpiRows]=await Promise.all([loadCsv(DATA_MAIN), loadCsv(DATA_KPI)]); populateFilters(); }catch(e){ console.error(e); alert('Failed to load CSVs from data/processed/. Run the ETL and commit the outputs.'); } })();
