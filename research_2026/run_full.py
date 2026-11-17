"""Public-data patent/industry and commercial-permit/employment forecast experiments.
Runs on GitHub Actions; stores source URL, model predictions, and backtest metrics.
No inference of causal effects. Original research pilot, 2026-11-17.
"""
import os, re, io, csv, json, math, time, warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import requests
import numpy as np
import pandas as pd
from openpyxl import load_workbook
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
warnings.filterwarnings("ignore", category=FutureWarning)
ROOT=Path("research_2026/results");ROOT.mkdir(parents=True,exist_ok=True)
CACHE=Path("/tmp/qcew_research_cache");CACHE.mkdir(exist_ok=True)
COUNTIES={"24031":"Montgomery MD","24021":"Frederick MD","24027":"Howard MD","24033":"Prince Georges MD",
"51059":"Fairfax VA","51107":"Loudoun VA","37183":"Wake NC","25017":"Middlesex MA"}
SECTORS={"10":"All private","11":"Agriculture","22":"Utilities","23":"Construction","31-33":"Manufacturing",
"42":"Wholesale","44-45":"Retail","48-49":"Transport","51":"Information","52":"Finance",
"53":"Real estate","54":"Professional & technical","55":"Management","56":"Administrative",
"61":"Education","62":"Health care","71":"Arts & recreation","72":"Accommodation & food","81":"Other services"}
CODES=set(SECTORS)
S=requests.Session();S.headers["User-Agent"]="EDO applied research pilot / source reproducibility"
def fetch(url,tries=4):
 for attempt in range(tries):
  try:
   r=requests.get(url,timeout=65,headers={"User-Agent":"Mozilla/5.0 (public research; academic data verification)"})
   r.raise_for_status();return r.content
  except Exception as e:
   if attempt+1==tries:raise RuntimeError(f"request failed {url}: {e}")
   time.sleep(1.5*(attempt+1))
def csv_rows(b):
 return list(csv.DictReader(io.StringIO(b.decode("utf-8-sig",errors="replace"))))
def qcew_one(item):
 county,year,qtr=item
 tag="a" if qtr==0 else str(qtr);path=CACHE/f"{county}_{year}_{tag}.csv"
 if not path.exists():
  data=fetch(f"https://data.bls.gov/cew/data/api/{year}/{tag}/area/{county}.csv")
  path.write_bytes(data)
 rows=[]
 for r in csv_rows(path.read_bytes()):
  if r.get("own_code")!="5" or r.get("size_code")!="0" or r.get("industry_code") not in CODES:continue
  if r.get("disclosure_code") not in (""," ","0",None):continue
  if qtr==0:
   value=r.get("annual_avg_emplvl")
   est=r.get("annual_avg_estabs")
  else:
   values=[float(r.get(k) or "nan") for k in ["month1_emplvl","month2_emplvl","month3_emplvl"]]
   value=np.nanmean(values) if np.isfinite(values).any() else None
   est=r.get("qtrly_estabs_count")
  try:value=float(value)
  except:value=np.nan
  if not np.isfinite(value) or value<=0:continue
  rows.append({"county":county,"year":year,"quarter":qtr,"sector":r["industry_code"],"jobs":value,"estabs":est})
 return rows
def download_qcew():
 print("QCEW fetch start",flush=True)
 tasks=[(f,y,0) for f in COUNTIES for y in range(2014,2025)]
 tasks += [("24031",y,q) for y in range(2014,2026) for q in range(1,5)]
 allrows=[];errs=[]
 with ThreadPoolExecutor(max_workers=10) as ex:
  futures={ex.submit(qcew_one,x):x for x in tasks}
  for i,fut in enumerate(as_completed(futures),1):
   try:allrows.extend(fut.result())
   except Exception as e:errs.append({"task":futures[fut],"error":str(e)[:300]})
   if i%40==0: print("QCEW progress",i,"/",len(tasks),"rows",len(allrows),"errors",len(errs),flush=True)
 pd.DataFrame(errs).to_csv(ROOT/"qcew_failures.csv",index=False)
 df=pd.DataFrame(allrows)
 if df.empty:raise RuntimeError("No QCEW data")
 df.to_csv(ROOT/"qcew_county_industry_panel.csv",index=False)
 print("QCEW complete",len(df),"tasks",len(tasks),"failures",len(errs),flush=True)
 return df,errs

def download_patents():
 url="https://ncses.nsf.gov/pubs/nsb20241/assets/data/swbinv-1.xlsx"
 b=fetch(url);print("NSF workbook",len(b),"bytes",flush=True)
 wb=load_workbook(io.BytesIO(b),read_only=True,data_only=True)
 fields=[]
 vals=defaultdict(dict)
 county_to_nsf={"24031":"us-md-031","24021":"us-md-021","24027":"us-md-027","24033":"us-md-033",
 "51059":"us-va-059","51107":"us-va-107","37183":"us-nc-183","25017":"us-ma-017"}
 lookup={v:k for k,v in county_to_nsf.items()}
 for i in range(2,39):
  ws=wb[f"Table SWBINV1-{i}"]
  it=ws.iter_rows(values_only=True)
  next(it);tit=next(it)[0];next(it);headers=next(it)
  label=re.sub(r"^USPTO utility patents granted to U.S. inventors (in )?","",str(tit))
  label=label.split(", by U.S. county")[0].strip()
  label=label.replace("USPTO utility patents granted to U.S. inventors","All technologies")
  col="pat_"+str(i).zfill(2)
  fields.append({"column":col,"title":label,"source_table":i})
  for row in it:
   key=lookup.get(str(row[0]).lower())
   if not key:continue
   for y,v in zip(headers[1:],row[1:]):
    try:y=int(y);v=float(v)
    except:continue
    if 1998<=y<=2022 and np.isfinite(v):
     vals[(key,y)][col]=v
  if i%8==0:print("NSF parsed",i,flush=True)
 data=[{"county":k[0],"year":k[1],**d} for k,d in vals.items()]
 p=pd.DataFrame(data).sort_values(["county","year"])
 p.to_csv(ROOT/"patents_county_technology_1998_2022.csv",index=False)
 pd.DataFrame(fields).to_csv(ROOT/"patent_technology_fields.csv",index=False)
 print("PATENTS",p.shape,flush=True)
 return p,fields

def metrics_df(df,actual="truth",forecast="pred"):
 a=df[actual].to_numpy(float);b=df[forecast].to_numpy(float)
 return {"N":len(df),"MAE":round(float(mean_absolute_error(a,b)),4),
 "RMSE":round(float(np.sqrt(mean_squared_error(a,b))),4),
 "direction_accuracy":round(float(np.mean((a>0)==(b>0))),4)}
def eval_forecasts(df,group):
 result=[]
 for name,g in df.groupby("model"):
  m=metrics_df(g);m.update({"study":group,"model":name});result.append(m)
 return result
def regress_predict(train,test,cols):
 features=train[cols].copy();f_test=test[cols].copy()
 model=Pipeline([("impute",SimpleImputer(strategy="constant",fill_value=0)),
 ("scale",StandardScaler()),("ridge",Ridge(alpha=30))])
 model.fit(features,train["target"])
 return model.predict(f_test)
def tree_predict(train,test,cols):
 model=Pipeline([("impute",SimpleImputer(strategy="constant",fill_value=0)),
 ("hgb",HistGradientBoostingRegressor(max_iter=120,max_leaf_nodes=8,
 learning_rate=0.055,l2_regularization=12,min_samples_leaf=35,random_state=37))])
 model.fit(train[cols],train["target"])
 return model.predict(test[cols])

def patent_study(qcew,patents):
 q=qcew[(qcew.quarter==0)&(qcew.sector!="10")].copy()
 q=q.drop_duplicates(["county","sector","year"])
 q=q.sort_values(["county","sector","year"])
 g=q.groupby(["county","sector"],sort=False)
 q["lag1"]=g["jobs"].shift(1);q["lag2"]=g["jobs"].shift(2);q["lead2"]=g["jobs"].shift(-2)
 q["g1"]=100*np.log(q.jobs/q.lag1);q["g2past"]=100*np.log(q.jobs/q.lag2)
 q["target"]=100*np.log(q.lead2/q.jobs)
 q["logjobs"]=np.log1p(q.jobs)
 total=qcew[(qcew.quarter==0)&(qcew.sector=="10")][["county","year","jobs"]].rename(columns={"jobs":"county_jobs"})
 q=q.merge(total,on=["county","year"],how="left")
 q["share"]=q.jobs/q.county_jobs
 tech=[c for c in patents if c.startswith("pat_")]
 # Patent grants in year t can be observed following the annual vintage; t is the latest completed year.
 p=patents.sort_values(["county","year"])
 for c in tech:
  p[c]=np.log1p(p[c].clip(lower=0))
  p[c+"_delta"]=p[c]-p.groupby("county")[c].shift(2)
 q=q.merge(p[["county","year"]+tech+[c+"_delta" for c in tech]],on=["county","year"],how="left")
 q=q[(q.year>=2011)&(q.year<=2022)&q.target.notna()&(q.jobs>=150)&q.g2past.notna()]
 q["target_fast"]=((q.target>=9.53)&((q.lead2-q.jobs)>=100)).astype(int)
 dummies=pd.get_dummies(q[["county","sector"]],dtype=float)
 q=pd.concat([q,dummies],axis=1)
 cat=list(dummies.columns)
 base=["logjobs","g1","g2past","share"]+cat
 # Full technology set, total and delta: compare additive information with identical model.
 added=tech+[c+"_delta" for c in tech]
 result=[]
 for t in [2018,2019,2020,2021,2022]:
  tr=q[(q.year<=t-2)&(q.target.notna())]
  te=q[q.year==t]
  if len(tr)<250 or len(te)<30:continue
  for name,fn,features in [("trend-naive",None,None),
    ("ridge-economic-only",regress_predict,base),
    ("gbdt-economic-only",tree_predict,base),
    ("gbdt-plus-patents",tree_predict,base+added)]:
   try:
    pred=te.g2past.to_numpy() if fn is None else fn(tr,te,features)
    for (_,r),v in zip(te.iterrows(),pred):
     result.append({"forecast_year":t,"county":r.county,"sector":r.sector,
     "jobs_at_origin":r.jobs,"jobs_at_target":r.lead2,"emerging_truth":r.target_fast,
     "truth":float(r.target),"pred":float(v),"model":name})
   except Exception as e:print("PATENT MODEL ERROR",t,name,repr(e),flush=True)
 d=pd.DataFrame(result);d.to_csv(ROOT/"patent_model_predictions.csv",index=False)
 stat=eval_forecasts(d,"patents")
 # top 10% emerging precision per year, each model
 for m in stat:
  selected=[]
  for yr,g in d[d.model==m["model"]].groupby("forecast_year"):
   top=g.sort_values("pred",ascending=False).head(max(1,round(len(g)*.10)))
   selected.append(top)
  s=pd.concat(selected)
  m["top10_actual_fast_growth_rate"]=round(float(s.emerging_truth.mean()),4)
  m["top10_count"]=len(s)
  m["overall_fast_growth_rate"]=round(float(d[d.model==m["model"]].emerging_truth.mean()),4)
 print("PATENT METRICS",json.dumps(stat),flush=True)
 return d,stat,q

def permit_download():
 src="https://data.montgomerycountymd.gov/api/views/i26v-w6bd/rows.csv?accessType=DOWNLOAD"
 b=fetch(src)
 print("PERMITS source bytes",len(b),flush=True)
 r=pd.read_csv(io.BytesIO(b),low_memory=False)
 r.columns=[re.sub(r"[^a-z0-9]","",str(x).lower()).replace("issuedate","issueddate").replace("finaldate","finaleddate") for x in r.columns]
 print("PERMIT SOURCE COLUMNS",list(r.columns),flush=True)
 date_cols=["addeddate","issueddate","finaleddate"]
 for c in date_cols:
  r[c]=pd.to_datetime(r[c],errors="coerce")
 r["approval_days"]=(r.issueddate-r.addeddate).dt.total_seconds()/86400
 r["execution_days"]=(r.finaleddate-r.issueddate).dt.total_seconds()/86400
 r["total_days"]=(r.finaleddate-r.addeddate).dt.total_seconds()/86400
 r["year"]=r.addeddate.dt.year
 r["valuation"]=pd.to_numeric(r.get("declaredvaluation"),errors="coerce")
 r["area"]=pd.to_numeric(r.get("buildingarea"),errors="coerce")
 r["worktype"]=r.get("worktype","").fillna("UNKNOWN").astype(str).str.upper()
 r["usecode"]=r.get("usecode","").fillna("UNKNOWN").astype(str).str.upper()
 detail=[]
 for key,d in r.groupby(["usecode","worktype"]):
  issued=d.approval_days[(d.approval_days>=0)&(d.approval_days<3650)]
  finished=d.execution_days[(d.execution_days>=0)&(d.execution_days<3650)]
  if len(d)<30:continue
  detail.append({"usecode":key[0],"worktype":key[1],"records":len(d),"issued_n":len(issued),
   "issued_median":issued.median(),"issued_p90":issued.quantile(.9),
   "execution_n":len(finished),"execution_median":finished.median(),
   "declared_valuation_m":d.valuation.sum()/1e6})
 pd.DataFrame(detail).sort_values("records",ascending=False).to_csv(ROOT/"permit_by_usecode_worktype.csv",index=False)
 r["q_index"]=r.issueddate.dt.year*4+r.issueddate.dt.quarter-1
 issued=r[(r.approval_days>=0)&(r.approval_days<3650)&r.q_index.notna()].copy()
 # Issuance-based measures are known at calendar quarter end; lag them one quarter for forecast.
 agg=issued.groupby("q_index").agg(permit_count=("approval_days","count"),
 review_median=("approval_days","median"),
 review_p90=("approval_days",lambda x:x.quantile(.9)),
 valuation_m=("valuation",lambda x:x.sum()/1e6),
 new_construction=("worktype",lambda x:(x=="CONSTRUCT").sum()),
 alteration=("worktype",lambda x:(x=="ALTER").sum())).reset_index()
 agg.q_index=agg.q_index.astype(int)
 agg["year"]=agg.q_index//4;agg["quarter"]=agg.q_index%4+1
 agg.to_csv(ROOT/"permits_quarterly_features.csv",index=False)
 print("PERMIT records",len(r),"valid issued",len(issued),"quarterly",len(agg),flush=True)
 return r,agg

def permit_study(qcew,features):
 # Use only county private employment; one observation per sector and time.
 q=qcew[(qcew.county=="24031")&(qcew.quarter>0)&qcew.sector.isin(["10","23","31-33","54","56","62","53","72"])].copy()
 q["q_index"]=q.year*4+q.quarter-1
 q=q.sort_values(["sector","q_index"])
 # Origin t; last observed QCEW available t-2 due to 5–6 month publication lag.
 # Target t+2; we forecast change since t-2, one year ahead on observable employment.
 q2=q[["sector","q_index","jobs"]].copy()
 q2["origin"]=q2.q_index+2
 latest=q2.rename(columns={"jobs":"jobs_latest"}).drop(columns=["q_index"]).rename(columns={"origin":"q_index"})
 q=q.merge(latest,on=["sector","q_index"],how="left")
 q["origin"]=q.q_index
 lookup=qcew[(qcew.county=="24031")&(qcew.quarter>0)].copy()
 lookup["q_index"]=lookup.year*4+lookup.quarter-1
 def shifted(shift,name):
  temp=lookup[["sector","q_index","jobs"]].copy();temp.q_index+=shift
  return temp.rename(columns={"jobs":name})
 # q at origin has target from origin+2 and latest from origin-2.
 current=q[["sector","q_index","year","quarter","jobs"]].rename(columns={"jobs":"not_used_current"})
 for shift,label in [(2,"emp_lag2"),(6,"emp_lag6"),(-2,"emp_fut2")]:
  current=current.merge(shifted(shift,label),on=["sector","q_index"],how="left")
 current["target"]=100*np.log(current.emp_fut2/current.emp_lag2)
 current["lag_growth"]=100*np.log(current.emp_lag2/current.emp_lag6)
 current["logjobs"]=np.log1p(current.emp_lag2)
 pp=features.copy();pp.q_index+=1
 # permit features for origin t are previous quarter's issued permit records t-1
 current=current.merge(pp[["q_index","permit_count","review_median","review_p90","valuation_m",
 "new_construction","alteration"]],on="q_index",how="left")
 for c in ["permit_count","valuation_m","new_construction","alteration"]:
  current[c]=np.log1p(current[c].clip(lower=0))
 current=current[(current.q_index>=2013*4)&(current.q_index<=2024*4+3)
   &current.target.notna()&current.lag_growth.notna()&current.permit_count.notna()]
 for c in ["review_median","review_p90"]:
  current[c]=current[c].clip(lower=0,upper=1000)
 dums=pd.get_dummies(current[["sector"]],dtype=float)
 current=pd.concat([current,dums],axis=1)
 base=["lag_growth","logjobs"]+list(dums.columns)+["quarter"]
 extra=["permit_count","review_median","review_p90","valuation_m","new_construction","alteration"]
 results=[]
 # Rolling annual test blocks, train excluding origins t-4+1.. current, to respect publication lag.
 for y in range(2019,2025):
  tr=current[current.q_index<(y*4-4)]
  te=current[(current.year==y)]
  if len(tr)<70 or len(te)<6:continue
  for name,fn,feats in [("trend-naive",None,None),
    ("ridge-economic-only",regress_predict,base),
    ("gbdt-economic-only",tree_predict,base),
    ("gbdt-plus-permits",tree_predict,base+extra)]:
   try:
    pred=te.lag_growth.to_numpy() if fn is None else fn(tr,te,feats)
    for (_,r),v in zip(te.iterrows(),pred):
     results.append({"origin_year":y,"quarter":int(r.quarter),"sector":r.sector,
     "truth":float(r.target),"pred":float(v),"model":name,
     "observed_employment_lag2":float(r.emp_lag2)})
   except Exception as e:print("PERMIT MODEL ERROR",y,name,repr(e),flush=True)
 p=pd.DataFrame(results);p.to_csv(ROOT/"permit_employment_model_predictions.csv",index=False)
 stats=eval_forecasts(p,"permits")
 # Additional construction-only stats
 for s in stats:
  g=p[(p.model==s["model"])&(p.sector=="23")]
  s["construction_MAE"]=round(float(mean_absolute_error(g.truth,g.pred)),4) if len(g) else None
 print("PERMIT METRICS",json.dumps(stats),flush=True)
 return p,stats,current

def probe_archive():
 import zipfile
 u="https://data.bls.gov/cew/data/files/2013/csv/2013_all_county_high_level.zip"
 try:
  b=fetch(u,2)
  print("ARCHIVE_PROBE downloaded",len(b),"bytes",flush=True)
  z=zipfile.ZipFile(io.BytesIO(b))
  names=z.namelist()
  print("ARCHIVE_FILES",len(names),"FIRST_15",names[:15],flush=True)
  choices=[x for x in names if x.lower().endswith(".csv")]
  if choices:
   with z.open(choices[0]) as f:print("ARCHIVE_CSV_SAMPLE",f.readline()[:350],flush=True)
  else:
   sheets=[x for x in names if x.lower().endswith((".xls",".xlsx"))]
   print("ARCHIVE_EXCEL_FILES",len(sheets),"EXAMPLES",sheets[:15],flush=True)
 except Exception as e:print("ARCHIVE_PROBE_FAILED",repr(e),flush=True)

def main():
 result={"timestamp_utc":pd.Timestamp.utcnow().isoformat(),"sources":{
 "patents":"https://ncses.nsf.gov/pubs/nsb20241/assets/data/swbinv-1.xlsx",
 "qcew":"https://data.bls.gov/cew/data/api/{year}/{qtr}/area/{fips}.csv",
 "permits":"https://data.montgomerycountymd.gov/api/views/i26v-w6bd/rows.csv?accessType=DOWNLOAD",
 "patent_naics_bridge":"https://github.com/CommerceDataService/cpc-naics (experimental crosswalk; not directly applied to NSF WIPO aggregates)"},
 "notes":["Exploratory 8-county panel not a representative national estimate.",
 "Historical revised releases; patent workbook assembled in 2024. Not a genuine real-time vintage backtest.",
 "Patent county is INVENTOR geography; private QCEW county is establishment geography.",
 "Permit data relate only to Montgomery County; no causal effect identified.",
 "Patent technology fields are input features without asserted 1:1 NAICS crosswalk."]}
 probe_archive()
 q,errors=download_qcew()
 result["qcew_rows"]=len(q);result["qcew_fetch_failures"]=len(errors)
 patents,fields=download_patents()
 result["patent_technology_fields"]=len(fields);result["patent_rows"]=len(patents)
 d,ps,panel=patent_study(q,patents)
 result["patent_backtest"]=ps
 result["patent_panel_rows"]=len(panel)
 permit,features=permit_download()
 result["permit_rows"]=len(permit)
 issued=permit.approval_days[(permit.approval_days>=0)&(permit.approval_days<3650)]
 result["permit_approval"]={"n":len(issued),"median":float(issued.median()),"p90":float(issued.quantile(.9)),
 "negative":int((permit.approval_days<0).sum()),"missing":int(permit.approval_days.isna().sum())}
 d2,rs,panel2=permit_study(q,features)
 result["permit_employment_backtest"]=rs
 result["permit_panel_rows"]=len(panel2)
 (ROOT/"summary.json").write_text(json.dumps(result,indent=2))
 print("SUMMARY",json.dumps(result),flush=True)
if __name__=="__main__":main()
