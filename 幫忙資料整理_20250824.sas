/*第一階段-整理目標函數資料*/
/* ===== 1) 匯入資料 ===== */
proc import out=ESG_data1
     datafile="/home/u64061874/ESG_data1.xlsx"
     dbms=xlsx replace;
run;

/* ===== 2) 資料處理：文字→數值、衍生欄位 ===== */
data ESG_data2;
  set ESG_data1;

  /* 文字轉數值（若原本就為數值會原值帶入） */
  expenditure2 = expenditure + 0;
  PPE2         = input(PPE,         ?? best32.);
  CLL2         = input(CLL,         ?? best32.);
  NCLL2        = input(NCLL,        ?? best32.);
  RD2          = input(RD,          ?? best32.);
  GIA2         = input(GIA,         ?? best32.);
  NGW2         = input(NGW,         ?? best32.);
  Tobins_Q12   = input(Tobins_Q1,   ?? best32.);
  Tobins_Q22   = input(Tobins_Q2,   ?? best32.);

  /* 刪除舊欄位、以新欄位覆蓋原名 */
  drop expenditure PPE CLL NCLL RD GIA NGW Tobins_Q1 Tobins_Q2;
  rename expenditure2=expenditure
         PPE2=PPE
         CLL2=CLL
         NCLL2=NCLL
         RD2=RD
         GIA2=GIA
         NGW2=NGW
         Tobins_Q12=Tobins_Q1
         Tobins_Q22=Tobins_Q2;

  /* 衍生欄位 */
  LL         = CLL + NCLL;
  OtherIntan = GIA - NGW;
run;

/* （可選）排序列順序：先 year, month, 再 companyID */
proc sort data=ESG_data2 out=ESG_data2_sor;
  by year month companyID;
run;

/* ===== 3) 固定最終欄位順序 + 僅保留指定欄位 ===== */
data ESG_data3;
  retain companyID companyName year month
         income cost expenditure
         PPE CLL NCLL LL RD GIA NGW OtherIntan
         Tobins_Q1 Tobins_Q2;
  set ESG_data2_sor(keep=
         companyID companyName year month
         income cost expenditure
         PPE CLL NCLL LL RD GIA NGW OtherIntan
         Tobins_Q1 Tobins_Q2);
run;

/* ===== 4) 匯出 ===== */
proc export data=ESG_data3
     outfile="/home/u64061874/ESG_data3.xlsx"
     dbms=xlsx replace;
run;

/*第二階段-整理Tobit迴歸變數*/
/* ===== 0) 開啟訊息方便除錯 ===== */
options notes stimer source nosyntaxcheck spool;

/* ===== 1) 匯入三個檔案 ===== */
proc import datafile="/home/u64061874/financial_data_stage2_1.xlsx"
  out=financial_data_stage2_1_raw dbms=xlsx replace; getnames=yes; run;

proc import datafile="/home/u64061874/age.xlsx"
  out=age_raw dbms=xlsx replace; getnames=yes; run;

proc import datafile="/home/u64061874/foreign_currency.xlsx"
  out=foreign_currency_raw dbms=xlsx replace; getnames=yes; run;

/* ===== 2) 標準化鍵（不更動原 companyID/companyName/year/month） ===== */
/* 用 OPEN+VARNUM 檢查是否有 month 欄位，避免 vexist 問題 */
%macro normalize_keys(in=, out=);
  %local dsid has_month rc;
  %let dsid = %sysfunc(open(&in,i));
  %let has_month = %sysfunc(varnum(&dsid, month));
  %let rc = %sysfunc(close(&dsid));

data &out;
  set &in;

  length companyID_key $32;

  /* companyID → 大寫字串鍵（cats() 避免 $BEST NOTE） */
  if vtype(companyID)='C' then companyID_key = upcase(strip(companyID));
  else companyID_key = upcase(strip(cats(companyID)));

  /* 新增 year_n / month_n 作為數值鍵（僅用於 join/彙總） */
  if vtype(year)='C' then year_n = input(strip(year), ?? best32.);
  else year_n = year;

  %if &has_month > 0 %then %do;
    if vtype(month)='C' then month_n = input(strip(month), ?? best32.);
    else if nmiss(month)=0 then month_n = month;
    else month_n = .;
  %end;
  %else %do;
    month_n = .;
  %end;
run;
%mend;

%normalize_keys(in=financial_data_stage2_1_raw, out=financial_data_stage2_1_k);
%normalize_keys(in=age_raw,                         out=age_k);
%normalize_keys(in=foreign_currency_raw,            out=foreign_currency_k);

/* ===== 2.5) 依公司ID「排除名單」→ 用 cats()/upcase() + NOT IN ===== */
data financial_data_stage2_1_keep;
  set financial_data_stage2_1_k;
  where not ( upcase(cats(companyID)) in (
    "M1100","M1200","M1300","M1400","M1500","M1600","M1700","M1721","M1722",
    "M1800","M1900","M2000","M2100","M2200","M2300","M2324","M2325","M2326",
    "M2327","M2328","M2329","M2330","M2331","M2500","M2600","M2700","M2900",
    "M9700","M9900","O1721","O1722","O2324","O2325","O2326","O2327","O2328",
    "O2329","O2330","O2331","OTC12","OTC13","OTC14","OTC15","OTC20","OTC21",
    "OTC23","OTC25","OTC26","OTC27","OTC29","OTC32","OTC89","OTC97","OTC99",
    "Y5555","Y8886","Y8888","Y9999"
  ));
run;

/* ===== 3) 精簡欄位 ===== */
data age_k2;                 /* age 僅需 companyID + year 對齊 */
  set age_k;
  keep companyID_key year_n age;
run;

data foreign_currency_k2;    /* 代碼/名稱維持字元型 */
  set foreign_currency_k;
  keep companyID_key year_n month_n
       TSE_number TSE_name TEJ_number TEJ_name
       FX_Gain FX_Loss;
run;

/* ===== 4) 合併（以 financial_data_stage2_1_keep 為主） ===== */
proc sql;
  create table merged_step1 as
  select a.*,
         b.age,
         c.TSE_number, c.TSE_name, c.TEJ_number, c.TEJ_name,
         c.FX_Gain, c.FX_Loss
  from financial_data_stage2_1_keep as a
  left join age_k2 as b
    on a.companyID_key=b.companyID_key and a.year_n=b.year_n
  left join foreign_currency_k2 as c
    on a.companyID_key=c.companyID_key and a.year_n=c.year_n and a.month_n=c.month_n
  ;
quit;

/* ===== 5) 將字元數字欄位轉成數值（排除鍵與代碼/名稱） ===== */
proc contents data=merged_step1 out=_vars_(keep=name type) noprint; run;

proc sql noprint;
  select name into :charvars separated by ' '
  from _vars_
  where type=2
    and upcase(name) not in (
      'COMPANYID','COMPANYNAME','YEAR','MONTH','COMPANYID_KEY',
      'TSE_NUMBER','TEJ_NUMBER','TSE_NAME','TEJ_NAME'
    );
quit;

%macro char2num(ds, out=);
data &out;
  set &ds;
  %local i var;
  %let i=1;
  %let var=%scan(&charvars,&i);
  %do %while(%length(&var) > 0);
    _n_&var = input(compress(&var, ', '), ?? best32.);
    drop &var;
    rename _n_&var = &var;
    %let i=%eval(&i+1);
    %let var=%scan(&charvars,&i);
  %end;
run;
%mend;

%char2num(merged_step1, out=merged_step1_num);

/* ===== 6) 衍生變數 ===== */
data merged_step2;
  set merged_step1_num;
  dummy_FCF = (not missing(free_cash_flow) and free_cash_flow ne 0);
  if age>0 then ln_age=log(age); else ln_age=.;
  foreign_currency = ((not missing(FX_Gain) and FX_Gain ne 0)
                   or (not missing(FX_Loss) and FX_Loss ne 0));
run;

/* ===== 7) 市佔率（year_n + month_n 基準） ===== */
/* TSE */
proc summary data=merged_step2 nway;
  class year_n month_n TSE_number;
  var income;
  output out=sum_TSE(drop=_type_ _freq_) sum=income_TSE_sum;
run;

proc sort data=merged_step2; by year_n month_n TSE_number; run;
proc sort data=sum_TSE;      by year_n month_n TSE_number; run;

data merged_step3;
  merge merged_step2(in=a) sum_TSE;
  by year_n month_n TSE_number;
  if a then do;
    if not missing(TSE_number) and not missing(income_TSE_sum) and income_TSE_sum>0
      then market_share_TSE = income / income_TSE_sum;
    else market_share_TSE = .;
  end;
run;

/* TEJ */
proc summary data=merged_step3 nway;
  class year_n month_n TEJ_number;
  var income;
  output out=sum_TEJ(drop=_type_ _freq_) sum=income_TEJ_sum;
run;

proc sort data=merged_step3; by year_n month_n TEJ_number; run;
proc sort data=sum_TEJ;      by year_n month_n TEJ_number; run;

data merged_final;
  merge merged_step3(in=a) sum_TEJ;
  by year_n month_n TEJ_number;
  if a then do;
    if not missing(TEJ_number) and not missing(income_TEJ_sum) and income_TEJ_sum>0
      then market_share_TEJ = income / income_TEJ_sum;
    else market_share_TEJ = .;
  end;
run;

/* ===== 8) 最終輸出（僅 9 欄；保留原 companyID/companyName/year/month 原型態） ===== */
data ESG_data2_final;
  set merged_final;
  keep companyID companyName year month
       market_share_TSE market_share_TEJ
       dummy_FCF ln_age foreign_currency;
run;

/* ===== 9) 匯出 ===== */
proc export data=ESG_data2_final
  outfile="/home/u64061874/ESG_data4.xlsx"
  dbms=xlsx replace;
  sheet="merged";
  putnames=yes;
run;

/*計算HHI*/
/* ===== 10) 計算月度 HHI（TSE 與 TEJ；*10000） ===== */
/* TSE：以 year_n + month_n + TSE_number 彙總 */
proc sql;
  create table hhi_TSE_month as
  select year_n, month_n, TSE_number,
         sum(market_share_TSE*market_share_TSE) as HHI_TSE_raw,
         calculated HHI_TSE_raw*10000 as HHI_TSE
  from merged_final
  where not missing(TSE_number)
    and not missing(market_share_TSE)
    and market_share_TSE >= 0
  group by year_n, month_n, TSE_number;
quit;

/* TEJ：以 year_n + month_n + TEJ_number 彙總 */
proc sql;
  create table hhi_TEJ_month as
  select year_n, month_n, TEJ_number,
         sum(market_share_TEJ*market_share_TEJ) as HHI_TEJ_raw,
         calculated HHI_TEJ_raw*10000 as HHI_TEJ
  from merged_final
  where not missing(TEJ_number)
    and not missing(market_share_TEJ)
    and market_share_TEJ >= 0
  group by year_n, month_n, TEJ_number;
quit;

/* ===== 11) 將 HHI 併回逐筆公司資料 ===== */
proc sort data=merged_final;   by year_n month_n TSE_number; run;
proc sort data=hhi_TSE_month;  by year_n month_n TSE_number; run;

data tmp_with_hhi_tse;
  merge merged_final(in=a) hhi_TSE_month;
  by year_n month_n TSE_number;
  if a;
run;

proc sort data=tmp_with_hhi_tse; by year_n month_n TEJ_number; run;
proc sort data=hhi_TEJ_month;    by year_n month_n TEJ_number; run;

data merged_with_hhi;
  merge tmp_with_hhi_tse(in=a) hhi_TEJ_month;
  by year_n month_n TEJ_number;
  if a;
run;

/* ===== 12) 產出最終欄位（加入 HHI_TSE / HHI_TEJ） ===== */
/* 保留原 companyID/companyName/year/month 的原始型態，其他為導出欄位 */
data ESG_data2_final_v2;
  set merged_with_hhi;
  keep companyID companyName year month
       market_share_TSE market_share_TEJ
       HHI_TSE HHI_TEJ
       dummy_FCF ln_age foreign_currency;
run;

/* ===== 13) 匯出最新版 ===== */
proc export data=ESG_data2_final_v2
  outfile="/home/u64061874/ESG_data5.xlsx"
  dbms=xlsx replace;
  sheet="merged_with_HHI";
  putnames=yes;
run;

/*第三階段-控制變數*/
options notes stimer source nosyntaxcheck;

/* === 0) 從現有 financial_control 建唯一 rowid（用來合併） === */
data fc_base;
  set financial_control;
  rowid = _n_;
run;

/* === 1) 建 TEJ 一熱編碼，之後轉成寬表：每一種分類變成一欄 dummy_TEJ_<code> === */
data tej_pre;
  set fc_base(keep=rowid TEJ_number);
  length code $64 name $32 val 8;
  code = upcase(cats(TEJ_number));
  if not missing(code) then do;
    name = cats('dummy_TEJ_', prxchange('s/[^A-Za-z0-9]+/_/o', -1, code));
    name = upcase(name);
    if length(name)>32 then name = substr(name, 1, 32);
    val = 1;
    output;
  end;
  keep rowid name val;
run;

proc sort data=tej_pre; by rowid name; run;
proc transpose data=tej_pre out=tej_wide(drop=_name_);
  by rowid;
  id name;    /* 以 name 的值動態當欄名（dummy_TEJ_*） */
  var val;    /* 值為 1（其餘缺值等下補 0） */
run;

/* === 2) 建 TSE 一熱編碼、轉寬表 === */
data tse_pre;
  set fc_base(keep=rowid TSE_number);
  length code $64 name $32 val 8;
  code = upcase(cats(TSE_number));
  if not missing(code) then do;
    name = cats('dummy_TSE_', prxchange('s/[^A-Za-z0-9]+/_/o', -1, code));
    name = upcase(name);
    if length(name)>32 then name = substr(name, 1, 32);
    val = 1;
    output;
  end;
  keep rowid name val;
run;

proc sort data=tse_pre; by rowid name; run;
proc transpose data=tse_pre out=tse_wide(drop=_name_);
  by rowid;
  id name;    /* 以 name 的值動態當欄名（dummy_TSE_*） */
  var val;
run;

/* === 3) 擷取 dummy 欄名清單（用來把缺值補 0） === */
proc sql noprint;
  select name into :tej_list separated by ' '
  from dictionary.columns
  where libname='WORK' and memname='TEJ_WIDE' and upcase(name) ne 'ROWID';

  select name into :tse_list separated by ' '
  from dictionary.columns
  where libname='WORK' and memname='TSE_WIDE' and upcase(name) ne 'ROWID';
quit;

/* === 4) 合併回主表、計算 Lev、補 0、建立年度虛擬變數 === */
proc sort data=fc_base;  by rowid; run;
proc sort data=tej_wide; by rowid; run;
proc sort data=tse_wide; by rowid; run;

data ESG_data6;
  merge fc_base tej_wide tse_wide;
  by rowid;

  /* Lev = total_asset / equity（避免除以 0；自動處理文字/數值） */
  length _ta _eq Lev 8;
  if vtype(total_asset)='C' then _ta = input(total_asset, best.); else _ta = total_asset;
  if vtype(equity)     ='C' then _eq = input(equity     , best.); else _eq = equity;
  if missing(_eq) or _eq=0 then Lev = .;
  else Lev = _ta / _eq;

  /* 把所有 dummy 缺值補成 0（transpose 來的缺值代表「不是該類別」） */
  %macro zero_fill;
    %if %length(&tej_list) > 0 %then %do;
      array a_tej {*} &tej_list;
      do _i = 1 to dim(a_tej); if missing(a_tej[_i]) then a_tej[_i] = 0; end;
    %end;
    %if %length(&tse_list) > 0 %then %do;
      array a_tse {*} &tse_list;
      do _j = 1 to dim(a_tse); if missing(a_tse[_j]) then a_tse[_j] = 0; end;
    %end;
  %mend;
  %zero_fill

  /* 年度虛擬變數：year_2015 ~ year_2024 */
  length year_n 8 year_2015-year_2024 8;
  if vtype(year)='C' then year_n = input(year, best.); else year_n = year;
  array y{10} year_2015-year_2024;
  do __k = 0 to 9;
    y{__k+1} = (year_n = (2015 + __k));
  end;

  drop _ta _eq _i _j __k year_n rowid;
run;

/* 檢查一下生成的欄位 */
proc contents data=ESG_data6; run;
/* proc freq data=ESG_data6; tables dummy_TEJ_M11A dummy_TSE_M11A / missing; run; */

/*輸出 Excel*/
proc export data=ESG_data6
  outfile="/home/u64061874/ESG_data6.xlsx"
  dbms=xlsx replace;
  sheet="ESG_data6";
  putnames=yes;
run;

/*ESG資料*/
/*匯入資料*/
/* ===== 1) 匯入 ESG.xlsx ===== */
proc import out=ESG
     datafile="/home/u64061874/ESG.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

/* ===== 2) 計算 ODR、OSR ===== */
data ESG_calc;
  set ESG;
  length ODR OSR 8.;
  if not missing(BS) and BS ne 0 then ODR = (BS - CBS) / BS; else ODR = .;
  if not missing(SS) and SS ne 0 then OSR = (SS - CSS) / SS; else OSR = .;
run;

/* ===== 3) 保留並排列指定欄位順序 ===== */
data ESG_keep;
  retain companyID companyName year month ODR OSR SED DPR SPR IOR COW;
  set ESG_calc(keep=companyID companyName year month ODR OSR SED DPR SPR IOR COW);
run;

/* ===== 4) 依 year、month、companyID 排序 ===== */
proc sort data=ESG_keep out=ESG_result;
  by year month companyID;
run;

/* ===== 5) 匯出到 Excel ===== */
proc export data=ESG_result
  outfile="/home/u64061874/ESG_data7.xlsx"
  dbms=xlsx replace;
  sheet="result";
  putnames=yes;
run;