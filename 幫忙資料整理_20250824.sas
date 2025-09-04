/*第一階段-整理目標函數資料*/
/* ===== 0) 基本設定（可選）===== */
options notes stimer source nosyntaxcheck;

/* ===== 1) 匯入 ===== */
proc import out=ESG_data1
     datafile="/home/u64061874/ESG_data1.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

/* ===== 2) 工具巨集：字串→數值（若已是數值則原值帶入），最後覆蓋原名 ===== */
%macro norm(var);
  length &var._num 8;
  if vtype(&var)='C' then &var._num = input(&var, best32.);
  else                    &var._num = &var;
  drop &var;
  rename &var._num = &var;
%mend;

/* ===== 2a) 清洗 ESG_data1，並建立衍生欄位 ===== */
data ESG_data2;
  set ESG_data1;

  /* 視需要也可把 year 轉成數值：取消下一行註解 */
  /* %norm(year) */

  /* 對可能為字串的數值欄位做標準化 */
  %norm(expenditure)
  %norm(PPE)
  %norm(CLL)
  %norm(NCLL)
  %norm(RD)
  %norm(NGW)
  %norm(GIA)
  %norm(Tobins_Q)

  /* 衍生欄位 */
  LL         = CLL + NCLL;
  OtherIntan = GIA - NGW;
run;

/* ===== 3) 排序（作為主資料）===== */
proc sort data=ESG_data2 out=ESG_data2_sor;
  by year companyID;
run;

/* ===== 4) 固定欄位順序 + 僅保留指定欄位 ===== */
data ESG_data3;
  retain company companyID companyName year
         income cost expenditure
         PPE CLL NCLL LL RD GIA NGW OtherIntan
         Tobins_Q;
  set ESG_data2_sor(keep=
         company companyID companyName year
         income cost expenditure
         PPE CLL NCLL LL RD GIA NGW OtherIntan
         Tobins_Q);
run;

/* ===== 5) 匯出 ===== */
proc export data=ESG_data3
  outfile="/home/u64061874/ESG_data3_stage1.xlsx"
  dbms=xlsx replace;
run;

/*第二階段-整理Tobit迴歸變數*/
/* ===== 0) 開啟訊息方便除錯 ===== */
options notes stimer source nosyntaxcheck spool;

/* ===== 1) 匯入四個主檔 + 新增：ESG_control_1 與 addition ===== */
proc import datafile="/home/u64061874/financial_data_stage2_1.xlsx"
  out=financial_data_stage2_1_raw dbms=xlsx replace; getnames=yes; run;

proc import datafile="/home/u64061874/age.xlsx"
  out=age_raw dbms=xlsx replace; getnames=yes; run;

proc import datafile="/home/u64061874/foreign_currency.xlsx"
  out=foreign_currency_raw dbms=xlsx replace; getnames=yes; run;

/* addition：之後用來覆蓋 income */
proc import datafile="/home/u64061874/addition.xlsx"
  out=addition_raw dbms=xlsx replace; getnames=yes; run;

/* ESG_control_1：提供 FCF 來計算 dummy_FCF */
proc import datafile="/home/u64061874/ESG_control_1.xlsx"
  out=esg_ctrl_raw dbms=xlsx replace; getnames=yes; run;

/* ===== 2) 標準化鍵 ===== */
%macro normalize_keys(in=, out=);
  %local dsid has_month rc;
  %let dsid = %sysfunc(open(&in,i));
  %let has_month = %sysfunc(varnum(&dsid, month));
  %let rc = %sysfunc(close(&dsid));

data &out;
  set &in;

  length companyID_key $32;

  /* companyID 統一為大寫字串鍵 */
  if vtype(companyID)='C' then companyID_key = upcase(strip(companyID));
  else companyID_key = upcase(strip(cats(companyID)));

  /* 建立數值用 year_n / month_n */
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

/* addition 專用（year 六碼 YYYYMM，需拆成年/月 + income 轉數值） */
%macro normalize_addition(in=, out=);
data &out;
  set &in;

  length companyID_key $32;

  if vtype(companyID)='C' then companyID_key = upcase(strip(companyID));
  else companyID_key = upcase(strip(cats(companyID)));

  length _y $32;
  if vtype(year)='C' then _y = compress(strip(year), , 'kd');
  else _y = compress(strip(cats(year)), , 'kd');

  if length(_y) >= 6 then do;
    year6   = input(substr(_y,1,6), best32.);
    year_n  = floor(year6/100);
    month_n = mod(year6,100);
  end;
  else do;
    year6 = .; year_n = .; month_n = .;
  end;

  length income_add 8;
  if vtype(income)='C' then income_add = inputn(strip(income),'best32.');
  else income_add = income;

  keep companyID companyName companyID_key year_n month_n income_add;
run;
proc sort data=&out nodupkey; by companyID_key year_n month_n; run;
%mend;

/* ESG_control_1 專用（若 year 為六碼，取前四碼；FCF 轉數值為 FCF_ctrl） */
%macro normalize_ctrl(in=, out=);
data &out;
  set &in;

  length companyID_key $32;
  if vtype(companyID)='C' then companyID_key = upcase(strip(companyID));
  else companyID_key = upcase(strip(cats(companyID)));

  length _y $32;
  if vtype(year)='C' then _y = compress(strip(year), , 'kd');
  else _y = compress(strip(cats(year)), , 'kd');

  if length(_y) >= 6 then year_n = input(substr(_y,1,4), best32.);
  else year_n = input(_y, ?? best32.);

  length FCF_ctrl 8;
  if vtype(FCF)='C' then FCF_ctrl = input(compress(FCF, ', '), ?? best32.);
  else FCF_ctrl = FCF;

  keep companyID_key year_n FCF_ctrl;
run;
proc sort data=&out nodupkey; by companyID_key year_n; run;
%mend;

%normalize_keys(in=financial_data_stage2_1_raw, out=financial_data_stage2_1_k);
%normalize_keys(in=age_raw,                         out=age_k);
%normalize_keys(in=foreign_currency_raw,            out=foreign_currency_k);
%normalize_addition(in=addition_raw,                out=addition_k);
%normalize_ctrl(in=esg_ctrl_raw,                    out=esg_ctrl_k);

/* ===== 2.5) 依公司ID排除名單 ===== */
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
data age_k2;  set age_k;  keep companyID_key year_n age; run;

data foreign_currency_k2;
  set foreign_currency_k;
  keep companyID_key year_n month_n
       TSE_number TSE_name TEJ_number TEJ_name
       FX_Gain FX_Loss;
run;

/* ===== 4) 合併（以 financial_data_stage2_1_keep 為主） =====
   串接 addition_k 取 income_add 覆蓋 income；串接 esg_ctrl_k 取 FCF_ctrl 計算 dummy_FCF */
proc sql;
  create table merged_step1 as
  select a.*,
         b.age,
         c.TSE_number, c.TSE_name, c.TEJ_number, c.TEJ_name,
         c.FX_Gain, c.FX_Loss,
         d.income_add,
         e.FCF_ctrl
  from financial_data_stage2_1_keep as a
  left join age_k2              as b
    on a.companyID_key=b.companyID_key and a.year_n=b.year_n
  left join foreign_currency_k2 as c
    on a.companyID_key=c.companyID_key and a.year_n=c.year_n and a.month_n=c.month_n
  left join addition_k          as d
    on a.companyID_key=d.companyID_key and a.year_n=d.year_n and a.month_n=d.month_n
  left join esg_ctrl_k          as e
    on a.companyID_key=e.companyID_key and a.year_n=e.year_n
  ;
quit;

/* ===== 4.5) income 覆蓋邏輯（addition 優先；無則用原 income） ===== */
data merged_step1;
  set merged_step1;
  length _income_base 8;
  if vtype(income)='C' then _income_base = input(compress(income, ', '), ?? best32.);
  else _income_base = income;

  if not missing(income_add) then income = income_add;
  else income = _income_base;

  drop _income_base income_add;
run;

/* ===== 5) 將字元數字欄位轉數值（排除鍵與代碼/名稱） ===== */
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
/* dummy_FCF：改用 ESG_control_1 的 FCF_ctrl（公司×年份），FCF_ctrl>0 → 1 */
data merged_step2;
  set merged_step1_num;

  dummy_FCF = (not missing(FCF_ctrl) and FCF_ctrl > 0);

  if age>0 then ln_age=log(age); else ln_age=.;

  foreign_currency = ((not missing(FX_Gain) and FX_Gain ne 0)
                   or (not missing(FX_Loss) and FX_Loss ne 0));
run;

/* ===== 7) 市佔率（以 year_n + month_n 彙總；income 已為 addition 版） ===== */
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

/* ===== 10) 計算月度 HHI（*10000） ===== */
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

/* 併回 HHI */
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

/* ===== 11.5) 2025 年 age 外推（四指標皆非缺 → 用 2024 age+1） ===== */
proc sql;
  create table age_2024 as
  select companyID_key, max(age) as age_2024
  from merged_with_hhi
  where year_n = 2024
  group by companyID_key;
quit;

proc sort data=merged_with_hhi; by companyID_key year_n month_n; run;
proc sort data=age_2024;       by companyID_key;                run;

data merged_with_hhi2;
  merge merged_with_hhi(in=a) age_2024;
  by companyID_key;
  if a;

  if year_n = 2025
     and nmiss(market_share_TSE, market_share_TEJ, HHI_TSE, HHI_TEJ) = 0 then do;
     if missing(age) or age<=0 then age = age_2024 + 1;
  end;

  if age>0 then ln_age = log(age);
  else ln_age = .;

  drop age_2024;
run;

/* ===== 11.6) 依 year/month/companyID 排序，並建立新的 year = YYYYMM ===== */
data merged_with_hhi3;
  set merged_with_hhi2(drop=year month);
  year = year_n*100 + month_n;
run;

proc sort data=merged_with_hhi3 out=merged_with_hhi_sorted;
  by year_n month_n companyID;
run;

/* ===== 12) 產出最終欄位 ===== */
data ESG_data2_final_v2;
  retain companyID companyName year
         market_share_TSE market_share_TEJ
         HHI_TSE HHI_TEJ
         dummy_FCF ln_age foreign_currency;
  set merged_with_hhi_sorted;

  keep   companyID companyName year
         market_share_TSE market_share_TEJ
         HHI_TSE HHI_TEJ
         dummy_FCF ln_age foreign_currency;
run;

/* ===== 13) 匯出 ===== */
proc export data=ESG_data2_final_v2
  outfile="/home/u64061874/ESG_data5_stage2.xlsx"
  dbms=xlsx replace;
  sheet="merged_with_HHI";
  putnames=yes;
run;

/*第三階段-控制變數*/
/* ====================== 0) 匯入 ====================== */
options notes stimer source nosyntaxcheck;

proc import out=ESG_control_1
     datafile="/home/u64061874/ESG_control_1.xlsx"
     dbms=xlsx replace; getnames=yes; run;

proc import out=ESG_control_2
     datafile="/home/u64061874/ESG_control_2.xlsx"
     dbms=xlsx replace; getnames=yes; run;

/* ====================== 1) 以 ESG_control_1 為主的穩健 LEFT JOIN ====================== */
/* 只帶 ESG_control_2 裡「非鍵、且主表沒有」的欄位，避免 b.* 造成重名衝突 */
proc sql noprint;
  create table _a_cols as
  select upcase(name) as name
  from dictionary.columns
  where libname='WORK' and memname='ESG_CONTROL_1';

  create table _b_cols as
  select cats('b.', name) as sel
  from dictionary.columns
  where libname='WORK' and memname='ESG_CONTROL_2'
    and upcase(name) not in ('YEAR','COMPANYID')
    and upcase(name) not in (select name from _a_cols);

  select sel into :b_list separated by ', '
  from _b_cols;
quit;

%macro do_join;
  %if %length(&b_list) %then %do;
    proc sql;
      create table financial_control as
      select a.*, &b_list
      from ESG_control_1 as a
      left join ESG_control_2 as b
        on a.year = b.year
       and a.companyID = b.companyID
      order by a.year, a.companyID;
    quit;
  %end;
  %else %do;
    proc sql;
      create table financial_control as
      select a.*
      from ESG_control_1 as a
      left join ESG_control_2 as b
        on a.year = b.year
       and a.companyID = b.companyID
      order by a.year, a.companyID;
    quit;
  %end;
%mend;
%do_join

/* ====================== 2) 建立 rowid ====================== */
data fc_base;
  set financial_control;
  rowid = _n_;
run;

/* ====================== 3) TEJ 一熱編碼 ====================== */
data tej_pre;
  set fc_base(keep=rowid TEJ_number);
  length code $64 name $32 val 8;
  code = upcase(cats(TEJ_number));
  if not missing(code) then do;
    name = cats('DUMMY_TEJ_', prxchange('s/[^A-Za-z0-9]+/_/o', -1, code));
    name = upcase(name);
    if length(name)>32 then name = substr(name, 1, 32);
    val = 1;
    output;
  end;
  keep rowid name val;
run;

proc sort data=tej_pre; by rowid name; run;
proc transpose data=tej_pre out=tej_wide(drop=_name_);
  by rowid; id name; var val; run;

/* ====================== 4) TSE 一熱編碼 ====================== */
data tse_pre;
  set fc_base(keep=rowid TSE_number);
  length code $64 name $32 val 8;
  code = upcase(cats(TSE_number));
  if not missing(code) then do;
    name = cats('DUMMY_TSE_', prxchange('s/[^A-Za-z0-9]+/_/o', -1, code));
    name = upcase(name);
    if length(name)>32 then name = substr(name, 1, 32);
    val = 1;
    output;
  end;
  keep rowid name val;
run;

proc sort data=tse_pre; by rowid name; run;
proc transpose data=tse_pre out=tse_wide(drop=_name_);
  by rowid; id name; var val; run;

/* ====================== 5) 擷取 dummy 欄名清單（可能為空） ====================== */
proc sql noprint;
  select name into :tej_list separated by ' '
  from dictionary.columns
  where libname='WORK' and memname='TEJ_WIDE' and upcase(name) ne 'ROWID';

  select name into :tse_list separated by ' '
  from dictionary.columns
  where libname='WORK' and memname='TSE_WIDE' and upcase(name) ne 'ROWID';
quit;

/* ====================== 6) 合併 + 指標計算（含年度虛擬變數） ====================== */
proc sort data=fc_base;  by rowid; run;
proc sort data=tej_wide; by rowid; run;
proc sort data=tse_wide; by rowid; run;

data ESG_data6_raw;
  merge fc_base tej_wide tse_wide;
  by rowid;

  /* ---- size / Lev ---- */
  length _ta _eq _fcf 8;
  if vtype(total_asset)='N' then _ta = total_asset; else _ta = inputn(total_asset, 'best.');
  if vtype(equity)     ='N' then _eq = equity;     else _eq = inputn(equity,     'best.');

  if missing(_ta) or _ta<=0 then size=.;
  else size = log(_ta);

  if missing(_eq) or _eq=0 then Lev = .;
  else Lev = _ta / _eq;

  /* ---- dummy_FCF 以 ESG_control_1/2 的 FCF 值判斷 ---- */
  if vtype(FCF)='N' then _fcf = FCF; else _fcf = inputn(FCF,'best.');
  dummy_FCF = (_fcf > 0);

  /* ---- 補 0：一熱欄位 ---- */
  %if %length(&tej_list) > 0 %then %do;
    array a_tej {*} &tej_list;
    do _i = 1 to dim(a_tej);
      if missing(a_tej[_i]) then a_tej[_i] = 0;
    end;
  %end;
  %if %length(&tse_list) > 0 %then %do;
    array a_tse {*} &tse_list;
    do _j = 1 to dim(a_tse);
      if missing(a_tse[_j]) then a_tse[_j] = 0;
    end;
  %end;

  /* ---- 年度虛擬變數：year 為 YYYYMM → 取前四碼 ---- */
  length _y $12 year4 8;
  if vtype(year)='C' then _y = compress(strip(year), , 'kd');
  else _y = compress(strip(cats(year)), , 'kd');
  if length(_y) >= 4 then year4 = input(substr(_y,1,4), best32.);
  else year4 = .;

  array y{2015:2024} year_2015-year_2024;
  do __k = 2015 to 2024;
    y{__k} = (year4 = __k);
  end;

  /* ---- 清掉暫存欄位（此步先不 drop，最後統一清）---- */
run;

/* ====================== 7) 欄位順序（固定在前；其餘順延） ====================== */
data ESG_data6;
  retain
    company companyID companyName year
    total_asset size RGR FCF DSR
    TEJ_number TEJ_name TSE_number TSE_name equity
    /* TEJ Dummy */
    &tej_list
    /* TSE Dummy */
    &tse_list
    /* 其他 */
    Lev dummy_FCF year_2015-year_2024
  ;
  set ESG_data6_raw;
run;

/* ====================== 8) 最後清除暫存欄位，再匯出 ====================== */
/* 即使部份欄位不存在，也不會出錯 */
data ESG_data6_clean;
  set ESG_data6;
  drop rowid _ta _eq _i _j __k _fcf year4 _y;
run;

proc export data=ESG_data6_clean
  outfile="/home/u64061874/ESG_data6_control.xlsx"
  dbms=xlsx replace;
  sheet="ESG_data6";
  putnames=yes;
run;

/*ESG資料*/
options notes stimer source msglevel=i;

/* 1) 匯入 */
proc import out=ESG_esg_data
     datafile="/home/u64061874/ESG_esg_data.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

/* 2) 建 year_new = YYYYMM；若 year 或 month 缺值/不合法，就設為缺值 . (匯出到 Excel 會是空白) */
data _prep1;
  set ESG_esg_data;
  length year_new 8;

  /* 正規化 year / month（可為字元或數值） */
  if vtype(year)  = 'N' then y = year;  else y = input(strip(year),  best.);
  if vtype(month) = 'N' then m = month; else m = input(strip(month), best.);

  if missing(y) or missing(m) or not (1 <= m <= 12) then year_new = .;
  else year_new = y*100 + m;  /* 例如 2016*100+3 = 201603 */

  drop y m;
run;

/* 3) 丟掉舊的 year / month，再把 year_new 改名為 year */
data _prep2;
  set _prep1;
  drop year month;
run;

data ESG_esg_export;
  /* 強制欄位順序 */
  retain companyID companyName year certificate ESG_score ESG_E ESG_S ESG_G;
  set _prep2(rename=(year_new=year));
  /* 只留要匯出的欄位 */
  keep companyID companyName year certificate ESG_score ESG_E ESG_S ESG_G;
run;

/* 4) 匯出（欄位順序就是上面 retain/keep 的順序） */
proc export data=ESG_esg_export
  outfile="/home/u64061874/ESG_esg_final.xlsx"
  dbms=xlsx replace;
  putnames=yes;
run;