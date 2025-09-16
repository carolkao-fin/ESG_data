/* ===============================
   0) 基本設定
================================ */
options notes stimer source msglevel=i;

/* ===== 0) 讀檔 ===== */
proc import out=ESG_data3_stage1  datafile="/home/u64061874/ESG_data3_stage1.xlsx"
     dbms=xlsx replace; getnames=yes; run;
proc import out=ESG_data5_stage2  datafile="/home/u64061874/ESG_data5_stage2.xlsx"
     dbms=xlsx replace; getnames=yes; run;
proc import out=addition          datafile="/home/u64061874/addition.xlsx"
     dbms=xlsx replace; getnames=yes; run;
proc import out=ESG_data6_control datafile="/home/u64061874/ESG_data6_control.xlsx"
     dbms=xlsx replace; getnames=yes; run;
/* 主表 */
proc import out=ESG_esg_final     datafile="/home/u64061874/ESG_esg_final.xlsx"
     dbms=xlsx replace; getnames=yes; run;

/* ===== 1) 只保留存在欄位 ===== */
%macro _filter_keep(ds=, list=, out=);
  proc contents data=&ds noprint out=_vars_(keep=name); run;
  %local i one _kept; %let _kept=;
  %let i=1; %let one=%scan(&list,&i,%str( ));
  %do %while(%length(&one));
    proc sql noprint;
      select count(*) into :_hit trimmed
      from _vars_ where upcase(name)=upcase("&one");
    quit;
    %if &_hit %then %let _kept=&_kept &one;
    %let i=%eval(&i+1);
    %let one=%scan(&list,&i,%str( ));
  %end;
  %global &out; %let &out=&_kept;
%mend;

/* ===== 2) 標準化鍵值（六碼年 + 只留數字的 companyID）===== */
%macro prep(ds=, out=, keep=);
  %_filter_keep(ds=&ds, list=&keep, out=_keep_ok);
  data &out;
    set &ds;
    length companyID_key $64 year6 8;

    /* companyID_key：轉字串、去空白、只留數字 */
    length _cid $200;
    if vtype(companyID)='C' then _cid=strip(companyID);
    else _cid=strip(cats(companyID));
    companyID_key = compress(upcase(_cid), , 'kd');

    /* year6：統一為六位數 YYYYMM（數值） */
    if vtype(year)='C' then do;
      length _y $32; _y=compress(strip(year), , 'kd');
      if length(_y)>=6 then year6 = input(substr(_y,1,6), best32.);
      else year6 = .;
    end;
    else if not missing(year) then do;
      if year>=100000 then year6 = int(year);
      else year6 = .;
    end;
    else year6=.;

    keep companyID companyName year companyID_key year6 &_keep_ok;
  run;
  proc sort data=&out; by companyID_key year6; run;
%mend;

/* ===== 字串數值欄位轉數值（僅對存在欄位） ===== */
%macro coerce_numeric(ds=, vars=);
  %_filter_keep(ds=&ds, list=&vars, out=_vars_ok);
  %if %superq(_vars_ok) ne %then %do;
    data &ds;
      set &ds;
      %local i v; %let i=1; %let v=%scan(%superq(_vars_ok),&i,%str( ));
      %do %while(%length(&v));
        length &v._n 8;
        if vtype(&v)='C' then &v._n = inputn(strip(&v),'best32.');
        else                   &v._n = &v;
        drop &v; rename &v._n = &v;
        %let i=%eval(&i+1);
        %let v=%scan(%superq(_vars_ok),&i,%str( ));
      %end;
    run;
  %end;
%mend;

/* ===== 3) 主表（ESG）===== */
%prep(ds=ESG_esg_final, out=work.main,
      keep=companyName certificate ESG_score ESG_E ESG_S ESG_G);

data work.main;
  set work.main;
  if cmiss(companyID, companyName, year, certificate,
           ESG_score, ESG_E, ESG_S, ESG_G)=8 then delete;
  if missing(year6) then delete;
run;

/* ===== d3（財務）===== */
%prep(ds=ESG_data3_stage1, out=work.d3,
      keep=income cost expenditure PPE CLL NCLL LL RD GIA NGW OtherIntan Tobins_Q);
%coerce_numeric(ds=work.d3,
      vars=income cost expenditure PPE CLL NCLL LL RD GIA NGW OtherIntan Tobins_Q);

/* ===== d5（產業/市場）===== */
%prep(ds=ESG_data5_stage2, out=work.d5,
      keep=dummy_FCF ln_age foreign_currency market_share_TSE HHI_TSE);

/* ===== d6（控制變數）===== */
%prep(ds=ESG_data6_control, out=work.d6,
      keep=size RGR FCF DSR
           DUMMY_TSE_M1100_ DUMMY_TSE_M1200_ DUMMY_TSE_M2700_ DUMMY_TSE_M1300_ DUMMY_TSE_M2500_
           DUMMY_TSE_M2200_ DUMMY_TSE_M2328_ DUMMY_TSE_M1400_ DUMMY_TSE_M9900_ DUMMY_TSE_M3700_
           DUMMY_TSE_M1500_ DUMMY_TSE_M1722_ DUMMY_TSE_M2325_ DUMMY_TSE_M1600_ DUMMY_TSE_M1721_
           DUMMY_TSE_M2331_ DUMMY_TSE_M1800_ DUMMY_TSE_M1900_ DUMMY_TSE_M2000_ DUMMY_TSE_M3800_
           DUMMY_TSE_M2100_ DUMMY_TSE_M2600_ DUMMY_TSE_M2324_ DUMMY_TSE_M2327_ DUMMY_TSE_M2326_
           DUMMY_TSE_M2329_ DUMMY_TSE_M2330_ DUMMY_TSE_M2900_ DUMMY_TSE_M9700_ DUMMY_TSE_M3600_
           DUMMY_TSE_M3200_ DUMMY_TSE_M3500_ DUMMY_TSE_M3300_ DUMMY_TSE_M2800_ DUMMY_TSE_W91_
           Lev
           year_2015 year_2016 year_2017 year_2018 year_2019
           year_2020 year_2021 year_2022 year_2023 year_2024);

/* ===== 3.5) addition：作為 d3 的補值來源（允許新增 key）===== */
%prep(ds=addition, out=work.addn_raw,
      keep=income cost expenditure PPE CLL NCLL RD NGW GIA Tobins_Q);
%coerce_numeric(ds=work.addn_raw,
      vars=income cost expenditure PPE CLL NCLL RD NGW GIA Tobins_Q);

/* 只保留補值欄位 + key，避免覆蓋 d3 的 companyID/companyName/year */
data work.addn; set work.addn_raw(drop=companyID companyName year); run;
proc sort data=work.addn nodupkey; by companyID_key year6; run;

/* ===== 用 addition 補 d3 ===== */
proc sort data=work.d3; by companyID_key year6; run;

data work.d3;
  merge work.d3(in=a)
        work.addn(rename=(
          income      = income_add
          cost        = cost_add
          expenditure = expenditure_add
          PPE         = PPE_add
          CLL         = CLL_add
          NCLL        = NCLL_add
          RD          = RD_add
          NGW         = NGW_add
          GIA         = GIA_add
          Tobins_Q    = Tobins_Q_add
        ));
  by companyID_key year6;

  if not missing(income_add) then income = income_add;

  array v_names  {*} cost expenditure PPE CLL NCLL RD NGW GIA Tobins_Q;
  array v_adds   {*} cost_add expenditure_add PPE_add CLL_add NCLL_add RD_add NGW_add GIA_add Tobins_Q_add;
  do _i=1 to dim(v_names);
    if missing(v_names[_i]) then v_names[_i] = v_adds[_i];
  end;

  if missing(LL)         then LL         = CLL + NCLL;
  if missing(OtherIntan) then OtherIntan = GIA - NGW;

  drop _i income_add cost_add expenditure_add PPE_add CLL_add NCLL_add RD_add NGW_add GIA_add Tobins_Q_add;
run;

/* ===== 4) 動態組欄位清單 ===== */
proc sql noprint;
  select cats('b.',name) into :d3_sel separated by ', '
  from dictionary.columns
  where libname='WORK' and memname='D3'
    and upcase(name) not in ('COMPANYID','COMPANYNAME','YEAR','COMPANYID_KEY','YEAR_N','YEAR6');

  select cats('c.',name) into :d5_sel separated by ', '
  from dictionary.columns
  where libname='WORK' and memname='D5'
    and upcase(name) not in ('COMPANYID','COMPANYNAME','YEAR','COMPANYID_KEY','YEAR_N','YEAR6');

  select cats('d.',name) into :d6_sel separated by ', '
  from dictionary.columns
  where libname='WORK' and memname='D6'
    and upcase(name) not in ('COMPANYID','COMPANYNAME','YEAR','COMPANYID_KEY','YEAR_N','YEAR6');
quit;

%let _sel=;
%macro _append(mv); %local v; %let v=&&&mv;
  %if %superq(v) ne %then %do;
    %if %length(&_sel) %then %let _sel=&_sel, &v; %else %let _sel=&v;
  %end;
%mend;
%_append(d3_sel); %_append(d5_sel); %_append(d6_sel);
%let sel_clause=%sysfunc(ifc(%length(%superq(_sel)), %str(, )%superq(_sel), ));

/* ===== 5) 合併（用 companyID_key + year6）===== */
proc sql;
  create table work.ESG_merged as
  select
    a.companyID, a.companyName, a.year,
    a.certificate, a.ESG_score, a.ESG_E, a.ESG_S, a.ESG_G
    &sel_clause
  from work.main as a
  left join work.d3 as b
    on a.companyID_key = b.companyID_key and a.year6 = b.year6
  left join work.d5 as c
    on a.companyID_key = c.companyID_key and a.year6 = c.year6
  left join work.d6 as d
    on a.companyID_key = d.companyID_key and a.year6 = d.year6
  order by a.year, a.companyID;
quit;

/* ===== 5.4) 排除名單（但保留 6526／6643／6741）===== */
data work.excl_ids;
  length companyID_key $16;
  infile datalines truncover;
  input companyID_key $;
datalines;
2801
2809
2812
2816
2820
2832
2834
2836
2838
2845
2849
2850
2851
2852
2855
2867
2880
2881
2882
2883
2884
2885
2886
2887
2889
2890
2891
2892
2897
5864
5876
5880
6005
6015
6016
6020
6021
6023
6024
6026
6526
6643
6741
;
run;

proc sql;
  delete from work.excl_ids
  where companyID_key in ('6526','6643','6741');
quit;

/* 在合併後表補上 companyID_key（純數字）以利比對 */
data work.ESG_merged_k;
  set work.ESG_merged;
  length companyID_key $64;
  if vtype(companyID)='C' then companyID_key = compress(companyID, , 'kd');
  else                         companyID_key = compress(cats(companyID), , 'kd');
run;

/* anti-join 排除名單，並依 year companyID 排序 */
proc sql;
  create table work.ESG_merged_s as
  select a.*
  from work.ESG_merged_k as a
  left join work.excl_ids   as x
    on a.companyID_key = x.companyID_key
  where x.companyID_key is null
  order by a.year, a.companyID;
quit;

/* ===== 5.5) 產生 DMU（用過濾後資料，DMU 不按年重置）===== */
data work.ESG_merged_DMU;
  set work.ESG_merged_s;
  length DMU $8;
  DMU = cats('DMU', put(_N_, z5.));
run;

/* 重新排欄位順序：把 DMU 放在 companyName 後，其餘依原順序 */
proc sql noprint;
  select name into :rest_cols separated by ' '
  from dictionary.columns
  where libname='WORK' and memname='ESG_MERGED_DMU'
    and upcase(name) not in 
      ('COMPANYID','COMPANYNAME','DMU','YEAR','CERTIFICATE','ESG_SCORE','ESG_E','ESG_S','ESG_G','COMPANYID_KEY')
  order by varnum;
quit;

data work.ESG_DMU_whole;
  retain companyID companyName DMU year certificate ESG_score ESG_E ESG_S ESG_G &rest_cols;
  set work.ESG_merged_DMU(drop=companyID_key);
run;

/* ===== 6) 匯出（全量） ===== */
proc export data=work.ESG_DMU_whole
  outfile="/home/u64061874/ESG_DMU_whole.xlsx"
  dbms=xlsx replace;
run;

/* ===== A) 匯入 ROU_asset 檔案 ===== */
proc import out=ROU_asset
  datafile="/home/u64061874/ROU_asset.xlsx"
  dbms=xlsx replace;
  getnames=yes;
run;

/* ===== B) 標準化 key：companyID_merge($64) + year(YYYYMM 字串) ===== */
data work.ESG_DMU_whole_fix;
  set work.ESG_DMU_whole;
  length year_c $6 _ys $32;
  if vtype(year)='C' then _ys = compress(strip(year), , 'kd');
  else _ys = strip(put(year, best32.));
  if      length(_ys) >= 6 then year_c = substr(_ys,1,6);
  else if length(_ys) = 4  then year_c = cats(_ys,'12');
  else year_c = '';
  length companyID_merge $64;
  if vtype(companyID)='C' then companyID_merge = strip(companyID);
  else companyID_merge = strip(cats(companyID));
  drop _ys year;
  rename year_c = year;
run;

data work.ROU_asset_fix;
  set ROU_asset;
  length year_c $6 _ys $32;
  if vtype(year)='C' then _ys = compress(strip(year), , 'kd');
  else _ys = strip(put(year, best32.));
  if      length(_ys) >= 6 then year_c = substr(_ys,1,6);
  else if length(_ys) = 4  then year_c = cats(_ys,'12');
  else year_c = '';
  length companyID_merge $64;
  if vtype(companyID)='C' then companyID_merge = strip(companyID);
  else companyID_merge = strip(cats(companyID));
  drop _ys year;
  rename year_c = year;
run;

/* ===== C) 合併（companyID_merge + year），並將 ROU_asset 放在 certificate 後 ===== */
proc sort data=work.ESG_DMU_whole_fix; by companyID_merge year; run;
proc sort data=work.ROU_asset_fix;     by companyID_merge year; run;

data work._merged_tmp;
  merge work.ESG_DMU_whole_fix(in=a)
        work.ROU_asset_fix(in=b keep=companyID_merge year ROU_asset);
  by companyID_merge year;
  if a;
run;

proc sql noprint;
  create table work.ESG_DMU_whole_rou as
  select 
    companyID, companyName, DMU, year,
    certificate, ROU_asset,
    ESG_score, ESG_E, ESG_S, ESG_G,
    size, RGR, FCF, DSR,
    income, cost, expenditure, PPE, CLL, NCLL, LL, RD, GIA, NGW, OtherIntan, Tobins_Q,
    market_share_TSE, HHI_TSE, dummy_FCF, ln_age, foreign_currency,
    DUMMY_TSE_M1100_, DUMMY_TSE_M1200_, DUMMY_TSE_M2700_, DUMMY_TSE_M1300_, DUMMY_TSE_M2500_,
    DUMMY_TSE_M2200_, DUMMY_TSE_M2328_, DUMMY_TSE_M1400_, DUMMY_TSE_M9900_, DUMMY_TSE_M3700_,
    DUMMY_TSE_M1500_, DUMMY_TSE_M1722_, DUMMY_TSE_M2325_, DUMMY_TSE_M1600_, DUMMY_TSE_M1721_,
    DUMMY_TSE_M2331_, DUMMY_TSE_M1800_, DUMMY_TSE_M1900_, DUMMY_TSE_M2000_, DUMMY_TSE_M3800_,
    DUMMY_TSE_M2100_, DUMMY_TSE_M2600_, DUMMY_TSE_M2324_, DUMMY_TSE_M2327_, DUMMY_TSE_M2326_,
    DUMMY_TSE_M2329_, DUMMY_TSE_M2330_, DUMMY_TSE_M2900_, DUMMY_TSE_M9700_, DUMMY_TSE_M3600_,
    DUMMY_TSE_M3200_, DUMMY_TSE_M3500_, DUMMY_TSE_M3300_, DUMMY_TSE_M2800_, DUMMY_TSE_W91_,
    Lev,
    year_2015, year_2016, year_2017, year_2018, year_2019,
    year_2020, year_2021, year_2022, year_2023, year_2024
  from work._merged_tmp;
quit;

/* ===== D) 只保留 2019(含)以後 + 穩定排序鍵 ===== */
data work._base_cYN;
  set work.ESG_DMU_whole_rou;
  if certificate in ('Y','N');
  length y6_key 8;
  if not missing(year) then y6_key = input(year, best32.);
  else y6_key = .;
  if y6_key >= 201901;
run;

proc sort data=work._base_cYN out=work._base_cYN_sorted;
  by y6_key companyID;
run;

/* ===== E) 產出 6 張表（各自 DMU 重新編號） ===== */
data work.ESG_short_manage_All;
  set work._base_cYN_sorted;
  length DMU $8; DMU = cats('DMU', put(_N_, z5.));
  retain DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
  keep   DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
run;

data work.ESG_short_manage_Y;
  set work._base_cYN_sorted;
  where certificate='Y';
  length DMU $8; DMU = cats('DMU', put(_N_, z5.));
  retain DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
  keep   DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
run;

data work.ESG_short_manage_N;
  set work._base_cYN_sorted;
  where certificate='N';
  length DMU $8; DMU = cats('DMU', put(_N_, z5.));
  retain DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
  keep   DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
run;

data work.ESG_full_manage_All;
  set work._base_cYN_sorted;
  length DMU $8; DMU = cats('DMU', put(_N_, z5.));
  retain companyID companyName year DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
  keep   companyID companyName year DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
run;

data work.ESG_full_manage_Y;
  set work._base_cYN_sorted;
  where certificate='Y';
  length DMU $8; DMU = cats('DMU', put(_N_, z5.));
  retain companyID companyName year DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
  keep   companyID companyName year DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
run;

data work.ESG_full_manage_N;
  set work._base_cYN_sorted;
  where certificate='N';
  length DMU $8; DMU = cats('DMU', put(_N_, z5.));
  retain companyID companyName year DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
  keep   companyID companyName year DMU certificate ROU_asset income cost expenditure PPE RD NGW OtherIntan;
run;

/* ===== F) 匯出 6 份原始檔 ===== */
proc export data=work.ESG_short_manage_All  outfile="/home/u64061874/ESG_short_manage_All.xlsx"  dbms=xlsx replace; run;
proc export data=work.ESG_short_manage_Y    outfile="/home/u64061874/ESG_short_manage_Y.xlsx"    dbms=xlsx replace; run;
proc export data=work.ESG_short_manage_N    outfile="/home/u64061874/ESG_short_manage_N.xlsx"    dbms=xlsx replace; run;
proc export data=work.ESG_full_manage_All   outfile="/home/u64061874/ESG_full_manage_All.xlsx"   dbms=xlsx replace; run;
proc export data=work.ESG_full_manage_Y     outfile="/home/u64061874/ESG_full_manage_Y.xlsx"     dbms=xlsx replace; run;
proc export data=work.ESG_full_manage_N     outfile="/home/u64061874/ESG_full_manage_N.xlsx"     dbms=xlsx replace; run;

/* ============================================
   full -> short（刪三欄）
   規則：
     - 缺失值補 0（字串欄補 '0'，數值欄補 0）
     - 負值刪除（ROU_asset income cost expenditure PPE RD NGW OtherIntan）
     - 篩選與排序後重新編 DMU（DMU00001, ...）
     - 匯出 6 檔 *_del.xlsx
   ============================================ */

options notes stimer source msglevel=i;
%let base=/home/u64061874;

/* 幫手：回傳實際存在的欄位清單字串（避免 drop 不存在欄位） */
%macro _filter_keep(ds=, list=, out=);
  proc contents data=&ds noprint out=_vars_(keep=name); run;
  %local i one keep; %let keep=;
  %let i=1; %let one=%scan(&list,&i,%str( ));
  %do %while(%length(&one));
    proc sql noprint;
      select count(*) into :_hit trimmed
      from _vars_ where upcase(name)=upcase("&one");
    quit;
    %if &_hit %then %let keep=&keep &one;
    %let i=%eval(&i+1);
    %let one=%scan(&list,&i,%str( ));
  %end;
  %global &out;
  %if %length(%superq(keep)) %then %let &out=%sysfunc(compbl(&keep));
  %else %let &out=;
%mend _filter_keep;

/* 主流程：處理三個 full，short 由 full_del 派生 */
%macro build_full_then_short();
  %local fulls i one in_full out_full short_base out_short;
  %let fulls =
    ESG_full_manage_All.xlsx
    ESG_full_manage_Y.xlsx
    ESG_full_manage_N.xlsx
  ;

  %let i=1; %let one=%scan(&fulls,&i,%str( ));
  %do %while(%length(&one));

    %let in_full  =&base/&one;
    %let out_full =&base/%scan(&one,1,.)_del.xlsx;

    /* 1) 讀 full */
    proc import out=work._raw datafile="&in_full" dbms=xlsx replace;
      getnames=yes;
    run;

    /* 2) 欄位存在性（保險） */
    proc contents data=work._raw noprint out=work.__vars(keep=name varnum); run;
    proc sql noprint;
      /* key（僅用於排序與穩定 DMU） */
      select (sum(upcase(name)='COMPANYID')>0) into :_has_cid  from work.__vars;
      select (sum(upcase(name)='YEAR')>0)      into :_has_year from work.__vars;
      /* 8 個檢查欄 */
      select (sum(upcase(name)='ROU_ASSET')>0)   into :_has_rou  from work.__vars;
      select (sum(upcase(name)='INCOME')>0)      into :_has_inc  from work.__vars;
      select (sum(upcase(name)='COST')>0)        into :_has_cost from work.__vars;
      select (sum(upcase(name)='EXPENDITURE')>0) into :_has_exp  from work.__vars;
      select (sum(upcase(name)='PPE')>0)         into :_has_ppe  from work.__vars;
      select (sum(upcase(name)='RD')>0)          into :_has_rd   from work.__vars;
      select (sum(upcase(name)='NGW')>0)         into :_has_ngw  from work.__vars;
      select (sum(upcase(name)='OTHERINTAN')>0)  into :_has_oi   from work.__vars;
    quit;

    %macro _def(v); %if %length(%superq(&v))=0 %then %let &v=0; %mend;
    %_def(_has_cid)  %_def(_has_year)
    %_def(_has_rou)  %_def(_has_inc)  %_def(_has_cost) %_def(_has_exp)
    %_def(_has_ppe)  %_def(_has_rd)   %_def(_has_ngw)  %_def(_has_oi)

    /* 小巨集：補 0 / 負值檢查（僅在該欄存在時執行） */
    %macro check_var(var=, flag=);
      %if &&_has_&flag %then %do;
        length __val 8;
        /* 轉數值以便檢查，並依原型別補 0 */
        if vtype(&var)='C' then do;
          __val = inputn(strip(&var),'best32.');
          if missing(__val) then do; &var='0'; __val=0; end; /* 字串欄補 '0' */
        end;
        else do;
          __val = &var;
          if missing(__val) then do; &var=0; __val=0; end;    /* 數值欄補 0 */
        end;
        if __val<0 then __bad=1;  /* 任何負值 → 刪 */
      %end;
    %mend check_var;

    /* 3) 補 0 / 刪負值；並建立排序鍵 */
    data work._filtered0;
      set work._raw;

      length companyID_c $64 year_c $6;
      %if &_has_cid %then %do;
        if vtype(companyID)='C' then companyID_c = compress(companyID, , 'kd');
        else                       companyID_c = compress(cats(companyID), , 'kd');
        if not missing(companyID_c) then do;
          companyID_c = prxchange('s/^0+//', 1, strip(companyID_c));
          if missing(companyID_c) then companyID_c = '0';
        end;
      %end; %else %do; companyID_c=''; %end;

      %if &_has_year %then %do;
        length _ys $32;
        if vtype(year)='C' then _ys = compress(strip(year), , 'kd');
        else _ys = compress(cats(year), , 'kd');
        if      length(_ys)>=6 then year_c = substr(_ys,1,6);
        else if length(_ys)=4  then year_c = cats(_ys,'12');
        else year_c=''; 
      %end; %else %do; year_c=''; %end;

      length __bad 3; __bad=0;

      %check_var(var=ROU_asset,  flag=rou);
      %check_var(var=income,     flag=inc);
      %check_var(var=cost,       flag=cost);
      %check_var(var=expenditure,flag=exp);
      %check_var(var=PPE,        flag=ppe);
      %check_var(var=RD,         flag=rd);
      %check_var(var=NGW,        flag=ngw);
      %check_var(var=OtherIntan, flag=oi);

      if __bad=1 then delete;

      drop _ys __bad __val;
    run;

    /* 4) 依正規化鍵排序（讓 DMU 穩定） */
    proc sort data=work._filtered0 out=work._sorted;
      by year_c companyID_c;
    run;

    /* 簡單日誌（前/後筆數） */
    proc sql noprint;
      select count(*) into :_n_before from work._raw;
      select count(*) into :_n_after  from work._filtered0;
    quit;
    %put NOTE: FILE &one : Rows before filtering = &_n_before ;
    %put NOTE: FILE &one : Rows after  filtering = &_n_after  ;
    %put NOTE: FILE &one : Dropped by NEGATIVE   = %eval(&_n_before - &_n_after) ;

    /* 5) 重新編 DMU（排序後才編，確保穩定連號） */
    data work._full_done;
      length DMU $12;
      set work._sorted(drop=DMU);
      DMU = cats('DMU', put(_N_, z5.));
      drop companyID_c year_c;
    run;

    /* 6) 匯出 full_del */
    proc export data=work._full_done outfile="&out_full" dbms=xlsx replace; run;

    /* 7) 由 full_del 產生 short_del（刪三欄：companyID companyName year） */
    %_filter_keep(ds=work._full_done, list=companyID companyName year, out=_to_drop3);
    %if %length(%superq(_to_drop3)) %then %do;
      data work._short_done; set work._full_done(drop=%superq(_to_drop3)); run;
    %end;
    %else %do;
      data work._short_done; set work._full_done; run;
    %end;

    /* short 檔名並輸出 */
    %let short_base=%sysfunc(tranwrd(&one,full_manage,short_manage));
    %let out_short =&base/%scan(&short_base,1,.)_del.xlsx;

    proc export data=work._short_done outfile="&out_short" dbms=xlsx replace; run;

    /* 8) 清理暫存 */
    proc datasets lib=work nolist;
      delete __vars _filtered0 _sorted _full_done _short_done _vars_;
    quit;

    %let i=%eval(&i+1);
    %let one=%scan(&fulls,&i,%str( ));
  %end;
%mend build_full_then_short;

/* 執行 */
%build_full_then_short();