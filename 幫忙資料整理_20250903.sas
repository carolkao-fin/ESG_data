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
    companyID_key = compress(upcase(_cid), , 'kd');  /* keep digits only */

    /* year6：統一為六位數 YYYYMM（數值） */
    if vtype(year)='C' then do;
      length _y $32; _y=compress(strip(year), , 'kd');
      if length(_y)>=6 then year6 = input(substr(_y,1,6), best32.);
      else year6 = .;
    end;
    else if not missing(year) then do;
      if year>=100000 then year6 = int(year);  /* 例如 201503 或 202506 */
      else year6 = .;
    end;
    else year6=.;

    keep companyID companyName year companyID_key year6 &_keep_ok;
  run;
  proc sort data=&out; by companyID_key year6; run;
%mend;

/* 把字串數字欄位轉數值（僅對存在欄位） */
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
        drop &v;
        rename &v._n = &v;
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

/* ===== 用 addition 補 d3（income 以 addition 覆蓋；其餘缺才補） ===== */
proc sort data=work.d3;   by companyID_key year6; run;

data work.d3;
  merge
    work.d3(in=a)
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
    ))
  ;
  by companyID_key year6;

  /* income：一律以 addition 覆蓋（若 addition 有值） */
  if not missing(income_add) then income = income_add;

  /* 其他欄位：原值缺才補 */
  array v_names  {*} cost expenditure PPE CLL NCLL RD NGW GIA Tobins_Q;
  array v_adds   {*} cost_add expenditure_add PPE_add CLL_add NCLL_add
                    RD_add NGW_add GIA_add Tobins_Q_add;
  do _i=1 to dim(v_names);
    if missing(v_names[_i]) then v_names[_i] = v_adds[_i];
  end;

  /* 派生欄位（缺才補） */
  if missing(LL)         then LL         = CLL + NCLL;
  if missing(OtherIntan) then OtherIntan = GIA - NGW;

  drop _i income_add cost_add expenditure_add PPE_add CLL_add NCLL_add
           RD_add NGW_add GIA_add Tobins_Q_add;
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
    %if %length(&_sel) %then %let _sel=&_sel, &v;
    %else %let _sel=&v;
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
/* 建立要排除的 companyID_key 名單 */
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
/* 例外：不要排除 6526、6643、6741 */
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

/* 以 anti-join 排除名單，並依 year companyID 排序 */
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
  DMU = cats('DMU', put(_N_, z5.));  /* DMU00001, DMU00002, ... */
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

/* ===== 6) 匯出 ===== */
proc export data=work.ESG_DMU_whole
  outfile="/home/u64061874/ESG_DMU_whole.xlsx"
  dbms=xlsx replace;
run;

/* ===== 7)（可選）檢核命中率 =====*/
proc sql;
  select count(*) as main_rows,
         sum(case when b.companyID_key is not null then 1 else 0 end) as hit_d3
  from work.main a
  left join work.d3 b
    on a.companyID_key=b.companyID_key and a.year6=b.year6;
quit;

/* ===== A) 建立基表：去掉 companyID / companyName / year ===== */
data work.ESG_DMU_reg;
  set work.ESG_DMU_whole(drop=companyID companyName year);
run;

/* ===== B) 依 certificate 分三組 ===== */
/* 僅 Y */
data _certY;  set work.ESG_DMU_reg;  where upcase(strip(certificate))='Y';  run;
/* 僅 N */
data _certN;  set work.ESG_DMU_reg;  where upcase(strip(certificate))='N';  run;
/* 僅 {Y,N} */
data _certYN; set work.ESG_DMU_reg;  where upcase(strip(certificate)) in ('Y','N'); run;

/* ===== C) 針對每組輸出三種 ESG 欄位保留方式 ===== */
%macro _emit(cert=, dsin=);
  /* 只留 ESG_score（移除 ESG_E, ESG_S, ESG_G） */
  data work.ESG_DMU_reg_&cert._score;
    set &dsin;
    drop ESG_E ESG_S ESG_G;
  run;

  /* 只留 ESG_E / ESG_S / ESG_G（移除 ESG_score） */
  data work.ESG_DMU_reg_&cert._esg;
    set &dsin;
    drop ESG_score;
  run;

  /* 全部保留四欄 */
  data work.ESG_DMU_reg_&cert._all;
    set &dsin;
  run;
%mend;

%_emit(cert=Y,  dsin=_certY);
%_emit(cert=N,  dsin=_certN);
%_emit(cert=YN, dsin=_certYN);

/* ===== 匯出 9 個分類後的表格 ===== */
%macro export9;
  %let path=/home/u64061874;

  %let certs=Y N YN;
  %let types=score esg all;

  %local i j cert type;
  %do i=1 %to 3;
    %let cert=%scan(&certs,&i);
    %do j=1 %to 3;
      %let type=%scan(&types,&j);

      proc export data=work.ESG_DMU_reg_&cert._&type
        outfile="&path/ESG_DMU_reg_&cert._&type..xlsx"
        dbms=xlsx replace;
      run;

    %end;
  %end;
%mend;

%export9;