clear all
set more off
set scheme s1mono
set matsize 11000
set maxvar 32767
#delimit;

*******************;
*  PLOT GRADIENTS *;
*******************;

* RUN LOCALLY?;
global LOCAL = 1;
if $LOCAL==1{;cd ..;};

* set parameters;
do subcode/parameters.do
`1'  `2'  `3'  `4'  `5'  `6'  `7'  `8'
`9' `10' `11' `12' `13' `14' `15' `16'; 
global bin   = 20;

* import plotreg program;
do subcode/import_plotreg.do;

* set cd;
cd "$cd";

* load data; 
use "${data}/${type}_gradplot.dta", clear;

**************************;
* Temporary Adjustments? *;
**************************;
*;
drop if ${type}_dist<0;
global dist_tr = 400;
*;
* re-set bw if centroid;
if "${type}"=="centroid"{;
   sum ${type}_dist;
   global bw = `r(mean)'+`r(sd)';
   global bw = round($bw,100);
   global dist_tr = round($bw/2,50);
};
*;
* re-set bw clusters if conhulls;
if "${type}"=="conhulls"{;
   global bw = 600;
   global dist_tr 300;
};
*;
**************************;
**************************;

* separate pre/post mode_yr transactions;
foreach num in 1 2 {;
   gen pre`num' = (purch_yr < mode_yr - `num' +1 );
   gen post`num' = (purch_yr > mode_yr + `num' -1 );
   replace post`num' =. if post`num'==0 & pre`num'==0;
};

* create date variables and dummies;
gen day_date = mdy(purch_mo,purch_day,purch_yr);
gen mo_date  = ym(purch_yr,purch_mo);
gen con_day  = mdy(07,02,mode_yr);
replace con_day = mdy(01,01,mode_yr+1 ) if mod(mode_yr,1)>0;
gen con_mo   = ym(mode_yr,07);
format day_date %td;
format mo_date %tm;
gen day2con = day_date - con_day;
gen mo2con  = mo_date - con_mo;
gen mo2con_reg = mo2con if abs(mo2con)<=12*$tw;
replace mo2con_reg = -12*$tw-1 if mo2con_reg==.;
replace mo2con_reg = mo2con_reg + 12*$tw+1;

*keep if frac2>.7;

**************************;
* Move This Eventually *;
local b = 12*$tw;
tw
(hist mo2con if rdp_`rdp'==0 & abs(mo2con)<=12*$tw, w(1) fc(none) lc(gs0))
(hist mo2con if rdp_`rdp'==1 & abs(mo2con)<=12*$tw, w(1) c(gs10)),
xtitle("months to event mode year")
xlabel(-`b'(12)`b')
legend(order(1 "non-RDP" 2 "RDP")ring(0) position(2) bmargin(small));
graphexportpdf summary_densitytime, dropeps;
**************************;

/*
* RDP counter;
bys ${type}_cluster: egen numrdp  = sum(rdp_$rdp);
bys ${type}_cluster: gen denomrdp = _N;
qui tab ${type}_cluster;
local totalclust = "`r(r)'";
gen fracrdp = numrdp/denomrdp;
*/

* keep non-rdp;
drop if ${type}_dist==0;
drop if rdp_`rdp'==1;
if $res==0{; drop if ever_rdp_$rdp==1;};

**************************;
* Temporary Adjustments? *;
* arbitrary treatment/control separation;
gen treatment = (${type}_dist<= $dist_tr);
**************************;

* select clusters and time-window;
keep if abs(purch_yr -mode_yr) <= $tw; 
drop if frac1 < $fr1;      
drop if frac2 < $fr2; 

* basic outlier removal;
bys ${type}_cluster: egen p$top = pctile(purch_price), p($top);
bys ${type}_cluster: egen p$bot = pctile(purch_price), p($bot);
drop if purch_price >= p$top | purch_price <= p$bot;
drop p$bot p$top;
bys ${type}_cluster: egen p$top = pctile(erf_size), p($top);
bys ${type}_cluster: egen p$bot = pctile(erf_size), p($bot);
drop if erf_size >= p$top | erf_size <= p$bot;
drop p$bot p$top;

* drop unpopulated clusters;
bys ${type}_cluster: egen count = count(_n);
bys ${type}_cluster: gen n = _n;
drop if count < $mcl; 

******************;
* Summary Plots  *;
******************; 
/*
* distribution of trans;
qui tab ${type}_cluster;
hist count if n ==1 & ${type}_dist<$bw, freq 
xtitle("# of transactions per cluster")
ytitle("")
note("Note: cleaning kept `r(r)' out of `totalclust' clusters");
graphexportpdf summary_transperclust, dropeps;

* distribution of RDP frac;
hist fracrdp if n ==1 & ${type}_dist<$bw, freq 
xtitle("% RDP transactions per cluster")
ytitle("");
graphexportpdf summary_rdpperclust, dropeps;

* distribution of dist;
hist ${type}_dist if ${type}_dist<$bw, freq 
xtitle("# of transactions per distance")
ytitle("")
xlabel(0(200)$bw);
graphexportpdf summary_disthist, dropeps;

* distribution of dist pre/post;
tw
(hist ${type}_dist if  pre1==1 & ${type}_dist<$bw , start(0) width($bin) c(gs10))
(hist ${type}_dist if post1==1 & ${type}_dist<$bw, start(0) width($bin) fc(none) lc(gs0)),
xtitle("# of transactions per distance")
xlabel(0(200)$bw)
legend(order(1 "pre" 2 "post")ring(0) position(2) bmargin(small));
graphexportpdf summary_disthist2, dropeps;
*/
*******************;
* DISTANCE PLOTS  *;
*******************;

* gen required vars;
replace purch_price= purch_price/1000000;
gen lprice = log(purch_price);
gen erf_size2 = erf_size^2;
gen erf_size3 = erf_size^3;
egen dists = cut(${type}_dist),at(0($bin)$bw);    
egen munic = group(munic_name);

/*
* #1 Raw-tight in logs;
tw 
(lpoly lprice ${type}_dist if pre1==1 & ${type}_dist<$bw, bw(100) lc(black))
(lpoly lprice ${type}_dist if post1==1 & ${type}_dist<$bw, bw(100) lc(black) lp(--)),
xtitle("meters")
ytitle("log-price")
xlabel(0(200)$bw)
legend(order(1 "pre" 2 "post"));
graphexportpdf raw_logspm1, dropeps;

* #2 Raw-tight in levels;
tw 
(lpoly purch_price ${type}_dist if pre1==1 & ${type}_dist<$bw, bw(100) lc(black))
(lpoly purch_price ${type}_dist if post1==1 & ${type}_dist<$bw, bw(100) lc(black) lp(--)),
xtitle("meters")
ytitle("price")
xlabel(0(200)$bw)
legend(order(1 "pre" 2 "post"));
graphexportpdf raw_levspm1, dropeps;

* #3 Raw-loose in logs;
tw 
(lpoly lprice ${type}_dist if pre2==1 & ${type}_dist<$bw, bw(100) lc(black))
(lpoly lprice ${type}_dist if post2==1 & ${type}_dist<$bw, bw(100) lc(black) lp(--)),
xtitle("meters")
ytitle("log-price")
xlabel(0(200)$bw)
legend(order(1 "pre" 2 "post"));
graphexportpdf raw_logspm2, dropeps;

* #4 Raw-loose in levels;
tw 
(lpoly purch_price ${type}_dist if pre2==1 & ${type}_dist<$bw, bw(100) lc(black))
(lpoly purch_price ${type}_dist if post2==1 & ${type}_dist<$bw, bw(100) lc(black) lp(--)),
xtitle("meters")
ytitle("price")
xlabel(0(200)$bw)
legend(order(1 "pre" 2 "post"));
graphexportpdf raw_levspm2, dropeps;
*/

/*
* #5 reg-adjusted in logs, tight;
areg lprice i.dists#i.post1 erf_size erf_size2 i.munic#i.purch_yr i.purch_mo, a(${type}_cluster);
local note = "Note: controls for quadratic in erf size, mun-by-year, month and cluster FE.";
plotreg distplot reg_fepm1 "`note'";
*/

* #6 reg-adjusted in logs, tight no cluster FE;
reg lprice i.dists#i.post1 erf_size erf_size2 i.munic#i.purch_yr i.purch_mo;
local note = "Note: controls for quadratic in erf size, mun-by-year and month FE.";
plotreg distplot reg_pm1 "`note'";


/*
* #7 reg-adjusted in logs, loose;
areg lprice i.dists#i.post2 erf_size erf_size2 i.munic#i.purch_yr i.purch_mo, a(${type}_cluster);
local note = "Note: controls for quadratic in erf size, mun-by-year, month and cluster FE.";
plotreg distplot reg_fepm2 "`note'";

* #8 reg-adjusted in logs, loose no cluster FE;
reg lprice i.dists#i.post2 erf_size erf_size2 i.munic#i.purch_yr i.purch_mo;
local note = "Note: controls for quadratic in erf size, mun-by-year and month FE.";
plotreg distplot reg_pm2 "`note'";
*/

**************;
* TIME PLOTS *;
**************;

/*
* #5 reg-adjusted in logs, tight;
areg lprice i.mo2con_reg#i.treatment erf_size erf_size2 i.munic#i.purch_yr i.purch_mo, a(${type}_cluster);
local note = "Note: controls for quadratic in erf size, mun-by-year, month and cluster FE.";
plotreg timeplot timereg_fepm1 "`note'";
*/

* #6 reg-adjusted in logs, tight no cluster FE;
reg lprice i.mo2con_reg#i.treatment erf_size erf_size2 i.munic#i.purch_yr i.purch_mo;
local note = "Note: controls for quadratic in erf size, mun-by-year and month FE.";
plotreg timeplot timereg_pm1 "`note'";


*********;
* EXIT  *;
*********;
*exit, STATA clear;  