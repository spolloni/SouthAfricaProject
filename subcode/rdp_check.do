clear all
set more off
set scheme s1mono
set matsize 11000
set maxvar 32767
#delimit;

******************;
*  PLOT DENSITY  *;
******************;

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
use "${data}/bblu_densityplot.dta", clear;

* indicate post-waves from pre-waves;
gen post = (substr(STR_FID,1,4)=="post");

* remove clusters with no pre;
bys cluster: egen precount = sum(abs(post-1));
drop if precount==0;
drop precount;

* remove clusters non-concentrated;
*keep if frac1>=.5 & frac2>=.7;

* cut and keep positive distances;
egen dists = cut(distance),at(0($bin)$bw);  
drop if dists ==.;

* count by cut;
bys cluster post dists: egen all_resid = count(_n);
bys cluster post dists: egen for_resid = sum(s_lu_code=="7.1"); 
bys cluster post dists: egen inf_resid = sum(s_lu_code=="7.2");
bys cluster post dists: drop if _n>1;
gen rel_inf_resid = inf_resid/all_resid;
gen rel_for_resid = for_resid/all_resid;
ds *_resid;
foreach var in `r(varlist)'{;
bys post dists: egen mean_`var' = mean(`var');
};
bys post dists: gen n = _n;

* mean for plots;
tw (sc mean_rel_inf_resid dists if n==1 & post==1) (sc mean_rel_inf_resid dists if n==1 & post==0);








