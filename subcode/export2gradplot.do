clear all
set more off
set scheme s1mono
set matsize 11000
set maxvar 32767
#delimit;

local rdp  = "`1'";
local algo = "`2'";
local par1 = "`3'";
local par2 = "`4'";
local bw   = "`5'";
local sig  = "`6'";
local type = "`7'";

local qry = "
	SELECT A.munic_name, A.purch_yr, A.purch_mo, A.purch_day,
		    A.purch_price, A.trans_id, A.property_id, B.erf_size,
        C.rdp_`rdp', C.ever_rdp_`rdp', C.gov, E.cluster, E.mode_yr, E.frac1, E.frac2,
        D.`type'_dist, D.`type'_cluster, A.seller_name, B.latitude, B.longitude
	FROM transactions AS A
	JOIN erven AS B ON A.property_id = B.property_id
  JOIN rdp   AS C ON A.trans_id = C.trans_id
  LEFT JOIN distance_nrdp_`rdp'_`algo'_`par1'_`par2'_`bw'_`sig' 
  AS D ON A.trans_id = D.trans_id
  LEFT JOIN (SELECT trans_id, cluster, mode_yr, frac1, frac2
             FROM rdp_clusters_`rdp'_`algo'_`par1'_`par2'
             WHERE cluster != 0 ) AS E ON A.trans_id = E.trans_id
  WHERE NOT (D.`type'_cluster IS NULL AND E.cluster IS NULL)
  AND (D.`type'_cluster = D.nearest_cluster OR D.`type'_cluster IS NULL)
	";

* load data; 
odbc query "lightstone";
odbc load, exec("`qry'") clear;

* set up frac vars;
destring purch_yr purch_mo purch_day, replace;
replace `type'_cluster=cluster if `type'_cluster==.;
foreach var in mode_yr frac1 frac2 {;
   bys `type'_cluster: egen max = max(`var');
   replace `var' = max if cluster ==.;
   drop max;
};

* save data;
save "`8'`type'_gradplot.dta", replace;

* exit stata;
exit, STATA clear;  