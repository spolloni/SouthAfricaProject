clear all
set more off
#delimit;

* load data; 
odbc query "gauteng";
odbc load,   table("transactions");

* date vars;
gen purch_yr  = substr(ipurchdate,1,4);
gen purch_mo  = substr(ipurchdate,5,2);
gen purch_day = substr(ipurchdate,7,2);
order purch_yr purch_mo purch_day, after(ipurchdate);

* ea vars;
gen prov_code = substr(ea_code,1,1);
gen mun_code = substr(ea_code,2,2);
order prov_code mun_code, after(ea_code);

* remove duplicates; 
sort property_id ipurchdate 
	 purch_price seller_name buyer_name;
by property_id ipurchdate: keep if _n == _N;

* create unique ID; 
by property_id: gen n = _n;
tostring n, replace;
tostring property_id, gen(trans_id);
replace trans_id = trans_id + "_" + n;
drop n;
label var trans_id "";

* insert back into database; 
odbc insert, table("temp") create;

* Exit;
exit, STATA clear;