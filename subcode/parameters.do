#delimit;

if $LOCAL == 0 {;

	global rdp  = "`1'";
	global algo = "`2'";
	global par1 = "`3'";
	global par2 = "`4'";
	global bw  = "`5'";
	global sig  = "`6'";
	global type = "`7'";
	global fr1  = "0.`8'";
	global fr2  = "0.`9'";
	global top  = "`10'";
	global bot  = "`11'";
	global mcl  = "`12'";
	global tw   = "`13'";
	global res  = "`14'";
	global data = "`15'";
	global cd   = "`16'";

};

if $LOCAL == 1 {;

	global rdp  = "ls";
	global algo = "1";
	global par1 = "0002";
	global par2 = "10";
	global bw   = "600";
	global sig  = "25";
	global type = "nearest";
	global fr1  = "0.7";
	global fr2  = "0.7";
	global sfr1 = "50";
	global sfr2 = "70";
	global top  = "99";
	global bot  = "1";
	global mcl  = "50";
	global tw   = "4";
	global res  = "0";
	global data = "/Users/stefanopolloni/GoogleDrive/";
	global data = "${data}Year4/SouthAfrica_Analysis/Generated/LIGHTSTONE";
	global cd   = "/Users/stefanopolloni/GoogleDrive/Year4/SouthAfrica_Analysis/";
	global cd   = "${cd}Output/LIGHTSTONE/local";

};