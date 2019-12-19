\l spark.q

test02table:([]
	ci:1000?1000000i; 
	cj:til 1000; 
	cf:1000?10.0;
	cp:2020.01.01D0+100000000*til 1000 
	)

test03:{
	([] cj:til 1000; cp:2020.01.01D0+1D*til 1000; ccc:1000#enlist "a string")
	}

test04:{
	([] cj:til 1000; cp:2020.01.01D0+1D*til 1000; ccc:1000#enlist "a string")
	}

test05:{[opt]
	tbl:([] cj:til 1000; cp:2018.09.01D0+1D*til 1000; ccc:1000#enlist "a string");

	if[-1=opt`partitionid;
		:0!meta tbl / Return the schema
	];

	tbl
	}

test06:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	tbl:([] cj:til 1000; cp:2010.10.01D0+1D*til 1000;ccc:1000#enlist "a string");

	if[-1=opt`partitionid;
		:0!meta tbl / Return the schema
		];

	.sp.pruneAndFilter[opt;tbl]
	}


test07table:([]
	cb:10#01b;
	cg:10?0Ng;
	cx:10?0xFF;
	ch:10?100h;
	ci:10?1000i;
	cj:10?10000j;
	ce:10?100e;
	cf:10?1000f;
	cc:10#"abcdef";
	cs:10#`$"s",/:string each til 100;
	cp:2020.01.01D00:00:00+10?31*24*3600*1000000000;
	cm:2000.01m+til 10;
	cd:2018.07.01+til 10;
	cz:2018.07.01T0+til 10;
	ct:00:00:00.000+500*til 10;
	cn:0D0+1000000*til 10;
	cu:10#00:00 23:59;
	cv:10#00:00:00 23:59:59
	)

test07:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	partitionid:.sp.optGet[opt;`partitionid;-1];

	//
	// A partition number of -1 means that Spark just wants the schema
	//
	if[partitionid=-1;
		res:0!meta test07table;
		.sp.logDebugSchema[res]; 
		:res
		];

	rtbl:.sp.pruneAndFilter[opt;test07table];
	.sp.logDebugTable[rtbl];
	rtbl
	}

test08table:([]
		cxx:10#(1#0x0;0x7f80;"x"$til 10);
		cbb:10#(1#1b;10101b),"b"$flip 2 vs til 10;
		chh:10#(1#1h;100 200h;"h"$til 10);
		cii:10#(1#1i;100 200i;"i"$reverse til 5);
		cjj:10#(1#1;100 200;0#0j);
		cee:10#(1#.1e;100 200e;"e"$.1*til 10);
		cff:10#(1#.1f;100 200f;"f"$.1*til 10);
		ccc:(10?26)#\:"abcdefghijklmnopqrstuvwxyz"; 
		cpp:10#("p"$1000000000*til 3;2#.z.p;3#.z.p);
		cdd:10#(1#.z.d;2#.z.d;3#.z.d)
		);

test08:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	partitionid:.sp.optGet[opt;`partitionid;-1];

	//
	// A partition number of -1 means that Spark just wants the schema
	//
	if[partitionid=-1;
		res:0!meta test08table;
		.sp.logDebugSchema[res]; 
		:res
		];

	test08table
	}


test09table:([]
		cg:0Ng,1?0Ng;
		ch:0Nh,1h;
		ci:0Ni,2i;
		cj:0Nj,3j;
		ce:0Ne,4.4e;
		cf:0Nf,5.5f;
		cs:``abc;
		cp:0Np,.z.p;
		cm:0Nm,2001.01m;
		cd:0Nd,2018.01.01;
		cz:0Nz,2018.01.01T0;
		ct:0Nt,01:02:03.456;
		cn:0Nn,0D01:02:03;
		cu:0Nu,12:23;
		cv:0Nv,12:23:56
		)

test09:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	if[-1=.sp.optGet[opt;`partitionid;-1];
		:update n:1b from 0!meta test09table;
		]

	test09table
	}

test15table:([]
		cp:2000.01.01D0 2000.01.01D12 2020.01.01D5 2020.01.02D17, .z.p;
		cd:2000.01.01 2000.01.01 2020.01.01 2020.01.02, .z.d
		)

test15:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	if[-1=.sp.optGet[opt;`partitionid;-1];
		:0!meta test15table;
		]

	test15table
	}

fttable:flip `strz`even`id!(-3#'"00",/:string each id;count[id]#10b;id:til 1000)

fttest:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	partitionid:.sp.optGet[opt;`partitionid;-1];

	if[partitionid=-1;
		res:0!meta fttable;
		.sp.logDebugSchema[res]; 
		:res
		];

	rtbl:.sp.pruneAndFilter[opt;fttable];
	.sp.logDebugTable[rtbl];
	rtbl
	}

test50:{[opt;tbl]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	T::tbl;
	0
	}

test51:{[opt;tbl]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	T51::tbl;
	0
	}

test52:{[opt;tbl]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	show tbl;
	T52::tbl;
	0
	}

test53table:([] cpp:1#enlist 2000.01.01 2000.01.02 2020.01.01, .z.d)

test53:{[opt;tbl]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	T53::tbl;
	0
	}





