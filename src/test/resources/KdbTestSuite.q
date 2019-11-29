\l spark.q

test02table:([]
	i:1000?1000000i; 
	j:til 1000; 
	f:1000?10.0;
	p:2020.01.01D0+100000000*til 1000 
	)

test03:{
	([] j:til 1000; p:2020.01.01D0+1D*til 1000; cc:1000#enlist "a string")
	}

test04:test03

test05:{[opt]
	tbl:([] j:til 1000; p:2018.09.01D0+1D*til 1000; cc:1000#enlist "a string");

	if[-1=opt`partitionid;
		:0!meta tbl / Return the schema
	];

	tbl
	}

test06:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	tbl:([] j:til 1000; p:2018.09.01D0+1D*til 1000; cc:1000#enlist "a string");

	if[-1=opt`partitionid;
		:0!meta tbl / Return the schema
		];

	.sp.pruneAndFilter[opt;tbl]
	}


test07table:([]
	b:10#01b;
	g:10?0Ng;
	x:10?0xFF;
	h:10?100h;
	ic:10?1000i;
	j:10?10000j;
	e:10?100e;
	f:10?1000f;
	c:10#"abcdef";
	s:10#`$"s",/:string each til 100;
	p:2018.07.01D00:00:00.123456+1000000000*til 10;
	m:2000.01m+til 10;
	d:2018.07.01+til 10;
	z:2018.07.01T0+til 10;
	t:00:00:00.000+500*til 10;
	n:0D0+1000000*til 10;
	u:10#00:00 23:59;
	v:10#00:00:00 23:59:59
	)

test07:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];

	.sp.logDebug "test07[]";
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
		xxc:10#(1#0x0;0x7f80;"x"$til 10);
		bbc:10#(1#1b;10101b),"b"$flip 2 vs til 10;
		hhc:10#(1#1h;100 200h;"h"$til 10);
		iic:10#(1#1i;100 200i;"i"$reverse til 5);
		jjc:10#(1#1;100 200;0#0j);
		eec:10#(1#.1e;100 200e;"e"$.1*til 10);
		ffc:10#(1#.1f;100 200f;"f"$.1*til 10);
		ccc:(10?26)#\:"abcdefghijklmnopqrstuvwxyz"; 
		ppc:10#("p"$1000000000*til 3;2#.z.p;3#.z.p);
		ddc:10#(1#.z.d;2#.z.d;3#.z.d)
		);

test08:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];

	.sp.logDebug "test08[]";
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
		gcolumn:0Ng,1?0Ng;
		hcolumn:0Nh,1h;
		icolumn:0Ni,2i;
		jcolumn:0Nj,3j;
		ecolumn:0Ne,4.4e;
		fcolumn:0Nf,5.5f;
		scolumn:``abc;
		pcolumn:0Np,.z.p;
		mcolumn:0Nm,2001.01m;
		dcolumn:0Nd,2018.01.01;
		zcolumn:0Nz,2018.01.01T0;
		tcolumn:0Nt,01:02:03.456;
		ncolumn:0Nn,0D01:02:03;
		ucolumn:0Nu,12:23;
		vcolumn:0Nv,12:23:56
		)

test09:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];

	.sp.logDebug "test09";
	.sp.logDebugOptions[opt];

	if[-1=.sp.optGet[opt;`partitionid;-1];
		:update n:1b from 0!meta test09table;
		]

	test09table
	}

test15table:([]
		pc:2000.01.01D0 2000.01.01D12 2020.01.01D5 2020.01.02D17, .z.p;
		dc:2000.01.01 2000.01.01 2020.01.01 2020.01.02, .z.d
		)

test15:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];

	.sp.logDebug "test15";
	.sp.logDebugOptions[opt];

	if[-1=.sp.optGet[opt;`partitionid;-1];
		:0!meta test15table;
		]

	test15table
	}



test50:{[opt;tbl]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebug "test50[]";
	.sp.logDebugOptions[opt];

	T::tbl;
	0
	}

test51:{[opt;tbl]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebug "test51[]";
	.sp.logDebugOptions[opt];

	T51::tbl;
	0
	}

test52:{[opt;tbl]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebug "test52[]";
	.sp.logDebugOptions[opt];

	show tbl;
	T52::tbl;
	0
	}

test53table:([] ppd:1#enlist 2000.01.01 2000.01.02 2020.01.01, .z.d)

test53:{[opt;tbl]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebug "test52[]";
	.sp.logDebugOptions[opt];

	T53::tbl;
	0
	}





