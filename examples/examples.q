\l spark.q



//
// @desc Sample query that makes used of helper functions from the sp namespace
//

sampleTable:([]
	shardid:1000?4i; / A column that denotes some sort of partitioning
	jcolumn:til[999],0Nj; / Include one null to demonstrate nullable support
	fcolumn:1000?10.0;
	pcolumn:2018.09.01D0+10000000*til 1000 
	)

sampleQuery:{[opt]
	//
	// The options parameter may contain a key whose value indicates a required
	// logging level (i.e., error, warn, trace). If none is provided, we default
	// to warn.
	//
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];

	//
	// Display the name of this function, to provide context to the trace 
	// message that follow
	//
	.sp.logDebug "sampleQuery[]";

	//
	// Display all values in options provided by the Spark caller
	//
	.sp.logDebugOptions[opt];

	//
	// Retrieve the partition number as provided by the Spark connector. The
	// value can either by -1 (request to return the query schema), or a number
	// between 0 and numPartitions-1, which is the ordinal number of the partition 
	// requesting query data.
	//
	partitionid:opt`partitionid;

	//
	// A call with partition -1 means that Spark wants the schema (or meta) of the
	// query result so it can prepare its data frames to receive the data.
	//
	if[partitionid=-1;
		//
		// We only need to return column c and t, but the Spark connector ignores
		// the other columns if they are provided
		//
		schema:`c`t#0!meta sampleTable; / Unkey meta table and select name and type

		//
		// Indicate to Spark which columns contain nulls, so that the user can
		// accommodate them in calculations. We do this by adding an additional
		// boolean column <n> to the schema result, where 1b denotes nullable
		//
		// Let's assume that column <jcolumn> can contain nulls
		//
		schema:update n:c in `jcolumn from schema;

		.sp.checkSchemaResult schema; / Handy utility that asserts if the result is formed correctly
		.sp.logDebugSchema schema; / Report schema summary

		:schema / Return the unkeyed meta result
		];

	//
	// Select some of our table. In this example, we use the partition number that
	// came from options in the where clause
	//
	res:select from sampleTable where shardid=partitionid;

	//
	// In the event that the Spark user wanted to prune some columns and apply
	// push-down filters, use this utility, which converts the filters and pruned
	// columns into kdb+ functional form.
	//
	res:.sp.pruneAndFilter[opt;res];

	//
	// As an alternative to above, and potentially more efficient, we could combine 
	// the constraint in the where clause above with the filters found in options, as
	// follows:
	//
	// 		filters:enlist[(`eq;`shardid;paritionid)],opt`filters;
	// 		res:.sp.pruneAndFilter[res;filters;opt`columns];
	//

	//
	// Report summary information about the result
	//
	.sp.logDebugTable[res];
	
	res
	}


//
// The following functions progress through a variety of features of the Spark-kdb
// interface. There is matching Spark/Scala code invoking these functions
//

exampleSimple:{
	([] jcolumn:til 1000; pcolumn:2018.09.01D0+1D*til 1000; clcolumn:1000#enlist "a string")
	}

exampleSchema:{[opt]
	tbl:([] jcolumn:til 1000; pcolumn:2018.09.01D0+1D*til 1000; clcolumn:1000#enlist "a string");

	if[-1=opt`partitionid;
		:0!meta tbl / Return the "schema back"
		];

	tbl
	}


exampleFilters:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebugOptions[opt];

	tbl:([] jcolumn:til 1000; pcolumn:2018.09.01D0+1D*til 1000; clcolumn:1000#enlist "a string");

	if[-1=opt`partitionid;
		:0!meta tbl / Return the schema
		];

	.sp.pruneAndFilter[opt;tbl]
	}


exampleMulti:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebug "exampleMulti[] -- Testing multiple partitions";
	.sp.logDebugOptions[opt];

	tbl:([] jcolumn:til 1000; pcolumn:2018.09.01D0+1D*til 1000; clcolumn:1000#enlist "a string");

	partitionid:opt`partitionid;

	if[-1=partitionid;
		:0!meta tbl / Return the "schema back"
		];

	tbl:.sp.pruneAndFilter[opt;tbl];

	lastdigit:"J"$(";" vs opt`ex5parms)[partitionid];
	maxrows:"J"$opt`ex5maxrows;

	select[maxrows] from tbl where mod[jcolumn;10]=lastdigit
	}


exampleNulls:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];

	.sp.logDebug "exampleNulls[] - Testing Null Support";
	.sp.logDebugOptions[opt];

	tbl:([]
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
		);

	//
	// A custom option is provided to test null/no null support:
	//		.option("nullSupport", boolean)
	//
	if[-1=.sp.optGet[opt;`partitionid;-1]; 
		nullsupport:.sp.optGetBoolean[opt;`nullsupport;1b];
		:update n:nullsupport from 0!meta tbl; 
		]

	tbl:.sp.pruneAndFilter[opt;tbl];
	.sp.logDebugTable[tbl];
	tbl
	}


exampleCommonTypes:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];

	.sp.logDebug "exampleCommonTypes[]";
	.sp.logDebugOptions[opt];

	partitionid:.sp.optGet[opt;`partitionid;-1];
	.sp.logDebug string partitionid;

	//
	// Build one-row table from which we can report the query's schema back to Spark
	//
	stbl:([]
		shardid:1#0h;
		clcolumn:1#enlist "abc"; / c-list
		bcolumn:1#0b;
		gcolumn:1#0Ng;
		xcolumn:1#0x00;
		hcolumn:1#0h;
		icolumn:1#0i;
		jcolumn:1#0j;
		ecolumn:1#0e;
		fcolumn:1#0f;
		ccolumn:1#"0";
		scolumn:1#`0;
		pcolumn:1#0p;
		mcolumn:1#2000.01m;
		dcolumn:1#2000.01.01;
		zcolumn:1#2000.01.01T0;
		tcolumn:1#0t;
		ncolumn:1#0D0;
		ucolumn:1#00:00;
		vcolumn:1#00:00:00
		);

	//
	// A partition number of -1 means that Spark just wants the schema
	//
	if[partitionid=-1;
		res:0!meta stbl;
		.sp.logDebugSchema[res]; 
		:res
		];

	//
	// "Query" code follows. In this case, provide all datatypes to test
	// Spark Datasource
	//

	numrows:"J"$.sp.optGet[opt;`numrows;"10"]; / Number of rows to generate for test

	system "S 1"; / Produce same random results each time (for testing)

	NSONESEC:1000000000; / One second in nanoseconds
	MSONESEC:1000; / and also in milliseconds

	rtbl:([]
		shardid:numrows#"h"$partitionid;
		clcolumn:(numrows?26)#\:"abcdefghijklmnopqrstuvwxyz"; / c-list
		bcolumn:numrows#01b;
		gcolumn:numrows?0Ng;
		xcolumn:numrows?0xFF;
		hcolumn:numrows?100h;
		icolumn:numrows?1000i;
		jcolumn:numrows?10000j;
		ecolumn:numrows?100e;
		fcolumn:numrows?1000f;
		ccolumn:numrows#"abcdef";
		scolumn:numrows#`$"s",/:string each til 100;
		pcolumn:2018.07.01D00:00:00.123456+NSONESEC*til numrows;
		mcolumn:2000.01m+til numrows;
		dcolumn:2018.07.01+til numrows;
		zcolumn:2018.07.01T0+til numrows;
		tcolumn:00:00:00.000+500*til numrows;
		ncolumn:0D0+1000000*til numrows;
		ucolumn:numrows#00:00 23:59;
		vcolumn:numrows#00:00:00 23:59:59
		);
//! fix rtbl/stbl

	rtbl:.sp.pruneAndFilter[opt;rtbl];
	.sp.logDebugTable[rtbl];
	rtbl
	}


exampleArrayTypes:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];

	.sp.logDebug "exampleArrayTypes[]";
	.sp.logDebugOptions[opt];

	partitionid:.sp.optGet[opt;`partitionid;-1];

	//
	// Build one-row table from which we can report the query's schema back to Spark
	//
	stbl:([]
		xlcolumn:1#enlist 1#0x;
		blcolumn:1#enlist 1#0b;
		hlcolumn:1#enlist 1#0h;
		ilcolumn:1#enlist 1#0i;
		jlcolumn:1#enlist 1#0;
		elcolumn:1#enlist 1#0e;
		flcolumn:1#enlist 1#0f;
		clcolumn:1#enlist "abc"; / c-list
		plcolumn:1#enlist 1#0p;
		dlcolumn:1#enlist 1#.z.d
		);

	//
	// A partition number of -1 means that Spark just wants the schema
	//
	if[partitionid=-1;
		res:0!meta stbl;
		.sp.logDebugSchema[res]; 
		:res
		];

	numrows:"J"$.sp.optGet[opt;`numrows;"3"]; / Number of rows to generate for test

	system "S 1"; / Produce same random results each time (for testing)

	rtbl:([]
		xlcolumn:numrows#(1#0x0;0x7f80;"x"$til 10);
		blcolumn:numrows#(1#1b;10101b),"b"$flip 2 vs til 10;
		hlcolumn:numrows#(1#1h;100 200h;"h"$til 10);
		ilcolumn:numrows#(1#1i;100 200i;"i"$reverse til 5);
		jlcolumn:numrows#(1#1;100 200;0#0j);
		elcolumn:numrows#(1#.1e;100 200e;"e"$.1*til 10);
		flcolumn:numrows#(1#.1f;100 200f;"f"$.1*til 10);
		clcolumn:(numrows?26)#\:"abcdefghijklmnopqrstuvwxyz"; / c-list
		plcolumn:numrows#(1#.z.p;2#.z.p;3#.z.p);
		dlcolumn:numrows#(1#.z.d;2#.z.d;3#.z.d)
		);

	rtbl:.sp.pruneAndFilter[opt;rtbl];
	.sp.logDebugTable[rtbl];
	rtbl
	}


largeTbl:([] icolumn:"i"$til 200000000)

exampleLarge:{[opt]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];

	.sp.logDebug "exampleLarge[]";
	.sp.logDebugOptions[opt];

	if[-1=opt`partitionid; / If request for schema
		:0!meta largeTbl]; / then return schema

	tbl:.sp.pruneAndFilter[opt;largeTbl];
	.sp.logDebugTable[tbl];
	tbl
	}


testWrite:{[opt;tbl]
	.sp.setLogLevel .sp.optGet[opt;`loglevel;`warn];
	.sp.logDebug "testWrite[]";
	.sp.logDebugOptions[opt];

	action:opt`writeaction;
	T::tbl;
	0
	}



