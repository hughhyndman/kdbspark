\l spark.q

//
// @desc Sample query that makes used of helper functions from the sp namespace
//

sampleTable:([]
	shardid:1000?4i; / A column that denotes some sort of partitioning
	jcolumn:til[999],0Nj; / Include one null to demonstrate nullable support
	fcolumn:1000?10.0;
	pcolumn:2020.01.01D0+10000000*til 1000 
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
		// accommodate them in calculations. We do this by adding an optional
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
