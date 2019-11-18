\d .sp

//
// Functions to pick things out of the options dictionary
//
optGet:{[o;k;d] $[k in key o;o k;d]}
optGetBoolean:{[o;k;d] $[k in key o;any o[k]~/:("true";"1");d]}

//
// Logging functions
//
LL:`error / Default log level
setLogLevel:{LL::x}
getLogLevel:{LL} 
isDebugEnabled:{LL=`debug}
isErrorEnabled:{LL=`error}
logDebug:{[s] if[.sp.isDebugEnabled[];.sp.writeLog["DEBUG";s]]}
logError:{[s] if[.sp.isErrorEnabled[];.sp.writeLog["ERROR";s]]}
fmtts:{2_@[string .z.Z;4 7 10;:;"// "]} / Mimic default Log4J pattern
writeLog:{[l;s] -1 fmtts[]," ",l," ",s;} / Prepend timestamp and write to stdout

logDebugOptions:{[o]
	if[isDebugEnabled[];
		.sp.logDebug "Options:";
		.sp.logDebug each ("  ",/:max[count each k]$k:string[key o],\: ": "),'-3!'value o
		]
	}

logDebugSchema:{[s]		
	if[.sp.isDebugEnabled[];
		.sp.logDebug "Schema result:";
		.sp.logDebug "  cols:  ",-3!s`c;
		.sp.logDebug "  types: ",-3!s`t;
		.sp.logDebug "  nulls: ",-3!s`n  
		]
	}

logDebugTable:{[t]
	if[.sp.isDebugEnabled[];
		.sp.logDebug "Table result:";
		.sp.logDebug "  #rows: ",string count t;
		.sp.logDebug "  cols:  ",-3!cols t;
		.sp.logDebug "  types: ",-3!(0!meta t)`t;
		.sp.logDebug "  row 0: ",-3!value t 0
		]
	}	

//
// @desc Apply Spark-requested column pruning and push-down filters to table
//
// @param opt {dict} Contains filters and columns entries 
//
// Filters are a list of operations, sent by the kdb+ datasource that is 
// canonically-similar to the where clause in kdb+ functional form queries. Each
// element of the list contains a predicate or conjunction. The following is an
// example of filter list with two predicates.
//
// spark> df.filter("fcolumn>0 and jcolumn<5000").show...    
// converts to filters being: ((`gt;`fcolumn;0f);(`lt;`jcolumn;5000))
//
// Depending whether the table is partitioned and has attributes, this function
// may not generate the most optimal where expression. The developer may have to
// make adjustments so that the most narrowing predicates are applied first.
//
pruneAndFilter:{[opt;tbl] 
	?[tbl;.sp.parseFilter[tbl;] each opt`filters;0b;c!c:opt`columns]
	}

//
// String comparison functions to be used in functional select
//
sge:{1_ r[0]<r:rank enlist[y],x}
slt:{not sge[x;y]}
sgt:{-1_ r[-1+count r]<r:rank x,enlist[y]}
sle:{not sgt[x;y]}

//
// Mapping dictionary between Spark filter operators and kdb+ parse functions
//
F2P:(!/) flip 0N 2#(
	`and;		&;
	`or;		|;
	`eq;		=;
	`gt;		>;
	`lt;		<;
	`le;		(';~:;>);
	`ge;		(';~:;<);
	`in;		in;
	`not;		~:;
	`ssw;		like;
	`sew;		like;
	`sc;		like;
	`isnull;	(^:);
	`isnotnull; () / Handled in code below
	)

//
// Map from filter function mnemonic to kdb+ function (for strings only)
//
F2SP:(!/) flip 0N 2#(
	`eq;	like;
	`gt;	sgt;
	`lt;	slt;
	`le;	sle;
	`ge;	sge
	)	

//
// Convert Spark filter to functional form constraint
//
parseFilter:{[tbl;f]
	fn:F2P f[0]; / Function

	/ Conjunctions
	if[f[0] in `and`or;
		:(fn;parseFilter[tbl;] f[1];parseFilter[tbl;] f[2])];

	/ Negation
	if[f[0] in `not;
		:(fn;parseFilter[tbl;] f[1])];

	col:f[1]; / Column name

	if[f[0] in `isnull;
		:(fn;col)];

	if[f[0] in `isnotnull;
		:((~:);((^:);col))]

	/ Comparitives/predicates

	if[any b:f[0]=`ssw`sc`sew; / string starts-with, contains, ends-with
		:(fn;col;(any[-2#b]#"*"),string[f 2],any[2#b]#"*")];

	/ =, >, >=, ...

	c:f[2]; / Predicate's constant
	b:0 2 10 11 13h=type tbl[col];
	c:$[
		b[0];string c; / string
		b[1];"G"$string c; / GUID
		b[2];string[c][0]; / char
		b[3];enlist c; / symbol
		b[4];"m"$c; / month
		c];

	if[b[0];fn:F2SP f[0]]; / For strings, map to their own comparison functions

	(fn;col;c)
	}


SUPTYPES:"bgxXhHiIjJeEfFcCspPmdDznuvt" / Supported types

//
// @desc Asserts that a condition is nonzero, signalling an error otherwise.
//
// @param x {int}		Specifies the condition result.
// @param y {symbol}	Specifies the error to be signalled.
//
assert:{if[x=0;'y]}


//
// @desc Validates the query schema result destined for Spark
//
checkSchemaResult:{[tbl]
	assert[98h=type tbl;"Result must be unkeyed table"];
	assert[all `c`t in cols tbl;"Require c (column name) and t (data type) columns"];
	assert[all 11 10h=type each tbl`c`t;"Column c must be symbol and t must be char"];
	if[`n in cols tbl;
		assert[1h=type tbl`n;"The optional n (nullable) column must be boolean"]
		];
	assert[all tbl[`t] in SUPTYPES;"The following kdb+ datatypes not supported: ", 
		tbl[`t] where not tbl[`t] in SUPTYPES];
	}

//
// Handy utitilies to generate Scala code segments to define schemas
//

TT:1!flip `t`s`n!flip 0N 3#(
	"b"; 	"BooleanType";		"boolean";
	"g"; 	"StringType";		"string";
	"x";	"ByteType";			"tinyint";
	"h";	"ShortType";		"short";
	"i";	"IntegerType";		"int";
	"j";	"LongType";			"long";
	"e";	"FloatType";		"float";
	"f";	"DoubleType";		"double";
	"c";	"StringType";		"string";
	"C";	"StringType";		"string";
	"s";	"StringType";		"string";
	"p";	"TimestampType";	"timestamp";
	"z";	"TimestampType";	"timestamp";
	"t";	"TimestampType";	"timestamp";
	"m";	"DateType";			"date";
	"d";	"DateType";			"date";
	"n";	"IntegerType";		"int";
	"u";	"IntegerType";		"int";
	"v";	"IntegerType";		"int"
	)

wwq:{"\"",x,"\""} / Wrap with quotes

//
// @desc Outputs a StructType column definition to be pasted in a Scala function
//
// @param tbl {table}	Unkeyed table from which meta information is used
//
// @returns nothing, but outputs Scala statements to STDOUT for copying and
// pasting into a Scala function. Note that it is assumed that all columns
// are NOT NULLABLE, thus the third argument to each StructField constructor is
// false. Set to true if that column is nullable.
// 
// @example
//
// q)tbl:([] j:til 3;s:`a`b`c)
// q).sp.schemaStructType tbl
// var kdbschema = StructType(List(
//   StructField("j", LongType, false),
//   StructField("s", StringType, false)
// ))
//
// scala> val df = spark.read.format("com.kx.spark.KdbDataSource").schema(kdbschema). ...
//
schemaStructType:{[tbl]
	sf:{"  StructField(",wwq[string x`c],", ",TT[x`t;`s],", false),"};
	res: enlist "val kdbSchema = StructType(List(";
	res:res,sf each 0!meta tbl;
	res:(-1_res),enlist -1_res[-1+count res];
	-1 res,enlist "))";	
	}

//
// @desc Outputs a SQL-like column definition to be pasted into a Scala function
//
// @param tbl {table}	Unkeyed table from which meta information is used
//
// @returns nothing, but outputs Scala string to STDOUT for copying and
// pasting as the argument to the Spark schema function. Note that in this form
// of specifying a schema to Spark, there is no option to indicate whether a 
// column is nullable; Spark assume all columns are NULLABLE. This has performance
// side effects since push-down filters will also request null checks
//
// @example
//
// q)tbl:([] j:til 3;s:`a`b`c)
// q).sp.sqlSchema tbl
// "j long, s string"
//
// scala> val df = spark.read.format("com.kx.spark.KdbDataSource").schema("j long, s string"). ...
//
schemaSQL:{[tbl]
	res:wwq -2_raze {string[x`c]," ",TT[x`t;`n],", "} each 0!meta tbl;
	-1 res;
	}

\d .

