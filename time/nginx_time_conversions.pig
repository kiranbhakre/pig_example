REGISTER file:/home/hadoop/lib/pig/piggybank.jar;                                       
 
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.EXTRACT();                    
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE DATE_TIME org.apache.pig.piggybank.evaluation.datetime.DATE_TIME();
DEFINE FORMAT_DT org.apache.pig.piggybank.evaluation.datetime.FORMAT_DT();
DEFINE FORMAT org.apache.pig.piggybank.evaluation.string.FORMAT();
 
RAW_LOGS = LOAD '$INPUT' as (line:chararray);
LOGS_BASE = FOREACH RAW_LOGS GENERATE                                                   
 
FLATTEN(                                                                                
	EXTRACT(line, '(\\S+) - - \\[([^\\[]+)\\]\\s+"([^"]+)"\\s+(\\d+)\\s+(\\d+)\\s+"([^"]+)"\\s+"([^"]+)"\\s+"([^"]+)"\\s+(\\S+)')
)
AS (
	ip: chararray, 
	timestamp: chararray, 
	url: chararray,
	status: chararray,
	bytes: chararray,
	referrer: chararray,
	useragent: chararray,
	xfwd: chararray,
	reqtime: chararray
);
DATE_URL = FOREACH LOGS_BASE GENERATE timestamp;
F = FILTER DATE_URL BY NOT timestamp IS NULL;
 
R1 = FOREACH F GENERATE timestamp, DATE_TIME(timestamp, 'dd/MMM/yyyy:HH:mm:ss Z') as dt;
DUMP R1;
 
-- ISOToUnix returns milliseconds, so we divide by 1000 to get seconds
-- toEpoch1 = FOREACH R1 GENERATE dt, ISOToUnix(dt) / 1000 as epoch:long;
DUMP toEpoch1;
 
FD = FOREACH R1 GENERATE FORMAT_DT('yyyy-MM-dd HH:mm:ss Z', dt) as fdt;
DUMP FD;
 
toISO = FOREACH FD GENERATE fdt, CustomFormatToISO(fdt, 'yyyy-MM-dd HH:mm:ss Z');
DUMP toISO;
