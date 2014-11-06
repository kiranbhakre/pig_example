
'how to execute it  pig mail_sendtime.pig 
REGISTER /usr/lib/hadoop/lib/piggybank.jar;                                       
 
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.EXTRACT();                    
 
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
 
DEFINE DATE_TIME org.apache.pig.piggybank.evaluation.datetime.DATE_TIME();
DEFINE FORMAT_DT org.apache.pig.piggybank.evaluation.datetime.FORMAT_DT();
 
DEFINE FORMAT org.apache.pig.piggybank.evaluation.string.FORMAT();
 
%default YEAR `date +%Y`;
 
RAW_LOGS = LOAD '$INPUT' as (line:chararray);
 
SRC = FOREACH RAW_LOGS GENERATE                                                   
FLATTEN(                                                                                
	EXTRACT(line, '(\\S+)\\s+(\\d+)\\s+(\\S+)\\s+(\\S+)\\s+sendmail\\[(\\d+)\\]:\\s+(\\w+):\\s+from=<([^>]+)>,\\s+size=(\\d+),\\s+class=(\\d+),\\s+nrcpts=(\\d+),\\s+msgid=<([^>]+)>.*relay=(\\S+)')
)
AS (
	month: chararray, 
	day: chararray, 
	time: chararray, 
	mailserver: chararray, 
	pid: chararray, 
	sendmailid: chararray, 
	src: chararray, 
	size: chararray, 
	classnumber: chararray, 
	nrcpts: chararray, 
	msgid: chararray, 
	relay: chararray
);
 
T1 = FOREACH SRC GENERATE sendmailid, FORMAT('%s-%s-%s %s', $YEAR, month, day, time) as timestamp;
FILTER_T1 = FILTER T1 BY NOT sendmailid IS NULL;
DUMP FILTER_T1;
 
R1 = FOREACH FILTER_T1 GENERATE sendmailid, DATE_TIME(timestamp, 'yyyy-MMM-d HH:mm:ss') as dt;
DUMP R1;
 
-- ISOToUnix returns milliseconds, so we divide by 1000 to get seconds
toEpoch1 = FOREACH R1 GENERATE sendmailid, dt, ISOToUnix(dt) / 1000 as epoch:long;
DUMP toEpoch1;
 
DEST = FOREACH RAW_LOGS GENERATE                                                   
FLATTEN(                                                                                
	EXTRACT(line, '(\\S+)\\s+(\\d+)\\s+(\\S+)\\s+(\\S+)\\s+sendmail\\[(\\d+)\\]:\\s+(\\w+):\\s+to=<([^>]+)>,\\s+delay=([^,]+),\\s+xdelay=([^,]+),.*relay=(\\S+)\\s+\\[\\S+\\],\\s+dsn=\\S+,\\s+stat=(.*)')
)
AS (
	month: chararray, 
	day: chararray, 
	time: chararray, 
	mailserver: chararray, 
	pid: chararray, 
	sendmailid: chararray, 
	dest: chararray, 
	delay: chararray, 
	xdelay: chararray, 
	relay: chararray, 
	stat: chararray
);
 
T2 = FOREACH DEST GENERATE sendmailid, FORMAT('%s-%s-%s %s', $YEAR, month, day, time) as timestamp, dest, stat;
FILTER_T2 = FILTER T2 BY NOT sendmailid IS NULL;
 
R2 = FOREACH FILTER_T2 GENERATE sendmailid, DATE_TIME(timestamp, 'yyyy-MMM-d HH:mm:ss') as dt, dest, stat;
 
-- ISOToUnix returns milliseconds, so we divide by 1000 to get seconds
toEpoch2 = FOREACH R2 GENERATE sendmailid, dt, ISOToUnix(dt) / 1000 AS epoch:long, dest, stat;
 
R3 = JOIN toEpoch1 BY sendmailid, toEpoch2 BY sendmailid;
R4 = FOREACH R3 GENERATE $0, $5 - $2, $6, $7;
R5 = ORDER R4 BY $1 DESC;
STORE R5 INTO '$OUTPUT';
