create procedure METADATA.SP_COMPACT_GS(I_TGT_DB VARCHAR, I_TGT_SCHEMA VARCHAR, I_MAX_TABLE_VERSIONS FLOAT)
    returns ARRAY
    language JAVASCRIPT
    execute as caller
as
$$
//note:  this proc returns an array, either success or fail right now
const tgt_db  = I_TGT_DB;
const tgt_schema  = I_TGT_SCHEMA;
const max_table_versions = I_MAX_TABLE_VERSIONS;

const smart_copy_compact="SMART_COPY_COMPACT";
const object_log="OBJECT_LOG";
const table_drop_tmp="DROP_TABLE_TMP";
const internal = "INTERNAL_";
const meta_schema = internal + tgt_schema + "_METADATA";
const tgt_schema_tmp = internal + tgt_schema + "_TMP";
const status_begin = "BEGIN";
const status_end = "END";
const status_failure = "FAILURE";
const version_initial = "000001";

var return_array = [];
var locked_schema_version = "";
var counter = 0;
var schema_name = "";
var status=status_end;
var schema_version="";
var data_version="";

var procName = Object.keys(this)[0];


function log ( msg ) {
   var d=new Date();
   var UTCTimeString=("00"+d.getUTCHours()).slice(-2)+":"+("00"+d.getUTCMinutes()).slice(-2)+":"+("00"+d.getUTCSeconds()).slice(-2);
   return_array.push(UTCTimeString+" "+msg);
}

function flush_log (status){
   var message="";
   var sqlquery="";
   for (i=0; i < return_array.length; i++) {
      message=message+String.fromCharCode(13)+return_array[i];
   }
   message=message.replace(/'/g,"");

   for (i=0; i<2; i++) {
      try {

         var sqlquery = "INSERT INTO \"" + tgt_db + "\"." + meta_schema + ".log (target_schema, version, status,message) values ";
         sqlquery = sqlquery + "('" + tgt_schema + "','" + "000000000000" + "','" + status + "','" + message + "');";
         snowflake.execute({sqlText: sqlquery});
         break;
      }
      catch (err) {
         sqlquery=`
            CREATE TABLE IF NOT EXISTS "`+ tgt_db + `".` + meta_schema + `.log (
               id integer AUTOINCREMENT (0,1)
               ,create_ts timestamp_ltz default current_timestamp
               ,target_schema varchar
               ,version varchar
               ,session_id number default to_number(current_session())
               ,status varchar
               ,message varchar)`;
         snowflake.execute({sqlText: sqlquery});
      }
   }
}

try {


    log("procName: "+procName+" "+status_begin);
    flush_log("START");

    // determine all schemas to be deleted, i.e. it's an obsolete schema version or and obsolete version
    // an update to the dataset in a specific schema version

    var sqlquery=`
       CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_drop_tmp  + `" AS
           SELECT table_name, curr_table_version, curr_schema_name
           FROM (
                SELECT view_name table_name, curr_table_version,  curr_schema_name
                       ,row_number() over (partition by view_name
                                           order by curr_table_version desc) rownum
                FROM "` + tgt_db + `"."` + meta_schema + `"."` + object_log  + `"
                WHERE curr_table_version::int > 1
                AND view_name||'_'||curr_table_version NOT IN (
                   SELECT view_name||'_'||curr_table_version
                   FROM "` + tgt_db + `"."` + meta_schema + `"."` + object_log  + `"
                   WHERE nvl(notification_type,'NULL') = '`+smart_copy_compact+`')
           )
           WHERE rownum > `+max_table_versions+`
           ORDER BY table_name
           `;

    snowflake.execute({sqlText: sqlquery});

    var sqlquery=`
       SELECT table_name, curr_table_version, curr_schema_name
       FROM "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_drop_tmp  + `"
       `;

    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    while (ResultSet.next())  {
       counter = counter + 1;
       table_name = ResultSet.getColumnValue(1)+"_"+ResultSet.getColumnValue(2);
       schema_name = ResultSet.getColumnValue(3);
        var sqlquery=`
           DROP /* # ` + counter + ` */
           TABLE IF EXISTS "` + tgt_db + `"."` + schema_name + `"."` + table_name  + `"
           `;

       snowflake.execute({sqlText: sqlquery});
       log("drop: " + schema_name + "." + table_name);
    }

    sqlquery=`
       INSERT INTO "` + tgt_db + `"."` + meta_schema + `"."` + object_log  + `"
             (notification_type,view_name,curr_table_version,curr_schema_name,create_ts)
          SELECT '`+smart_copy_compact+`', table_name, curr_table_version, curr_schema_name
                 , convert_timezone('UTC',current_timestamp())
          FROM "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_drop_tmp  + `"
          `;
    snowflake.execute({sqlText: sqlquery});

    log("procName: " + procName + " " + status_end)
    flush_log(status)

    return return_array
}
catch (err) {
    log("ERROR found - MAIN try command");
    log("err.code: " + err.code);
    log("err.state: " + err.state);
    log("err.message: " + err.message);
    log("err.stacktracetxt: " + err.stacktracetxt);
    log("procName: " + procName );

    flush_log(status_failure);

    return return_array;
}
$$;


