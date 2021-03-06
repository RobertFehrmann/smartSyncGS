create or replace procedure METADATA.SP_REFRESH_GS(I_TGT_DB VARCHAR, I_SVW_DB VARCHAR, I_SCHEMA VARCHAR, I_SHARE VARCHAR)
    returns ARRAY
    language JAVASCRIPT
    execute as caller
as
$$
//note:  this proc returns an array, either success or fail right now
const tgt_db  = I_TGT_DB;
const svw_db  = I_SVW_DB;
const tgt_schema  = I_SCHEMA;
const share = I_SHARE;

const internal = "INTERNAL_";
const tgt_meta_schema = tgt_schema + "_METADATA";
const meta_schema = internal + tgt_schema + "_METADATA";
const tgt_schema_version_latest = "LATEST";
const tgt_schema_new = internal + tgt_schema + "_NEW";
const object_log = "OBJECT_LOG";

const status_begin = "BEGIN";
const status_end = "END";
const status_failure = "FAILURE";
const version_default = "000000";
const reference_type_direct = "DIRECT";
const reference_type_indirect = "INDIRECT";

var return_array = [];
var locked_schema_version = "";
var tgt_schema_curr = "";
var counter = 0;
var table_name = "";
var status=status_end;
var schema_version=version_default;
var data_version=version_default;
var curr_schema_version=version_default;
var curr_data_version=version_default; 

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
         var sqlquery = "INSERT INTO \"" + svw_db + "\"." + meta_schema + ".log (target_schema, version, status,message) values ";
         sqlquery = sqlquery + "('" + tgt_schema + "','" + curr_schema_version + curr_data_version + "','" + status + "','" + message + "');";
         snowflake.execute({sqlText: sqlquery});
         break;
      }
      catch (err) {
         sqlquery=`
            CREATE TABLE IF NOT EXISTS "`+ svw_db + `".` + meta_schema + `.log (
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

function grant_schema() {
    var src_schema_name="";
    var tgt_schema_name="";
    var table_version=""
    var table_version_name=""

    sqlquery = `
       SELECT view_name, curr_table_version, curr_schema_name
       FROM (
            SELECT view_name, curr_table_version, curr_schema_name
                   ,row_number() over (partition by view_name
                                       order by curr_table_version desc) rownum
            FROM "` + tgt_db + `"."` + meta_schema + `"."` + object_log  + `" )
       WHERE rownum=1
       ORDER BY VIEW_NAME
       `;

    var tableNameResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    while (tableNameResultSet.next())  {
          counter = counter + 1;
          table_name = tableNameResultSet.getColumnValue(1);
          table_version = tableNameResultSet.getColumnValue(2);
          schema_name = tableNameResultSet.getColumnValue(3);
          table_version_name = table_name+"_"+table_version;

          // create a secure view to all tables in the the target schema and a grant select to the secure view to the share
          // since the share does not yet have the usage permission on the new share, these changes are not visible
          // to the consumer

         sqlquery = `
            CREATE OR REPLACE /* # ` + counter + ` */ SECURE VIEW "` + svw_db + `"."` + tgt_schema_new + `"."` + table_name + `" AS
                SELECT * FROM "` + tgt_db + `"."` + schema_name + `"."` + table_version_name + `"`;
         snowflake.execute({sqlText: sqlquery});

         sqlquery = `
            GRANT SELECT ON /* # ` + counter + ` */ VIEW "` + svw_db + `"."` + tgt_schema_new + `"."` + table_name + `" TO SHARE "` + share + `"`;
         snowflake.execute({sqlText: sqlquery});
         log("CREATED SECURE VIEW FOR TABLE " + table_name );

    }

    // Grant usage on the new schema to the share

    sqlquery = `
          GRANT USAGE ON SCHEMA "` + svw_db + `"."` + tgt_schema_new + `" TO SHARE "` + share + `"`;
    snowflake.execute({sqlText: sqlquery});

}

try {

    snowflake.execute({sqlText: "CREATE SHARE IF NOT EXISTS \"" + share + "\""});
    //snowflake.execute({sqlText: "CREATE DATABASE IF NOT EXISTS \"" + svw_db + "\""});
    snowflake.execute({sqlText: "CREATE SCHEMA IF NOT EXISTS \"" + svw_db + "\".\"" + tgt_meta_schema + "\""});
    snowflake.execute({sqlText: "CREATE SCHEMA IF NOT EXISTS \"" + svw_db + "\".\"" + tgt_schema + "\""});
    snowflake.execute({sqlText: "CREATE OR REPLACE SCHEMA \"" + svw_db + "\".\"" + tgt_schema_new + "\""});

    // determine the latest data version in the latest (or locked) schema

    sqlquery = `
          GRANT USAGE ON DATABASE "` + svw_db + `" TO SHARE "` + share + `"`;
    snowflake.execute({sqlText: sqlquery});

    sqlquery = `
          GRANT REFERENCE_USAGE ON DATABASE "` + tgt_db + `" TO SHARE "` + share + `"`;
    snowflake.execute({sqlText: sqlquery});

    sqlquery = `
          GRANT USAGE ON SCHEMA "` + svw_db + `"."` + meta_schema + `" TO SHARE "` + share + `"`;
    snowflake.execute({sqlText: sqlquery});

     sqlquery = `
        GRANT SELECT ON VIEW "` + svw_db + `"."` + meta_schema + `"."` +object_log+ `" TO SHARE "` + share + `"`;
     snowflake.execute({sqlText: sqlquery});

     sqlquery = `
        GRANT SELECT ON VIEW "` + svw_db + `"."` + meta_schema + `".log TO SHARE "` + share + `"`;
     snowflake.execute({sqlText: sqlquery});

    grant_schema();

    sqlquery = `
          ALTER SCHEMA "` + svw_db + `"."` + tgt_schema + `" 
          SWAP WITH "` + svw_db + `"."` + tgt_schema_new + `"`;
    snowflake.execute({sqlText: sqlquery});
       
    sqlquery = `
          DROP SCHEMA "` + svw_db + `"."` + tgt_schema_new + `"`;
    snowflake.execute({sqlText: sqlquery});

    log("procName: " + procName + " END " + status)
    flush_log(status)

    return return_array
}
catch (err) {
    log("ERROR found - MAIN try command");
    log("err.code: " + err.code);
    log("err.state: " + err.state);
    log("err.message: " + err.message);
    log("err.stacktracetxt: " + err.stacktracetxt);
    log("procName: " + procName + " " + status_end)

    flush_log(status_failure);
    
    return return_array;
}
$$;


