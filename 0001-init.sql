DO $$
declare
    execSql varchar;
    nObjExist integer;
BEGIN

    -- Table: ${user}.adm_partitionlog

    IF NOT EXISTS (SELECT FROM pg_catalog.pg_tables
                   WHERE  schemaname = '${user}'
                     AND    tablename  = 'adm_partitionlog') THEN

        execSql := 'CREATE TABLE IF NOT EXISTS ${user}.adm_partitionlog
(
    dateexecuted timestamp without time zone,
    owner character varying(60),
    tablename character varying(60),
    exectype character varying(20),
    commandtext character varying(255)
)
TABLESPACE ${ts.infix}';

EXECUTE   execSql;

    END IF;

execSql := 'CREATE OR REPLACE FUNCTION ${user}.create_new_part(
	p_owner text,
	p_tablename text,
	p_drop_days integer DEFAULT 31)
    RETURNS boolean
    LANGUAGE ''plpgsql''
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

 
declare 
v_max_partition_timestamp timestamptz;
v_min_partition_timestamp timestamptz;
v_row record;
v_date_list_sql text;
v_create_cmd text;

begin 

--- create new partitions ---
	
select
	(
    (
      regexp_match(part_expr, '' TO \(''''(.*)''''\)'')
    ) [1]
  ):: timestamptz
into
	v_max_partition_timestamp
from
	(
	select
		format(''%I.%I'',
		n.nspname,
		c.relname) as part_name,
		pg_catalog.pg_get_expr(c.relpartbound,
		c.oid) as part_expr
	from
		pg_class p
	join pg_inherits i on
		i.inhparent = p.oid
	join pg_class c on
		c.oid = i.inhrelid
	join pg_namespace n on
		n.oid = c.relnamespace
	where
		p.relname = p_tablename :: name
		and n.nspname = p_owner :: name
		and p.relkind = ''p''
  ) x
order by
	1 desc
limit 
  1;

for v_row in 
select
	generate_series(
    v_max_partition_timestamp, current_date + interval ''1 day'', 
    ''1 day'' :: interval
  ) dt 
  loop 
  
  v_create_cmd := ''CREATE TABLE IF NOT EXISTS '' || p_owner || ''.'' || p_tablename || ''_'' || extract(
      year
from
	v_row.dt
    ) || lpad(
      (
        extract(
          month 
          from 
            v_row.dt
        )
      ):: text, 
      2, 
      ''0''
    ) || lpad(
      (
        extract(
          day 
          from 
            v_row.dt
        )
      ):: text, 
      2, 
      ''0''
    ) || '' PARTITION OF '' || p_owner || ''.'' || p_tablename || '' FOR VALUES FROM ('''''' || v_row.dt || '''''') TO ('''''' || v_row.dt + ''1 day'' :: interval || '''''')'';

execute v_create_cmd;

INSERT INTO upos.adm_partitionlog
(dateexecuted, owner, tablename, exectype, commandtext)
VALUES(CURRENT_TIMESTAMP(2), p_owner, p_tablename, ''create'', v_create_cmd);

end loop;

--- drop old partitions ---

select
	(
    (
      regexp_match(part_expr, '' FROM \(''''(.*)''''\) TO'')
    ) [1]
  ):: timestamptz
into
	v_min_partition_timestamp
from
	(
	select
		format(''%I.%I'',
		n.nspname,
		c.relname) as part_name,
		pg_catalog.pg_get_expr(c.relpartbound,
		c.oid) as part_expr
	from
		pg_class p
	join pg_inherits i on
		i.inhparent = p.oid
	join pg_class c on
		c.oid = i.inhrelid
	join pg_namespace n on
		n.oid = c.relnamespace
	where
		p.relname = p_tablename :: name
		and n.nspname = p_owner :: name
		and p.relkind = ''p''
  ) x
order by
	1
limit 1;

for v_row in 
select
	generate_series(
    v_min_partition_timestamp, current_date - p_drop_days, 
    ''1 day'' :: interval
  ) dt 
  loop 
  
  v_create_cmd := ''DROP TABLE IF EXISTS '' || p_owner || ''.'' || p_tablename || ''_'' 
    || extract(year
from
	v_row.dt) 
	|| lpad(
      (
        extract(
          month 
          from 
            v_row.dt
        )
      ):: text, 
      2, 
      ''0''
    ) || lpad(
      (
        extract(
          day 
          from 
            v_row.dt
        )
      ):: text, 
      2, 
      ''0''
    );

execute v_create_cmd;

INSERT INTO upos.adm_partitionlog
(dateexecuted, owner, tablename, exectype, commandtext)
VALUES(CURRENT_TIMESTAMP(2), p_owner, p_tablename, ''drop'', v_create_cmd);

end loop;

return true;

exception
when others then 

    raise notice ''% %'',
sqlerrm,
sqlstate;
end;

$BODY$;';

EXECUTE   execSql;

END $$;