  SELECT 
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) as type
  FROM pg_class c,
   pg_attribute a,
   pg_type t
   WHERE c.relname = 'tablename'
   AND a.attnum > 0
   AND a.attrelid = c.oid
   AND a.atttypid = t.oid
 ORDER BY a.attnum