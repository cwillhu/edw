CREATE OR REPLACE VIEW edw.functions AS
    SELECT routine_name FROM information_schema.routines 
    WHERE routine_type='FUNCTION' AND specific_schema='edw';
