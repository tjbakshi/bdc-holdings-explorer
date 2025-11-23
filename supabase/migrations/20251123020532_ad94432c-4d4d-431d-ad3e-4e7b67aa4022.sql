-- Fix the security definer view by making it security invoker
CREATE OR REPLACE VIEW public.latest_filings 
WITH (security_invoker=true)
AS
SELECT DISTINCT ON (bdc_id) 
  f.id,
  f.bdc_id,
  f.period_end,
  f.filing_type,
  f.sec_accession_no,
  f.filing_url,
  f.data_source,
  f.parsed_successfully,
  f.created_at
FROM public.filings f
ORDER BY bdc_id, period_end DESC;