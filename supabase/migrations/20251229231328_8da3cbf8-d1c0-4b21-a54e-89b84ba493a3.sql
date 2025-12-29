-- Create ingestion_runs table to track SEC data fetches
CREATE TABLE public.ingestion_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  cik TEXT NOT NULL,
  started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  finished_at TIMESTAMP WITH TIME ZONE,
  status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'success', 'error')),
  inserted_count INTEGER DEFAULT 0,
  error_message TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Enable RLS
ALTER TABLE public.ingestion_runs ENABLE ROW LEVEL SECURITY;

-- Allow public read access for viewing ingestion history
CREATE POLICY "Public read access for ingestion_runs"
ON public.ingestion_runs
FOR SELECT
USING (true);

-- Add unique constraint on filings if not exists (cik via bdc_id + accession_no)
-- Note: filings already has sec_accession_no, we'll use bdc_id + sec_accession_no as unique
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'filings_bdc_id_sec_accession_no_key'
  ) THEN
    ALTER TABLE public.filings ADD CONSTRAINT filings_bdc_id_sec_accession_no_key UNIQUE (bdc_id, sec_accession_no);
  END IF;
END $$;