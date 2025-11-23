-- Create bdcs table
CREATE TABLE IF NOT EXISTS public.bdcs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bdc_name TEXT NOT NULL,
  ticker TEXT,
  cik TEXT NOT NULL UNIQUE,
  fiscal_year_end_month INTEGER NOT NULL,
  fiscal_year_end_day INTEGER NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Create index on CIK for faster lookups
CREATE INDEX IF NOT EXISTS idx_bdcs_cik ON public.bdcs(cik);
CREATE INDEX IF NOT EXISTS idx_bdcs_name ON public.bdcs(bdc_name);

-- Create filings table
CREATE TABLE IF NOT EXISTS public.filings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bdc_id UUID NOT NULL REFERENCES public.bdcs(id) ON DELETE CASCADE,
  period_end DATE NOT NULL,
  filing_type TEXT NOT NULL,
  sec_accession_no TEXT,
  filing_url TEXT,
  data_source TEXT,
  parsed_successfully BOOLEAN DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Create indexes on filings
CREATE INDEX IF NOT EXISTS idx_filings_bdc_id ON public.filings(bdc_id);
CREATE INDEX IF NOT EXISTS idx_filings_period_end ON public.filings(period_end);

-- Create holdings table
CREATE TABLE IF NOT EXISTS public.holdings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filing_id UUID NOT NULL REFERENCES public.filings(id) ON DELETE CASCADE,
  company_name TEXT NOT NULL,
  investment_type TEXT,
  industry TEXT,
  description TEXT,
  interest_rate TEXT,
  reference_rate TEXT,
  maturity_date DATE,
  par_amount NUMERIC,
  cost NUMERIC,
  fair_value NUMERIC,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Create indexes on holdings
CREATE INDEX IF NOT EXISTS idx_holdings_filing_id ON public.holdings(filing_id);
CREATE INDEX IF NOT EXISTS idx_holdings_company_name ON public.holdings(company_name);

-- Create view for latest filing per BDC
CREATE OR REPLACE VIEW public.latest_filings AS
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

-- Enable RLS (Row Level Security) on all tables
ALTER TABLE public.bdcs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.filings ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.holdings ENABLE ROW LEVEL SECURITY;

-- Create policies for public read access (since this is a public read-only app)
CREATE POLICY "Public read access for bdcs" ON public.bdcs FOR SELECT USING (true);
CREATE POLICY "Public read access for filings" ON public.filings FOR SELECT USING (true);
CREATE POLICY "Public read access for holdings" ON public.holdings FOR SELECT USING (true);