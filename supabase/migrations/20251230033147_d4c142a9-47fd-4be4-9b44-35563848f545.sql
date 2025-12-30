-- Add value_scale column to filings table to track detected scale
ALTER TABLE public.filings ADD COLUMN IF NOT EXISTS value_scale TEXT DEFAULT 'unknown';

-- Add comment to document the column
COMMENT ON COLUMN public.filings.value_scale IS 'Detected value scale from filing: thousands, millions, or unknown';