-- Add columns for resumable parsing state tracking
ALTER TABLE public.filings 
ADD COLUMN IF NOT EXISTS current_byte_offset bigint DEFAULT 0,
ADD COLUMN IF NOT EXISTS current_industry_state text DEFAULT NULL,
ADD COLUMN IF NOT EXISTS total_file_size bigint DEFAULT NULL;