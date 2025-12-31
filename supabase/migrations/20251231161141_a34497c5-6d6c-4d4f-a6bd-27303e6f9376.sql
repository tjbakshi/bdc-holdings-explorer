-- Add period_date column to holdings table to track extraction source period
ALTER TABLE public.holdings 
ADD COLUMN period_date date NULL;

-- Add a comment explaining the column
COMMENT ON COLUMN public.holdings.period_date IS 'The period date from the Schedule of Investments header (e.g., 2025-09-30 for Q3 2025 filing)';