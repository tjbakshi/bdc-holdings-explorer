-- Add row_number column to preserve extraction order from the original document
ALTER TABLE public.holdings 
ADD COLUMN row_number integer;

-- Create an index for efficient ordering by row_number
CREATE INDEX idx_holdings_row_number ON public.holdings(filing_id, row_number);