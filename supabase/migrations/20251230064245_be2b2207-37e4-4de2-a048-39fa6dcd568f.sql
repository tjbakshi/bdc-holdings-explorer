-- Preserve approximate HTML order for segmented extractions
ALTER TABLE public.holdings
ADD COLUMN source_pos bigint;

CREATE INDEX idx_holdings_source_pos ON public.holdings(filing_id, source_pos);