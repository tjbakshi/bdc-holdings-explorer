-- Step 1: Create the normalize_cik trigger function FIRST
CREATE OR REPLACE FUNCTION public.normalize_cik()
RETURNS TRIGGER AS $$
BEGIN
  NEW.cik = LTRIM(NEW.cik, '0');
  IF NEW.cik = '' THEN
    NEW.cik = '0';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = public;

-- Step 2: Delete duplicate filings (keep ones with padded CIK BDCs, we'll migrate those)
DELETE FROM filings f
WHERE f.id IN (
  SELECT f1.id 
  FROM filings f1
  JOIN bdcs b1 ON f1.bdc_id = b1.id
  WHERE b1.cik NOT LIKE '0%'
  AND EXISTS (
    SELECT 1 FROM filings f2
    JOIN bdcs b2 ON f2.bdc_id = b2.id
    WHERE b2.cik = LPAD(b1.cik, 10, '0')
    AND f2.sec_accession_no = f1.sec_accession_no
  )
);

-- Step 3: Delete BDCs with unpadded CIKs that have a padded duplicate
DELETE FROM bdcs b
WHERE b.cik NOT LIKE '0%'
AND EXISTS (
  SELECT 1 FROM bdcs b2 
  WHERE b2.cik = LPAD(b.cik, 10, '0')
);

-- Step 4: Normalize all remaining CIKs to unpadded format
UPDATE bdcs SET cik = LTRIM(cik, '0') WHERE cik LIKE '0%';
UPDATE ingestion_runs SET cik = LTRIM(cik, '0') WHERE cik LIKE '0%';

-- Step 5: Add trigger to bdcs table
DROP TRIGGER IF EXISTS normalize_cik_trigger ON bdcs;
CREATE TRIGGER normalize_cik_trigger
  BEFORE INSERT OR UPDATE ON bdcs
  FOR EACH ROW
  EXECUTE FUNCTION public.normalize_cik();

-- Step 6: Add trigger to ingestion_runs table
DROP TRIGGER IF EXISTS normalize_cik_trigger ON ingestion_runs;
CREATE TRIGGER normalize_cik_trigger
  BEFORE INSERT OR UPDATE ON ingestion_runs
  FOR EACH ROW
  EXECUTE FUNCTION public.normalize_cik();

-- Step 7: Add UNIQUE constraint on CIK (drop if exists first to be safe)
ALTER TABLE bdcs DROP CONSTRAINT IF EXISTS bdcs_cik_unique;
ALTER TABLE bdcs ADD CONSTRAINT bdcs_cik_unique UNIQUE (cik);