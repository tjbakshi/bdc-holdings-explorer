import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { DOMParser, Element } from "https://deno.land/x/deno_dom@v0.1.38/deno-dom-wasm.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const SEC_USER_AGENT = "BDCTrackerApp/1.0 (contact@bdctracker.com)";

interface Holding {
  company_name: string;
  investment_type?: string | null;
  industry?: string | null;
  description?: string | null;
  interest_rate?: string | null;
  reference_rate?: string | null;
  maturity_date?: string | null;
  par_amount?: number | null;
  cost?: number | null;
  fair_value?: number | null;
}

interface ScaleDetectionResult {
  scale: number; // Multiplier to convert to millions (e.g., 0.001 for thousands, 1 for millions)
  detected: 'thousands' | 'millions' | 'unknown';
  confidence: 'high' | 'medium' | 'low';
}

// Detect the scale of values in a filing (thousands vs millions)
function detectScale(html: string): ScaleDetectionResult {
  const lowerHtml = html.toLowerCase();
  
  // High-confidence patterns for thousands
  const thousandPatterns = [
    /\(in thousands\)/i,
    /\(in\s+000s?\)/i,
    /amounts?\s+in\s+thousands/i,
    /\$\s*in\s+thousands/i,
    /dollars?\s+in\s+thousands/i,
    /000s?\s*omitted/i,
    /expressed\s+in\s+thousands/i,
    /stated\s+in\s+thousands/i,
  ];
  
  // High-confidence patterns for millions  
  const millionPatterns = [
    /\(in millions\)/i,
    /amounts?\s+in\s+millions/i,
    /\$\s*in\s+millions/i,
    /dollars?\s+in\s+millions/i,
    /expressed\s+in\s+millions/i,
    /stated\s+in\s+millions/i,
  ];
  
  // Check for thousands indicator
  for (const pattern of thousandPatterns) {
    if (pattern.test(lowerHtml)) {
      console.log(`📊 Scale detected: THOUSANDS (pattern: ${pattern})`);
      return { scale: 0.001, detected: 'thousands', confidence: 'high' };
    }
  }
  
  // Check for millions indicator
  for (const pattern of millionPatterns) {
    if (pattern.test(lowerHtml)) {
      console.log(`📊 Scale detected: MILLIONS (pattern: ${pattern})`);
      return { scale: 1, detected: 'millions', confidence: 'high' };
    }
  }
  
  // Default assumption: most BDC filings report in thousands
  console.log(`📊 Scale detected: THOUSANDS (default assumption)`);
  return { scale: 0.001, detected: 'thousands', confidence: 'low' };
}

// Validate scale by checking if values are reasonable for BDC holdings
function validateScale(holdings: Holding[], scale: ScaleDetectionResult): { valid: boolean; warning?: string } {
  if (holdings.length === 0) return { valid: true };
  
  // Calculate average fair value after applying scale
  const validValues = holdings
    .filter(h => h.fair_value !== null)
    .map(h => (h.fair_value as number) * scale.scale);
  
  if (validValues.length === 0) return { valid: true };
  
  const avgValue = validValues.reduce((sum, v) => sum + v, 0) / validValues.length;
  const maxValue = Math.max(...validValues);
  const minValue = Math.min(...validValues);
  
  console.log(`📊 Scale validation: avg=$${avgValue.toFixed(1)}M, min=$${minValue.toFixed(1)}M, max=$${maxValue.toFixed(1)}M`);
  
  // BDC holdings typically range from $0.1M to $500M per position
  // Average should be between $1M and $100M
  if (avgValue > 1000) {
    return { 
      valid: false, 
      warning: `Average holding value ($${avgValue.toFixed(1)}M) too large - values may already be in thousands, not actual amounts`
    };
  }
  
  if (avgValue < 0.01) {
    return {
      valid: false,
      warning: `Average holding value ($${(avgValue * 1000).toFixed(1)}K) too small - values may be in millions, not thousands`
    };
  }
  
  return { valid: true };
}

// Helper to fetch SEC files with retry
async function fetchSecFile(url: string, retries = 2): Promise<string> {
  for (let i = 0; i <= retries; i++) {
    try {
      console.log(`Fetching: ${url}`);
      const response = await fetch(url, {
        headers: { "User-Agent": SEC_USER_AGENT },
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      return await response.text();
    } catch (error) {
      if (i === retries) throw error;
      console.log(`Retry ${i + 1}/${retries} for ${url}`);
      await new Promise((resolve) => setTimeout(resolve, 1000 * (i + 1)));
    }
  }
  throw new Error("Failed to fetch after retries");
}

// Parse numeric value from string (handles $, commas, parentheses for negatives)
function parseNumeric(value: string | null | undefined): number | null {
  if (!value) return null;
  
  const cleaned = value.replace(/[$,\s]/g, "").trim();
  if (!cleaned || cleaned === "-" || cleaned === "—") return null;
  
  // Handle parentheses as negative
  const isNegative = cleaned.startsWith("(") && cleaned.endsWith(")");
  const numStr = isNegative ? cleaned.slice(1, -1) : cleaned;
  
  const parsed = parseFloat(numStr);
  if (isNaN(parsed)) return null;
  
  return isNegative ? -parsed : parsed;
}

// Convert a value to millions using the detected scale, round to 1 decimal
function toMillions(value: number | null | undefined, scale: number): number | null {
  if (value === null || value === undefined) return null;
  const inMillions = value * scale;
  return Math.round(inMillions * 10) / 10;
}

// Parse date from various formats
function parseDate(value: string | null | undefined): string | null {
  if (!value) return null;
  
  const cleaned = value.trim();
  if (!cleaned || cleaned === "-" || cleaned === "—" || cleaned.toLowerCase() === "n/a") return null;
  
  // Try MM/DD/YYYY format (common in SEC filings)
  const mmddyyyyMatch = cleaned.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (mmddyyyyMatch) {
    const [, month, day, year] = mmddyyyyMatch;
    const date = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
    if (!isNaN(date.getTime())) {
      return date.toISOString().split("T")[0];
    }
  }
  
  // Try Month DD, YYYY format (e.g., "December 31, 2027")
  const monthDayYearMatch = cleaned.match(/^([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})$/);
  if (monthDayYearMatch) {
    const date = new Date(cleaned);
    if (!isNaN(date.getTime())) {
      return date.toISOString().split("T")[0];
    }
  }
  
  // Try YYYY-MM-DD format (ISO)
  const isoMatch = cleaned.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    return cleaned;
  }
  
  // Try standard Date parsing as fallback
  const date = new Date(cleaned);
  if (!isNaN(date.getTime())) {
    return date.toISOString().split("T")[0];
  }
  
  console.log(`⚠️ Could not parse date: "${cleaned}"`);
  return null;
}

// Extract interest rate info
function extractInterestRate(text: string): { rate: string | null; reference: string | null } {
  if (!text) return { rate: null, reference: null };
  
  const lowerText = text.toLowerCase();
  
  // Common reference rates
  const referenceRates = ["sofr", "libor", "prime", "euribor"];
  let reference = null;
  
  for (const ref of referenceRates) {
    if (lowerText.includes(ref)) {
      reference = ref.toUpperCase();
      break;
    }
  }
  
  // If we found a reference rate, return the full text as rate
  if (reference) {
    return { rate: text.trim(), reference };
  }
  
  // Check for fixed rate (e.g., "8.5%", "10.00%")
  const fixedRateMatch = text.match(/(\d+\.?\d*)\s*%/);
  if (fixedRateMatch) {
    return { rate: text.trim(), reference: null };
  }
  
  return { rate: text.trim(), reference: null };
}

// Extract candidate HTML snippets containing Schedule of Investments
function extractCandidateTableHtml(html: string): string[] {
  // Normalize HTML to handle &nbsp; and extra whitespace
  const normalized = html.replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ');
  const lower = normalized.toLowerCase();
  
  // Broaden keywords to catch more variations
  const keywords = [
    "consolidated schedule of investments",
    "schedule of investments (unaudited)",
    "schedule of investments (continued)",
    "schedule of investments",
    "portfolio investments",
  ];
  
  const snippets: string[] = [];
  
  for (const keyword of keywords) {
    let searchStart = 0;
    while (true) {
      const idx = lower.indexOf(keyword, searchStart);
      if (idx === -1) break;
      
      // Take a window around the keyword (±150kb)
      const start = Math.max(0, idx - 150_000);
      const end = Math.min(html.length, idx + 150_000);
      snippets.push(html.slice(start, end));
      
      searchStart = idx + keyword.length;
      // Only find first occurrence per keyword to avoid too many snippets
      break;
    }
  }
  
  // If we didn't find any keyword, fall back to the first ~300kb of the file
  if (snippets.length === 0) {
    console.log("No SOI keywords found, using first 300KB");
    snippets.push(html.slice(0, 300_000));
  } else {
    console.log(`Found ${snippets.length} candidate SOI regions`);
  }
  
  return snippets;
}

// Helper to find header row in a table
function findHeaderRow(table: Element, debugMode = false): Element | null {
  const rows = (table as Element).querySelectorAll("tr");
  
  // Scan first ~10 rows to find the header (some tables have multi-row headers)
  const maxHeaderScan = Math.min(10, rows.length);
  
  for (let i = 0; i < maxHeaderScan; i++) {
    const row = rows[i] as Element;
    const rowText = row.textContent?.toLowerCase() || "";
    
    // Header must contain fair value/market value (relaxed: cost is optional)
    const hasFairValue = rowText.includes("fair value") || 
                        rowText.includes("market value") ||
                        rowText.includes("fair");
    const hasCost = rowText.includes("cost") || rowText.includes("amortized");
    const hasCompany = rowText.includes("company") || 
                       rowText.includes("portfolio") || 
                       rowText.includes("name") ||
                       rowText.includes("issuer") ||
                       rowText.includes("investment") ||
                       rowText.includes("borrower");
    
    // Accept header if it has fair value + company, cost is optional
    if (hasFairValue && hasCompany) {
      if (debugMode) {
        const headerCells = Array.from(row.querySelectorAll("th, td"));
        const headers = headerCells.map(h => (h as Element).textContent?.trim() || "");
        console.log("✓ Found candidate header row:", headers);
      }
      return row;
    }
    
    // Fallback: accept if it has fair value alone (company might be in a different label)
    if (hasFairValue && hasCost) {
      if (debugMode) {
        const headerCells = Array.from(row.querySelectorAll("th, td"));
        const headers = headerCells.map(h => (h as Element).textContent?.trim() || "");
        console.log("✓ Found fallback header row (fair + cost):", headers);
      }
      return row;
    }
  }
  
  return null;
}

// Keywords that indicate summary/total rows (not actual holdings)
const SKIP_KEYWORDS = [
  "total", "subtotal", "net", "assets", "portfolio investments",
  "investments at", "fair value at", "cost at", "balance",
  "beginning", "ending", "change", "increase", "decrease",
  "weighted average", "percentage", "% of", "footnote",
  "see accompanying", "notes to", "schedule continued",
  "non-controlled", "controlled", "affiliate", // These are section headers, not companies
  // Industry categories (these are section headers, not companies)
  "software and services", "health care", "commercial and professional",
  "financial services", "consumer services", "consumer distribution",
  "sports, media", "investment funds", "capital goods",
  "pharmaceuticals", "biotechnology", "insurance",
  "real estate", "transportation", "utilities", "energy",
  "materials", "telecommunications", "technology",
  "media and entertainment", "food and beverage", "retail",
  // Investment type headers (section labels, not companies)
  "first lien", "second lien", "senior secured", "subordinated",
  "mezzanine", "equity", "preferred", "common stock", "warrants",
  "senior subordinated loans", "other equity", "preferred equity",
  "subordinated certificates",
  // Accounting/transaction terms (not company names)
  "contributed capital", "management fees", "distributions",
  "income", "expenses", "interest expense", "dividend",
  "unrealized", "realized", "gain", "loss",
];

// Patterns that indicate the row is NOT a real holding
const SKIP_PATTERNS = [
  /^\d+\.?\d*\s*%/, // Starts with percentage
  /^\$[\d,]+/, // Starts with dollar amount
  /^\(\d/, // Starts with parenthetical number
  /^[\d,]+$/, // Just a number
  /^-+$/, // Just dashes
  /^—+$/, // Just em-dashes
  /^\s*$/, // Empty or whitespace
  /^\d{1,2}\/\d{1,2}\/\d{2,4}/, // Starts with date
  /^[A-Z][a-z]+ \d{1,2}, \d{4}/, // Date format like "September 30, 2025"
];

// Company suffixes that strongly indicate a real holding
// More restrictive list - only true legal entity suffixes
const COMPANY_SUFFIXES = [
  "inc.", "inc", "llc", "l.l.c.", "lp", "l.p.", "corp.", "corp",
  "corporation", "company", "co.", "ltd.", "ltd", "limited",
];


// Check if a row represents an actual portfolio holding
function isRealHolding(companyName: string, fairValue: number | null, cost: number | null): { valid: boolean; reason: string } {
  const name = companyName.trim();
  const lowerName = name.toLowerCase();
  
  // Must have a non-empty name
  if (!name || name.length < 5) {
    return { valid: false, reason: "Name too short" };
  }
  
  // Skip if matches any skip keywords
  for (const keyword of SKIP_KEYWORDS) {
    if (lowerName.includes(keyword)) {
      return { valid: false, reason: `Contains skip keyword: "${keyword}"` };
    }
  }
  
  // Skip if matches skip patterns
  for (const pattern of SKIP_PATTERNS) {
    if (pattern.test(name)) {
      return { valid: false, reason: `Matches skip pattern: ${pattern}` };
    }
  }
  
  // Must have fair value
  if (fairValue === null) {
    return { valid: false, reason: "No fair value" };
  }
  
  // Skip if fair value is exactly 0 (usually summary rows)
  if (fairValue === 0) {
    return { valid: false, reason: "Fair value is 0" };
  }
  
  // Check for company suffix (REQUIRED for acceptance)
  // Use word boundary matching to avoid false positives
  const hasCompanySuffix = COMPANY_SUFFIXES.some(suffix => {
    const regex = new RegExp(`\\b${suffix.replace(/\./g, "\\.")}\\b`, "i");
    return regex.test(lowerName);
  });
  
  if (!hasCompanySuffix) {
    // Special case: names ending with common company name patterns
    // e.g., "Acme Enterprises" or "ABC Group" (but "group" is too generic without other suffix)
    const endsWithEntityWord = /\b(enterprises|industries|technologies|systems|group|capital|solutions|services)\s*$/i.test(lowerName);
    if (!endsWithEntityWord) {
      return { valid: false, reason: "No company suffix found" };
    }
  }
  
  // Additional sanity check: name should have at least 2 words
  const wordCount = name.split(/\s+/).filter(w => w.length > 0).length;
  if (wordCount < 2) {
    return { valid: false, reason: "Single word name" };
  }
  
  return { valid: true, reason: "Has company suffix" };
}

// Parse tables looking for Schedule of Investments
function parseTables(tables: Iterable<Element>, maxRowsPerTable: number, maxHoldings: number, debugMode = false): Holding[] {
  const holdings: Holding[] = [];
  const debugAccepted: string[] = [];
  const debugRejected: { name: string; reason: string }[] = [];
  
  let tableIndex = 0;
  for (const table of tables) {
    tableIndex++;
    
    // Find the header row (don't assume it's the first row)
    const headerRow = findHeaderRow(table as Element, debugMode);
    if (!headerRow) {
      if (debugMode && tableIndex <= 10) {
        console.log(`⊗ Table ${tableIndex}: No valid header row found`);
      }
      continue;
    }
    
    // Find column indices - handle colspan by tracking actual cell positions
    const headerCells = Array.from(headerRow.querySelectorAll("th, td"));
    
    // Build a position-aware header map that accounts for colspan
    interface HeaderInfo {
      text: string;
      position: number;
    }
    const headerMap: HeaderInfo[] = [];
    let position = 0;
    for (const cell of headerCells) {
      const el = cell as Element;
      const text = el.textContent?.toLowerCase().trim() || "";
      const colspan = parseInt(el.getAttribute("colspan") || "1", 10);
      
      // Only add non-empty headers
      if (text) {
        headerMap.push({ text, position });
      }
      
      position += colspan;
    }
    
    if (debugMode) {
      console.log(`\n=== Table ${tableIndex} Headers ===`);
      console.log("Header map:", headerMap.map(h => `${h.text}@${h.position}`));
    }
    
    // Find column positions using the header map
    const findHeader = (patterns: string[]): number => {
      for (const h of headerMap) {
        for (const p of patterns) {
          if (h.text.includes(p)) return h.position;
        }
      }
      return -1;
    };
    
    const colIndices = {
      company: findHeader(["company", "portfolio", "name", "issuer", "borrower"]),
      investmentType: findHeader(["investment", "type", "instrument", "class"]),
      industry: findHeader(["industry", "sector", "business"]),
      description: findHeader(["description", "notes"]),
      interestRate: findHeader(["interest", "rate", "coupon"]),
      spread: findHeader(["spread"]),
      maturity: findHeader(["maturity date", "maturity", "expiration", "due date", "due"]),
      par: findHeader(["par", "principal", "face"]),
      cost: findHeader(["cost", "amortized"]),
      fairValue: findHeader(["fair value", "fairvalue", "fair", "market"]),
    };
    
    if (debugMode) {
      console.log("Column indices:", colIndices);
    }
    
    // Must have at least company and fair value columns
    if (colIndices.company === -1 || colIndices.fairValue === -1) {
      if (debugMode) {
        console.log(`⊗ Table ${tableIndex}: Missing required columns (company: ${colIndices.company}, fairValue: ${colIndices.fairValue})`);
      }
      continue;
    }
    
    if (debugMode) {
      console.log(`✓ Table ${tableIndex}: Valid structure, attempting to parse rows...`);
    }
    
    // Parse data rows
    const rows = (table as Element).querySelectorAll("tr");
    
    // Find the index of the header row
    let headerRowIndex = 0;
    for (let idx = 0; idx < rows.length; idx++) {
      if (rows[idx] === headerRow) {
        headerRowIndex = idx;
        break;
      }
    }
    
    // Cap the number of rows we process per table to prevent blowup
    const rowsToProcess = Math.min(rows.length, headerRowIndex + maxRowsPerTable + 1);
    
    for (let i = headerRowIndex + 1; i < rowsToProcess; i++) {
      const row = rows[i] as Element;
      const cellNodes = Array.from(row.querySelectorAll("td"));
      const cells = cellNodes.map(c => c as Element);
      
      if (cells.length === 0) continue;
      
      // Helper to get cell at a given column position (accounting for colspan)
      const getCellAtPosition = (pos: number): Element | null => {
        let currentPos = 0;
        for (const cell of cells) {
          const colspan = parseInt(cell.getAttribute("colspan") || "1", 10);
          if (currentPos <= pos && pos < currentPos + colspan) {
            return cell;
          }
          currentPos += colspan;
        }
        return null;
      };
      
      const companyCell = getCellAtPosition(colIndices.company);
      const companyName = companyCell?.textContent?.trim() || "";
      if (!companyName || companyName.length < 2) continue;
      
      // Parse numeric values first for validation
      const fairValueCell = getCellAtPosition(colIndices.fairValue);
      const fairValue = parseNumeric(fairValueCell?.textContent?.trim());
      const costCell = colIndices.cost >= 0 ? getCellAtPosition(colIndices.cost) : null;
      const cost = parseNumeric(costCell?.textContent?.trim());
      
      // Use the new validation function
      const validation = isRealHolding(companyName, fairValue, cost);
      
      if (!validation.valid) {
        // Log first 5 rejected for debugging
        if (debugRejected.length < 5) {
          debugRejected.push({ name: companyName.substring(0, 50), reason: validation.reason });
        }
        continue;
      }
      
      const interestRateCell = colIndices.interestRate >= 0 ? getCellAtPosition(colIndices.interestRate) : null;
      const interestRateText = interestRateCell?.textContent?.trim() || "";
      
      const { rate, reference } = extractInterestRate(interestRateText);
      
      // Get spread column value for reference_rate (overrides extracted reference if present)
      const spreadCell = colIndices.spread >= 0 ? getCellAtPosition(colIndices.spread) : null;
      const spreadText = spreadCell?.textContent?.trim() || "";
      // Use spread column if available, otherwise fall back to extracted reference from interest rate
      const referenceRate = spreadText || reference;
      
      const investmentTypeCell = colIndices.investmentType >= 0 ? getCellAtPosition(colIndices.investmentType) : null;
      const industryCell = colIndices.industry >= 0 ? getCellAtPosition(colIndices.industry) : null;
      const descriptionCell = colIndices.description >= 0 ? getCellAtPosition(colIndices.description) : null;
      const maturityCell = colIndices.maturity >= 0 ? getCellAtPosition(colIndices.maturity) : null;
      const parCell = colIndices.par >= 0 ? getCellAtPosition(colIndices.par) : null;
      
      const holding: Holding = {
        company_name: companyName,
        investment_type: investmentTypeCell?.textContent?.trim() || null,
        industry: industryCell?.textContent?.trim() || null,
        description: descriptionCell?.textContent?.trim() || null,
        interest_rate: rate,
        reference_rate: referenceRate || null,
        maturity_date: parseDate(maturityCell?.textContent?.trim()),
        par_amount: parseNumeric(parCell?.textContent?.trim()),
        cost,
        fair_value: fairValue,
      };
      
      holdings.push(holding);
      
      // Log first 5 accepted for debugging
      if (debugAccepted.length < 5) {
        debugAccepted.push(companyName.substring(0, 50));
      }
      
      // Cap total holdings to prevent excessive memory usage
      if (holdings.length >= maxHoldings) {
        console.log(`Reached max holdings cap (${maxHoldings}), stopping parse`);
        break;
      }
    }
    
    // If we found holdings in this table, log debug info and stop searching
    if (holdings.length > 0) {
      console.log(`\n=== Parsing Results ===`);
      console.log(`✅ Accepted ${holdings.length} holdings from table ${tableIndex}`);
      console.log(`First 5 accepted:`, debugAccepted);
      console.log(`First 5 rejected:`, debugRejected);
      console.log(`========================\n`);
      return holdings;
    }
  }
  
  // Log debug info even if no holdings found
  if (debugRejected.length > 0) {
    console.log(`\n=== Parsing Results (no holdings found) ===`);
    console.log(`First 5 rejected:`, debugRejected);
    console.log(`============================================\n`);
  }
  
  return holdings;
}

// Parse HTML Schedule of Investments table from snippets
function parseHtmlScheduleOfInvestments(html: string, debugMode = false): { holdings: Holding[]; scaleResult: ScaleDetectionResult } {
  const maxRowsPerTable = 500;
  const maxHoldings = 1000;
  
  // Detect scale from the HTML (look for "in thousands" or "in millions")
  const scaleResult = detectScale(html);
  
  try {
    // First try: Extract only relevant HTML snippets
    const snippets = extractCandidateTableHtml(html);
    
    for (const snippet of snippets) {
      const doc = new DOMParser().parseFromString(snippet, "text/html");
      if (!doc) continue;
      
      const tables = Array.from(doc.querySelectorAll("table")) as Element[];
      const holdings = parseTables(tables, maxRowsPerTable, maxHoldings, debugMode);
      
      if (holdings.length > 0) {
        console.log(`Found ${holdings.length} holdings in snippet`);
        return { holdings, scaleResult };
      }
    }
    
    // No full-document fallback to avoid WORKER_LIMIT on large filings
    console.log("No holdings found in snippets; returning empty result without full-document fallback");
  }
  catch (error) {
    console.error("Error parsing HTML:", error);
  }
  
  return { holdings: [], scaleResult };
}

// Main serve function
serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { filingId, mode } = await req.json();

    if (!filingId) {
      throw new Error("filingId is required");
    }

    const supabaseClient = createClient(
      Deno.env.get("SUPABASE_URL") ?? "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
    );

    // Fetch filing details
    const { data: filing, error: filingError } = await supabaseClient
      .from("filings")
      .select(`
        *,
        bdcs (cik, bdc_name)
      `)
      .eq("id", filingId)
      .single();

    if (filingError) {
      throw new Error(`Filing not found: ${filingError.message}`);
    }

    const cik = filing.bdcs.cik;
    const accessionNo = filing.sec_accession_no;
    const bdcName = filing.bdcs.bdc_name;

    console.log(`Extracting holdings for filing ${accessionNo} (CIK: ${cik}, BDC: ${bdcName})`);
    console.log(`Using local parser`);

    // Build filing document URL
    const accessionNoNoDashes = accessionNo.replace(/-/g, "");
    const paddedCik = cik.replace(/^0+/, ""); // Remove leading zeros for URL
    
    // Try to fetch the primary filing document (usually the .htm file)
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accessionNoNoDashes}/index.json`;
    console.log(`Index URL: ${indexUrl}`);
    
    let holdings: Holding[] = [];
    let scaleResult: ScaleDetectionResult = { scale: 0.001, detected: 'thousands', confidence: 'low' };
    const warnings: string[] = [];
    let docUrl = "";
    
    // Enable debug mode for specific test filing
    const debugMode = accessionNo === "0001104659-25-108820";
    if (debugMode) {
      console.log("\n🔍 DEBUG MODE ENABLED for test filing 0001104659-25-108820\n");
    }
    
    try {
      // Fetch the filing index to find the primary document
      const indexJson = await fetchSecFile(indexUrl);
      const index = JSON.parse(indexJson);
      
      // Log all available documents for debugging
      if (debugMode && index.directory?.item) {
        console.log("\n=== Available documents in index.json ===");
        index.directory.item.forEach((item: any, idx: number) => {
          console.log(`${idx + 1}. ${item.name} (type: ${item.type || 'N/A'})`);
        });
        console.log("=====================================\n");
      }
      
      // Find all .htm documents (not just primary)
      const htmDocs = (index.directory?.item || []).filter(
        (item: any) => item.name.endsWith(".htm") || item.name.endsWith(".html")
      );
      
      // Prioritize documents that might contain schedules
      const prioritizedDocs = [...htmDocs].sort((a: any, b: any) => {
        const aName = a.name.toLowerCase();
        const bName = b.name.toLowerCase();
        
        // Highest priority: documents with "schedule", "soi", "portfolio" in name
        const aIsSchedule = aName.includes("schedule") || aName.includes("soi") || aName.includes("portfolio");
        const bIsSchedule = bName.includes("schedule") || bName.includes("soi") || bName.includes("portfolio");
        if (aIsSchedule && !bIsSchedule) return -1;
        if (!aIsSchedule && bIsSchedule) return 1;
        
        // Next priority: primary document
        if (a.type === "primary" && b.type !== "primary") return -1;
        if (a.type !== "primary" && b.type === "primary") return 1;
        
        // Deprioritize documents with underscores (usually graphics/exhibits)
        const aHasUnderscore = aName.includes("_");
        const bHasUnderscore = bName.includes("_");
        if (!aHasUnderscore && bHasUnderscore) return -1;
        if (aHasUnderscore && !bHasUnderscore) return 1;
        
        return 0;
      });
      
      if (debugMode) {
        console.log("\n=== Document processing order ===");
        prioritizedDocs.forEach((doc: any, idx: number) => {
          console.log(`${idx + 1}. ${doc.name} (type: ${doc.type || 'N/A'})`);
        });
        console.log("=================================\n");
      }
      
      // Try parsing each document until we find holdings (limit to 15 docs to avoid timeout)
      const maxDocsToTry = Math.min(prioritizedDocs.length, 15);
      
      for (let docIdx = 0; docIdx < maxDocsToTry; docIdx++) {
        const doc = prioritizedDocs[docIdx];
        docUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accessionNoNoDashes}/${doc.name}`;
        console.log(`\n📄 Trying document ${docIdx + 1}/${maxDocsToTry}: ${doc.name}`);
        console.log(`   URL: ${docUrl}`);
        
        try {
          const html = await fetchSecFile(docUrl);
          console.log(`   Size: ${(html.length / 1024).toFixed(0)} KB`);
          
          // For very large documents (>5MB), try to extract just the SOI section first
          let textToParse = html;
          if (html.length > 5_000_000) {
            console.log(`   📦 Large document, extracting SOI sections only...`);
            // Look for Schedule of Investments sections
            const soiMatch = html.match(/schedule\s+of\s+investments[\s\S]{0,2000000}/gi);
            if (soiMatch && soiMatch.length > 0) {
              // Take first match plus surrounding context
              const firstMatch = soiMatch[0];
              textToParse = firstMatch;
              console.log(`   📦 Extracted ${(textToParse.length / 1024).toFixed(0)} KB SOI section`);
            } else {
              warnings.push(`Document ${doc.name} too large (${(html.length / 1024).toFixed(0)} KB) and no SOI section found. Skipping.`);
              console.log(`   ⚠️ No SOI section found in large document, skipping`);
              continue;
            }
          }
          
          // Hard limit: Skip if still too large after extraction
          if (textToParse.length > 5_000_000) {
            warnings.push(`Document ${doc.name} section too large to safely parse (${(textToParse.length / 1024).toFixed(0)} KB > 5MB). Skipping.`);
            console.log(`   ⚠️ Section still too large, skipping`);
            continue;
          }
          
          // If we haven't found holdings yet, enable debug mode on later documents
          const useDebug = debugMode || (docIdx >= 2 && holdings.length === 0);
          const result = parseHtmlScheduleOfInvestments(textToParse, useDebug);
          holdings = result.holdings;
          scaleResult = result.scaleResult;
          
          console.log(`   Result: ${holdings.length} holdings found`);
          
          if (holdings.length > 0) {
            console.log(`✅ Successfully extracted ${holdings.length} holdings from ${doc.name}`);
            break; // Stop trying more documents
          }
        } catch (docError) {
          console.error(`   Error parsing ${doc.name}:`, docError);
          continue; // Try next document
        }
      }
      
      if (holdings.length === 0 && prioritizedDocs.length === 0) {
        warnings.push("Could not locate any HTML documents in filing");
      }
    } catch (error) {
      console.error("Error fetching/parsing filing:", error);
      warnings.push(`Parsing error: ${error instanceof Error ? error.message : "Unknown error"}`);
    }
    
    // If we found holdings, apply scale conversion and insert them
    if (holdings.length > 0) {
      // Validate scale detection
      const scaleValidation = validateScale(holdings, scaleResult);
      if (!scaleValidation.valid) {
        console.warn(`⚠️ Scale validation warning: ${scaleValidation.warning}`);
        warnings.push(scaleValidation.warning || "Scale validation failed");
      }
      
      // Apply scale conversion - convert all values to millions
      console.log(`📊 Applying scale conversion: ${scaleResult.detected} -> millions (multiplier: ${scaleResult.scale})`);
      
      const holdingsToInsert = holdings.map((h) => ({
        filing_id: filingId,
        company_name: h.company_name,
        investment_type: h.investment_type,
        industry: h.industry,
        description: h.description,
        interest_rate: h.interest_rate,
        reference_rate: h.reference_rate,
        maturity_date: h.maturity_date,
        par_amount: toMillions(h.par_amount, scaleResult.scale),
        cost: toMillions(h.cost, scaleResult.scale),
        fair_value: toMillions(h.fair_value, scaleResult.scale),
      }));
      
      // Log sample converted values
      if (holdingsToInsert.length > 0) {
        const sample = holdingsToInsert[0];
        console.log(`📊 Sample conversion: ${holdings[0].company_name}`);
        console.log(`   Fair Value: ${holdings[0].fair_value} -> $${sample.fair_value}M`);
        console.log(`   Cost: ${holdings[0].cost} -> $${sample.cost}M`);
        console.log(`   Par: ${holdings[0].par_amount} -> $${sample.par_amount}M`);
      }

      const { error: insertError } = await supabaseClient
        .from("holdings")
        .insert(holdingsToInsert);

      if (insertError) {
        throw new Error(`Error inserting holdings: ${insertError.message}`);
      }

      // Mark filing as parsed successfully and store the detected scale
      const { error: updateError } = await supabaseClient
        .from("filings")
        .update({ 
          parsed_successfully: true,
          value_scale: scaleResult.detected
        })
        .eq("id", filingId);

      if (updateError) {
        console.error("Error updating filing status:", updateError);
      }

      console.log(`Inserted ${holdingsToInsert.length} holdings for filing ${accessionNo} (scale: ${scaleResult.detected})`);

      return new Response(
        JSON.stringify({
          filingId,
          holdingsInserted: holdingsToInsert.length,
          valueScale: scaleResult.detected,
          scaleConfidence: scaleResult.confidence,
          warnings: warnings.length > 0 ? warnings : undefined,
        }),
        {
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        }
      );
    } else {
      // No holdings found - return 200 with warnings
      warnings.push("No holdings found in filing (snippet-based parsing only)");
      
      console.log(`No holdings found. Index URL: ${indexUrl}`);
      console.log(`Doc URL: ${docUrl}`);
      
      return new Response(
        JSON.stringify({
          filingId,
          holdingsInserted: 0,
          warnings,
        }),
        {
          headers: { ...corsHeaders, "Content-Type": "application/json" },
          status: 200,
        }
      );
    }
  } catch (error) {
    console.error("Error in extract_holdings_for_filing:", error);
    const errorMessage = error instanceof Error ? error.message : "Unknown error";
    return new Response(
      JSON.stringify({ error: errorMessage }),
      {
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      }
    );
  }
});
