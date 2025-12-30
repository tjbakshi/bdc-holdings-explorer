import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { DOMParser, Element } from "https://deno.land/x/deno_dom@v0.1.38/deno-dom-wasm.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const SEC_USER_AGENT = "BDCTrackerApp/1.0 (contact@bdctracker.com)";

// Constants for resumable parsing
const CHUNK_SIZE = 500_000; // 500KB chunks for Range header fetching
const CPU_TIME_LIMIT_MS = 35; // Stop if we've used 35ms of CPU time
const SEGMENT_SIZE = 150_000; // 150KB parsing segments
const OVERLAP_SIZE = 15_000; // 15KB overlap

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
  source_pos?: number; // Approximate character position in original HTML
}

interface ScaleDetectionResult {
  scale: number; // Multiplier to convert to millions (e.g., 0.001 for thousands, 1 for millions)
  detected: 'thousands' | 'millions' | 'unknown';
  confidence: 'high' | 'medium' | 'low';
}

interface ResumableState {
  byteOffset: number;
  industryState: string | null;
  totalFileSize: number | null;
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

// Helper to fetch SEC files with retry - supports Range headers for partial fetching
async function fetchSecFile(url: string, retries = 2, rangeStart?: number, rangeEnd?: number): Promise<{ text: string; totalSize?: number }> {
  for (let i = 0; i <= retries; i++) {
    try {
      const headers: Record<string, string> = { "User-Agent": SEC_USER_AGENT };
      
      if (rangeStart !== undefined && rangeEnd !== undefined) {
        headers["Range"] = `bytes=${rangeStart}-${rangeEnd}`;
        console.log(`Fetching range: bytes=${rangeStart}-${rangeEnd} from ${url}`);
      } else {
        console.log(`Fetching: ${url}`);
      }
      
      const response = await fetch(url, { headers });

      // Handle 206 Partial Content for range requests
      if (response.status === 206 || response.ok) {
        const text = await response.text();
        
        // Parse Content-Range header if present to get total size
        const contentRange = response.headers.get("Content-Range");
        let totalSize: number | undefined;
        if (contentRange) {
          const match = contentRange.match(/bytes \d+-\d+\/(\d+)/);
          if (match) {
            totalSize = parseInt(match[1], 10);
          }
        }
        
        return { text, totalSize };
      }

      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    } catch (error) {
      if (i === retries) throw error;
      console.log(`Retry ${i + 1}/${retries} for ${url}`);
      await new Promise((resolve) => setTimeout(resolve, 1000 * (i + 1)));
    }
  }
  throw new Error("Failed to fetch after retries");
}

// Clean footnote references from company names
function cleanCompanyName(name: string | null | undefined): string {
  if (!name) return "";
  
  let cleaned = name
    // Remove parentheses with numbers at the end: (13) or (13, 14) or (1)(2)(3)
    .replace(/(\s*\(\d+(?:,\s*\d+)*\))+\s*$/g, '')
    // Remove superscript HTML tags if present
    .replace(/<sup>.*?<\/sup>/g, '')
    // Remove other footnote indicators like *, †, ‡, §, ¶, #
    .replace(/\s*[\\*†‡§¶#]+\s*$/g, '')
    // Trim any trailing whitespace
    .trim();
  
  return cleaned;
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
  
  // Try MM/YYYY format (common in SEC filings for maturity dates) - use last day of month
  const mmyyyyMatch = cleaned.match(/^(\d{1,2})\/(\d{4})$/);
  if (mmyyyyMatch) {
    const [, month, year] = mmyyyyMatch;
    const monthNum = parseInt(month);
    const yearNum = parseInt(year);
    // Use last day of the month
    const lastDay = new Date(yearNum, monthNum, 0).getDate();
    const date = new Date(yearNum, monthNum - 1, lastDay);
    if (!isNaN(date.getTime())) {
      return date.toISOString().split("T")[0];
    }
  }
  
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
  
  // For very large documents like ARCC, the Schedule of Investments can be 3-4MB
  // Strategy: Find the start of the SOI and extract a large chunk from there
  
  // Broaden keywords to catch more variations
  const keywords = [
    "consolidated schedule of investments",
    "schedule of investments (unaudited)",
    "schedule of investments (continued)",
    "schedule of investments",
    "portfolio investments",
  ];
  
  // Find ALL occurrences of SOI keywords to understand the document structure
  const allMatches: { keyword: string; position: number }[] = [];
  
  for (const keyword of keywords) {
    let searchStart = 0;
    while (true) {
      const idx = lower.indexOf(keyword, searchStart);
      if (idx === -1) break;
      allMatches.push({ keyword, position: idx });
      searchStart = idx + keyword.length;
    }
  }
  
  // Sort by position
  allMatches.sort((a, b) => a.position - b.position);
  
  console.log(`Found ${allMatches.length} SOI keyword occurrences in document`);
  
  if (allMatches.length === 0) {
    console.log("No SOI keywords found, using first 500KB");
    return [html.slice(0, 500_000)];
  }
  
  // For large documents, extract from the first SOI occurrence to the end of the SOI section
  // ARCC's SOI can be 3-4MB, so we need a much larger window
  const firstMatchPos = allMatches[0].position;
  const lastMatchPos = allMatches[allMatches.length - 1].position;
  
  // Extract from ~50KB before first match to ~200KB after last match
  // This should capture the entire SOI section
  const start = Math.max(0, firstMatchPos - 50_000);
  const end = Math.min(html.length, lastMatchPos + 500_000);
  
  // If the window is larger than 4MB, we need to be smart about it
  const maxWindowSize = 4_000_000;
  
  if (end - start <= maxWindowSize) {
    console.log(`Extracting single SOI window: ${((end - start) / 1024).toFixed(0)} KB from positions ${start} to ${end}`);
    return [html.slice(start, end)];
  }
  
  // For very large SOI sections, split into multiple overlapping chunks
  console.log(`SOI section very large (${((end - start) / 1024).toFixed(0)} KB), splitting into chunks`);
  const snippets: string[] = [];
  const chunkSize = 2_000_000; // 2MB chunks
  const overlap = 200_000; // 200KB overlap to avoid missing entries at boundaries
  
  let chunkStart = start;
  while (chunkStart < end) {
    const chunkEnd = Math.min(chunkStart + chunkSize, end);
    snippets.push(html.slice(chunkStart, chunkEnd));
    console.log(`  Chunk ${snippets.length}: ${(chunkStart / 1024).toFixed(0)}KB - ${(chunkEnd / 1024).toFixed(0)}KB`);
    chunkStart = chunkEnd - overlap;
    if (chunkEnd === end) break;
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
  // Accounting/transaction terms (not company names)
  "contributed capital", "management fees", "distributions",
  "income", "expenses", "interest expense", "dividend",
  "unrealized", "realized", "gain", "loss",
];

// ARCC's exact industry categories from their SEC filings
// These are the section headers that appear in the Schedule of Investments
const ARCC_INDUSTRY_CATEGORIES = [
  // Services sectors
  'Software and Services',
  'Consumer Services',
  'Commercial and Professional Services',
  
  // Healthcare
  'Health Care Equipment and Services',
  'Pharmaceuticals, Biotechnology and Life Sciences',
  
  // Consumer sectors
  'Consumer Discretionary Distribution and Retail',
  'Consumer Durables and Apparel',
  'Consumer Staples Distribution and Retail',
  'Household and Personal Products',
  'Food, Beverage and Tobacco',
  
  // Financial
  'Financial Services',
  'Insurance',
  'Banks',
  
  // Industrial/Business
  'Capital Goods',
  'Transportation',
  'Materials',
  
  // Technology
  'Technology Hardware and Equipment',
  'Semiconductors and Semiconductor Equipment',
  'Media and Entertainment',
  'Telecommunication Services',
  
  // Energy/Utilities
  'Energy',
  'Utilities',
  'Gas Utilities',
  
  // Real Estate
  'Real Estate Management and Development',
  'Equity Real Estate Investment Trusts (REITs)',
  
  // Other
  'Automobiles and Components',
  'Food and Staples Retailing',
  'Retailing',
];

// Industry name mappings for normalization (variant -> standard)
const INDUSTRY_NAME_MAPPINGS: Record<string, string> = {
  'software & services': 'Software and Services',
  'healthcare equipment & services': 'Health Care Equipment and Services',
  'commercial & professional services': 'Commercial and Professional Services',
  'pharma, biotech & life sciences': 'Pharmaceuticals, Biotechnology and Life Sciences',
  'technology hardware & equipment': 'Technology Hardware and Equipment',
  'media & entertainment': 'Media and Entertainment',
  'real estate': 'Real Estate Management and Development',
  'reits': 'Equity Real Estate Investment Trusts (REITs)',
  'consumer discretionary': 'Consumer Discretionary Distribution and Retail',
  'consumer staples': 'Consumer Staples Distribution and Retail',
  'food & beverage': 'Food, Beverage and Tobacco',
  'food and beverage': 'Food, Beverage and Tobacco',
  'telecom services': 'Telecommunication Services',
  'telecommunications': 'Telecommunication Services',
  'semiconductors': 'Semiconductors and Semiconductor Equipment',
};

// Normalize industry name to standard format
function normalizeIndustryName(industryText: string): string {
  const cleaned = industryText.trim();
  const lower = cleaned.toLowerCase();
  
  // Check if we have a direct mapping
  if (INDUSTRY_NAME_MAPPINGS[lower]) {
    return INDUSTRY_NAME_MAPPINGS[lower];
  }
  
  // Check if it's an exact match to a known category (case-insensitive)
  for (const category of ARCC_INDUSTRY_CATEGORIES) {
    if (lower === category.toLowerCase()) {
      return category;
    }
  }
  
  return cleaned;
}

// Check if text is an exact ARCC industry category
function isExactIndustryCategory(text: string): boolean {
  const lower = text.toLowerCase().trim();
  
  // Check exact match to known categories
  for (const category of ARCC_INDUSTRY_CATEGORIES) {
    if (lower === category.toLowerCase()) {
      return true;
    }
  }
  
  // Check mappings
  if (INDUSTRY_NAME_MAPPINGS[lower]) {
    return true;
  }
  
  return false;
}

// Check if text matches universal industry pattern
// Returns normalized industry name if it's a valid industry, null otherwise
function matchUniversalIndustryPattern(text: string): string | null {
  const lower = text.toLowerCase().trim();
  
  // Check exact match to known categories first
  for (const category of ARCC_INDUSTRY_CATEGORIES) {
    if (lower === category.toLowerCase()) {
      return category;
    }
  }
  
  // Check mappings
  if (INDUSTRY_NAME_MAPPINGS[lower]) {
    return INDUSTRY_NAME_MAPPINGS[lower];
  }
  
  // Skip if it has characteristics of a company name, not an industry
  // Industry headers typically don't have these patterns:
  const companyPatterns = [
    /\b(inc|llc|llp|corp|corporation|company|co|ltd|limited|lp|l\.p\.|holdings?|partners?|group)\b/i,
    /\bfirst lien\b/i,
    /\bsecond lien\b/i,
    /\bsenior\b/i,
    /\bsubordinated\b/i,
    /\bequity\b/i,
    /\bpreferred\b/i,
    /\bunits?\b/i,
    /\bclass [a-z]\b/i,
    /\bwarrants?\b/i,
    /\brevolv/i,
    /\bterm loan\b/i,
    /\$\d/,  // Dollar amounts
    /\d{1,2}\/\d{1,2}\/\d{2,4}/, // Dates
    /\bS\+\d/i, // SOFR spread patterns
  ];
  
  for (const pattern of companyPatterns) {
    if (pattern.test(text)) {
      return null;
    }
  }
  
  // Industry-like patterns (broad categories)
  const industryPatterns = [
    /^(software|healthcare|health care|consumer|commercial|financial|capital|technology|media|energy|utilities|insurance|banks?|transportation|materials|real estate|retail|food|pharma|telecom)/i,
    /services$/i,
    /equipment$/i,
    /products$/i,
    /retail$/i,
  ];
  
  for (const pattern of industryPatterns) {
    if (pattern.test(lower)) {
      // This looks like an industry category - return normalized name
      return normalizeIndustryName(text);
    }
  }
  
  return null;
}

// Common company suffixes that indicate we've seen a complete company name
const COMPANY_SUFFIXES = [
  "Inc", "Inc.", "LLC", "L.L.C.", "LP", "L.P.", "Corp", "Corp.", "Corporation",
  "Co", "Co.", "Company", "Ltd", "Ltd.", "Limited", "LLP", "L.L.P.",
  "Holdings", "Holding", "Partners", "Group", "Fund", "Trust",
  "Capital", "Acquisition", "Investments", "Investment"
];

// Check if text has a company suffix (to detect end of multi-line company names)
function hasCompanySuffix(name: string): boolean {
  const lowerName = name.toLowerCase();
  
  // Check for company suffix - handle both word boundaries and end-of-string
  // The word boundary \b doesn't work well after periods (e.g., "L.P." at end of string)
  const hasSuffix = COMPANY_SUFFIXES.some(suffix => {
    const escapedSuffix = suffix.replace(/\./g, "\\.");
    // Match suffix at word boundary OR at end of string (with optional trailing whitespace)
    const regex = new RegExp(`\\b${escapedSuffix}(?:\\b|\\s*$)`, "i");
    return regex.test(lowerName);
  });
  
  return hasSuffix;
}

// Parse the Schedule of Investments from HTML
// Returns holdings and the last industry seen (for carry-forward between segments)
function parseHtmlScheduleOfInvestments(
  html: string, 
  debugMode = false, 
  carryIndustry: string | null = null
): { holdings: Holding[]; scaleResult: ScaleDetectionResult; lastIndustry: string | null } {
  const holdings: Holding[] = [];
  const scaleResult = detectScale(html);
  
  // Parse HTML
  const doc = new DOMParser().parseFromString(html, "text/html");
  if (!doc) {
    console.log("Failed to parse HTML");
    return { holdings, scaleResult, lastIndustry: carryIndustry };
  }
  
  // Find all tables
  const tables = doc.querySelectorAll("table");
  console.log(`Found ${tables.length} tables in HTML`);
  
  // Track current industry context (for associating companies with industries)
  let currentIndustry: string | null = carryIndustry;
  let currentCompany: string | null = null;
  
  // Debug tracking
  const debugAccepted: string[] = [];
  const debugRejected: { name: string; reason: string }[] = [];
  const industryCompanyMap = new Map<string, Set<string>>();
  const industriesFound = new Set<string>();
  
  // Process each table
  for (let tableIdx = 0; tableIdx < tables.length; tableIdx++) {
    const table = tables[tableIdx] as Element;
    
    // Find header row
    const headerRow = findHeaderRow(table, debugMode && tableIdx === 0);
    if (!headerRow) {
      if (debugMode) {
        console.log(`⊗ Table ${tableIdx + 1}: No valid header row found`);
      }
      continue;
    }
    
    // Map header columns
    const headerCells = Array.from(headerRow.querySelectorAll("th, td"));
    const headers = headerCells.map(h => (h as Element).textContent?.toLowerCase().trim() || "");
    
    // Find column indices for fields we care about
    let companyIdx = -1;
    let investmentTypeIdx = -1;
    let industryIdx = -1;
    let interestRateIdx = -1;
    let maturityIdx = -1;
    let parIdx = -1;
    let costIdx = -1;
    let fairValueIdx = -1;
    let sharesUnitsIdx = -1;
    
    headers.forEach((h, idx) => {
      if (companyIdx === -1 && (h.includes("company") || h.includes("portfolio") || h.includes("name") || h.includes("issuer") || h.includes("borrower") || h.includes("investment"))) {
        companyIdx = idx;
      }
      if (investmentTypeIdx === -1 && (h.includes("type") || h.includes("instrument"))) {
        investmentTypeIdx = idx;
      }
      if (industryIdx === -1 && h.includes("industry")) {
        industryIdx = idx;
      }
      if (interestRateIdx === -1 && (h.includes("rate") || h.includes("interest") || h.includes("coupon"))) {
        interestRateIdx = idx;
      }
      if (maturityIdx === -1 && (h.includes("maturity") || h.includes("matur"))) {
        maturityIdx = idx;
      }
      if (parIdx === -1 && (h.includes("par") || h.includes("principal") || h.includes("commitment"))) {
        parIdx = idx;
      }
      if (costIdx === -1 && (h.includes("cost") || h.includes("amortized"))) {
        costIdx = idx;
      }
      if (fairValueIdx === -1 && (h.includes("fair value") || h.includes("fair") || h.includes("market value") || h.includes("value"))) {
        fairValueIdx = idx;
      }
      if (sharesUnitsIdx === -1 && (h.includes("shares") || h.includes("units") || h.includes("quantity") || h.includes("number of"))) {
        sharesUnitsIdx = idx;
      }
    });
    
    if (debugMode) {
      console.log(`Table ${tableIdx + 1} column mapping:`, {
        company: companyIdx,
        investmentType: investmentTypeIdx,
        industry: industryIdx,
        interestRate: interestRateIdx,
        maturity: maturityIdx,
        par: parIdx,
        cost: costIdx,
        fairValue: fairValueIdx,
        sharesUnits: sharesUnitsIdx,
      });
    }
    
    // Process data rows
    const rows = Array.from(table.querySelectorAll("tr"));
    let headerRowIdx = rows.findIndex(r => r === headerRow);
    if (headerRowIdx === -1) headerRowIdx = 0;
    
    // Track row context for multi-line entries
    let pendingHolding: Partial<Holding> | null = null;
    
    for (let rowIdx = headerRowIdx + 1; rowIdx < rows.length; rowIdx++) {
      const row = rows[rowIdx] as Element;
      const cells = Array.from(row.querySelectorAll("td, th"));
      
      if (cells.length === 0) continue;
      
      // Get cell at specific positions, accounting for colspan
      const getCellPosition = (targetIdx: number): number => {
        let position = 0;
        for (let i = 0; i < cells.length; i++) {
          const colspan = parseInt((cells[i] as Element).getAttribute("colspan") || "1", 10);
          if (position <= targetIdx && targetIdx < position + colspan) {
            return i;
          }
          position += colspan;
        }
        return -1;
      };
      
      const getCellText = (idx: number): string => {
        if (idx < 0) return "";
        const cellIdx = getCellPosition(cells, idx);
        if (cellIdx < 0 || cellIdx >= cells.length) return "";
        return (cells[cellIdx] as Element).textContent?.trim().replace(/\s+/g, ' ') || "";
      };
      
      // Find value cells (numeric cells that likely contain amounts)
      const findValueCell = (preferredIdx: number, cellType: 'fairValue' | 'cost' | 'par'): number | null => {
        // First try preferred index
        if (preferredIdx >= 0) {
          const cellIdx = getCellPosition(cells, preferredIdx);
          if (cellIdx >= 0 && cellIdx < cells.length) {
            const text = getCellText(preferredIdx);
            if (text && /[\d,]+\.?\d*/.test(text)) {
              return parseNumeric(text);
            }
          }
        }
        
        // For fair value and cost, search from the end but skip sharesUnitsIdx
        if (cellType === 'fairValue' || cellType === 'cost') {
          // Get the actual cell index for shares/units to exclude it
          const sharesUnitsCellIdx = sharesUnitsIdx >= 0 ? getCellPosition(cells, sharesUnitsIdx) : -1;
          
          for (let i = cells.length - 1; i >= 0; i--) {
            // Skip the shares/units column
            if (i === sharesUnitsCellIdx) continue;
            
            const text = (cells[i] as Element).textContent?.trim() || "";
            if (/[\d,]+\.?\d*/.test(text)) {
              const val = parseNumeric(text);
              if (val !== null) {
                // For fair value, make sure we're getting the LAST numeric cell (not shares/units)
                // Skip if this is the cell at sharesUnitsIdx position
                const cellLogicalPos = cells.slice(0, i).reduce((sum, c) => 
                  sum + parseInt((c as Element).getAttribute("colspan") || "1", 10), 0);
                if (sharesUnitsIdx >= 0 && Math.abs(cellLogicalPos - sharesUnitsIdx) <= 1) continue;
                
                return val;
              }
            }
          }
        }
        
        return null;
      };
      
      // Get all cell text for pattern matching
      const allCellText = cells.map(c => (c as Element).textContent?.trim() || "");
      const firstCellText = allCellText[0] || "";
      const rowText = allCellText.join(" ").toLowerCase();
      
      // Count non-empty cells (excluding pure whitespace)
      const nonEmptyCells = allCellText.filter(t => t.replace(/\s/g, '').length > 0).length;
      
      // Check if this is a "universal" industry row (single cell spanning most of the table)
      // These appear as section headers between different industry groups
      const isUniversalIndustry = cells.length === 1 || 
        (nonEmptyCells === 1 && firstCellText.length > 3 && !firstCellText.includes("$"));
      
      // Check for exact industry match
      const exactIndustryMatch = isExactIndustryCategory(firstCellText);
      
      // Check for universal industry pattern match
      const universalIndustryName = matchUniversalIndustryPattern(firstCellText);
      
      // Debug row info (sample rows only to avoid log spam)
      if (debugMode && rowIdx - headerRowIdx <= 25 && rowIdx - headerRowIdx > 0) {
        console.log(`Row ${rowIdx - headerRowIdx}: total_cells=${cells.length}, non_empty=${nonEmptyCells}, universal_industry=${isUniversalIndustry}, exact_match=${exactIndustryMatch}, first="${firstCellText.slice(0, 60)}"`);
      }
      
      // Industry row detection
      if ((isUniversalIndustry && universalIndustryName) || exactIndustryMatch) {
        const industryName = exactIndustryMatch ? normalizeIndustryName(firstCellText) : universalIndustryName!;
        currentIndustry = industryName;
        industriesFound.add(industryName);
        if (debugMode) {
          console.log(`📂 Industry section (${exactIndustryMatch ? 'exact' : 'universal'}): ${industryName}`);
        }
        continue;
      }
      
      // Skip rows that are clearly not holdings
      const lowerFirstCell = firstCellText.toLowerCase();
      
      // Skip empty rows
      if (!firstCellText || nonEmptyCells < 2) continue;
      
      // Skip skip-keyword rows
      if (SKIP_KEYWORDS.some(kw => lowerFirstCell.startsWith(kw))) {
        if (debugMode && rowIdx - headerRowIdx <= 10) {
          debugRejected.push({ name: firstCellText.slice(0, 40), reason: "Skip keyword" });
        }
        continue;
      }
      
      // Try to extract holding data
      let companyName = companyIdx >= 0 ? getCellText(companyIdx) : firstCellText;
      companyName = cleanCompanyName(companyName);
      
      if (!companyName || companyName.length < 3) continue;
      
      // Check if this looks like a company name vs an investment type continuation
      const looksLikeCompany = hasCompanySuffix(companyName) || 
        /^[A-Z][a-zA-Z\s,\.&\-']+/.test(companyName);
      
      // If the row has a new company, update current company context
      if (looksLikeCompany) {
        currentCompany = companyName;
      } else if (currentCompany) {
        // This might be a continuation row - use current company
        companyName = currentCompany;
      }
      
      // Extract investment type
      let investmentType = investmentTypeIdx >= 0 ? getCellText(investmentTypeIdx) : null;
      
      // If no dedicated investment type column, look for type keywords in other cells
      if (!investmentType) {
        for (const cellText of allCellText.slice(1)) { // Skip first cell (company name)
          const lower = cellText.toLowerCase();
          if (lower.includes("first lien") || lower.includes("second lien") || 
              lower.includes("senior") || lower.includes("subordinated") ||
              lower.includes("equity") || lower.includes("preferred") ||
              lower.includes("class ") || lower.includes("units") ||
              lower.includes("warrant") || lower.includes("membership") ||
              lower.includes("limited partnership")) {
            investmentType = cellText;
            break;
          }
        }
      }
      
      // Get fair value - this is required
      const fairValue = findValueCell(fairValueIdx, 'fairValue');
      
      // Skip if no fair value found
      if (fairValue === null) {
        // This might be a subtotal row or header row
        if (debugMode && rowIdx - headerRowIdx <= 30) {
          debugRejected.push({ name: `${companyName.slice(0, 40)} (FV=null)`, reason: "No fair value" });
        }
        continue;
      }
      
      // Skip subtotal rows (company name with fair value but no investment type)
      // These are lines like "Company ABC    $100" without investment details
      if (!investmentType && fairValue > 0) {
        // Check if this is a subtotal by seeing if the next few rows are also this company
        // For now, be more lenient - only reject if it's clearly a subtotal pattern
        if (rowText.includes("subtotal") || rowText.includes("total")) {
          debugRejected.push({ name: `${companyName.slice(0, 40)} (FV=$${fairValue})`, reason: "Subtotal row" });
          continue;
        }
        // This is a valid case for equity positions that don't have "type" specified
        // Skip the row but continue (could be company subtotal row in ARCC format)
        debugRejected.push({ name: `${companyName.slice(0, 40)} (FV=$${fairValue})`, reason: "Subtotal row (no investment type)" });
        continue;
      }
      
      // Get optional fields
      const cost = findValueCell(costIdx, 'cost');
      const par = findValueCell(parIdx, 'par');
      const maturityText = maturityIdx >= 0 ? getCellText(maturityIdx) : null;
      const maturityDate = parseDate(maturityText);
      
      // Get interest rate
      let interestRateText = interestRateIdx >= 0 ? getCellText(interestRateIdx) : null;
      if (!interestRateText) {
        // Look for rate patterns in cells
        for (const cellText of allCellText) {
          if (/\d+\.?\d*\s*%/.test(cellText) || /s\+\d/i.test(cellText) || /sofr|libor|prime/i.test(cellText)) {
            interestRateText = cellText;
            break;
          }
        }
      }
      const { rate: interestRate, reference: referenceRate } = extractInterestRate(interestRateText || "");
      
      // Create holding
      const holding: Holding = {
        company_name: companyName,
        investment_type: investmentType,
        industry: currentIndustry,
        interest_rate: interestRate,
        reference_rate: referenceRate,
        maturity_date: maturityDate,
        par_amount: par,
        cost: cost,
        fair_value: fairValue,
      };
      
      holdings.push(holding);
      
      // Track for debug output
      debugAccepted.push(`${companyName.slice(0, 40)} [${(investmentType || '').slice(0, 20)}] FV=$${fairValue}`);
      
      // Track industry mapping
      if (currentIndustry) {
        if (!industryCompanyMap.has(currentIndustry)) {
          industryCompanyMap.set(currentIndustry, new Set());
        }
        industryCompanyMap.get(currentIndustry)!.add(companyName);
      }
    }
  }
  
  // Debug output summary
  if (debugMode || holdings.length > 0) {
    console.log(`\n=== Parsing Results ===`);
    console.log(`✅ Accepted ${holdings.length} investment records from ${new Set(holdings.map(h => h.company_name)).size} unique companies`);
    console.log(`📂 Industries found (${industriesFound.size}): ${Array.from(industriesFound).join(', ')}`);
    
    // Warn if expected industries are missing
    const expectedIndustries = ['Health Care Equipment and Services', 'Financial Services', 'Consumer Services', 'Capital Goods'];
    const missingIndustries = expectedIndustries.filter(i => !industriesFound.has(i));
    if (missingIndustries.length > 0 && holdings.length > 0) {
      console.log(`⚠️ Some expected industries not found: ${missingIndustries.join(', ')}`);
    }
    
    console.log(`📊 Companies by industry:`);
    industryCompanyMap.forEach((companies, industry) => {
      const companyList = Array.from(companies).slice(0, 3).join(', ');
      const more = companies.size > 3 ? '...' : '';
      console.log(`   ${industry}: ${companies.size} companies (${companyList}${more})`);
    });
    
    if (debugMode) {
      console.log(`First 10 accepted:`, JSON.stringify(debugAccepted.slice(0, 10), null, 2));
      console.log(`First 10 rejected:`, JSON.stringify(debugRejected.slice(0, 10), null, 2));
    }
    console.log(`========================\n`);
  }
  
  return { holdings, scaleResult, lastIndustry: currentIndustry };
}

serve(async (req) => {
  // Handle CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  const startTime = performance.now();

  try {
    const supabaseClient = createClient(
      Deno.env.get("SUPABASE_URL") ?? "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
    );

    const { filingId, debugMode = false, resumeFromOffset } = await req.json();

    if (!filingId) {
      return new Response(
        JSON.stringify({ error: "filingId is required" }),
        {
          status: 400,
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        }
      );
    }

    // Fetch filing details including resumable state
    const { data: filing, error: filingError } = await supabaseClient
      .from("filings")
      .select("*, bdcs!inner(cik)")
      .eq("id", filingId)
      .maybeSingle();

    if (filingError || !filing) {
      throw new Error(`Filing not found: ${filingId}`);
    }

    const accessionNo = filing.sec_accession_no;
    const cik = filing.bdcs.cik.padStart(10, "0");
    const warnings: string[] = [];

    // Get resumable state from database or request
    const currentOffset = resumeFromOffset ?? filing.current_byte_offset ?? 0;
    const currentIndustry = filing.current_industry_state ?? null;
    const totalFileSize = filing.total_file_size ?? null;

    console.log(`Processing filing ${accessionNo}, offset=${currentOffset}, industry=${currentIndustry || 'none'}`);

    // Construct URLs
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${accessionNo.replace(/-/g, "")}/index.json`;
    
    if (debugMode) {
      console.log(`\n🔍 DEBUG MODE ENABLED for filing ${accessionNo}\n`);
    }

    console.log(`Index URL: ${indexUrl}`);

    let holdings: Holding[] = [];
    let scaleResult: ScaleDetectionResult = { scale: 0.001, detected: 'thousands', confidence: 'low' };

    // Use local parser
    console.log("Using local parser");
    
    try {
      // Fetch filing index
      const { text: indexJson } = await fetchSecFile(indexUrl);
      const indexData = JSON.parse(indexJson);
      
      // Find all HTML/HTM documents
      const documents = indexData.directory?.item || [];
      const htmlDocs = documents.filter((doc: { name: string }) => 
        doc.name.toLowerCase().endsWith('.htm') || 
        doc.name.toLowerCase().endsWith('.html')
      );
      
      console.log(`\n📁 Found ${documents.length} documents. Primary: ${indexData.directory?.['primary-doc'] || 'none'}`);
      console.log(`   Total HTM docs: ${htmlDocs.length}`);
      
      // Prioritize documents that are likely to contain the Schedule of Investments
      const prioritizedDocs = [...htmlDocs].sort((a: { name: string }, b: { name: string }) => {
        const aName = a.name.toLowerCase();
        const bName = b.name.toLowerCase();
        
        // Deprioritize exhibit files
        const aIsExhibit = aName.includes('exhibit') || aName.includes('ex-') || aName.match(/ex\d/);
        const bIsExhibit = bName.includes('exhibit') || bName.includes('ex-') || bName.match(/ex\d/);
        if (aIsExhibit && !bIsExhibit) return 1;
        if (!aIsExhibit && bIsExhibit) return -1;
        
        // Prioritize main document (usually matches the filing date pattern or accession number)
        const aIsMain = aName.includes(accessionNo.toLowerCase()) || aName.match(/\d{8}\.htm/);
        const bIsMain = bName.includes(accessionNo.toLowerCase()) || bName.match(/\d{8}\.htm/);
        if (aIsMain && !bIsMain) return -1;
        if (!aIsMain && bIsMain) return 1;
        
        return 0;
      });
      
      console.log(`   Processing order: ${prioritizedDocs.slice(0, 5).map((d: { name: string }) => d.name).join(', ')}...`);
      
      // Try each document until we find holdings
      const maxDocsToTry = Math.min(15, prioritizedDocs.length);
      for (let docIdx = 0; docIdx < maxDocsToTry && holdings.length === 0; docIdx++) {
        const doc = prioritizedDocs[docIdx];
        const docUrl = `https://www.sec.gov/Archives/edgar/data/${cik}/${accessionNo.replace(/-/g, "")}/${doc.name}`;
        
        console.log(`\n📄 Trying document ${docIdx + 1}/${maxDocsToTry}: ${doc.name}`);
        console.log(`   URL: ${docUrl}`);
        
        try {
          const { text: html, totalSize } = await fetchSecFile(docUrl);
          const docSizeKB = html.length / 1024;
          console.log(`   Size: ${docSizeKB.toFixed(0)} KB`);
          
          let textToParse = html;
          
          // For very large documents (like ARCC's 22MB filing), use RESUMABLE SEGMENTED PARSING
          if (html.length > 5_000_000) {
            console.log(`   📦 Large document (${(html.length / 1024 / 1024).toFixed(1)} MB), extracting SOI section...`);
            
            const lower = html.toLowerCase();
            
            // Find all SOI occurrences to understand document structure
            const soiKeywords = [
              "consolidated schedule of investments",
              "schedule of investments",
            ];
            
            const soiOccurrences: number[] = [];
            for (const keyword of soiKeywords) {
              let pos = 0;
              while ((pos = lower.indexOf(keyword, pos)) !== -1) {
                soiOccurrences.push(pos);
                pos += keyword.length;
              }
            }
            soiOccurrences.sort((a, b) => a - b);
            
            console.log(`   Found ${soiOccurrences.length} SOI keyword occurrences`);
            
            if (soiOccurrences.length === 0) {
              console.log(`   ⚠️ No SOI section found in large document, skipping`);
              continue;
            }
            
            // Find SOI boundaries
            const endMarkers = [
              "notes to consolidated financial statements",
              "notes to financial statements",
            ];
            
            let documentEnd = html.length;
            for (const marker of endMarkers) {
              let lastIdx = -1;
              let pos = 0;
              while ((pos = lower.indexOf(marker, pos)) !== -1) {
                lastIdx = pos;
                pos += marker.length;
              }
              if (lastIdx !== -1) {
                documentEnd = lastIdx;
                console.log(`   📍 Found end marker "${marker}" last occurrence at position ${lastIdx}`);
                break;
              }
            }
            
            // Find best SOI start (with nearby table)
            let bestStart = soiOccurrences[0];
            for (const soiPos of soiOccurrences) {
              if (soiPos < html.length * 0.05) continue;
              const nearbyHtml = lower.slice(soiPos, Math.min(soiPos + 10_000, html.length));
              if (nearbyHtml.includes('<table')) {
                bestStart = soiPos;
                console.log(`   📍 Found SOI with nearby table at position ${soiPos}`);
                break;
              }
            }
            
            // Detect prior period markers
            const priorPeriodMarkers = [
              'december 31, 2024', 'december 31,2024', 
              'december&#160;31, 2024', 'december&nbsp;31, 2024',
              'as of december 31, 2024', 'at december 31, 2024',
              '12/31/2024', '12/31/24',
              'december 31, 2023', 'december 31, 2022',
            ];
            
            let priorPeriodStart = documentEnd;
            const searchStartOffset = bestStart + 100_000;
            
            for (const marker of priorPeriodMarkers) {
              const markerIdx = lower.indexOf(marker, searchStartOffset);
              if (markerIdx !== -1 && markerIdx < priorPeriodStart) {
                const nearbyAfter = lower.slice(markerIdx, Math.min(markerIdx + 20_000, html.length));
                if (nearbyAfter.includes('<table') || nearbyAfter.includes('portfolio company') || nearbyAfter.includes('fair value')) {
                  priorPeriodStart = markerIdx;
                  console.log(`   📍 Found PRIOR PERIOD marker "${marker}" at position ${markerIdx}`);
                  break;
                }
              }
            }
            
            // Find last "Total Investments" before prior period
            let lastTotalInvestmentsIdx = -1;
            let searchPos = bestStart;
            while (true) {
              const idx = lower.indexOf('total investments', searchPos);
              if (idx === -1 || idx >= priorPeriodStart) break;
              lastTotalInvestmentsIdx = idx;
              searchPos = idx + 20;
            }
            
            let currentQuarterEnd = priorPeriodStart;
            if (lastTotalInvestmentsIdx !== -1) {
              currentQuarterEnd = Math.min(priorPeriodStart, lastTotalInvestmentsIdx + 10_000);
              console.log(`   📍 Found LAST "Total Investments" at ${lastTotalInvestmentsIdx}, setting end to ${currentQuarterEnd}`);
            }
            
            // Final SOI boundaries
            const soiStart = Math.max(0, bestStart - 10_000);
            const soiEnd = Math.min(html.length, currentQuarterEnd);
            const totalSoiSize = soiEnd - soiStart;
            
            console.log(`   📊 CURRENT QUARTER SOI section only:`);
            console.log(`      Start: ${soiStart}, End: ${soiEnd}`);
            console.log(`      Size: ${(totalSoiSize / 1024 / 1024).toFixed(1)} MB`);
            if (priorPeriodStart < documentEnd) {
              console.log(`      ⚠️ Excluded prior period starting at position ${priorPeriodStart}`);
            }
            
            // ============ RESUMABLE SEGMENTED PARSING ============
            if (totalSoiSize > 2_000_000) {
              console.log(`   📦 Using RESUMABLE segmented parsing for ${(totalSoiSize / 1024 / 1024).toFixed(1)} MB SOI section...`);
              
              // Store total file size if not already stored
              if (!filing.total_file_size) {
                await supabaseClient
                  .from("filings")
                  .update({ total_file_size: totalSoiSize })
                  .eq("id", filingId);
              }
              
              // Check if we're resuming
              const effectiveOffset = currentOffset > 0 ? currentOffset : soiStart;
              const isResume = currentOffset > 0;
              
              if (!isResume) {
                // Starting fresh - delete any existing holdings
                const { error: deleteError } = await supabaseClient
                  .from("holdings")
                  .delete()
                  .eq("filing_id", filingId);
                
                if (deleteError) {
                  console.error(`   ⚠️ Error clearing existing holdings:`, deleteError);
                }
                console.log(`   📦 Starting fresh extraction`);
              } else {
                console.log(`   📦 RESUMING extraction from offset ${currentOffset}`);
              }
              
              // Get existing holdings for deduplication
              const { data: existingHoldings } = await supabaseClient
                .from("holdings")
                .select("company_name, investment_type, fair_value, row_number")
                .eq("filing_id", filingId)
                .order("row_number", { ascending: false })
                .limit(1000);
              
              const seenHoldingKeys = new Set<string>();
              let nextRowNumber = 1;
              
              if (existingHoldings && existingHoldings.length > 0) {
                for (const h of existingHoldings) {
                  seenHoldingKeys.add(`${h.company_name}|${h.investment_type || ''}|${h.fair_value || ''}`);
                }
                nextRowNumber = (existingHoldings[0]?.row_number || 0) + 1;
                console.log(`   📦 ${existingHoldings.length} existing holdings, next row: ${nextRowNumber}`);
              }
              
              // Detect scale
              const segmentScaleResult = detectScale(html.slice(soiStart, Math.min(soiStart + 100_000, soiEnd)));
              console.log(`   📊 Scale detected: ${segmentScaleResult.detected}`);
              const scale = segmentScaleResult?.scale || 1;
              
              let currentPosition = effectiveOffset;
              let carryIndustry: string | null = currentIndustry;
              let totalInserted = 0;
              let segmentCount = 0;
              let cpuTimeUsed = 0;
              
              // Process segments until CPU limit or completion
              while (currentPosition < soiEnd) {
                // Check CPU time
                cpuTimeUsed = performance.now() - startTime;
                if (cpuTimeUsed > CPU_TIME_LIMIT_MS) {
                  console.log(`   ⏱️ CPU time limit reached (${cpuTimeUsed.toFixed(1)}ms), saving state...`);
                  
                  // Save current state to database
                  await supabaseClient
                    .from("filings")
                    .update({ 
                      current_byte_offset: currentPosition,
                      current_industry_state: carryIndustry,
                      total_file_size: totalSoiSize,
                    })
                    .eq("id", filingId);
                  
                  const percentComplete = ((currentPosition - soiStart) / totalSoiSize * 100).toFixed(1);
                  
                  return new Response(
                    JSON.stringify({
                      status: "PARTIAL",
                      filingId,
                      next_offset: currentPosition,
                      percentage_complete: parseFloat(percentComplete),
                      total_file_size: totalSoiSize,
                      holdings_inserted_this_run: totalInserted,
                      current_industry: carryIndustry,
                      message: `Processed ${percentComplete}% - will resume from offset ${currentPosition}`,
                    }),
                    { headers: { ...corsHeaders, "Content-Type": "application/json" } }
                  );
                }
                
                segmentCount++;
                const segmentEnd = Math.min(currentPosition + SEGMENT_SIZE, soiEnd);
                
                if (segmentCount % 20 === 1) {
                  console.log(`   📦 Segment ${segmentCount}, pos ${(currentPosition / 1024 / 1024).toFixed(1)}MB, inserted ${totalInserted}, industry=${carryIndustry || 'none'}`);
                }
                
                try {
                  const segment = html.slice(currentPosition, segmentEnd);
                  
                  // Skip segments without table content
                  if (!segment.toLowerCase().includes('<tr')) {
                    const nextPosition = segmentEnd - OVERLAP_SIZE;
                    if (nextPosition <= currentPosition) break;
                    currentPosition = nextPosition;
                    continue;
                  }
                  
                  const segmentResult = parseHtmlScheduleOfInvestments(segment, false, carryIndustry);
                  const segmentHoldings = segmentResult.holdings;
                  carryIndustry = segmentResult.lastIndustry;
                  
                  if (segmentHoldings.length > 0) {
                    const newHoldings: Holding[] = [];
                    
                    for (const h of segmentHoldings) {
                      const key = `${h.company_name}|${h.investment_type || ''}|${h.fair_value || ''}`;
                      if (!seenHoldingKeys.has(key)) {
                        seenHoldingKeys.add(key);
                        newHoldings.push(h);
                      }
                    }
                    
                    if (newHoldings.length > 0) {
                      const holdingsToInsert = newHoldings.map((h, idx) => ({
                        filing_id: filingId,
                        company_name: h.company_name,
                        investment_type: h.investment_type,
                        industry: h.industry,
                        description: h.description,
                        interest_rate: h.interest_rate,
                        reference_rate: h.reference_rate,
                        maturity_date: h.maturity_date,
                        par_amount: h.par_amount != null ? Math.round((h.par_amount * scale) * 10) / 10 : null,
                        cost: h.cost != null ? Math.round((h.cost * scale) * 10) / 10 : null,
                        fair_value: h.fair_value != null ? Math.round((h.fair_value * scale) * 10) / 10 : null,
                        row_number: nextRowNumber + idx,
                        source_pos: currentPosition + idx,
                      }));
                      
                      const { error: insertError } = await supabaseClient
                        .from("holdings")
                        .insert(holdingsToInsert);
                      
                      if (!insertError) {
                        totalInserted += holdingsToInsert.length;
                        nextRowNumber += holdingsToInsert.length;
                      }
                    }
                  }
                } catch (segmentError) {
                  // Silently continue on segment errors
                }
                
                const nextPosition = segmentEnd - OVERLAP_SIZE;
                if (nextPosition <= currentPosition) break;
                currentPosition = nextPosition;
              }
              
              // Completed all segments
              console.log(`   ✅ Completed all segments! Total inserted: ${totalInserted}`);
              
              // Get final count
              const { count: finalCount } = await supabaseClient
                .from("holdings")
                .select("*", { count: "exact", head: true })
                .eq("filing_id", filingId);
              
              const { data: totalValueData } = await supabaseClient
                .from("holdings")
                .select("fair_value")
                .eq("filing_id", filingId);
              
              const finalTotalValue = totalValueData?.reduce((sum, h) => sum + (h.fair_value || 0), 0) || 0;
              
              // Reset state and mark as complete
              await supabaseClient
                .from("filings")
                .update({ 
                  parsed_successfully: (finalCount || 0) > 100,
                  value_scale: segmentScaleResult?.detected || 'unknown',
                  current_byte_offset: 0,
                  current_industry_state: null,
                })
                .eq("id", filingId);
              
              return new Response(
                JSON.stringify({
                  status: "COMPLETE",
                  filingId,
                  holdingsInserted: totalInserted,
                  totalHoldings: finalCount,
                  totalValue: finalTotalValue,
                  valueScale: segmentScaleResult?.detected || 'unknown',
                  percentage_complete: 100,
                }),
                { headers: { ...corsHeaders, "Content-Type": "application/json" } }
              );
            }
            
            // For smaller SOI sections, use standard DOM parsing
            textToParse = html.slice(soiStart, soiEnd);
            console.log(`   📦 Extracted SOI section: ${(textToParse.length / 1024 / 1024).toFixed(1)} MB`);
          }
          
          // Standard processing path (for smaller documents)
          if (textToParse && holdings.length === 0) {
            const useDebug = debugMode || (docIdx >= 2 && holdings.length === 0);
            const result = parseHtmlScheduleOfInvestments(textToParse, useDebug);
            holdings = result.holdings;
            scaleResult = result.scaleResult;
            
            console.log(`   Result: ${holdings.length} holdings found`);
            
            if (holdings.length > 0) {
              console.log(`✅ Successfully extracted ${holdings.length} holdings from ${doc.name}`);
              break;
            }
          }
        } catch (docError) {
          console.error(`   Error parsing ${doc.name}:`, docError);
          continue;
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
      
      const holdingsToInsert = holdings.map((h, idx) => ({
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
        row_number: idx + 1,
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
          value_scale: scaleResult.detected,
          current_byte_offset: 0,
          current_industry_state: null,
        })
        .eq("id", filingId);

      if (updateError) {
        console.error("Error updating filing status:", updateError);
      }

      console.log(`Inserted ${holdingsToInsert.length} holdings for filing ${accessionNo} (scale: ${scaleResult.detected})`);

      return new Response(
        JSON.stringify({
          status: "COMPLETE",
          filingId,
          holdingsInserted: holdingsToInsert.length,
          valueScale: scaleResult.detected,
          scaleConfidence: scaleResult.confidence,
          percentage_complete: 100,
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
      
      return new Response(
        JSON.stringify({
          status: "COMPLETE",
          filingId,
          holdingsInserted: 0,
          percentage_complete: 100,
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
      JSON.stringify({ status: "ERROR", error: errorMessage }),
      {
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      }
    );
  }
});
