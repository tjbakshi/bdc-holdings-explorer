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

// Clean footnote references from company names
function cleanCompanyName(name: string | null | undefined): string {
  if (!name) return "";
  
  let cleaned = name
    // Remove parentheses with numbers at the end: (13) or (13, 14) or (1)(2)(3)
    .replace(/(\s*\(\d+(?:,\s*\d+)*\))+\s*$/g, '')
    // Remove superscript HTML tags if present
    .replace(/<sup>.*?<\/sup>/g, '')
    // Remove other footnote indicators like *, †, ‡, §, ¶, #
    .replace(/\s*[\*†‡§¶#]+\s*$/g, '')
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
  return ARCC_INDUSTRY_CATEGORIES.some(cat => cat.toLowerCase() === lower) ||
         Object.keys(INDUSTRY_NAME_MAPPINGS).includes(lower);
}

// Investment type labels that indicate this is a type description, not a company
const INVESTMENT_TYPE_LABELS = [
  "first lien", "second lien", "senior secured", "subordinated",
  "mezzanine", "equity", "preferred", "common stock", "warrants",
  "senior subordinated loans", "other equity", "preferred equity",
  "subordinated certificates", "unitranche",
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
  /^[A-Za-z]+ \d{1,2}, \d{4}/, // Date format like "September 30, 2025"
];

// Company suffixes that strongly indicate a real holding
const COMPANY_SUFFIXES = [
  "inc.", "inc", "llc", "l.l.c.", "lp", "l.p.", "corp.", "corp",
  "corporation", "company", "co.", "ltd.", "ltd", "limited",
];

// Check if text is an industry section header
function isIndustrySectionHeader(text: string): boolean {
  const trimmed = text.trim();
  if (!trimmed || trimmed.length < 3) return false;
  
  // First check for exact ARCC industry category match
  if (isExactIndustryCategory(trimmed)) {
    return true;
  }
  
  // Industry headers don't have company entity suffixes
  const hasCompanyEntity = /\b(LLC|Inc\.|Inc|Corp\.|Corp|L\.P\.|LP|Ltd\.|Ltd|Limited|Holdings|Co\.|Company|Enterprises|Partners|Group)\b/i.test(trimmed);
  if (hasCompanyEntity) return false;
  
  // Industry headers typically don't have $ amounts or percentages
  const hasNumericValues = /\$[\d,]+|\d+\.\d+%|\d{1,3}(?:,\d{3})+/.test(trimmed);
  if (hasNumericValues) return false;
  
  // Additional pattern matching for industries not in the exact list
  const industryPatterns = [
    /^software/i, /^technology/i, /^health care/i, /^healthcare/i,
    /^financial/i, /^consumer/i, /^commercial/i, /^capital goods/i,
    /^transportation/i, /^materials/i, /^energy/i, /^utilities/i,
    /^media/i, /^telecommunication/i, /^insurance/i, /^banks/i,
    /^real estate/i, /^automobiles/i, /^food/i, /^retail/i,
    /^pharmaceuticals/i, /^semiconductors/i, /^household/i,
  ];
  
  for (const pattern of industryPatterns) {
    if (pattern.test(trimmed)) {
      return true;
    }
  }
  
  return false;
}

// UNIVERSAL industry header detection based on ROW STRUCTURE
// Key insight: Industry headers have text in first cell and NOTHING else in the row
// This works for any BDC regardless of industry naming conventions
function isUniversalIndustryHeader(cells: Element[], firstCellText: string): boolean {
  // Must have at least 2 cells to compare
  if (cells.length < 2) return false;
  
  // Must have text in first cell
  if (!firstCellText || firstCellText.length === 0) return false;
  
  // First cell text should be reasonable length (not too long like a description)
  if (firstCellText.length > 100) return false;
  
  // Check if ALL other cells are empty or contain only whitespace/non-breaking spaces
  const otherCellsEmpty = cells.slice(1).every(cell => {
    const content = cell.textContent?.trim() || '';
    // Empty or just contains whitespace/non-breaking spaces
    return content === '' || 
           content === '\u00A0' || 
           content === '-' ||
           content === '—' ||
           /^[\s\u00A0—-]*$/.test(content);
  });
  
  // If other cells have content, it's not an industry header
  if (!otherCellsEmpty) return false;
  
  // Exclude known non-industry patterns
  const excludePatterns = [
    /^Total/i,
    /^Subtotal/i,
    /^Sub-total/i,
    /^Net\b/i,
    /^See accompanying/i,
    /^The accompanying/i,
    /^Notes?\s*(to|$)/i,
    /^\(/,  // Starts with parenthesis (likely a note)
    /^\d+$/,  // Just a number
    /^Page \d+/i,  // Page numbers
    /^Portfolio/i,
    /^Balance/i,
    /^Investments\s+at/i,
    /^\$[\d,]+/,  // Starts with dollar amount
    /^\d+\.?\d*\s*%/,  // Starts with percentage
    /^(First|Second|Third|Senior|Junior|Subordinated|Mezzanine|Equity|Preferred|Common)/i, // Investment types
    /^(As of|For the|During)/i,  // Date/period phrases
    /^Schedule of Investments/i,  // Table title
    /^Consolidated/i,  // Table title
  ];
  
  const shouldExclude = excludePatterns.some(pattern => pattern.test(firstCellText));
  if (shouldExclude) return false;
  
  // Exclude if it has a company entity suffix (it's a company, not an industry)
  const hasCompanyEntity = /\b(LLC|Inc\.|Inc|Corp\.|Corp|L\.P\.|LP|Ltd\.|Ltd|Limited|Holdings|Co\.|Company|Enterprises|Partners|Group)\b/i.test(firstCellText);
  if (hasCompanyEntity) return false;
  
  // Passed all checks - this is likely an industry header
  return true;
}

// Legacy function for backward compatibility - now calls universal detection
function isRowAnIndustrySectionHeader(cells: Element[], companyCellText: string, expectedCellCount: number): boolean {
  // First try exact match (highest confidence)
  if (isExactIndustryCategory(companyCellText)) return true;
  
  // Then try legacy text-based detection
  if (isIndustrySectionHeader(companyCellText)) return true;
  
  // Finally use universal structure-based detection
  return isUniversalIndustryHeader(cells, companyCellText);
}

// Check if text is an investment type label (not a company name)
function isInvestmentTypeLabel(text: string): boolean {
  const lower = text.toLowerCase().trim();
  return INVESTMENT_TYPE_LABELS.some(t => lower.startsWith(t) && lower.length < 50);
}

// Check if a company name appears valid (has legal suffix or entity pattern)
function hasCompanySuffix(name: string): boolean {
  const lowerName = name.toLowerCase();
  
  // Check for company suffix
  const hasSuffix = COMPANY_SUFFIXES.some(suffix => {
    const regex = new RegExp(`\\b${suffix.replace(/\./g, "\\.")}\\b`, "i");
    return regex.test(lowerName);
  });
  
  if (hasSuffix) return true;
  
  // Special case: names ending with common company name patterns
  const endsWithEntityWord = /\b(enterprises|industries|technologies|systems|group|capital|solutions|services|holdings)\s*$/i.test(lowerName);
  return endsWithEntityWord;
}

// Check if a row represents an actual portfolio holding
function isRealHolding(companyName: string, fairValue: number | null, cost: number | null): { valid: boolean; reason: string } {
  const name = companyName.trim();
  const lowerName = name.toLowerCase();
  
  // Must have a non-empty name
  if (!name || name.length < 5) {
    return { valid: false, reason: "Name too short" };
  }
  
  // Skip if it's an industry section header
  if (isIndustrySectionHeader(name)) {
    return { valid: false, reason: "Industry section header" };
  }
  
  // Skip if it's an investment type label
  if (isInvestmentTypeLabel(name)) {
    return { valid: false, reason: "Investment type label" };
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
  
  // Check for company suffix
  if (!hasCompanySuffix(name)) {
    return { valid: false, reason: "No company suffix found" };
  }
  
  // Additional sanity check: name should have at least 2 words
  const wordCount = name.split(/\s+/).filter(w => w.length > 0).length;
  if (wordCount < 2) {
    return { valid: false, reason: "Single word name" };
  }
  
  return { valid: true, reason: "Has company suffix" };
}

// Parse tables looking for Schedule of Investments with multi-line investment support
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
      // NOTE: Industry is detected from section HEADERS (rows with empty other cells)
      // NOT from "business description" columns which contain company descriptions
      industry: -1, // Disabled - use section headers only via universal detection
      description: findHeader(["description", "notes", "business"]), // Business description moved here
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
    
    // Parse data rows with multi-line investment tracking
    const rows = (table as Element).querySelectorAll("tr");
    
    // Find the index of the header row
    let headerRowIndex = 0;
    for (let idx = 0; idx < rows.length; idx++) {
      if (rows[idx] === headerRow) {
        headerRowIndex = idx;
        break;
      }
    }
    
    // State for tracking current company across multi-line investments
    let currentCompany: string | null = null;
    let currentIndustry: string | null = null;
    
    // Helper to get cell at a given column position (accounting for colspan)
    // Also supports fallback to cell index if position-based lookup fails
    const getCellAtPosition = (cells: Element[], pos: number, fallbackIndex?: number): Element | null => {
      let currentPos = 0;
      for (const cell of cells) {
        const colspan = parseInt(cell.getAttribute("colspan") || "1", 10);
        if (currentPos <= pos && pos < currentPos + colspan) {
          return cell;
        }
        currentPos += colspan;
      }
      // If position-based lookup failed, try fallback index
      if (fallbackIndex !== undefined && fallbackIndex >= 0 && fallbackIndex < cells.length) {
        return cells[fallbackIndex];
      }
      return null;
    };
    
    // Helper to find cell by searching for numeric value near expected position
    const findValueCell = (cells: Element[], expectedPos: number, searchLabel?: string): Element | null => {
      // First try exact position
      const exactCell = getCellAtPosition(cells, expectedPos);
      if (exactCell) {
        const value = parseNumeric(exactCell.textContent?.trim());
        if (value !== null) return exactCell;
      }
      
      // If exact position fails, search backwards from end for numeric values
      // Fair value, cost, par are typically the last few columns
      if (searchLabel === 'fairValue' || searchLabel === 'cost' || searchLabel === 'par') {
        for (let i = cells.length - 1; i >= Math.max(0, cells.length - 5); i--) {
          const cell = cells[i];
          const value = parseNumeric(cell.textContent?.trim());
          if (value !== null && value > 0) {
            // For fairValue, check if this is the last numeric cell
            if (searchLabel === 'fairValue') {
              // Verify no more numeric cells after this
              let isLast = true;
              for (let j = i + 1; j < cells.length; j++) {
                if (parseNumeric(cells[j].textContent?.trim()) !== null) {
                  isLast = false;
                  break;
                }
              }
              if (isLast) return cell;
            } else if (searchLabel === 'cost') {
              // Cost is typically second to last numeric
              let numericCount = 0;
              for (let j = i; j < cells.length; j++) {
                if (parseNumeric(cells[j].textContent?.trim()) !== null) numericCount++;
              }
              if (numericCount === 2) return cell;
            }
          }
        }
      }
      
      return exactCell;
    };
    
    // Cap the number of rows we process per table to prevent blowup
    const rowsToProcess = Math.min(rows.length, headerRowIndex + maxRowsPerTable + 1);
    
    // Track expected cell count for structure-based industry header detection
    let expectedCellCount = headerCells.length;
    
    // Debug: Log first few rows to understand structure
    let debugRowsLogged = 0;
    const maxDebugRows = 15;
    
    for (let i = headerRowIndex + 1; i < rowsToProcess; i++) {
      const row = rows[i] as Element;
      // Check for both td and th cells (industry headers may use th)
      const tdCells = Array.from(row.querySelectorAll("td"));
      const thCells = Array.from(row.querySelectorAll("th"));
      const allCellNodes = tdCells.length > 0 ? tdCells : thCells;
      const cells = allCellNodes.map(c => c as Element);
      
      if (cells.length === 0) continue;
      
      // Get the first cell text (could be in first td or th)
      const firstCellText = cells[0]?.textContent?.trim() || "";
      
      // Debug logging for first few rows - show universal detection results
      if (debugRowsLogged < maxDebugRows && firstCellText) {
        const nonEmptyCells = cells.filter(c => {
          const txt = c.textContent?.trim() || '';
          return txt !== '' && txt !== '\u00A0' && txt !== '-' && txt !== '—';
        }).length;
        const isUniversal = isUniversalIndustryHeader(cells, firstCellText);
        const isExactMatch = isExactIndustryCategory(firstCellText);
        console.log(`Row ${i - headerRowIndex}: total_cells=${cells.length}, non_empty=${nonEmptyCells}, universal_industry=${isUniversal}, exact_match=${isExactMatch}, first="${firstCellText.substring(0, 60)}"`);
        debugRowsLogged++;
      }
      
      // Check if this is an industry section header FIRST (before company parsing)
      // PRIMARY METHOD: Universal detection based on row structure (text only in first cell)
      if (firstCellText && firstCellText.length >= 3) {
        // Try universal structure-based detection FIRST (most reliable)
        if (isUniversalIndustryHeader(cells, firstCellText)) {
          currentIndustry = normalizeIndustryName(firstCellText);
          currentCompany = null;
          console.log(`📂 Industry section (universal): ${currentIndustry}`);
          continue;
        }
        
        // Fallback: Check if it's an exact known industry category
        if (isExactIndustryCategory(firstCellText)) {
          currentIndustry = normalizeIndustryName(firstCellText);
          currentCompany = null;
          console.log(`📂 Industry section (exact match): ${currentIndustry}`);
          continue;
        }
      }
      
      const companyCell = getCellAtPosition(cells, colIndices.company);
      const companyCellText = companyCell?.textContent?.trim() || "";
      
      // Check for rowspan - if the company cell has rowspan, subsequent rows won't have a company cell
      const companyRowspan = companyCell ? parseInt(companyCell.getAttribute("rowspan") || "1", 10) : 1;
      
      // Get the industry/business description cell (may also serve as industry indicator)
      const industryCell = colIndices.industry >= 0 ? getCellAtPosition(cells, colIndices.industry) : null;
      const industryCellText = industryCell?.textContent?.trim() || "";
      
      // Check if the first cell has a company name (new company) or is empty (continuation of previous)
      let effectiveCompanyName: string;
      let effectiveIndustry: string | null;
      
      if (companyCellText && companyCellText.length >= 5) {
        // Non-empty company cell - this is a new company
        // Clean footnotes from the company name
        const cleanedCompanyName = cleanCompanyName(companyCellText);
        
        // Check if it has a company suffix before accepting as new company
        if (hasCompanySuffix(cleanedCompanyName)) {
          currentCompany = cleanedCompanyName;
          // Industry comes ONLY from section headers (currentIndustry) detected via universal detection
          // NOT from the "business description" column which contains company-specific descriptions
          effectiveIndustry = currentIndustry;
          
          if (debugMode && companyRowspan > 1) {
            console.log(`🔗 Company ${cleanedCompanyName} has rowspan=${companyRowspan}`);
          }
        } else {
          // Might be an investment type label or subtotal - skip as company
          if (debugRejected.length < 10) {
            debugRejected.push({ name: cleanedCompanyName.substring(0, 50), reason: "No company suffix (continuation row handling)" });
          }
          // Don't update currentCompany, treat as potential investment line for current company
          effectiveIndustry = currentIndustry;
        }
        effectiveCompanyName = currentCompany || cleanedCompanyName;
      } else if (currentCompany) {
        // Empty or short company cell - this is a continuation row for the current company
        effectiveCompanyName = currentCompany;
        effectiveIndustry = currentIndustry;
      } else {
        // No current company and no company name - skip this row
        continue;
      }
      
      // Parse numeric values for validation - use smart cell finding for key values
      let fairValueCell = findValueCell(cells, colIndices.fairValue, 'fairValue');
      let fairValue = parseNumeric(fairValueCell?.textContent?.trim());
      let costCell = colIndices.cost >= 0 ? findValueCell(cells, colIndices.cost, 'cost') : null;
      let cost = parseNumeric(costCell?.textContent?.trim());
      
      // If position-based lookup failed, try finding values from the end of the row
      // SEC filings typically have: ... Principal | Amortized Cost | Fair Value | % of Net Assets
      if (fairValue === null && cells.length > 0) {
        // Collect all numeric values from the last 6 cells, excluding percentage columns
        const numericCells: { index: number; value: number; text: string }[] = [];
        
        for (let j = cells.length - 1; j >= Math.max(0, cells.length - 6); j--) {
          const cellText = cells[j].textContent?.trim() || "";
          // Skip cells that contain % or are likely percentage values
          if (cellText.includes('%') || cellText.includes('(') && cellText.includes(')') && cellText.length < 10) {
            continue;
          }
          const value = parseNumeric(cellText);
          // Only accept positive values that look like dollar amounts
          if (value !== null && value > 0) {
            numericCells.push({ index: j, value, text: cellText });
          }
        }
        
        // Fair value is typically the last positive number before % of Net Assets
        // Cost is typically the second to last positive number
        // Sort by index (ascending) to get order: principal, cost, fair_value
        numericCells.sort((a, b) => a.index - b.index);
        
        if (numericCells.length >= 1) {
          // Last numeric cell is fair value
          const fvCell = numericCells[numericCells.length - 1];
          fairValue = fvCell.value;
          fairValueCell = cells[fvCell.index];
          
          if (numericCells.length >= 2) {
            // Second to last is cost
            const costData = numericCells[numericCells.length - 2];
            cost = costData.value;
            costCell = cells[costData.index];
          }
        }
      }
      
      // Debug logging for first few rows
      if (debugMode && i <= headerRowIndex + 20) {
        const totalCellSpan = cells.reduce((sum, c) => sum + parseInt(c.getAttribute("colspan") || "1", 10), 0);
        console.log(`Row ${i - headerRowIndex}: cells=${cells.length}, span=${totalCellSpan}, company="${companyCellText.substring(0, 30)}", FV=${fairValue}, cost=${cost}`);
      }
      
      // Skip rows without fair value (could be subtotals, headers, or empty lines)
      if (fairValue === null || fairValue === 0) {
        // Don't log as rejected - these are expected empty rows in multi-line format
        continue;
      }
      
      // Validate the effective company name for the holding
      const validation = isRealHolding(effectiveCompanyName, fairValue, cost);
      
      if (!validation.valid) {
        // Log first 10 rejected for debugging
        if (debugRejected.length < 10) {
          debugRejected.push({ name: effectiveCompanyName.substring(0, 50), reason: validation.reason });
        }
        continue;
      }
      
      // Extract investment details
      const interestRateCell = colIndices.interestRate >= 0 ? getCellAtPosition(cells, colIndices.interestRate) : null;
      const interestRateText = interestRateCell?.textContent?.trim() || "";
      
      const { rate, reference } = extractInterestRate(interestRateText);
      
      // Get spread column value for reference_rate
      const spreadCell = colIndices.spread >= 0 ? getCellAtPosition(cells, colIndices.spread) : null;
      const spreadText = spreadCell?.textContent?.trim() || "";
      const referenceRate = spreadText || reference;
      
      const investmentTypeCell = colIndices.investmentType >= 0 ? getCellAtPosition(cells, colIndices.investmentType) : null;
      const investmentType = investmentTypeCell?.textContent?.trim() || null;
      
      // Skip subtotal rows - these have fair value but no investment type
      // Real investment rows should have an investment type like "First lien senior secured loan"
      // Exception: equity positions may not have investment type in some filings
      if (!investmentType && colIndices.investmentType >= 0) {
        // This is likely a subtotal row - skip it
        if (debugRejected.length < 10) {
          debugRejected.push({ name: `${effectiveCompanyName.substring(0, 40)} (FV=$${fairValue})`, reason: "Subtotal row (no investment type)" });
        }
        continue;
      }
      
      const descriptionCell = colIndices.description >= 0 ? getCellAtPosition(cells, colIndices.description) : null;
      const maturityCell = colIndices.maturity >= 0 ? getCellAtPosition(cells, colIndices.maturity) : null;
      const parCell = colIndices.par >= 0 ? getCellAtPosition(cells, colIndices.par) : null;
      
      const holding: Holding = {
        company_name: effectiveCompanyName,
        investment_type: investmentType,
        industry: effectiveIndustry,
        description: descriptionCell?.textContent?.trim() || null,
        interest_rate: rate,
        reference_rate: referenceRate || null,
        maturity_date: parseDate(maturityCell?.textContent?.trim()),
        par_amount: parseNumeric(parCell?.textContent?.trim()),
        cost,
        fair_value: fairValue,
      };
      
      holdings.push(holding);
      
      // Log first 10 accepted for debugging
      if (debugAccepted.length < 10) {
        const investType = holding.investment_type || 'unknown';
        debugAccepted.push(`${effectiveCompanyName.substring(0, 40)} [${investType.substring(0, 20)}] FV=$${fairValue}`);
      }
      
      // Cap total holdings to prevent excessive memory usage
      if (holdings.length >= maxHoldings) {
        console.log(`Reached max holdings cap (${maxHoldings}), stopping parse`);
        break;
      }
    }
    
    // If we found holdings in this table, log debug info and stop searching
    if (holdings.length > 0) {
      // Count unique companies and industries
      const uniqueCompanies = new Set(holdings.map(h => h.company_name)).size;
      const uniqueIndustries = new Set(holdings.filter(h => h.industry).map(h => h.industry));
      
      // Group companies by industry for logging
      const industryGroups: Record<string, string[]> = {};
      for (const h of holdings) {
        const ind = h.industry || 'Unknown';
        if (!industryGroups[ind]) industryGroups[ind] = [];
        if (!industryGroups[ind].includes(h.company_name)) {
          industryGroups[ind].push(h.company_name);
        }
      }
      
      console.log(`\n=== Parsing Results ===`);
      console.log(`✅ Accepted ${holdings.length} investment records from ${uniqueCompanies} unique companies`);
      console.log(`📂 Industries found (${uniqueIndustries.size}): ${Array.from(uniqueIndustries).join(', ')}`);
      console.log(`📊 Companies by industry:`);
      for (const [ind, companies] of Object.entries(industryGroups)) {
        console.log(`   ${ind}: ${companies.length} companies (${companies.slice(0, 3).join(', ')}${companies.length > 3 ? '...' : ''})`);
      }
      
      // Validate expected major industries for ARCC
      const expectedMajorIndustries = [
        'Software and Services',
        'Health Care Equipment and Services',
        'Financial Services',
        'Consumer Services',
        'Capital Goods',
      ];
      const foundIndustryNames = Array.from(uniqueIndustries) as string[];
      const missingIndustries = expectedMajorIndustries.filter(exp => 
        !foundIndustryNames.some(found => found?.toLowerCase() === exp.toLowerCase())
      );
      if (missingIndustries.length > 0 && missingIndustries.length < expectedMajorIndustries.length) {
        console.log(`⚠️ Some expected industries not found: ${missingIndustries.join(', ')}`);
      }
      
      console.log(`First 10 accepted:`, debugAccepted);
      console.log(`First 10 rejected:`, debugRejected);
      console.log(`========================\n`);
      return holdings;
    }
  }
  
  // Log debug info even if no holdings found
  if (debugRejected.length > 0) {
    console.log(`\n=== Parsing Results (no holdings found) ===`);
    console.log(`First 10 rejected:`, debugRejected);
    console.log(`============================================\n`);
  }
  
  return holdings;
}

// Parse HTML Schedule of Investments table from snippets
function parseHtmlScheduleOfInvestments(html: string, debugMode = false): { holdings: Holding[]; scaleResult: ScaleDetectionResult } {
  // ARCC and other large BDCs can have 500+ companies with 2-5 investment lines each
  // Need to process at least 3000 rows to capture the full Schedule of Investments
  const maxRowsPerTable = 3000;
  const maxHoldings = 2000;
  
  // Detect scale from the HTML (look for "in thousands" or "in millions")
  const scaleResult = detectScale(html);
  
  // Accumulate holdings from ALL snippets instead of returning on first match
  const allHoldings: Holding[] = [];
  
  try {
    // Extract only relevant HTML snippets
    const snippets = extractCandidateTableHtml(html);
    console.log(`Found ${snippets.length} candidate snippets to parse`);
    
    for (let i = 0; i < snippets.length; i++) {
      const snippet = snippets[i];
      const doc = new DOMParser().parseFromString(snippet, "text/html");
      if (!doc) continue;
      
      // Calculate remaining capacity
      const remainingCapacity = maxHoldings - allHoldings.length;
      if (remainingCapacity <= 0) {
        console.log(`Reached max holdings cap (${maxHoldings}), stopping snippet processing`);
        break;
      }
      
      const tables = Array.from(doc.querySelectorAll("table")) as Element[];
      const snippetHoldings = parseTables(tables, maxRowsPerTable, remainingCapacity, debugMode);
      
      if (snippetHoldings.length > 0) {
        console.log(`Snippet ${i + 1}/${snippets.length}: Found ${snippetHoldings.length} holdings`);
        allHoldings.push(...snippetHoldings);
      }
    }
    
    // Deduplicate holdings in case of overlapping chunks
    // Use company_name + investment_type + fair_value as a composite key
    if (allHoldings.length > 0) {
      const seen = new Set<string>();
      const deduplicatedHoldings: Holding[] = [];
      
      for (const h of allHoldings) {
        const key = `${h.company_name}|${h.investment_type || ''}|${h.fair_value || ''}`;
        if (!seen.has(key)) {
          seen.add(key);
          deduplicatedHoldings.push(h);
        }
      }
      
      if (deduplicatedHoldings.length < allHoldings.length) {
        console.log(`Deduplicated: ${allHoldings.length} -> ${deduplicatedHoldings.length} holdings`);
      }
      console.log(`Total unique holdings from all snippets: ${deduplicatedHoldings.length}`);
      return { holdings: deduplicatedHoldings, scaleResult };
    } else {
      console.log("No holdings found in any snippets");
    }
  }
  catch (error) {
    console.error("Error parsing HTML:", error);
  }
  
  return { holdings: allHoldings, scaleResult };
}

// ======================================================================
// LIGHTWEIGHT REGEX-BASED PARSER FOR LARGE FILINGS
// ======================================================================
// This parser avoids DOM parsing entirely to minimize CPU usage.
// It uses regex to extract table rows and parse holdings directly from HTML strings.

function parseLargeFilingSegment(htmlSegment: string): Holding[] {
  const holdings: Holding[] = [];
  
  // Find all table rows in this segment
  const rowRegex = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let rowMatch;
  
  // Track current industry from section headers
  let currentIndustry: string | null = null;
  let currentCompany: string | null = null;
  
  while ((rowMatch = rowRegex.exec(htmlSegment)) !== null) {
    const rowHtml = rowMatch[1];
    
    // Extract all cell contents - handle both td and th
    const cellRegex = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
    const cells: string[] = [];
    let cellMatch;
    
    while ((cellMatch = cellRegex.exec(rowHtml)) !== null) {
      // Clean HTML tags from cell content
      const text = cellMatch[1]
        .replace(/<[^>]+>/g, ' ')
        .replace(/&nbsp;/g, ' ')
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/\s+/g, ' ')
        .trim();
      cells.push(text);
    }
    
    if (cells.length < 3) continue;
    
    const firstCell = cells[0];
    
    // Skip empty rows
    if (!firstCell || firstCell.length < 2) continue;
    
    // Check if this is an industry section header (text in first cell, mostly empty after)
    const nonEmptyCells = cells.filter(c => c && c.length > 1 && !/^[-—\s]*$/.test(c));
    if (nonEmptyCells.length === 1 && firstCell.length > 5 && firstCell.length < 80) {
      // Potential industry header
      const isIndustryHeader = !/(LLC|Inc\.|Corp\.|L\.P\.|Ltd\.|Holdings|Company|Total|Subtotal)/i.test(firstCell);
      if (isIndustryHeader) {
        currentIndustry = firstCell;
        continue;
      }
    }
    
    // Skip summary/total rows
    if (/^(Total|Subtotal|Sub-total|Net\s|Balance|See accompanying|Notes to)/i.test(firstCell)) {
      continue;
    }
    
    // Try to find numeric values (fair value, cost) - typically in last few cells
    const numericCells: { index: number; value: number; text: string }[] = [];
    for (let i = cells.length - 1; i >= 0 && numericCells.length < 5; i--) {
      const cellText = cells[i];
      // Parse numeric: handle $, commas, parentheses for negatives
      const cleaned = cellText.replace(/[$,\s]/g, '').trim();
      if (cleaned && cleaned !== '-' && cleaned !== '—') {
        const isNegative = cleaned.startsWith('(') && cleaned.endsWith(')');
        const numStr = isNegative ? cleaned.slice(1, -1) : cleaned;
        const parsed = parseFloat(numStr);
        if (!isNaN(parsed) && parsed !== 0) {
          numericCells.unshift({ index: i, value: isNegative ? -parsed : parsed, text: cellText });
        }
      }
    }
    
    // Need at least one numeric value (fair value)
    if (numericCells.length === 0) continue;
    
    // Determine company name - first cell or carry forward from previous row
    let companyName = firstCell;
    
    // Check if first cell looks like a company name
    const hasCompanyEntity = /(LLC|Inc\.|Inc|Corp\.|Corp|L\.P\.|LP|Ltd\.|Ltd|Limited|Holdings|Company|Partners|Group)\b/i.test(firstCell);
    
    if (hasCompanyEntity || firstCell.length > 10) {
      // Clean footnotes
      companyName = firstCell
        .replace(/\(\d+(?:,\s*\d+)*\)\s*$/g, '')
        .replace(/\s*[\*†‡§¶#]+\s*$/g, '')
        .trim();
      currentCompany = companyName;
    } else if (currentCompany) {
      // This might be a continuation row for the same company
      companyName = currentCompany;
    }
    
    // Skip if company name is too short or invalid
    if (!companyName || companyName.length < 5) continue;
    if (/^[\d\s$%.,()-]+$/.test(companyName)) continue; // Just numbers/symbols
    
    // Extract investment type from second cell if it looks like one
    let investmentType: string | null = null;
    if (cells.length > 1) {
      const secondCell = cells[1];
      if (/(first lien|second lien|senior|subordinated|mezzanine|equity|preferred|common|warrant|unitranche|loan|note|bond)/i.test(secondCell)) {
        investmentType = secondCell.slice(0, 100);
      }
    }
    
    // Get fair value (last numeric), cost (second to last if exists)
    const fairValue = numericCells[numericCells.length - 1]?.value || null;
    const cost = numericCells.length > 1 ? numericCells[numericCells.length - 2]?.value : null;
    const parAmount = numericCells.length > 2 ? numericCells[numericCells.length - 3]?.value : null;
    
    // Skip if fair value is null or zero
    if (!fairValue || fairValue === 0) continue;
    
    // Try to find maturity date - look for date patterns in cells
    let maturityDate: string | null = null;
    for (const cell of cells) {
      // MM/YYYY or MM/DD/YYYY patterns
      const dateMatch = cell.match(/(\d{1,2}\/\d{1,2}\/\d{4}|\d{1,2}\/\d{4})/);
      if (dateMatch) {
        maturityDate = parseDate(dateMatch[1]);
        break;
      }
    }
    
    // Try to find interest rate - look for rate patterns
    let interestRate: string | null = null;
    let referenceRate: string | null = null;
    for (const cell of cells) {
      if (/(SOFR|LIBOR|Prime)/i.test(cell)) {
        interestRate = cell.slice(0, 100);
        referenceRate = cell.match(/(SOFR|LIBOR|Prime)/i)?.[1]?.toUpperCase() || null;
        break;
      }
      if (/\d+\.?\d*\s*%/.test(cell) && !interestRate) {
        interestRate = cell.slice(0, 50);
      }
    }
    
    holdings.push({
      company_name: companyName,
      investment_type: investmentType,
      industry: currentIndustry,
      description: null,
      interest_rate: interestRate,
      reference_rate: referenceRate,
      maturity_date: maturityDate,
      par_amount: parAmount,
      cost: cost,
      fair_value: fairValue,
    });
  }
  
  return holdings;
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
    
    // Enable debug mode for ARCC or specific test filings
    const debugMode = accessionNo === "0001287750-25-000046" || accessionNo === "0001104659-25-108820";
    if (debugMode) {
      console.log(`\n🔍 DEBUG MODE ENABLED for filing ${accessionNo}\n`);
    }
    
    try {
      // Fetch the filing index to find the primary document
      const indexJson = await fetchSecFile(indexUrl);
      const index = JSON.parse(indexJson);
      
      // Quick summary of documents (don't log all to save time)
      const totalDocs = index.directory?.item?.length || 0;
      const primaryDoc = index.directory?.item?.find((i: any) => i.type === "primary");
      console.log(`\n📁 Found ${totalDocs} documents. Primary: ${primaryDoc?.name || 'none'}`);
      
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
      
      // Log first few documents for reference
      console.log(`   Processing order: ${prioritizedDocs.slice(0, 5).map((d: any) => d.name).join(', ')}${prioritizedDocs.length > 5 ? '...' : ''}`);
      console.log(`   Total HTM docs: ${htmDocs.length}`);
      
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
          
          // For very large documents (>5MB), extract just the SOI section to avoid memory issues
          let textToParse = html;
          if (html.length > 5_000_000) {
            console.log(`   📦 Large document (${(html.length / 1024 / 1024).toFixed(1)} MB), extracting SOI section...`);
            
            // Find the boundaries of the Schedule of Investments section
            const lower = html.toLowerCase();
            
            // Find ALL occurrences of "schedule of investments" - we want the main data section, not TOC
            const soiKeywords = [
              "consolidated schedule of investments",
              "schedule of investments",
            ];
            
            // Collect all SOI occurrences
            const soiOccurrences: number[] = [];
            for (const kw of soiKeywords) {
              let pos = 0;
              while ((pos = lower.indexOf(kw, pos)) !== -1) {
                soiOccurrences.push(pos);
                pos += kw.length;
              }
            }
            
            soiOccurrences.sort((a, b) => a - b);
            console.log(`   Found ${soiOccurrences.length} SOI keyword occurrences`);
            
            if (soiOccurrences.length === 0) {
              warnings.push(`No Schedule of Investments found in large document ${doc.name}. Skipping.`);
              console.log(`   ⚠️ No SOI section found in large document, skipping`);
              continue;
            }
            
            // For ARCC-style documents, the SOI header appears on EVERY page
            // We need to find the FIRST real SOI section (after table of contents)
            // and extract from there to the end markers
            
            const endMarkers = [
              "notes to consolidated financial statements",
              "notes to financial statements",
            ];
            
            // Find the LAST occurrence of end markers (the actual notes section)
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
                break; // Use the first end marker type found
              }
            }
            
            // Find where actual TABLE data starts (not just the header)
            // Look for SOI occurrence that has a TABLE tag within 10KB after it
            let bestStart = soiOccurrences[0];
            for (const soiPos of soiOccurrences) {
              // Skip if this is in the first 10% of document (likely TOC)
              if (soiPos < html.length * 0.05) continue;
              
              // Check if there's a table within 10KB of this SOI
              const nearbyHtml = lower.slice(soiPos, Math.min(soiPos + 10_000, html.length));
              if (nearbyHtml.includes('<table')) {
                bestStart = soiPos;
                console.log(`   📍 Found SOI with nearby table at position ${soiPos}`);
                break;
              }
            }
            
            // ============ CURRENT QUARTER ONLY EXTRACTION ============
            // SEC filings often include both current quarter AND prior year-end for comparison
            // We only want the CURRENT quarter, not the prior period
            
            // Look for prior period markers that indicate the start of comparison SOI
            const priorPeriodMarkers = [
              'december 31, 2024',
              'december 31,2024',
              'as of december 31, 2024',
              'at december 31, 2024',
              '12/31/2024',
              '12/31/24',
              // Also check for any "prior" or "comparison" language
              'consolidated schedule of investments</b><br>(prior',
              'schedule of investments (prior',
              // For filings from other periods, check generic prior year markers
              'december 31, 2023',
              'december 31, 2022',
            ];
            
            // Find where prior period SOI starts (after our main SOI start)
            let priorPeriodStart = documentEnd;
            const searchStartOffset = bestStart + 50_000; // Skip at least 50KB from start
            
            for (const marker of priorPeriodMarkers) {
              const markerIdx = lower.indexOf(marker, searchStartOffset);
              if (markerIdx !== -1 && markerIdx < priorPeriodStart) {
                // Verify this is actually in a "Schedule of Investments" context
                // Look back up to 5KB for SOI header
                const contextBefore = lower.slice(Math.max(0, markerIdx - 5000), markerIdx);
                if (contextBefore.includes('schedule of investments') || 
                    contextBefore.includes('consolidated schedule')) {
                  priorPeriodStart = markerIdx;
                  console.log(`   📍 Found PRIOR PERIOD marker "${marker}" at position ${markerIdx}`);
                }
              }
            }
            
            // Also look for a SECOND "Schedule of Investments" header that might indicate comparison
            // Skip the first occurrence and look for the next complete header
            const secondSoiPatterns = [
              'consolidated schedule of investments',
              '<b>schedule of investments',
              '>schedule of investments<',
            ];
            
            for (const pattern of secondSoiPatterns) {
              // Start looking after the first 100KB of SOI section
              const searchStart = bestStart + 100_000;
              const patternIdx = lower.indexOf(pattern, searchStart);
              
              if (patternIdx !== -1 && patternIdx < priorPeriodStart) {
                // Check if this is followed by a date that looks like prior period
                const nearbyText = lower.slice(patternIdx, Math.min(patternIdx + 1000, html.length));
                if (nearbyText.includes('december 31') || nearbyText.includes('12/31')) {
                  priorPeriodStart = patternIdx;
                  console.log(`   📍 Found SECOND SOI header at position ${patternIdx} (prior period)`);
                }
              }
            }
            
            // Find the LAST "Total Investments" BEFORE the prior period marker
            // This is the actual end of the current quarter's SOI
            // (The first "Total Investments" might be a subtotal within the SOI)
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
              // Include some buffer after "Total Investments" to capture the totals row
              currentQuarterEnd = Math.min(priorPeriodStart, lastTotalInvestmentsIdx + 10_000);
              console.log(`   📍 Found LAST "Total Investments" at ${lastTotalInvestmentsIdx}, setting end to ${currentQuarterEnd}`);
            } else {
              console.log(`   📍 No "Total Investments" found before prior period, using prior period start`);
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
            
            console.log(`   📊 Full SOI section: ${(totalSoiSize / 1024 / 1024).toFixed(1)} MB`);
            
            // For very large SOI sections (like ARCC's $28.6B portfolio), use segment-based extraction
            // Use very small segments (300KB) with quick pre-checks to avoid heavy DOM parsing
            const SEGMENT_SIZE = 300_000; // 300KB per segment - very small for fast parsing
            const OVERLAP_SIZE = 30_000; // 30KB overlap to avoid cutting holdings
            
            if (totalSoiSize > 4_000_000) {
              const numSegments = Math.ceil(totalSoiSize / (SEGMENT_SIZE - OVERLAP_SIZE));
              console.log(`   📦 Large SOI section, using segmented DOM-parser approach...`);
              console.log(`   📦 Will process ~${numSegments} segments of ~300KB, saving to DB after each`);
              
              // First delete any existing holdings for this filing to avoid duplicates
              const { error: deleteError } = await supabaseClient
                .from("holdings")
                .delete()
                .eq("filing_id", filingId);
              
              if (deleteError) {
                console.error(`   ⚠️ Error clearing existing holdings:`, deleteError);
              }
              
              // Detect scale from the first part of the document
              const segmentScaleResult = detectScale(html.slice(soiStart, Math.min(soiStart + 100_000, soiEnd)));
              console.log(`   📊 Scale detected: ${segmentScaleResult.detected}`);
              
              // Track holdings keys across segments for deduplication
              const seenHoldingKeys = new Set<string>();
              let currentPosition = soiStart;
              let segmentCount = 0;
              let totalInserted = 0;
              let runningTotalValue = 0;
              
              while (currentPosition < soiEnd) {
                segmentCount++;
                const segmentEnd = Math.min(currentPosition + SEGMENT_SIZE, soiEnd);
                
                // Log every 5th segment to reduce log spam
                if (segmentCount % 5 === 1 || segmentCount === numSegments) {
                  console.log(`   📦 Segment ${segmentCount}/${numSegments}`);
                }
                
                try {
                  // Extract this segment 
                  const segment = html.slice(currentPosition, segmentEnd);
                  
                  // Quick pre-check: skip segments without table markers (saves CPU)
                  const lowerSegment = segment.toLowerCase();
                  if (!lowerSegment.includes('<table') && !lowerSegment.includes('<tr')) {
                    // No table content, skip this segment
                    const nextPosition = segmentEnd - OVERLAP_SIZE;
                    if (nextPosition <= currentPosition) break;
                    currentPosition = nextPosition;
                    continue;
                  }
                  
                  // Parse with DOM parser
                  const segmentResult = parseHtmlScheduleOfInvestments(segment, false);
                  const segmentHoldings = segmentResult.holdings;
                  
                  if (segmentHoldings.length > 0) {
                    // Deduplicate against previously seen holdings
                    // Scale: 1 = already millions, 0.001 = needs conversion from thousands
                    const scale = segmentScaleResult?.scale || 1;
                    const newHoldings: Holding[] = [];
                    
                    for (const h of segmentHoldings) {
                      const key = `${h.company_name}|${h.investment_type || ''}|${h.fair_value || ''}|${h.maturity_date || ''}`;
                      if (!seenHoldingKeys.has(key)) {
                        seenHoldingKeys.add(key);
                        newHoldings.push(h);
                      }
                    }
                    
                    if (newHoldings.length > 0) {
                      // Convert and save to DB immediately
                      // For millions (scale=1): values stay as-is
                      // For thousands (scale=0.001): multiply to convert to millions
                      const holdingsToInsert = newHoldings.map((h) => ({
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
                      }));
                      
                      const { error: insertError } = await supabaseClient
                        .from("holdings")
                        .insert(holdingsToInsert);
                      
                      if (insertError) {
                        console.error(`   ⚠️ Insert error in segment ${segmentCount}:`, insertError.message);
                      } else {
                        totalInserted += holdingsToInsert.length;
                        const segmentValue = holdingsToInsert.reduce((sum, h) => sum + (h.fair_value || 0), 0);
                        runningTotalValue += segmentValue;
                        // Log every 10 segments or when done
                        if (segmentCount % 10 === 0) {
                          console.log(`   ✅ Progress: ${totalInserted} holdings ($${runningTotalValue.toFixed(1)}M)`);
                        }
                      }
                    }
                  }
                } catch (segmentError) {
                  console.error(`   ⚠️ Error processing segment ${segmentCount}:`, segmentError);
                }
                
                // Move to next segment with overlap - but ensure we always advance
                const nextPosition = segmentEnd - OVERLAP_SIZE;
                if (nextPosition <= currentPosition) {
                  // We've reached the end, break the loop
                  break;
                }
                currentPosition = nextPosition;
                
                // Brief pause every 5 segments to allow garbage collection  
                if (segmentCount % 5 === 0) {
                  await new Promise(resolve => setTimeout(resolve, 10));
                }
              }
              
              console.log(`   📦 Segmented extraction complete!`);
              console.log(`   📊 Total: ${totalInserted} holdings, $${runningTotalValue.toFixed(1)}M`);
              
              scaleResult = segmentScaleResult;
              
              // For segmented extraction, we've already saved to DB, so mark filing as parsed and return
              if (totalInserted > 0) {
                const { error: updateError } = await supabaseClient
                  .from("filings")
                  .update({ 
                    parsed_successfully: true,
                    value_scale: scaleResult?.detected || 'unknown'
                  })
                  .eq("id", filingId);
                
                if (updateError) {
                  console.error("Error updating filing status:", updateError);
                }
                
                console.log(`✅ Segmented extraction: Inserted ${totalInserted} holdings for filing ${accessionNo}`);
                
                return new Response(
                  JSON.stringify({
                    filingId,
                    holdingsInserted: totalInserted,
                    totalValue: runningTotalValue,
                    valueScale: scaleResult?.detected || 'unknown',
                    scaleConfidence: scaleResult?.confidence || 'low',
                    method: 'segmented',
                    segments: segmentCount,
                    warnings: warnings.length > 0 ? warnings : undefined,
                  }),
                  {
                    headers: { ...corsHeaders, "Content-Type": "application/json" },
                  }
                );
              }
              
              // If no holdings found in segmented mode, continue to try other docs
              console.log(`   ⚠️ Segmented extraction found no holdings, trying other documents...`);
            } else {
              // Standard processing for smaller sections
              textToParse = html.slice(soiStart, soiEnd);
              console.log(`   📦 Extracted SOI section: ${(textToParse.length / 1024 / 1024).toFixed(1)} MB`);
            }
          }
          
          // Standard processing path (for smaller SOI sections or non-SOI documents)
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
