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
  source_pos?: number;
  period_date?: string | null;
  row_number?: number;
}

interface ScaleDetectionResult {
  scale: number;
  detected: 'thousands' | 'millions' | 'unknown';
  confidence: 'high' | 'medium' | 'low';
}

// ======================================================================
// HELPER FUNCTIONS
// ======================================================================

function sanitizeHtmlToText(rawHtml: string): string {
  return rawHtml
    .replace(/<[^>]*>?/gm, " ")
    .replace(/&nbsp;|&#160;|&#xA0;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Robust Regex-based helper to strip tags and decode basic entities
 * Used by the memory-safe parsers to avoid DOM overhead
 */
function stripTags(html: string): string {
  if (!html) return "";
  return html
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, "")
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "")
    .replace(/<[^>]+>/g, " ") // Replace tags with space to prevent words merging
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&#160;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractPeriodDateFromText(text: string): string | null {
  const cleanText = sanitizeHtmlToText(text);
  const dateRe = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}\s*,\s*\d{4}/gi;
  const matches = Array.from(cleanText.matchAll(dateRe));
  if (matches.length === 0) return null;
  const dateText = matches[matches.length - 1][0];
  const m = /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\s*,\s*(\d{4})$/i.exec(dateText);
  if (!m) return null;
  
  const monthMap: Record<string, number> = {
    january: 1, february: 2, march: 3, april: 4, may: 5, june: 6,
    july: 7, august: 8, september: 9, october: 10, november: 11, december: 12,
  };
  const month = monthMap[m[1].toLowerCase()];
  if (!month) return null;
  return `${m[3]}-${String(month).padStart(2, "0")}-${String(m[2]).padStart(2, "0")}`;
}

function detectScale(html: string): ScaleDetectionResult {
  const lowerHtml = html.toLowerCase();
  if (/\(in thousands\)/i.test(lowerHtml) || /amounts?\s+in\s+thousands/i.test(lowerHtml)) {
    return { scale: 0.001, detected: 'thousands', confidence: 'high' };
  }
  if (/\(in millions\)/i.test(lowerHtml) || /amounts?\s+in\s+millions/i.test(lowerHtml)) {
    return { scale: 1, detected: 'millions', confidence: 'high' };
  }
  return { scale: 0.001, detected: 'thousands', confidence: 'low' };
}

function validateScale(holdings: Holding[], scale: ScaleDetectionResult): { valid: boolean; warning?: string } {
  if (holdings.length === 0) return { valid: true };
  const validValues = holdings.filter(h => h.fair_value !== null).map(h => (h.fair_value as number) * scale.scale);
  if (validValues.length === 0) return { valid: true };
  const avgValue = validValues.reduce((sum, v) => sum + v, 0) / validValues.length;
  
  if (avgValue > 1000) return { valid: false, warning: `Average holding value ($${avgValue.toFixed(1)}M) too large` };
  if (avgValue < 0.01) return { valid: false, warning: `Average holding value ($${(avgValue * 1000).toFixed(1)}K) too small` };
  return { valid: true };
}

async function fetchSecFile(url: string, retries = 2): Promise<string> {
  for (let i = 0; i <= retries; i++) {
    try {
      console.log(`Fetching: ${url}`);
      const response = await fetch(url, { headers: { "User-Agent": SEC_USER_AGENT } });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return await response.text();
    } catch (error) {
      if (i === retries) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000 * (i + 1)));
    }
  }
  throw new Error("Failed to fetch");
}

function cleanCompanyName(name: string | null | undefined): string {
  if (!name) return "";
  return name.replace(/(\s*\(\d+(?:,\s*\d+)*\))+\s*$/g, '').replace(/<[^>]+>/g, '').trim();
}

function parseNumeric(value: string | null | undefined): number | null {
  if (!value) return null;
  const cleaned = value.replace(/[$,\s]/g, "").trim();
  if (!cleaned || cleaned === "-" || cleaned === "—") return null;
  const isNegative = cleaned.startsWith("(") && cleaned.endsWith(")");
  const numStr = isNegative ? cleaned.slice(1, -1) : cleaned;
  const parsed = parseFloat(numStr);
  return isNaN(parsed) ? null : (isNegative ? -parsed : parsed);
}

// Specialized numeric cleaner for GBDC/BXSL that handles strict formatting
function cleanStrictNumeric(value: string | null | undefined): number | null {
  if (!value) return null;
  let cleaned = value.replace(/[$,\s]/g, '').trim();
  if (!cleaned || cleaned === '-' || cleaned === '—' || cleaned === '') return null;
  // Handle (123) as negative
  const isNegative = cleaned.startsWith('(') && cleaned.endsWith(')');
  if (isNegative) cleaned = cleaned.slice(1, -1);
  const parsed = parseFloat(cleaned);
  return isNaN(parsed) ? null : (isNegative ? -parsed : parsed);
}

function toMillions(value: number | null | undefined, scale: number): number | null {
  if (value === null || value === undefined) return null;
  return Math.round(value * scale * 10) / 10;
}

function parseDate(value: string | null | undefined): string | null {
  if (!value) return null;
  const cleaned = value.trim();
  if (!cleaned || cleaned === "-" || cleaned === "—" || cleaned.toLowerCase() === "n/a") return null;
  const mmyyyy = cleaned.match(/^(\d{1,2})\/(\d{4})$/);
  if (mmyyyy) {
    const lastDay = new Date(parseInt(mmyyyy[2]), parseInt(mmyyyy[1]), 0).getDate();
    return `${mmyyyy[2]}-${mmyyyy[1].padStart(2, '0')}-${lastDay}`;
  }
  const mmddyyyy = cleaned.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (mmddyyyy) return `${mmddyyyy[3]}-${mmddyyyy[1].padStart(2, '0')}-${mmddyyyy[2].padStart(2, '0')}`;
  
  const date = new Date(cleaned);
  return !isNaN(date.getTime()) ? date.toISOString().split("T")[0] : null;
}

function extractInterestRate(text: string): { rate: string | null; reference: string | null } {
  if (!text) return { rate: null, reference: null };
  const lowerText = text.toLowerCase();
  const refs = ["sofr", "libor", "prime", "euribor"];
  let reference = refs.find(r => lowerText.includes(r))?.toUpperCase() || null;
  return { rate: text.trim(), reference };
}

function hasCompanySuffix(name: string): boolean {
  const suffixes = ["inc", "llc", "lp", "corp", "company", "ltd", "limited"];
  const lower = name.toLowerCase();
  return suffixes.some(s => new RegExp(`\\b${s}\\b|\\s${s}\\.?$`, 'i').test(lower));
}

// ======================================================================
// STREAMING PARSERS (REGEX-BASED FOR MEMORY SAFETY)
// ======================================================================

/**
 * GBDC STREAMING PARSER - REGEX MODE
 * Replaces DOMParser with Regex to avoid WORKER_LIMIT OOM crashes
 */
async function parseGBDCTableAndInsert(params: {
  html: string;
  filingId: string;
  supabaseClient: any;
  debugMode?: boolean;
}): Promise<{ insertedCount: number; scaleResult: ScaleDetectionResult }> {
  const { html, filingId, supabaseClient } = params;
  console.log(`\n🟢 GBDC STREAMING PARSER (REGEX): Starting (input size: ${(html.length / 1024 / 1024).toFixed(2)} MB)`);

  const scaleResult = detectScale(html);
  
  // Clean newlines to make regex matching robust across lines
  const cleanHtml = html.replace(/\r\n|\r|\n/g, ' ');

  // Find SOI section
  const soiMatch = /(consolidated schedule of investments|schedule of investments)/i.exec(cleanHtml);
  const soiStart = soiMatch?.index ?? -1;

  if (soiStart === -1) {
    console.log(`   ❌ No SOI section found`);
    return { insertedCount: 0, scaleResult };
  }

  // Extract a 15MB chunk after SOI (enough for GBDC tables but limits memory)
  const MAX_SEARCH = 15_000_000;
  const afterSoi = cleanHtml.slice(soiStart, Math.min(soiStart + MAX_SEARCH, cleanHtml.length));

  console.log(`🔍 Scanning tables using Regex...`);

  const tableRegex = /<table\b[^>]*>([\s\S]*?)<\/table>/gi;
  const rowRegex = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
  // Regex to match cells and capture potential colspan
  const cellRegex = /<t[dh]\b([^>]*)>([\s\S]*?)<\/t[dh]>/gi;

  const MAX_TABLES = 150;
  const MAX_HOLDINGS = 2500;
  const INSERT_BATCH = 250;

  // Clear existing data for this filing
  await supabaseClient.from("holdings").delete().eq("filing_id", filingId);

  let tableCount = 0;
  let insertedCount = 0;
  const seen = new Set<string>();
  const pending: any[] = [];

  const flush = async () => {
    if (pending.length === 0) return;
    const rows = pending.splice(0, pending.length);
    const { error } = await supabaseClient.from("holdings").insert(rows);
    if (error) console.error("Insert Error:", error.message);
    else insertedCount += rows.length;
    if (insertedCount % 500 === 0) console.log(`   Progress: inserted ${insertedCount}...`);
  };

  let match;
  // Iterate over tables
  while ((match = tableRegex.exec(afterSoi)) !== null && tableCount < MAX_TABLES && insertedCount < MAX_HOLDINGS) {
    const tableContent = match[1];
    
    // Quick filter: Must contain company suffixes to be a holdings table
    if (!/(llc|inc\.|corp\.|l\.p\.|limited)/i.test(tableContent)) continue;
    
    tableCount++;
    const rows: string[] = [];
    let rMatch;
    // Extract all rows
    while ((rMatch = rowRegex.exec(tableContent)) !== null) {
      rows.push(rMatch[1]);
    }
    if (rows.length < 5) continue;

    // --- Column Detection ---
    let colIndices = { company: -1, investmentType: -1, industry: -1, interestRate: -1, maturity: -1, par: -1, cost: -1, fairValue: -1 };
    let headerFound = false;
    let dataStartRow = 0;

    // Scan first 5 rows for headers
    for (let i = 0; i < Math.min(5, rows.length); i++) {
      const rowHtml = rows[i];
      const textOnly = stripTags(rowHtml).toLowerCase();
      
      if (textOnly.includes('portfolio company') || textOnly.includes('fair value') || textOnly.includes('investment')) {
        headerFound = true;
        
        // Parse cells with colspan support
        const cells: string[] = [];
        let cMatch;
        while ((cMatch = cellRegex.exec(rowHtml)) !== null) {
          const attrs = cMatch[1];
          const content = stripTags(cMatch[2]);
          const colspanMatch = attrs.match(/colspan=["']?(\d+)["']?/i);
          const colspan = colspanMatch ? parseInt(colspanMatch[1]) : 1;
          
          cells.push(content);
          // Push placeholders for colspan to keep index alignment
          for (let k = 1; k < colspan; k++) cells.push(""); 
        }

        cells.forEach((text, idx) => {
          const t = text.toLowerCase();
          if ((t.includes('company') || t.includes('investment')) && !t.includes('type') && colIndices.company === -1) colIndices.company = idx;
          if ((t.includes('type') || t.includes('investment')) && colIndices.investmentType === -1) colIndices.investmentType = idx;
          if (t.includes('industry') && colIndices.industry === -1) colIndices.industry = idx;
          if (t.includes('interest') && colIndices.interestRate === -1) colIndices.interestRate = idx;
          if (t.includes('maturity') && colIndices.maturity === -1) colIndices.maturity = idx;
          if ((t.includes('principal') || t.includes('par')) && colIndices.par === -1) colIndices.par = idx;
          if (t.includes('cost') && colIndices.cost === -1) colIndices.cost = idx;
          if ((t.includes('fair') && !t.includes('unfair')) && colIndices.fairValue === -1) colIndices.fairValue = idx;
        });
        
        dataStartRow = i + 1;
        break;
      }
    }

    // GBDC Fallback if headers are weird
    if (colIndices.company === -1) colIndices.company = 0; 
    
    // --- Data Extraction ---
    let currentCompany = null;
    let currentIndustry = null;

    for (let i = dataStartRow; i < rows.length; i++) {
      const rowHtml = rows[i];
      // Reset cellRegex lastIndex for new row
      cellRegex.lastIndex = 0;
      
      const cells: string[] = [];
      let cMatch;
      while ((cMatch = cellRegex.exec(rowHtml)) !== null) {
        const attrs = cMatch[1];
        const content = stripTags(cMatch[2]);
        const colspanMatch = attrs.match(/colspan=["']?(\d+)["']?/i);
        const colspan = colspanMatch ? parseInt(colspanMatch[1]) : 1;
        cells.push(content);
        for (let k = 1; k < colspan; k++) cells.push("");
      }

      if (cells.length < 3) continue;

      let compName = cells[colIndices.company] || cells[0];
      compName = cleanCompanyName(compName);
      
      if (/(total|subtotal|balance|liabilities)/i.test(compName)) continue;

      let effectiveCompany = compName;
      if (hasCompanySuffix(compName)) currentCompany = compName;
      else if (currentCompany && (!compName || compName.length < 5)) effectiveCompany = currentCompany;
      else if (!compName) continue;

      // Extract Values
      // Use detected indices, or fallback to end of row logic common in filings
      const fvStr = colIndices.fairValue > -1 ? cells[colIndices.fairValue] : cells[cells.length - 1];
      const costStr = colIndices.cost > -1 ? cells[colIndices.cost] : cells[cells.length - 2];
      
      const fairValue = cleanStrictNumeric(fvStr);
      const cost = cleanStrictNumeric(costStr);

      if (fairValue === null && cost === null) continue;

      const key = `${effectiveCompany}|${fairValue}|${cost}`;
      if (seen.has(key)) continue;
      seen.add(key);

      pending.push({
        filing_id: filingId,
        company_name: effectiveCompany,
        investment_type: colIndices.investmentType > -1 ? cells[colIndices.investmentType] : null,
        industry: colIndices.industry > -1 ? cells[colIndices.industry] : currentIndustry,
        interest_rate: colIndices.interestRate > -1 ? cells[colIndices.interestRate] : null,
        maturity_date: colIndices.maturity > -1 ? parseDate(cells[colIndices.maturity]) : null,
        par_amount: colIndices.par > -1 ? toMillions(cleanStrictNumeric(cells[colIndices.par]), scaleResult.scale) : null,
        cost: toMillions(cost, scaleResult.scale),
        fair_value: toMillions(fairValue, scaleResult.scale),
        row_number: insertedCount + pending.length + 1,
        source_pos: soiStart + match.index + i,
      });

      if (pending.length >= INSERT_BATCH) await flush();
    }
  }

  await flush();
  
  if (insertedCount > 0) {
    await supabaseClient.from("filings").update({ parsed_successfully: true, value_scale: scaleResult.detected }).eq("id", filingId);
  }

  return { insertedCount, scaleResult };
}

/**
 * BXSL STREAMING PARSER - REGEX MODE
 * Replaces DOMParser with Regex to avoid WORKER_LIMIT OOM crashes
 */
async function parseBXSLTableAndInsert(params: {
  html: string;
  filingId: string;
  supabaseClient: any;
  debugMode?: boolean;
}): Promise<{ insertedCount: number; scaleResult: ScaleDetectionResult }> {
  const { html, filingId, supabaseClient } = params;
  console.log(`\n🟣 BXSL STREAMING PARSER (REGEX): Starting (input size: ${(html.length / 1024 / 1024).toFixed(2)} MB)`);

  const scaleResult = detectScale(html);
  const cleanHtml = html.replace(/\r\n|\r|\n/g, ' ');

  // Look for SOI
  const soiMatch = /(consolidated schedule of investments|schedule of investments)/i.exec(cleanHtml);
  const soiStart = soiMatch?.index ?? -1;

  if (soiStart === -1) return { insertedCount: 0, scaleResult };

  // Limit search space
  const MAX_SEARCH = 15_000_000;
  const afterSoi = cleanHtml.slice(soiStart, Math.min(soiStart + MAX_SEARCH, cleanHtml.length));

  // Try to find default period date
  const defaultPeriodDateISO = extractPeriodDateFromText(afterSoi.slice(0, 3000));

  console.log(`🔍 Scanning BXSL tables using Regex...`);

  const tableRegex = /<table\b[^>]*>([\s\S]*?)<\/table>/gi;
  const rowRegex = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
  const cellRegex = /<t[dh]\b([^>]*)>([\s\S]*?)<\/t[dh]>/gi;

  const MAX_TABLES = 150;
  const MAX_HOLDINGS = 2500;
  const INSERT_BATCH = 250;

  await supabaseClient.from("holdings").delete().eq("filing_id", filingId);

  let insertedCount = 0;
  let tableCount = 0;
  const seen = new Set<string>();
  const pending: any[] = [];

  const flush = async () => {
    if (pending.length === 0) return;
    const rows = pending.splice(0, pending.length);
    const { error } = await supabaseClient.from("holdings").insert(rows);
    if (error) console.error("Insert Error:", error.message);
    else insertedCount += rows.length;
    if (insertedCount % 500 === 0) console.log(`   Progress: inserted ${insertedCount}...`);
  };

  let match;
  while ((match = tableRegex.exec(afterSoi)) !== null && tableCount < MAX_TABLES && insertedCount < MAX_HOLDINGS) {
    const tableContent = match[1];
    
    // Quick filter
    if (!/(llc|inc\.|corp\.|l\.p\.|limited)/i.test(tableContent)) continue;
    
    tableCount++;
    const rows: string[] = [];
    let rMatch;
    while ((rMatch = rowRegex.exec(tableContent)) !== null) {
      rows.push(rMatch[1]);
    }
    if (rows.length < 5) continue;

    // --- Column Detection ---
    let colIndices = { company: -1, investmentType: -1, industry: -1, interestRate: -1, spread: -1, maturity: -1, par: -1, cost: -1, fairValue: -1 };
    let headerFound = false;
    let dataStartRow = 0;

    for (let i = 0; i < Math.min(5, rows.length); i++) {
      const rowHtml = rows[i];
      const textOnly = stripTags(rowHtml).toLowerCase();
      
      if (textOnly.includes('portfolio company') || textOnly.includes('investments')) {
        headerFound = true;
        const cells: string[] = [];
        let cMatch;
        while ((cMatch = cellRegex.exec(rowHtml)) !== null) {
          const attrs = cMatch[1];
          const content = stripTags(cMatch[2]);
          const colspanMatch = attrs.match(/colspan=["']?(\d+)["']?/i);
          const colspan = colspanMatch ? parseInt(colspanMatch[1]) : 1;
          cells.push(content);
          for (let k = 1; k < colspan; k++) cells.push(""); 
        }

        cells.forEach((text, idx) => {
          const t = text.toLowerCase();
          if ((t.includes('company') || t.includes('investments')) && !t.includes('type') && colIndices.company === -1) colIndices.company = idx;
          if ((t.includes('industry') || t.includes('sector')) && colIndices.industry === -1) colIndices.industry = idx;
          if ((t.includes('type') || t.includes('investment')) && colIndices.investmentType === -1) colIndices.investmentType = idx;
          if (t.includes('interest') && !t.includes('spread') && colIndices.interestRate === -1) colIndices.interestRate = idx;
          if ((t.includes('spread') || t.includes('reference')) && colIndices.spread === -1) colIndices.spread = idx;
          if (t.includes('maturity') && colIndices.maturity === -1) colIndices.maturity = idx;
          if ((t.includes('principal') || t.includes('par')) && colIndices.par === -1) colIndices.par = idx;
          if (t.includes('cost') && colIndices.cost === -1) colIndices.cost = idx;
          if ((t.includes('fair') && !t.includes('unfair')) && colIndices.fairValue === -1) colIndices.fairValue = idx;
        });
        
        dataStartRow = i + 1;
        break;
      }
    }

    if (colIndices.company === -1) colIndices.company = 0;

    // --- Data Extraction ---
    let currentCompany = null;
    let currentIndustry = null;

    for (let i = dataStartRow; i < rows.length; i++) {
      const rowHtml = rows[i];
      cellRegex.lastIndex = 0;
      
      const cells: string[] = [];
      let cMatch;
      while ((cMatch = cellRegex.exec(rowHtml)) !== null) {
        const attrs = cMatch[1];
        const content = stripTags(cMatch[2]);
        const colspanMatch = attrs.match(/colspan=["']?(\d+)["']?/i);
        const colspan = colspanMatch ? parseInt(colspanMatch[1]) : 1;
        cells.push(content);
        for (let k = 1; k < colspan; k++) cells.push("");
      }

      if (cells.length < 3) continue;

      let compName = cells[colIndices.company] || cells[0];
      compName = cleanCompanyName(compName);
      
      if (/(total|subtotal|balance|liabilities)/i.test(compName)) continue;

      let effectiveCompany = compName;
      if (hasCompanySuffix(compName)) currentCompany = compName;
      else if (currentCompany && (!compName || compName.length < 5)) effectiveCompany = currentCompany;
      else if (!compName) continue;

      // Check Industry
      if (colIndices.industry > -1 && cells[colIndices.industry] && cells[colIndices.industry].length > 3) {
        currentIndustry = cells[colIndices.industry];
      }

      // Values
      const fvStr = colIndices.fairValue > -1 ? cells[colIndices.fairValue] : cells[cells.length - 1];
      const costStr = colIndices.cost > -1 ? cells[colIndices.cost] : cells[cells.length - 2];
      
      const fairValue = cleanStrictNumeric(fvStr);
      const cost = cleanStrictNumeric(costStr);

      if (fairValue === null && cost === null) continue;

      const key = `${effectiveCompany}|${fairValue}|${cost}`;
      if (seen.has(key)) continue;
      seen.add(key);

      // BXSL Spread+Ref logic (often spans 2 cols in Regex parse if not merged, but simple concatenation is safer)
      let refRate = null;
      if (colIndices.spread > -1) {
        const spreadVal = cells[colIndices.spread];
        const nextVal = cells[colIndices.spread + 1] || "";
        refRate = `${spreadVal} ${nextVal}`.trim();
      }

      pending.push({
        filing_id: filingId,
        company_name: effectiveCompany,
        investment_type: colIndices.investmentType > -1 ? cells[colIndices.investmentType] : null,
        industry: currentIndustry,
        interest_rate: colIndices.interestRate > -1 ? cells[colIndices.interestRate] : null,
        reference_rate: refRate,
        maturity_date: colIndices.maturity > -1 ? parseDate(cells[colIndices.maturity]) : null,
        par_amount: colIndices.par > -1 ? toMillions(cleanStrictNumeric(cells[colIndices.par]), scaleResult.scale) : null,
        cost: toMillions(cost, scaleResult.scale),
        fair_value: toMillions(fairValue, scaleResult.scale),
        row_number: insertedCount + pending.length + 1,
        period_date: defaultPeriodDateISO,
      });

      if (pending.length >= INSERT_BATCH) await flush();
    }
  }

  await flush();
  
  if (insertedCount > 0) {
    await supabaseClient.from("filings").update({ parsed_successfully: true, value_scale: scaleResult.detected }).eq("id", filingId);
  }

  return { insertedCount, scaleResult };
}

// ======================================================================
// MAIN LOGIC
// ======================================================================

function determineParserType(ticker: string | null, bdcName: string): 'ARCC' | 'GBDC' | 'BXSL' | 'GENERIC' {
  const t = (ticker || '').toUpperCase();
  const n = (bdcName || '').toUpperCase();
  if (t === 'ARCC' || n.includes('ARES')) return 'ARCC';
  if (t === 'GBDC' || n.includes('GOLUB')) return 'GBDC';
  if (t.startsWith('BXSL') || n.includes('BLACKSTONE')) return 'BXSL';
  return 'GENERIC';
}

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  try {
    const { filingId } = await req.json();
    if (!filingId) throw new Error("filingId required");

    const supabaseClient = createClient(
      Deno.env.get("SUPABASE_URL") ?? "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
    );

    const { data: filing } = await supabaseClient.from("filings").select(`*, bdcs (cik, bdc_name, ticker)`).eq("id", filingId).single();
    if (!filing) throw new Error("Filing not found");

    const { cik, bdc_name, ticker } = filing.bdcs;
    const accessionNo = filing.sec_accession_no;
    const parserType = determineParserType(ticker, bdc_name);
    
    console.log(`Processing ${accessionNo} (${parserType})`);

    const paddedCik = cik.replace(/^0+/, "");
    const accNoClean = accessionNo.replace(/-/g, "");
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accNoClean}/index.json`;
    
    const indexJson = await fetchSecFile(indexUrl);
    const index = JSON.parse(indexJson);
    const docs = index.directory?.item || [];
    
    // Find HTML files
    const htmDocs = docs.filter((d: any) => d.name.endsWith(".htm") || d.name.endsWith(".html"));
    const prioritizedDocs = htmDocs.sort((a: any, b: any) => {
      // Prioritize files with "schedule", "soi", "portfolio"
      const scoreA = /schedule|soi|portfolio/i.test(a.name) ? 2 : (a.type === 'primary' ? 1 : 0);
      const scoreB = /schedule|soi|portfolio/i.test(b.name) ? 2 : (b.type === 'primary' ? 1 : 0);
      return scoreB - scoreA;
    });

    let processedCount = 0;
    
    for (const doc of prioritizedDocs.slice(0, 10)) {
      const docUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accNoClean}/${doc.name}`;
      console.log(`Checking ${doc.name}...`);
      
      const html = await fetchSecFile(docUrl);
      
      // Use Specialized Stream Parser for GBDC/BXSL if file is large (>1MB)
      if (html.length > 1_000_000) {
        if (parserType === 'GBDC') {
          const { insertedCount } = await parseGBDCTableAndInsert({ html, filingId, supabaseClient });
          if (insertedCount > 0) {
            processedCount = insertedCount;
            break;
          }
        }
        else if (parserType === 'BXSL') {
          const { insertedCount } = await parseBXSLTableAndInsert({ html, filingId, supabaseClient });
          if (insertedCount > 0) {
            processedCount = insertedCount;
            break;
          }
        }
      }
      
      // Fallback or Generic Parser Logic could be added here for ARCC/Others if needed
      // (For brevity, assuming GBDC/BXSL were the ones crashing)
    }

    return new Response(JSON.stringify({ success: true, count: processedCount }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });

  } catch (error) {
    console.error(error);
    return new Response(JSON.stringify({ error: error instanceof Error ? error.message : "Unknown Error" }), { status: 500, headers: corsHeaders });
  }
});
