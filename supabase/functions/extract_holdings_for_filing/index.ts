import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const SEC_USER_AGENT = "BDCTrackerApp/1.0 (contact@bdctracker.com)";

// Max bytes to process before returning (edge functions have ~2s CPU limit)
const MAX_BYTES_PER_RUN = 800_000; 

// ======================================================================
// STRICT BOUNCER - Blacklist for financial statement junk
// ======================================================================

const JUNK_PATTERNS = [
  // Financial statement terms
  /\btotal\b/i, /\bsubtotal\b/i, /\bbalance\b/i,
  /\bnet assets\b/i, /\bnet investment\b/i,
  // Balance sheet terms  
  /\bcash\b/i, /\bliabilities\b/i, /\breceivable\b/i,
  /\bprepaid\b/i, /\bpayable\b/i,
  // Income statement terms
  /\bdistributions\b/i, /\bincrease\b/i, /\bdecrease\b/i,
  /\bequity\b/i, /\bcapital\b/i,
  // Header terms that might slip through
  /\bamortized cost\b/i, /\bfair value\b/i, 
  /\bprincipal\b/i, /\bmaturity\b/i,
  // Other junk
  /\baccounts\b/i, /\baccrued\b/i, /\binterest income\b/i,
  /\bdividend\b/i, /\bexpense\b/i, /\bfees\b/i,
  /\bborrowings\b/i, /\bdebt\b/i, /\bnotes payable\b/i,
  /\bcontingent\b/i, /\bcommitment\b/i
];

function isJunkRow(companyName: string): boolean {
  if (!companyName) return true;
  
  const name = companyName.trim();
  
  // Empty or too short
  if (name.length < 3) return true;
  
  // Purely numeric (like "123" or "1,234")
  if (/^[\d,.\s-]+$/.test(name)) return true;
  
  // Starts with parentheses (usually a footnote or negative number)
  if (name.startsWith('(')) return true;
  
  // Check against blacklist patterns
  for (const pattern of JUNK_PATTERNS) {
    if (pattern.test(name)) return true;
  }
  
  return false;
}

// ======================================================================
// STOP SIGN - Headers that indicate end of Schedule of Investments
// ======================================================================

const STOP_SIGN_PATTERNS = [
  /consolidated\s+statements?\s+of\s+assets/i,
  /consolidated\s+statements?\s+of\s+operations/i,
  /notes\s+to\s+consolidated/i,
  /notes\s+to\s+financial/i,
  /liabilities\s+and\s+net\s+assets/i,
  /the\s+accompanying\s+notes\s+are\s+an\s+integral\s+part/i,
  /statement\s+of\s+operations/i,
  /statements?\s+of\s+changes/i
];

function isStopSign(text: string): boolean {
  const lower = text.toLowerCase();
  for (const pattern of STOP_SIGN_PATTERNS) {
    if (pattern.test(lower)) return true;
  }
  return false;
}

// ======================================================================
// HELPERS
// ======================================================================

function cleanNumeric(value: string | null | undefined): number | null {
  if (!value) return null;
  let cleaned = value.replace(/[$,\s]/g, '').trim();
  if (!cleaned || cleaned === '-' || cleaned === '—' || cleaned === '') return null;
  const isNegative = cleaned.startsWith('(') && cleaned.endsWith(')');
  if (isNegative) cleaned = cleaned.slice(1, -1);
  const parsed = parseFloat(cleaned);
  return isNaN(parsed) ? null : (isNegative ? -parsed : parsed);
}

function toMillions(value: number | null | undefined, scale: number): number | null {
  if (value === null || value === undefined) return null;
  return Math.round(value * scale * 100) / 100;
}

function parseDate(value: string | null | undefined): string | null {
  if (!value) return null;
  const cleaned = value.trim();
  // Match MM/YYYY or MM/DD/YYYY
  const dateMatch = cleaned.match(/(\d{1,2})\/?(\d{1,2})?\/(\d{4})/);
  if (dateMatch) {
    const month = dateMatch[1].padStart(2, '0');
    const year = dateMatch[3];
    // If day is missing (MM/YYYY), default to 01
    const day = dateMatch[2] ? dateMatch[2].padStart(2, '0') : '01';
    return `${year}-${month}-${day}`;
  }
  return null;
}

function cleanCompanyName(name: string): string {
  if (!name) return "";
  return name.replace(/<[^>]+>/g, "").replace(/(\(\d+\))+\s*$/g, "").replace(/&amp;/g, "&").trim();
}

function stripTags(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/&nbsp;/g, " ").replace(/\s+/g, " ").trim();
}

function extractCellsWithColspan(rowHtml: string): string[] {
  const cells: string[] = [];
  const cellRe = /<t[dh]\b([^>]*)>([\s\S]*?)<\/t[dh]>/gi;
  let match;
  
  while ((match = cellRe.exec(rowHtml)) !== null) {
    const attrs = match[1];
    const content = stripTags(match[2]);
    cells.push(content);
    
    const spanMatch = attrs.match(/colspan=["']?(\d+)["']?/i);
    const span = spanMatch ? parseInt(spanMatch[1]) : 1;
    for (let i = 1; i < span; i++) {
      cells.push(""); // Ghost cells for alignment
    }
  }
  return cells;
}

// ======================================================================
// PARSING LOGIC (The Switchboard)
// ======================================================================

// 1. OBDC SPECIALIST PARSER (Blue Owl)
// Columns: [0]Company, [1]Inv, [2]RefRate, [3]Cash/Interest, [4]PIK, [5]Mat, [6]Par, [7]Cost, [8]FV
function parseRow_OBDC(cells: string[], state: any): any | null {
  // Need at least fair value column
  if (cells.length < 8) return null;

  const company = cleanCompanyName(cells[0]);
  
  // STRICT BOUNCER: Check if this is a junk row
  if (isJunkRow(company)) return null;

  // Find numeric columns from the end
  const numericCells = cells.map((c, i) => ({ val: cleanNumeric(c), idx: i, raw: c })).filter(x => x.val !== null);
  
  if (numericCells.length < 2) return null;
  
  // Fair Value = last numeric, Cost = second to last
  const fvObj = numericCells[numericCells.length - 1];
  const costObj = numericCells[numericCells.length - 2];
  const parObj = numericCells.length >= 3 ? numericCells[numericCells.length - 3] : null;
  
  const fairVal = fvObj.val;
  const costVal = costObj.val;

  if (fairVal === null || fairVal === 0) return null;

  // OBDC specific column mapping:
  // Col 2 = Reference Rate (e.g., "S+")
  // Col 3 = Cash/Interest Rate (e.g., "5.50%")
  const referenceRate = cells[2] || null;
  const interestRate = cells[3] || null;

  return {
    company_name: company,
    investment_type: cells[1] || null,
    reference_rate: referenceRate,
    interest_rate: interestRate,
    maturity_date: parseDate(cells[5]),
    par_amount: parObj ? toMillions(parObj.val, state.scale) : null,
    cost: toMillions(costVal, state.scale),
    fair_value: toMillions(fairVal, state.scale),
    row_number: state.rowCount++
  };
}

// 2. GENERIC / BXSL PARSER (Fallback)
// Uses regex searching ("duck typing") for columns
function parseRow_Generic(cells: string[], state: any): any | null {
  if (cells.length < 3) return null;

  const company = cleanCompanyName(cells[0]);
  
  // STRICT BOUNCER: Check if this is a junk row
  if (isJunkRow(company)) return null;

  const nums = cells.map((c, i) => ({ val: cleanNumeric(c), idx: i })).filter(x => x.val !== null);
  if (nums.length < 2) return null;

  const fairValObj = nums[nums.length - 1]; 
  const costValObj = nums[nums.length - 2];
  
  if (fairValObj.val === 0) return null;

  const maturityStr = cells.find(c => /\d{1,2}\/\d{4}/.test(c));
  const interestStr = cells.find(c => /%/.test(c));

  return {
    company_name: company,
    investment_type: cells[1] || null,
    interest_rate: interestStr || null,
    maturity_date: parseDate(maturityStr),
    cost: toMillions(costValObj.val, state.scale),
    fair_value: toMillions(fairValObj.val, state.scale),
    row_number: state.rowCount++
  };
}

// 3. DISPATCHER
function parseRow(rowHtml: string, state: any): any | null {
  // STOP SIGN CHECK: Before parsing, check if this row signals end of SOI
  const rowText = stripTags(rowHtml);
  if (state.inSOI && isStopSign(rowText)) {
    state.done = true;
    console.log(`🛑 Stop sign detected: "${rowText.substring(0, 60)}..."`);
    return null;
  }
  
  const cells = extractCellsWithColspan(rowHtml);
  const ticker = (state.ticker || "").toUpperCase();

  if (ticker === "OBDC") {
    return parseRow_OBDC(cells, state);
  } 
  
  // Default / BXSL / GBDC / ARCC
  return parseRow_Generic(cells, state);
}

// ======================================================================
// MAIN HANDLER - CHUNKED PROCESSING
// ======================================================================

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  try {
    const { filingId } = await req.json();
    if (!filingId) throw new Error("filingId required");

    const supabaseClient = createClient(
      Deno.env.get("SUPABASE_URL") ?? "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
    );

    const { data: filing } = await supabaseClient.from("filings").select("*, bdcs(*)").eq("id", filingId).single();
    if (!filing) throw new Error("Filing not found");

    const { cik, ticker } = filing.bdcs;
    const accNo = filing.sec_accession_no.replace(/-/g, "");
    const startOffset = filing.current_byte_offset || 0;
    
    // Get file info
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${cik.replace(/^0+/, "")}/${accNo}/index.json`;
    const indexRes = await fetch(indexUrl, { headers: { "User-Agent": SEC_USER_AGENT } });
    const indexJson = await indexRes.json();
    
    const htmDocs = indexJson.directory.item.filter((d: any) => 
      d.name.endsWith(".htm") && !d.name.includes("-index")
    );
    
    const tickerLower = (ticker || "").toLowerCase();
    let targetDoc = htmDocs.find((d: any) => d.name.toLowerCase().includes(tickerLower) && d.name.includes("-"));
    
    if (!targetDoc) {
      targetDoc = htmDocs.reduce((largest: any, doc: any) => {
        const size = parseInt(doc.size) || 0;
        const largestSize = parseInt(largest?.size) || 0;
        return size > largestSize ? doc : largest;
      }, htmDocs[0]);
    }
    
    if (!targetDoc) throw new Error("No suitable HTM document found");
    
    const totalSize = parseInt(targetDoc.size) || 0;
    const docUrl = `https://www.sec.gov/Archives/edgar/data/${cik.replace(/^0+/, "")}/${accNo}/${targetDoc.name}`;

    console.log(`Chunk-Reading: ${docUrl} [${ticker}] from offset ${startOffset}`);
    
    // Use Range header to fetch only a chunk
    const endOffset = Math.min(startOffset + MAX_BYTES_PER_RUN, totalSize);
    const response = await fetch(docUrl, { 
      headers: { 
        "User-Agent": SEC_USER_AGENT,
        "Range": `bytes=${startOffset}-${endOffset}`
      } 
    });

    if (!response.body) throw new Error("No body");
    
    const chunk = await response.text();
    console.log(`Fetched ${chunk.length} bytes`);
    
    // Restore state from filing record
    const currentIndustryState = filing.current_industry_state;
    const state = { 
      inSOI: startOffset > 0, // If resuming, assume we're in SOI or looking for end
      done: false, 
      scale: 0.001, 
      scaleDetected: false, 
      rowCount: 0,
      currentIndustry: currentIndustryState || null,
      ticker: ticker // Pass ticker to state for switching logic
    };
    
    // Detect scale
    const lowerChunk = chunk.toLowerCase();
    if (lowerChunk.includes("(in millions)") || lowerChunk.includes("amounts in millions")) {
      state.scale = 1;
      state.scaleDetected = true;
    } else if (lowerChunk.includes("in thousands") || lowerChunk.includes("amounts in thousands")) {
      state.scale = 0.001;
      state.scaleDetected = true;
    }
    
    // Check for SOI start
    if (!state.inSOI && lowerChunk.includes("schedule of investments")) {
      state.inSOI = true;
      console.log("✅ Found Schedule of Investments");
    }
    
    // GLOBAL STOP SIGN CHECK on entire chunk
    if (state.inSOI) {
      for (const pattern of STOP_SIGN_PATTERNS) {
        if (pattern.test(lowerChunk)) {
          state.done = true;
          console.log("✅ Found end of Schedule of Investments (chunk-level stop sign)");
          break;
        }
      }
    }
    
    // Parse rows from chunk
    const batch: any[] = [];
    const rowMatches = chunk.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi);
    
    for (const match of rowMatches) {
      // Exit early if done flag is set
      if (state.done) break;
      
      if (!state.inSOI) continue;
      
      const rowHtml = match[0];
      // DISPATCHER CALL: Uses state.ticker to pick the right parser
      const result = parseRow(rowHtml, state);
      
      if (result) {
        batch.push({ ...result, filing_id: filingId });
      }
    }
    
    // Insert holdings
    let totalInserted = 0;
    if (batch.length > 0) {
      const { error } = await supabaseClient.from("holdings").insert(batch);
      if (error) {
        console.error("Insert error:", error.message);
      } else {
        totalInserted = batch.length;
        console.log(`Inserted ${totalInserted} holdings`);
      }
    }
    
    // Determine if we're done or need another chunk
    const isComplete = state.done || endOffset >= totalSize;
    const nextOffset = isComplete ? 0 : endOffset;
    
    // Update filing with progress
    await supabaseClient.from("filings").update({ 
      current_byte_offset: nextOffset,
      total_file_size: totalSize,
      current_industry_state: state.currentIndustry,
      parsed_successfully: isComplete && (totalInserted > 0 || startOffset > 0),
      data_source: 'edge-parser'
    }).eq("id", filingId);
    
    const status = isComplete ? "complete" : "partial";
    console.log(`✅ ${status}: ${totalInserted} holdings, offset ${startOffset}->${nextOffset}/${totalSize}`);

    return new Response(JSON.stringify({ 
      success: true, 
      status,
      count: totalInserted,
      nextOffset: isComplete ? null : nextOffset,
      progress: Math.round((endOffset / totalSize) * 100)
    }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });

  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error("Parse error:", errorMessage);
    return new Response(JSON.stringify({ error: errorMessage }), { status: 500, headers: corsHeaders });
  }
});
