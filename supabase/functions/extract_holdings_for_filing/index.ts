import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const SEC_USER_AGENT = "BDCTrackerApp/1.0 (contact@bdctracker.com)";

// Max bytes to process (safety limit)
const MAX_BYTES_PER_RUN = 800_000; 

// ======================================================================
// 1. FILTERS & CLEANERS (The "Strict Bouncer")
// ======================================================================

const JUNK_TERMS = [
  "total", "subtotal", "balance", "net assets", "net investment", 
  "cash", "liabilities", "receivable", "prepaid", "payable", 
  "distributions", "increase", "decrease", "equity", "capital",
  "($ in millions)", "($ in thousands)", "amounts in", 
  "amortized cost", "fair value", "principal", "maturity",
  "restricted", "unrealized", "realized", "gain", "loss",
  "beginning", "ending", "transfers", "purchases", "sales",
  "adjusted", "weighted", "average", "borrowings", "debt",
  "expenses", "income", "fees", "tax", "attributable",
  "voting", "non-voting", "member", "issuer", "investment",
  "at the market", "shares", "units"
];

function isJunkRow(text: string): boolean {
  const lower = text.toLowerCase().trim();
  
  // 1. Check Blacklist
  if (JUNK_TERMS.some(term => lower.includes(term))) return true;
  
  // 2. Check for numeric-only or garbage names (e.g. "1,048,546" or "1 &nbsp;")
  if (/^[\d,.\s&#;]+$/.test(lower)) return true;
  
  // 3. Check for parenthetical starts often used in financial statements
  if (lower.startsWith("(")) return true;

  // 4. Too short
  if (lower.length < 3) return true;

  return false;
}

function cleanNumeric(value: string | null | undefined): number | null {
  if (!value) return null;

  // Avoid parsing percentages (interest rates) as numeric amounts.
  if (/%/.test(value)) return null;

  let cleaned = value.replace(/[$,\s]/g, "").trim();
  if (!cleaned || cleaned === "-" || cleaned === "—") return null;

  const isNegative = cleaned.startsWith("(") && cleaned.endsWith(")");
  if (isNegative) cleaned = cleaned.slice(1, -1);

  if (!cleaned || /^(n\/?a|null)$/i.test(cleaned)) return null;

  const parsed = Number.parseFloat(cleaned);
  return Number.isNaN(parsed) ? null : isNegative ? -parsed : parsed;
}

function toMillions(value: number | null | undefined, scale: number): number | null {
  if (value === null || value === undefined) return null;
  return Math.round(value * scale * 100) / 100;
}

function parseDate(value: string | null | undefined): string | null {
  if (!value) return null;
  const cleaned = value.trim();
  const dateMatch = cleaned.match(/(\d{1,2})\/?(\d{1,2})?\/(\d{4})/);
  if (dateMatch) {
    const month = dateMatch[1].padStart(2, '0');
    const year = dateMatch[3];
    const day = dateMatch[2] ? dateMatch[2].padStart(2, '0') : '01';
    return `${year}-${month}-${day}`;
  }
  return null;
}

function cleanCompanyName(name: string): string {
  if (!name) return "";
  let clean = name.replace(/<[^>]+>/g, "")
                  .replace(/(\(\d+\))+\s*$/g, "") 
                  .replace(/&nbsp;/g, " ")
                  .replace(/&#160;/g, " ")
                  .trim();
  return clean;
}

function stripTags(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
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
    for (let i = 1; i < span; i++) cells.push(""); 
  }
  return cells;
}

// ======================================================================
// 2. PARSERS
// ======================================================================

// --- OBDC PARSER (Strict Stop + Specific Columns) ---
function processLine_OBDC(line: string, state: any): any | null {
  const lower = line.toLowerCase();

  // 1. Detect Scale
  if (!state.scaleDetected) {
    if (lower.includes("amounts in thousands")) {
      state.scale = 0.001;
      state.scaleDetected = true;
    } else if (lower.includes("amounts in millions")) {
      state.scale = 1;
      state.scaleDetected = true;
    }
  }

  // 2. START Logic
  if (!state.inSOI) {
    if (lower.includes("schedule of investments")) {
      state.inSOI = true;
      console.log("✅ OBDC: Entered Schedule of Investments");
    }
    return null;
  }

  // 3. STOP Logic (Critical Fix)
  if (state.inSOI) {
    if (lower.includes("consolidated statements of assets") || 
        lower.includes("consolidated statements of operations") ||
        lower.includes("notes to consolidated") ||
        lower.includes("liabilities and net assets")) {
      state.done = true;
      console.log("🛑 OBDC: Reached end of Schedule of Investments (Stop Sign Found)");
      return null;
    }
  }

  // 4. Row Processing
  if (line.includes("<tr")) {
    state.inRow = true;
    state.currentRow = "";
  }

  if (state.inRow) {
    state.currentRow += " " + line;
    if (line.includes("</tr>")) {
      state.inRow = false;
      return parseSingleRow_OBDC(state.currentRow, state);
    }
  }
  return null;
}

function parseSingleRow_OBDC(rowHtml: string, state: any): any | null {
  const cells = extractCellsWithColspan(rowHtml);
  if (cells.length < 5) return null;

  const company = cleanCompanyName(cells[0]);
  if (isJunkRow(company)) return null;

  // OBDC MAPPING (Column Indices):
  // [0] Company
  // [1] Investment Type
  // [2] Ref Rate (e.g. S+)
  // [3] Cash/Interest (e.g. 5.50%)
  // ...
  // [Last] Fair Value
  // [Last-1] Cost
  
  // Find Fair Value at the end (safest way)
  let fvIdx = cells.length - 1;
  let costIdx = cells.length - 2;
  
  // Backtrack to find last populated number if there are trailing empty cells
  for (let i = cells.length - 1; i >= 6; i--) {
     if (cleanNumeric(cells[i]) !== null) {
        fvIdx = i;
        costIdx = i - 1;
        break;
     }
  }

  const fv = cleanNumeric(cells[fvIdx]);
  const cost = cleanNumeric(cells[costIdx]);

  if (fv === null && cost === null) return null;

  // Attempt to find Par (usually before cost)
  const par = cleanNumeric(cells[costIdx - 1]);

  return {
    company_name: company,
    investment_type: cells[1] || null,
    reference_rate: cells[2] || null,
    interest_rate: cells[3] || null,
    maturity_date: parseDate(cells[5]) || parseDate(cells[6]), 
    par_amount: toMillions(par, state.scale),
    cost: toMillions(cost, state.scale),
    fair_value: toMillions(fv, state.scale),
    row_number: state.rowCount++
  };
}

// --- ARCC PARSER (Strict Stop) ---
function processLine_ARCC(line: string, state: any): any | null {
  const lower = line.toLowerCase();
  
  if (!state.scaleDetected) {
    if (lower.includes("in millions")) state.scale = 1;
    else if (lower.includes("in thousands")) state.scale = 0.001;
    if (state.scale !== 0.001) state.scaleDetected = true;
  }

  if (!state.inSOI && lower.includes("schedule of investments")) {
    state.inSOI = true;
    console.log("✅ ARCC: Entered SOI");
  }
  
  // ARCC Stop logic
  if (state.inSOI && (lower.includes("notes to consolidated") || lower.includes("notes to financial"))) {
    state.done = true;
    return null;
  }

  if (line.includes("<tr")) {
    state.inRow = true;
    state.currentRow = "";
  }
  if (state.inRow) {
    state.currentRow += " " + line;
    if (line.includes("</tr>")) {
      state.inRow = false;
      return parseSingleRow_Generic(state.currentRow, state);
    }
  }
  return null;
}

// --- GENERIC PARSER (Fallback) ---
function processLine_Generic(line: string, state: any): any | null {
  const lower = line.toLowerCase();

  if (!state.scaleDetected) {
    if (lower.includes("in thousands")) state.scale = 0.001;
    else if (lower.includes("in millions")) state.scale = 1;
    if (state.scale !== 0.001) state.scaleDetected = true;
  }

  if (!state.inSOI && lower.includes("schedule of investments")) {
    state.inSOI = true;
  }
  if (state.inSOI && (lower.includes("notes to") || lower.includes("consolidated statements"))) {
    state.done = true;
    return null;
  }

  if (line.includes("<tr")) {
    state.inRow = true;
    state.currentRow = "";
  }
  if (state.inRow) {
    state.currentRow += " " + line;
    if (line.includes("</tr>")) {
      state.inRow = false;
      return parseSingleRow_Generic(state.currentRow, state);
    }
  }
  return null;
}

function parseSingleRow_Generic(rowHtml: string, state: any): any | null {
  const cells = extractCellsWithColspan(rowHtml);
  if (cells.length < 3) return null;

  const company = cleanCompanyName(cells[0]);
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

// ======================================================================
// 3. MAIN HANDLER
// ======================================================================

function getProcessor(ticker: string, bdcName: string) {
  const t = (ticker || "").toUpperCase();
  const n = (bdcName || "").toUpperCase();

  if (t === 'OBDC' || n.includes('BLUE OWL')) return processLine_OBDC;
  if (t === 'ARCC' || n.includes('ARES')) return processLine_ARCC;
  
  return processLine_Generic; 
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

    const { data: filing } = await supabaseClient.from("filings").select("*, bdcs(*)").eq("id", filingId).single();
    if (!filing) throw new Error("Filing not found");

    const { cik, ticker, bdc_name } = filing.bdcs;
    const accNo = filing.sec_accession_no.replace(/-/g, "");
    const startOffset = filing.current_byte_offset || 0;
    
    // File Fetching
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${cik.replace(/^0+/, "")}/${accNo}/index.json`;
    const indexRes = await fetch(indexUrl, { headers: { "User-Agent": SEC_USER_AGENT } });
    const indexJson = await indexRes.json();
    
    const htmDocs = indexJson.directory.item.filter((d: any) => d.name.endsWith(".htm") && !d.name.includes("-index"));
    let targetDoc = htmDocs.find((d: any) => d.name.toLowerCase().includes((ticker || "").toLowerCase()));
    
    // Fallback: Pick largest HTM file if no ticker match
    if (!targetDoc) targetDoc = htmDocs.sort((a: any, b: any) => parseInt(b.size) - parseInt(a.size))[0];
    
    if (!targetDoc) throw new Error("No suitable HTM document found");
    
    const totalSize = parseInt(targetDoc.size) || 0;
    const docUrl = `https://www.sec.gov/Archives/edgar/data/${cik.replace(/^0+/, "")}/${accNo}/${targetDoc.name}`;

    console.log(`Chunk-Reading: ${docUrl} [${ticker}] Offset: ${startOffset}`);
    
    const endOffset = Math.min(startOffset + MAX_BYTES_PER_RUN, totalSize);
    const response = await fetch(docUrl, { 
      headers: { "User-Agent": SEC_USER_AGENT, "Range": `bytes=${startOffset}-${endOffset}` } 
    });

    if (!response.body) throw new Error("No body");
    
    const chunk = await response.text();
    const processLine = getProcessor(ticker, bdc_name);
    
    // Restore state (persisted between chunk calls in filings.current_industry_state)
    let prev: any = null;
    try {
      const raw = (filing.current_industry_state ?? "").trim();
      if (raw.startsWith("{")) prev = JSON.parse(raw);
    } catch {
      prev = null;
    }

    const state: any = {
      inSOI: prev?.inSOI ?? false,
      done: false,
      scale: prev?.scale ?? 0.001,
      scaleDetected: prev?.scaleDetected ?? false,
      rowCount: prev?.rowCount ?? 0,
      inRow: false,
      currentRow: "",
      carry: prev?.carry ?? "",
    };
    // Combine with any carryover from a previous chunk so we don't lose rows split across boundaries
    const combinedHtml = state.carry + chunk;
    const lastClose = combinedHtml.lastIndexOf("</tr>");
    const parseHtml = lastClose >= 0 ? combinedHtml.slice(0, lastClose + 5) : "";
    state.carry = lastClose >= 0 ? combinedHtml.slice(lastClose + 5) : combinedHtml;

    const batch: any[] = [];
    const rowMatches = parseHtml.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi);

    for (const match of rowMatches) {
      if (state.done) break;
      const result = processLine(match[0], state);
      if (result) batch.push({ ...result, filing_id: filingId });
    }
    
    let totalInserted = 0;
    if (batch.length > 0) {
      const { error } = await supabaseClient.from("holdings").insert(batch);
      if (error) console.error("Insert error:", error.message);
      else totalInserted = batch.length;
    }
    
    const isComplete = state.done || endOffset >= totalSize;
    const nextOffset = isComplete ? 0 : endOffset;
    
    const persistedState = isComplete
      ? null
      : JSON.stringify({
          inSOI: state.inSOI,
          scale: state.scale,
          scaleDetected: state.scaleDetected,
          rowCount: state.rowCount,
          carry: state.carry,
        });

    await supabaseClient.from("filings").update({ 
      current_byte_offset: nextOffset,
      total_file_size: totalSize,
      current_industry_state: persistedState,
      parsed_successfully: isComplete && totalInserted > 0,
    }).eq("id", filingId);
    
    return new Response(JSON.stringify({ 
      success: true, 
      status: isComplete ? "complete" : "partial",
      count: totalInserted,
      nextOffset 
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });

  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Parse error:", message);
    return new Response(JSON.stringify({ error: message }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
