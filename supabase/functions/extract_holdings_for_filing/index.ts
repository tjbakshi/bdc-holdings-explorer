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
// 1. HELPERS & FILTERS (The "Strict Bouncer")
// ======================================================================

// Terms that indicate a row is NOT a portfolio company
const JUNK_TERMS = [
  "total",
  "subtotal",
  "balance",
  "net assets",
  "net investment",
  "cash",
  "liabilities",
  "receivable",
  "prepaid",
  "payable",
  "distributions",
  "increase",
  "decrease",
  "equity",
  "capital",
  "($ in millions)",
  "($ in thousands)",
  "amounts in",
  "amortized cost",
  "fair value",
  "principal",
  "maturity",
  "restricted",
  "unrealized",
  "realized",
  "gain",
  "loss",
  "beginning",
  "ending",
  "transfers",
  "purchases",
  "sales",
  "adjusted",
  "weighted",
  "average",
  "borrowings",
  "debt",
  "expenses",
  "income",
  "fees",
  "tax",
  "attributable",
  "voting",
  "non-voting",
  "member",
  "issuer",
  "investment",
  "at the market",
  "shares",
  "units",
];

function isJunkRow(text: string): boolean {
  const lower = text.toLowerCase().trim();

  // 1. Check Blacklist
  if (JUNK_TERMS.some((term) => lower.includes(term))) return true;

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
  let cleaned = value.replace(/[$,\s]/g, "").trim();
  // Remove HTML entities like &nbsp;
  cleaned = cleaned.replace(/&nbsp;/g, "").replace(/&#160;/g, "");

  if (!cleaned || cleaned === "-" || cleaned === "—" || cleaned === "") return null;

  const isNegative = cleaned.startsWith("(") && cleaned.endsWith(")");
  if (isNegative) cleaned = cleaned.slice(1, -1);

  const parsed = parseFloat(cleaned);
  return isNaN(parsed) ? null : isNegative ? -parsed : parsed;
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
    const month = dateMatch[1].padStart(2, "0");
    const year = dateMatch[3];
    const day = dateMatch[2] ? dateMatch[2].padStart(2, "0") : "01";
    return `${year}-${month}-${day}`;
  }
  return null;
}

function cleanCompanyName(name: string): string {
  if (!name) return "";
  let clean = name
    .replace(/<[^>]+>/g, "")
    .replace(/(\(\d+\))+\s*$/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&#160;/g, " ")
    .trim();
  return clean;
}

function stripTags(html: string): string {
  return html
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
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
// 2. PARSING LOGIC (OBDC Specialist)
// ======================================================================

function parseSingleRow(rowHtml: string, state: any): any | null {
  const cells = extractCellsWithColspan(rowHtml);

  // Validation: Row must have enough columns (OBDC usually has ~9)
  if (cells.length < 5) return null;

  const company = cleanCompanyName(cells[0]);

  // Validation: Must be a valid company name (The Strict Bouncer)
  if (isJunkRow(company)) return null;

  // OBDC MAPPING (Column Indices based on your screenshot):
  // [0] Company
  // [1] Investment Type
  // [2] Ref Rate (e.g. S+)
  // [3] Cash/Interest (e.g. 5.50%)
  // ...
  // [Last] Fair Value
  // [Last-1] Cost

  // Find Fair Value at the end (safest way to handle variable columns)
  let fvIdx = cells.length - 1;
  let costIdx = cells.length - 2;

  // Backtrack if there are empty trailing cells
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
    row_number: state.rowCount++,
  };
}

// ======================================================================
// 3. MAIN HANDLER
// ======================================================================

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  try {
    const { filingId } = await req.json();
    if (!filingId) throw new Error("filingId required");

    const supabaseClient = createClient(
      Deno.env.get("SUPABASE_URL") ?? "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "",
    );

    const { data: filing } = await supabaseClient.from("filings").select("*, bdcs(*)").eq("id", filingId).single();
    if (!filing) throw new Error("Filing not found");

    const { cik, ticker } = filing.bdcs;
    const accNo = filing.sec_accession_no.replace(/-/g, "");
    const startOffset = filing.current_byte_offset || 0;

    // File Fetching Logic
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
      headers: { "User-Agent": SEC_USER_AGENT, Range: `bytes=${startOffset}-${endOffset}` },
    });

    if (!response.body) throw new Error("No body");

    const chunk = await response.text();

    // Restore state
    const currentIndustryState = filing.current_industry_state;
    const state = {
      inSOI: startOffset > 0, // Assume inside if resuming
      done: false,
      scale: 0.001,
      scaleDetected: false,
      rowCount: 0,
      currentIndustry: currentIndustryState || null,
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

    // Check for START of SOI
    if (!state.inSOI && lowerChunk.includes("schedule of investments")) {
      state.inSOI = true;
      console.log("✅ Found Schedule of Investments");
    }

    // Check for STOP Signs (Critical for OBDC)
    // OBDC puts Balance Sheet immediately after SOI, often causing data bleed
    if (
      lowerChunk.includes("notes to consolidated") ||
      lowerChunk.includes("notes to financial") ||
      lowerChunk.includes("consolidated statements of assets") ||
      lowerChunk.includes("consolidated statements of operations") ||
      lowerChunk.includes("liabilities and net assets")
    ) {
      state.done = true;
      console.log("🛑 Found End of Schedule (Stop Sign)");
    }

    const batch: any[] = [];
    const rowMatches = chunk.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi);

    for (const match of rowMatches) {
      if (!state.inSOI) continue;
      // If we hit the done flag in previous chunk logic, stop parsing rows
      if (state.done) break;

      const rowHtml = match[0];
      const result = parseSingleRow(rowHtml, state);

      if (result) {
        batch.push({ ...result, filing_id: filingId });
      }
    }

    let totalInserted = 0;
    if (batch.length > 0) {
      const { error } = await supabaseClient.from("holdings").insert(batch);
      if (error) console.error("Insert error:", error.message);
      else totalInserted = batch.length;
    }

    const isComplete = state.done || endOffset >= totalSize;
    const nextOffset = isComplete ? 0 : endOffset;

    await supabaseClient
      .from("filings")
      .update({
        current_byte_offset: nextOffset,
        total_file_size: totalSize,
        current_industry_state: state.currentIndustry,
        parsed_successfully: isComplete && totalInserted > 0,
        data_source: "edge-parser",
      })
      .eq("id", filingId);

    const status = isComplete ? "complete" : "partial";
    console.log(`✅ ${status}: ${totalInserted} holdings, offset ${startOffset}->${nextOffset}/${totalSize}`);

    return new Response(
      JSON.stringify({
        success: true,
        status,
        count: totalInserted,
        nextOffset: isComplete ? null : nextOffset,
        progress: Math.round((endOffset / totalSize) * 100),
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error("Parse error:", errorMessage);
    return new Response(JSON.stringify({ error: errorMessage }), { status: 500, headers: corsHeaders });
  }
});
