import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { TextLineStream } from "https://deno.land/std@0.168.0/streams/mod.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const SEC_USER_AGENT = "BDCTrackerApp/1.0 (contact@bdctracker.com)";

// Max bytes to process before returning (edge functions have ~2s CPU limit)
const MAX_BYTES_PER_RUN = 800_000;

// ======================================================================
// 1. HELPERS
// ======================================================================

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
  // Match MM/YYYY or MM/DD/YYYY
  const dateMatch = cleaned.match(/(\d{1,2})\/?(\d{1,2})?\/(\d{4})/);
  if (dateMatch) {
    const month = dateMatch[1].padStart(2, "0");
    const year = dateMatch[3];
    // If day is missing (MM/YYYY), default to 01
    const day = dateMatch[2] ? dateMatch[2].padStart(2, "0") : "01";
    return `${year}-${month}-${day}`;
  }
  return null;
}

function cleanCompanyName(name: string): string {
  if (!name) return "";
  let clean = name
    .replace(/<[^>]+>/g, "")
    .replace(/(\(\d+\))+\s*$/g, "") // Remove footnote refs like (1)(2)
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

    // Handle colspan to keep indices aligned
    const spanMatch = attrs.match(/colspan=["']?(\d+)["']?/i);
    const span = spanMatch ? parseInt(spanMatch[1]) : 1;
    for (let i = 1; i < span; i++) {
      cells.push(""); // Ghost cells
    }
  }
  return cells;
}

// ======================================================================
// 2. PARSING LOGIC (The "Backup" / Generic Logic)
// ======================================================================

// Terms that indicate a row is probably a Header or Footer (Junk)
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
];

function isJunkRow(text: string): boolean {
  const lower = text.toLowerCase().trim();
  if (lower.length < 3) return true;
  if (JUNK_TERMS.some((term) => lower.includes(term))) return true;
  // Numeric only or garbage
  if (/^[\d,.\s&#;()]+$/.test(lower)) return true;
  return false;
}

function processLine_Generic(line: string, state: any): any | null {
  const lower = line.toLowerCase();

  // 1. Detect Scale
  if (!state.scaleDetected) {
    if (lower.includes("amounts in thousands") || lower.includes("(in thousands)")) {
      state.scale = 0.001;
      state.scaleDetected = true;
    } else if (lower.includes("amounts in millions") || lower.includes("(in millions)")) {
      state.scale = 1;
      state.scaleDetected = true;
    }
  }

  // 2. Start Logic
  if (!state.inSOI) {
    if (lower.includes("schedule of investments")) {
      state.inSOI = true;
      console.log("✅ Entered Schedule of Investments");
    }
    return null;
  }

  // 3. Stop Logic (Strict)
  // Stop if we hit Financial Statements or Notes
  if (state.inSOI) {
    if (
      lower.includes("consolidated statements of assets") ||
      lower.includes("consolidated statements of operations") ||
      lower.includes("notes to consolidated") ||
      lower.includes("liabilities and net assets")
    ) {
      state.done = true;
      console.log("🛑 Reached end of Schedule of Investments (Stop Sign Found)");
      return null;
    }
  }

  // 4. Row Capture
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

  // Validation
  if (!company || isJunkRow(company)) return null;

  // DUCK TYPING (Dynamic Column Finding)
  // Instead of hardcoded indices, we look for data that "looks right"

  // 1. Find all numeric values
  const nums = cells.map((c, i) => ({ val: cleanNumeric(c), idx: i })).filter((x) => x.val !== null);
  if (nums.length < 2) return null;

  // Assume Fair Value is the LAST number, Cost is 2nd to LAST
  const fairValObj = nums[nums.length - 1];
  const costValObj = nums[nums.length - 2];

  // 2. Find Dates (Maturity)
  // Look for any cell that matches a date regex
  const maturityStr = cells.find((c) => /\d{1,2}\/?\d{0,2}\/\d{4}/.test(c));

  // 3. Find Interest Rates
  // Look for any cell containing "%"
  const interestStr = cells.find((c) => /%/.test(c));

  if (fairValObj.val === 0 && costValObj.val === 0) return null; // Skip empty rows

  return {
    company_name: company,
    investment_type: cells[1] || null, // Usually col 1
    interest_rate: interestStr || null,
    maturity_date: parseDate(maturityStr),
    cost: toMillions(costValObj.val, state.scale),
    fair_value: toMillions(fairValObj.val, state.scale),
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

    // Default to GENERIC parser for everyone (Back to Backup)
    const processLine = processLine_Generic;

    // Restore state
    const currentIndustryState = filing.current_industry_state;
    const state = {
      inSOI: startOffset > 0,
      done: false,
      scale: 0.001, // Default to thousands
      scaleDetected: false,
      rowCount: 0,
      currentIndustry: currentIndustryState || null,
    };

    // Pre-scan chunk for scale if just starting
    if (startOffset === 0) {
      const lowerChunk = chunk.toLowerCase();
      if (lowerChunk.includes("(in millions)") || lowerChunk.includes("amounts in millions")) {
        state.scale = 1;
        state.scaleDetected = true;
      }
    }

    const batch: any[] = [];
    const rowMatches = chunk.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi);

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
