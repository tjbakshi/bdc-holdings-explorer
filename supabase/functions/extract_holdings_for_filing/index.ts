import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { TextLineStream } from "https://deno.land/std@0.168.0/streams/mod.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const SEC_USER_AGENT = "BDCTrackerApp/1.0 (contact@bdctracker.com)";

// ======================================================================
// 1. HELPERS
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
  const mmddyyyy = cleaned.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (mmddyyyy) return `${mmddyyyy[3]}-${mmddyyyy[1].padStart(2, '0')}-${mmddyyyy[2].padStart(2, '0')}`;
  return null;
}

function cleanCompanyName(name: string): string {
  if (!name) return "";
  return name.replace(/<[^>]+>/g, "").replace(/(\(\d+\))+\s*$/g, "").trim();
}

function stripTags(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

// ======================================================================
// 2. PARSING LOGIC (State Machine)
// ======================================================================

function processLine_ARCC(line: string, state: any): any | null {
  const lower = line.toLowerCase();

  // 1. Detect Scale
  if (!state.scaleDetected) {
    if (lower.includes("(in millions)") || lower.includes("amounts in millions")) {
      state.scale = 1;
      state.scaleDetected = true;
    }
  }

  // 2. State Switcher
  if (!state.inSOI) {
    if (lower.includes("schedule of investments")) {
      state.inSOI = true;
      console.log("✅ Entered Schedule of Investments");
    }
    return null;
  }

  // 3. Stop Logic
  if (state.inSOI && (lower.includes("notes to consolidated") || lower.includes("notes to financial"))) {
    state.done = true;
    return null;
  }

  // 4. Row Capture Logic
  // We collect lines until we have a full <tr>...</tr> block
  if (line.includes("<tr")) {
    state.inRow = true;
    state.currentRow = "";
  }

  if (state.inRow) {
    state.currentRow += " " + line; // Add space to prevent merged words
    
    if (line.includes("</tr>")) {
      state.inRow = false;
      const rowHtml = state.currentRow;
      
      // Parse the completed row immediately
      return parseSingleRow(rowHtml, state);
    }
  }

  return null;
}

function parseSingleRow(rowHtml: string, state: any): any | null {
  // Simple Regex to extract cells (handling colspan logic is hard in pure line-stream, 
  // but this is robust enough for data extraction)
  const cells: string[] = [];
  const cellRe = /<t[dh]\b[^>]*>([\s\S]*?)<\/t[dh]>/gi;
  let match;
  
  while ((match = cellRe.exec(rowHtml)) !== null) {
    const content = stripTags(match[1]);
    cells.push(content);
  }

  if (cells.length < 3) return null;

  // Basic Column Mapping (Simplified for ARCC/General)
  // We assume Fair Value is last numeric, Cost is second to last
  const nums = cells.map(c => cleanNumeric(c));
  const validNums = nums.map((n, i) => ({ val: n, idx: i })).filter(x => x.val !== null);

  if (validNums.length < 2) return null;

  const fairValObj = validNums[validNums.length - 1]; // Last number
  const costValObj = validNums[validNums.length - 2]; // Second last number

  const company = cleanCompanyName(cells[0]);
  
  // Validation
  if (!company || company.length < 3 || /(total|subtotal)/i.test(company)) return null;
  if (!fairValObj || fairValObj.val === 0) return null;

  return {
    company_name: company,
    fair_value: toMillions(fairValObj.val, state.scale),
    cost: toMillions(costValObj.val, state.scale),
    // Fallbacks for other fields to save logic complexity
    investment_type: cells[1] || null, 
    industry: null, 
    row_number: state.rowCount++
  };
}

// ======================================================================
// 3. MAIN HANDLER (Line Streaming)
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

    // Fetch Filing Info
    const { data: filing } = await supabaseClient.from("filings").select("*, bdcs(*)").eq("id", filingId).single();
    const { cik, ticker } = filing.bdcs;
    const accNo = filing.sec_accession_no.replace(/-/g, "");
    
    // Get Document URL
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${cik.replace(/^0+/, "")}/${accNo}/index.json`;
    const indexRes = await fetch(indexUrl, { headers: { "User-Agent": SEC_USER_AGENT } });
    const indexJson = await indexRes.json();
    const targetDoc = indexJson.directory.item.find((d: any) => d.name.endsWith(".htm"));
    const docUrl = `https://www.sec.gov/Archives/edgar/data/${cik.replace(/^0+/, "")}/${accNo}/${targetDoc.name}`;

    console.log(`Stream-Reading: ${docUrl}`);
    const response = await fetch(docUrl, { headers: { "User-Agent": SEC_USER_AGENT } });

    if (!response.body) throw new Error("No body");

    // --- THE FIX: Line-by-Line Streaming ---
    // Pipe the body through a TextDecoder and Splitter
    const lineStream = response.body
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(new TextLineStream());

    const reader = lineStream.getReader();
    const state = { inSOI: false, done: false, scale: 0.001, scaleDetected: false, inRow: false, currentRow: "", rowCount: 0 };
    
    let batch: any[] = [];
    let totalInserted = 0;

    while (true) {
      const { value: line, done } = await reader.read();
      if (done || state.done) break;

      const result = processLine_ARCC(line, state);
      
      if (result) {
        batch.push({ ...result, filing_id: filingId });
      }

      // Small Batch Insert (50 rows) keeps memory tiny
      if (batch.length >= 50) {
        await supabaseClient.from("holdings").insert(batch);
        totalInserted += batch.length;
        batch = [];
      }
    }

    // Flush remaining
    if (batch.length > 0) {
      await supabaseClient.from("holdings").insert(batch);
      totalInserted += batch.length;
    }

    return new Response(JSON.stringify({ success: true, count: totalInserted }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });

  } catch (error) {
    return new Response(JSON.stringify({ error: error.message }), { status: 500, headers: corsHeaders });
  }
});
