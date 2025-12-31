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
  // Match MM/YYYY or MM/DD/YYYY
  const dateMatch = cleaned.match(/(\d{1,2})\/?(\d{1,2})?\/(\d{4})/);
  if (dateMatch) {
    const month = dateMatch[1].padStart(2, '0');
    const year = dateMatch[3];
    // If day is missing (MM/YYYY), default to 01, otherwise use day
    const day = dateMatch[2] ? dateMatch[2].padStart(2, '0') : '01';
    return `${year}-${month}-${day}`;
  }
  return null;
}

function cleanCompanyName(name: string): string {
  if (!name) return "";
  return name.replace(/<[^>]+>/g, "").replace(/(\(\d+\))+\s*$/g, "").trim();
}

function stripTags(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

// Helper to extract cells handling colspan (Crucial for column alignment)
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
      cells.push(""); // Add ghost cell
    }
  }
  return cells;
}

// ======================================================================
// 2. PARSING LOGIC (State Machines)
// ======================================================================

// --- A. OBDC PARSER (Blue Owl) ---
// Based on image: 
// Col 0: Company, Col 1: Inv, Col 2: Ref Rate, Col 3: Cash (Interest), 
// Col 4: PIK, Col 5: Maturity, Col 6: Par, Col 7: Cost, Col 8: Fair Value
function processLine_OBDC(line: string, state: any): any | null {
  const lower = line.toLowerCase();

  // 1. Detect Scale (OBDC usually thousands, but check header)
  if (!state.scaleDetected) {
    if (lower.includes("(amounts in thousands") || lower.includes("amounts in thousands")) {
      state.scale = 0.001;
      state.scaleDetected = true;
    } else if (lower.includes("amounts in millions")) {
      state.scale = 1;
      state.scaleDetected = true;
    }
  }

  // 2. Start/Stop Logic
  if (!state.inSOI) {
    if (lower.includes("schedule of investments")) {
      state.inSOI = true;
      console.log("✅ OBDC: Entered Schedule of Investments");
    }
    return null;
  }
  if (state.inSOI && (lower.includes("notes to consolidated") || lower.includes("notes to financial"))) {
    state.done = true;
    return null;
  }

  // 3. Row Collection
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
  
  // OBDC table usually has ~9 columns. If less than 5, likely a header/separator.
  if (cells.length < 5) return null;

  const company = cleanCompanyName(cells[0]);
  // Validation: Must have a company name and not be a subtotal row
  if (!company || company.length < 3 || /(total|subtotal|balance)/i.test(company)) return null;

  // MAPPING based on Blue Owl Image
  // [0] Company
  // [1] Investment Type
  // [2] Ref. Rate (e.g. "S+") -> Mapped to reference_rate
  // [3] Cash (e.g. "4.50%") -> Mapped to interest_rate
  // [4] PIK (e.g. "0.48%") -> Ignored for now, or could append to interest
  // [5] Maturity Date
  // [6] Par / Units
  // [7] Amortized Cost
  // [8] Fair Value

  // Locate Fair Value (Always last populated column)
  // We use fixed indices if length is standard (9), otherwise fallback to searching end
  let fairValIdx = 8;
  let costIdx = 7;
  
  if (cells.length !== 9) {
     // Fallback: assume last numeric is FV
     for (let i = cells.length - 1; i >= 0; i--) {
        if (cleanNumeric(cells[i]) !== null) {
           fairValIdx = i;
           costIdx = i - 1;
           break;
        }
     }
  }

  const fairVal = cleanNumeric(cells[fairValIdx]);
  const costVal = cleanNumeric(cells[costIdx]);

  if (fairVal === null || fairVal === 0) return null;

  return {
    company_name: company,
    investment_type: cells[1] || null,
    reference_rate: cells[2] || null, // The "Ref. Rate" column
    interest_rate: cells[3] || null,  // The "Cash" column
    maturity_date: parseDate(cells[5]),
    par_amount: toMillions(cleanNumeric(cells[6]), state.scale),
    cost: toMillions(costVal, state.scale),
    fair_value: toMillions(fairVal, state.scale),
    row_number: state.rowCount++
  };
}

// --- B. GENERIC / BXSL PARSER (Fallback) ---
// Used for BXSL or when no specific parser exists. 
// Uses "duck typing" to find columns dynamically.
function processLine_BXSL(line: string, state: any): any | null {
  const lower = line.toLowerCase();

  // 1. Detect Scale
  if (!state.scaleDetected) {
    if (lower.includes("in thousands")) {
      state.scale = 0.001; 
      state.scaleDetected = true;
    } else if (lower.includes("in millions")) {
      state.scale = 1;
      state.scaleDetected = true;
    }
  }

  // 2. Start/Stop
  if (!state.inSOI) {
    if (lower.includes("schedule of investments")) {
      state.inSOI = true;
      console.log("✅ BXSL/Generic: Entered Schedule of Investments");
    }
    return null;
  }
  if (state.inSOI && (lower.includes("notes to consolidated") || lower.includes("notes to financial"))) {
    state.done = true;
    return null;
  }

  // 3. Row Capture
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
  if (!company || company.length < 3 || /(total|subtotal)/i.test(company)) return null;

  // Duck Typing: Find numerics at the end
  const nums = cells.map((c, i) => ({ val: cleanNumeric(c), idx: i })).filter(x => x.val !== null);
  if (nums.length < 2) return null;

  const fairValObj = nums[nums.length - 1]; 
  const costValObj = nums[nums.length - 2];
  
  if (fairValObj.val === 0) return null;

  // Try to find Maturity (looks like date)
  const maturityStr = cells.find(c => /\d{1,2}\/\d{4}/.test(c)); // MM/YYYY

  // Try to find Interest Rate (looks like %)
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

// --- C. ARCC PARSER (Original Integrity Kept) ---
function processLine_ARCC(line: string, state: any): any | null {
  const lower = line.toLowerCase();
  if (!state.scaleDetected) {
    if (lower.includes("(in millions)") || lower.includes("amounts in millions")) {
      state.scale = 1;
      state.scaleDetected = true;
    }
  }
  if (!state.inSOI) {
    if (lower.includes("schedule of investments")) {
      state.inSOI = true;
      console.log("✅ ARCC: Entered Schedule of Investments");
    }
    return null;
  }
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
      return parseSingleRow_Generic(state.currentRow, state); // Use Generic logic for ARCC too as it works well
    }
  }
  return null;
}

// ======================================================================
// 3. MAIN HANDLER (Switchboard)
// ======================================================================

function getProcessor(ticker: string, bdcName: string) {
  const t = (ticker || "").toUpperCase();
  const n = (bdcName || "").toUpperCase();

  if (t === 'OBDC' || n.includes('BLUE OWL')) return processLine_OBDC;
  if (t === 'ARCC' || n.includes('ARES')) return processLine_ARCC;
  if (t === 'GBDC' || n.includes('GOLUB')) return processLine_BXSL; // Use BXSL logic for GBDC too as requested "similar"
  
  // Default fallback -> BXSL parser (robust generic)
  return processLine_BXSL; 
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
    const { cik, ticker, bdc_name } = filing.bdcs;
    const accNo = filing.sec_accession_no.replace(/-/g, "");
    
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${cik.replace(/^0+/, "")}/${accNo}/index.json`;
    const indexRes = await fetch(indexUrl, { headers: { "User-Agent": SEC_USER_AGENT } });
    const indexJson = await indexRes.json();
    
    // Find the main filing document - prefer ticker-based name, then largest .htm file
    const htmDocs = indexJson.directory.item.filter((d: any) => 
      d.name.endsWith(".htm") && !d.name.includes("-index")
    );
    
    // Try to find doc with ticker in name (e.g. "obdc-20250930.htm")
    const tickerLower = (ticker || "").toLowerCase();
    let targetDoc = htmDocs.find((d: any) => d.name.toLowerCase().includes(tickerLower) && d.name.includes("-"));
    
    // Fallback to largest .htm file
    if (!targetDoc) {
      targetDoc = htmDocs.reduce((largest: any, doc: any) => {
        const size = parseInt(doc.size) || 0;
        const largestSize = parseInt(largest?.size) || 0;
        return size > largestSize ? doc : largest;
      }, htmDocs[0]);
    }
    
    if (!targetDoc) throw new Error("No suitable HTM document found");
    
    const docUrl = `https://www.sec.gov/Archives/edgar/data/${cik.replace(/^0+/, "")}/${accNo}/${targetDoc.name}`;

    console.log(`Stream-Reading: ${docUrl} [${ticker}]`);
    const response = await fetch(docUrl, { headers: { "User-Agent": SEC_USER_AGENT } });

    if (!response.body) throw new Error("No body");

    // Select the correct parser line-processor based on BDC
    const processLine = getProcessor(ticker, bdc_name);

    const lineStream = response.body
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(new TextLineStream());

    const reader = lineStream.getReader();
    // Default scale is 0.001 (thousands) unless detected otherwise
    const state = { inSOI: false, done: false, scale: 0.001, scaleDetected: false, inRow: false, currentRow: "", rowCount: 0 };
    
    let batch: any[] = [];
    let totalInserted = 0;

    while (true) {
      const { value: line, done } = await reader.read();
      if (done || state.done) break;

      const result = processLine(line, state);
      
      if (result) {
        batch.push({ ...result, filing_id: filingId });
      }

      if (batch.length >= 50) {
        await supabaseClient.from("holdings").insert(batch);
        totalInserted += batch.length;
        batch = [];
      }
    }

    if (batch.length > 0) {
      await supabaseClient.from("holdings").insert(batch);
      totalInserted += batch.length;
    }

    return new Response(JSON.stringify({ success: true, count: totalInserted }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });

  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return new Response(JSON.stringify({ error: errorMessage }), { status: 500, headers: corsHeaders });
  }
});
