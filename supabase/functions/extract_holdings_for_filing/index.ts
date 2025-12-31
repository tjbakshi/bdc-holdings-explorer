import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

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
  return name.replace(/<[^>]+>/g, "").replace(/(\(\d+\))+\s*$/g, "").trim();
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
    for (let i = 1; i < span; i++) {
      cells.push("");
    }
  }
  return cells;
}

// ======================================================================
// 2. PARSING LOGIC - STREAMLINED FOR MEMORY EFFICIENCY
// ======================================================================

function parseSingleRow_Generic(rowHtml: string, state: any): any | null {
  const cells = extractCellsWithColspan(rowHtml);
  if (cells.length < 3) return null;

  const company = cleanCompanyName(cells[0]);
  if (!company || company.length < 3 || /(total|subtotal)/i.test(company)) return null;

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

// Unified line processor - handles all BDC types
function processLine(line: string, state: any): any | null {
  const lower = line.toLowerCase();

  // 1. Detect Scale (check early)
  if (!state.scaleDetected) {
    if (lower.includes("(in millions)") || lower.includes("amounts in millions")) {
      state.scale = 1;
      state.scaleDetected = true;
    } else if (lower.includes("in thousands") || lower.includes("amounts in thousands")) {
      state.scale = 0.001;
      state.scaleDetected = true;
    }
  }

  // 2. Start Detection - Enter SOI section
  if (!state.inSOI) {
    if (lower.includes("schedule of investments")) {
      state.inSOI = true;
      console.log("✅ Entered Schedule of Investments");
    }
    return null;
  }

  // 3. Stop Detection - Exit SOI section (CRITICAL for memory)
  if (lower.includes("notes to consolidated") || 
      lower.includes("notes to financial") ||
      lower.includes("the accompanying notes are an integral part")) {
    state.done = true;
    console.log("✅ Exiting Schedule of Investments - stopping parse");
    return null;
  }

  // 4. Row Capture (minimal memory usage)
  if (line.includes("<tr")) {
    state.inRow = true;
    state.currentRow = "";
  }

  if (state.inRow) {
    state.currentRow += line;
    if (line.includes("</tr>")) {
      state.inRow = false;
      const row = state.currentRow;
      state.currentRow = ""; // Clear immediately
      return parseSingleRow_Generic(row, state);
    }
  }
  
  return null;
}

// ======================================================================
// 3. MAIN HANDLER - OPTIMIZED FOR MEMORY
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
    const { cik, ticker } = filing.bdcs;
    const accNo = filing.sec_accession_no.replace(/-/g, "");
    
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
    
    const docUrl = `https://www.sec.gov/Archives/edgar/data/${cik.replace(/^0+/, "")}/${accNo}/${targetDoc.name}`;

    console.log(`Stream-Reading: ${docUrl} [${ticker}]`);
    const response = await fetch(docUrl, { headers: { "User-Agent": SEC_USER_AGENT } });

    if (!response.body) throw new Error("No body");

    // MEMORY-EFFICIENT: Process line-by-line without buffering entire file
    const decoder = new TextDecoder();
    const reader = response.body.getReader();
    
    const state = { 
      inSOI: false, 
      done: false, 
      scale: 0.001, 
      scaleDetected: false, 
      inRow: false, 
      currentRow: "", 
      rowCount: 0 
    };
    
    let batch: any[] = [];
    let totalInserted = 0;
    let buffer = "";
    let linesProcessed = 0;

    while (!state.done) {
      const { value, done } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      
      // Process complete lines only
      const lines = buffer.split('\n');
      buffer = lines.pop() || ""; // Keep incomplete last line in buffer
      
      for (const line of lines) {
        linesProcessed++;
        
        // Quick skip for lines that can't contain data
        if (line.length < 5) continue;
        
        const result = processLine(line, state);
        
        if (result) {
          batch.push({ ...result, filing_id: filingId });
        }

        // Flush batch periodically
        if (batch.length >= 50) {
          await supabaseClient.from("holdings").insert(batch);
          totalInserted += batch.length;
          console.log(`Inserted ${totalInserted} holdings...`);
          batch = [];
        }

        // Break early if done
        if (state.done) break;
      }
    }

    // Process any remaining buffer
    if (buffer.length > 0 && !state.done) {
      const result = processLine(buffer, state);
      if (result) {
        batch.push({ ...result, filing_id: filingId });
      }
    }

    // Final batch insert
    if (batch.length > 0) {
      await supabaseClient.from("holdings").insert(batch);
      totalInserted += batch.length;
    }

    // Update filing status
    await supabaseClient.from("filings").update({ 
      parsed_successfully: totalInserted > 0,
      data_source: 'edge-parser'
    }).eq("id", filingId);

    console.log(`✅ Complete: ${totalInserted} holdings from ${linesProcessed} lines`);

    return new Response(JSON.stringify({ success: true, count: totalInserted }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });

  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error("Parse error:", errorMessage);
    return new Response(JSON.stringify({ error: errorMessage }), { status: 500, headers: corsHeaders });
  }
});
