import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from "https://esm.sh/@supabase/supabase-js@2"
import { DOMParser, Element } from "https://deno.land/x/deno_dom/deno-dom-wasm.ts"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
}

const SEC_USER_AGENT = "YourName yourname@example.com";
const CPU_TIME_LIMIT_MS = 35; // Safety margin for 50ms limit
const SEGMENT_SIZE = 500000;  // 500KB chunks
const OVERLAP_SIZE = 10000;   // 10KB overlap

interface Holding {
  company_name: string;
  investment_type: string | null;
  industry: string | null;
  description?: string | null;
  interest_rate?: string | null;
  reference_rate?: string | null;
  maturity_date?: string | null;
  par_amount?: number | null;
  cost?: number | null;
  fair_value?: number | null;
}

// --- HELPER FUNCTIONS ---

function detectScale(text: string) {
  const lower = text.toLowerCase();
  if (lower.includes("millions") || lower.includes("in millions")) return { scale: 1, detected: 'millions' };
  if (lower.includes("thousands") || lower.includes("in thousands")) return { scale: 0.001, detected: 'thousands' };
  return { scale: 0.001, detected: 'thousands (default)' };
}

function cleanAndParseNumeric(value: string): number | null {
  if (!value) return null;
  const cleaned = value.replace(/[$(),\s]/g, '').trim();
  if (!cleaned || cleaned === '-' || cleaned === '—') return null;
  const parsed = parseFloat(cleaned);
  return isNaN(parsed) ? null : (value.includes('(') ? -parsed : parsed);
}

function cleanCompanyName(name: string): string {
  return name.replace(/(\s*\(?\d+(?:,\s*\d+)*\)?)+\s*$/g, '').trim();
}

// --- PARSING ENGINE ---

function parseSegment(html: string, initialIndustry: string | null): { holdings: Holding[], lastIndustry: string | null } {
  const doc = new DOMParser().parseFromString(html, "text/html");
  if (!doc) return { holdings: [], lastIndustry: initialIndustry };

  const holdings: Holding[] = [];
  let currentIndustry = initialIndustry;
  const tables = doc.querySelectorAll("table");

  tables.forEach(table => {
    const rows = (table as Element).querySelectorAll("tr");
    rows.forEach(row => {
      const cells = Array.from((row as Element).querySelectorAll("td"));
      if (cells.length < 2) return;

      const firstCellText = cells[0].textContent?.trim() || "";
      
      // Industry Detection Logic
      if (cells.length === 1 || (cells.length > 1 && cells.slice(1).every(c => !c.textContent?.trim()))) {
        if (firstCellText.length > 3 && firstCellText.length < 100 && !firstCellText.includes('$')) {
            currentIndustry = firstCellText;
        }
        return;
      }

      // Basic Data Row Detection (looking for numbers at the end)
      const lastCellText = cells[cells.length - 1].textContent?.trim() || "";
      const fairValue = cleanAndParseNumeric(lastCellText);

      if (fairValue !== null && fairValue !== 0) {
        holdings.push({
          company_name: cleanCompanyName(firstCellText),
          investment_type: cells[1]?.textContent?.trim() || null,
          industry: currentIndustry,
          fair_value: fairValue,
          cost: cleanAndParseNumeric(cells[cells.length - 2]?.textContent || "")
        });
      }
    });
  });

  return { holdings, lastIndustry: currentIndustry };
}

// --- MAIN HANDLER ---

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  const startTime = performance.now();
  const supabaseClient = createClient(
    Deno.env.get("SUPABASE_URL") ?? "",
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
  );

  try {
    const { filingId, resumeFromOffset } = await req.json();

    const { data: filing } = await supabaseClient
      .from("filings")
      .select("*, bdcs(cik)")
      .eq("id", filingId)
      .single();

    const offset = resumeFromOffset ?? filing.current_byte_offset ?? 0;
    const industryState = filing.current_industry_state ?? null;
    const cik = filing.bdcs.cik.padStart(10, '0');
    const docUrl = filing.document_url; // Ensure this is stored in your DB

    // Fetch the specific chunk using Range Header
    const response = await fetch(docUrl, {
      headers: { 
        "User-Agent": SEC_USER_AGENT,
        "Range": `bytes=${offset}-${offset + SEGMENT_SIZE}`
      }
    });

    const contentRange = response.headers.get("Content-Range");
    const totalSize = contentRange ? parseInt(contentRange.split('/')[1]) : 0;
    const htmlChunk = await response.text();

    // RUN THE PARSER
    const { holdings, lastIndustry } = parseSegment(htmlChunk, industryState);

    // BATCH INSERT
    if (holdings.length > 0) {
      await supabaseClient.from("holdings").insert(
        holdings.map(h => ({ ...h, filing_id: filingId }))
      );
    }

    const nextOffset = offset + SEGMENT_SIZE - OVERLAP_SIZE;
    const isFinished = nextOffset >= totalSize;

    // UPDATE PROGRESS
    await supabaseClient.from("filings").update({
      current_byte_offset: isFinished ? 0 : nextOffset,
      current_industry_state: lastIndustry,
      parsed_successfully: isFinished,
      total_file_size: totalSize
    }).eq("id", filingId);

    return new Response(JSON.stringify({
      status: isFinished ? "COMPLETE" : "PARTIAL",
      percentage_complete: Math.min(100, Math.round((nextOffset / totalSize) * 100)),
      next_offset: nextOffset
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });

  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "Unknown error";
    return new Response(JSON.stringify({ error: errorMessage }), { 
      status: 500, 
      headers: { ...corsHeaders, "Content-Type": "application/json" } 
    });
  }
});
