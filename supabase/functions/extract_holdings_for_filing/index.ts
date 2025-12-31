import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// 🚨 MEMORY SAFE: No heavy DOM libraries. Strict Streaming.

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const SEC_USER_AGENT = "BDCTrackerApp/1.0 (contact@bdctracker.com)";

// ======================================================================
// 1. EXTENDED SUFFIX LISTS (The Fix for "Missing Names")
// ======================================================================

// Primary Suffixes (Must exist at the end of the name)
const COMPANY_SUFFIXES = [
  "inc.", "inc", "incorporated", 
  "llc", "l.l.c.", 
  "lp", "l.p.", 
  "corp.", "corp", "corporation", 
  "company", "co.", 
  "ltd.", "ltd", "limited",
  "plc", "p.l.c.", 
  "bv", "b.v.", 
  "gmbh", 
  "s.a.r.l", "s.a.r.l.", "s.à r.l.", "sar", // Luxembourg/French
  "sa", "s.a.",
  "lp", "l.p.", "llp", "l.l.p."
];

// Entity Words (If no suffix, checks if name ENDS with one of these)
const ENTITY_WORDS = [
  "enterprises", "industries", "technologies", "systems", 
  "group", "capital", "solutions", "services", "holdings",
  "bidco", "midco", "topco", "purchaser", "aggregator", "merger sub",
  "partners", "management", "international", "global", "brands"
];

function hasCompanySuffix(name: string): boolean {
  const lower = name.toLowerCase().trim();
  
  // 1. Check Primary Suffixes (Word Boundary Match)
  // Matches "Company Inc" or "Company Inc."
  const suffixMatch = COMPANY_SUFFIXES.some(s => {
    // Escape dots for regex
    const esc = s.replace('.', '\\.'); 
    return new RegExp(`\\b${esc}$`, 'i').test(lower);
  });
  if (suffixMatch) return true;

  // 2. Check Entity Words (End of String Match)
  // Matches "Halex Holdings" or "Datix Bidco"
  const wordMatch = ENTITY_WORDS.some(w => lower.endsWith(w));
  if (wordMatch) return true;

  return false;
}

// ======================================================================
// 2. PARSING HELPERS
// ======================================================================

function stripTags(html: string): string {
  if (!html) return "";
  return html
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, "")
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "")
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/\s+/g, " ")
    .trim();
}

function getRows(tableHtml: string): string[] {
  const rows: string[] = [];
  const rowRe = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
  let match;
  while ((match = rowRe.exec(tableHtml)) !== null) {
    rows.push(match[1]);
  }
  return rows;
}

function getCells(rowHtml: string): string[] {
  const cells: string[] = [];
  const cellRe = /<t[dh]\b([^>]*)>([\s\S]*?)<\/t[dh]>/gi;
  let match;
  while ((match = cellRe.exec(rowHtml)) !== null) {
    const attrs = match[1];
    const content = stripTags(match[2]);
    const colspanMatch = attrs.match(/colspan=["']?(\d+)["']?/i);
    const span = colspanMatch ? parseInt(colspanMatch[1]) : 1;
    cells.push(content);
    for (let i = 1; i < span; i++) cells.push(""); 
  }
  return cells;
}

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
  const mmyyyy = cleaned.match(/(\d{1,2})\/(\d{4})/);
  if (mmyyyy) {
    const lastDay = new Date(parseInt(mmyyyy[2]), parseInt(mmyyyy[1]), 0).getDate();
    return `${mmyyyy[2]}-${mmyyyy[1].padStart(2, '0')}-${lastDay}`;
  }
  return null;
}

function cleanCompanyName(name: string): string {
  if (!name) return "";
  return name.replace(/(\(\d+\))+\s*$/g, "").trim();
}

// ======================================================================
// 3. PARSING LOGIC (Streaming Switchboard)
// ======================================================================

function processTable_GBDC(tableHtml: string, scale: number): any[] {
  // Relaxed: Just needs to look like a holding table
  if (!/(llc|inc|corp|l\.p\.|limited|\$|fair value)/i.test(tableHtml)) return [];
  const rows = getRows(tableHtml);
  if (rows.length < 3) return [];

  const results: any[] = [];
  let col = { company: -1, type: -1, industry: -1, interest: -1, maturity: -1, par: -1, cost: -1, fair: -1 };
  let dataStart = 0;

  for (let i = 0; i < Math.min(8, rows.length); i++) {
    const cells = getCells(rows[i]);
    cells.forEach((t, idx) => {
      const lt = t.toLowerCase();
      if (lt.includes('company') && !lt.includes('type') && col.company === -1) col.company = idx;
      if (lt.includes('type') && col.type === -1) col.type = idx;
      if (lt.includes('industry') && col.industry === -1) col.industry = idx;
      if ((lt.includes('interest') || lt.includes('spread') || lt.includes('floor')) && col.interest === -1) col.interest = idx;
      if (lt.includes('maturity') && col.maturity === -1) col.maturity = idx;
      if ((lt.includes('principal') || lt.includes('par')) && col.par === -1) col.par = idx;
      if (lt.includes('cost') && col.cost === -1) col.cost = idx;
      if (lt.includes('fair') && !lt.includes('un') && col.fair === -1) col.fair = idx;
    });
    if (col.company > -1 || col.fair > -1) { dataStart = i + 1; if (col.fair > -1) break; }
  }
  if (col.company === -1) col.company = 0;

  let currentCompany = null;
  let currentIndustry = null;

  for (let i = dataStart; i < rows.length; i++) {
    const cells = getCells(rows[i]);
    if (cells.length < 3) continue;

    let comp = cleanCompanyName(cells[col.company] || cells[0]);
    if (/(total|subtotal|balance)/i.test(comp)) continue;

    let effectiveComp = comp;
    if (hasCompanySuffix(comp)) currentCompany = comp;
    else if (currentCompany && (!comp || comp.length < 5)) effectiveComp = currentCompany;
    else if (!comp) continue;

    if (col.industry > -1 && cells[col.industry].length > 3) currentIndustry = cells[col.industry];

    const fairVal = col.fair > -1 ? cleanNumeric(cells[col.fair]) : cleanNumeric(cells[cells.length - 1]);
    const costVal = col.cost > -1 ? cleanNumeric(cells[col.cost]) : cleanNumeric(cells[cells.length - 2]);

    if (fairVal === null && costVal === null) continue;

    results.push({
      company_name: effectiveComp,
      investment_type: col.type > -1 ? cells[col.type] : null,
      industry: currentIndustry,
      interest_rate: col.interest > -1 ? cells[col.interest] : null,
      maturity_date: col.maturity > -1 ? parseDate(cells[col.maturity]) : null,
      par_amount: col.par > -1 ? toMillions(cleanNumeric(cells[col.par]), scale) : null,
      cost: toMillions(costVal, scale),
      fair_value: toMillions(fairVal, scale),
    });
  }
  return results;
}

function processTable_BXSL(tableHtml: string, scale: number): any[] {
  if (!/(llc|inc|corp|limited|\$|fair value)/i.test(tableHtml)) return [];
  const rows = getRows(tableHtml);
  if (rows.length < 3) return [];

  const results: any[] = [];
  let col = { company: -1, industry: -1, type: -1, interest: -1, spread: -1, maturity: -1, par: -1, cost: -1, fair: -1 };
  let dataStart = 0;

  for (let i = 0; i < Math.min(8, rows.length); i++) {
    const cells = getCells(rows[i]);
    cells.forEach((t, idx) => {
      const lt = t.toLowerCase();
      if ((lt.includes('company') || lt.includes('investments')) && !lt.includes('type') && col.company === -1) col.company = idx;
      if ((lt.includes('industry') || lt.includes('sector')) && col.industry === -1) col.industry = idx;
      if (lt.includes('type') && col.type === -1) col.type = idx;
      if (lt.includes('interest') && !lt.includes('spread') && col.interest === -1) col.interest = idx;
      if ((lt.includes('spread') || lt.includes('reference')) && col.spread === -1) col.spread = idx;
      if (lt.includes('maturity') && col.maturity === -1) col.maturity = idx;
      if ((lt.includes('principal') || lt.includes('par')) && col.par === -1) col.par = idx;
      if (lt.includes('cost') && col.cost === -1) col.cost = idx;
      if (lt.includes('fair') && col.fair === -1) col.fair = idx;
    });
    if (col.company > -1 || col.fair > -1) { dataStart = i + 1; break; }
  }
  if (col.company === -1) col.company = 0;

  let currentCompany = null;
  let currentIndustry = null;

  for (let i = dataStart; i < rows.length; i++) {
    const cells = getCells(rows[i]);
    if (cells.length < 3) continue;

    let comp = cleanCompanyName(cells[col.company] || cells[0]);
    if (/(total|subtotal)/i.test(comp)) continue;

    let effectiveComp = comp;
    if (hasCompanySuffix(comp)) currentCompany = comp;
    else if (currentCompany && (!comp || comp.length < 5)) effectiveComp = currentCompany;
    else if (!comp) continue;

    if (col.industry > -1 && cells[col.industry].length > 3) currentIndustry = cells[col.industry];

    const fairVal = col.fair > -1 ? cleanNumeric(cells[col.fair]) : cleanNumeric(cells[cells.length - 1]);
    const costVal = col.cost > -1 ? cleanNumeric(cells[col.cost]) : cleanNumeric(cells[cells.length - 2]);

    if (fairVal === null && costVal === null) continue;

    let refRate = null;
    if (col.spread > -1) refRate = cells[col.spread];

    results.push({
      company_name: effectiveComp,
      investment_type: col.type > -1 ? cells[col.type] : null,
      industry: currentIndustry,
      interest_rate: col.interest > -1 ? cells[col.interest] : null,
      reference_rate: refRate,
      maturity_date: col.maturity > -1 ? parseDate(cells[col.maturity]) : null,
      par_amount: col.par > -1 ? toMillions(cleanNumeric(cells[col.par]), scale) : null,
      cost: toMillions(costVal, scale),
      fair_value: toMillions(fairVal, scale),
    });
  }
  return results;
}

function processTable_ARCC(tableHtml: string, scale: number): any[] {
  // Relaxed ARCC filter (accepts numbers only tables)
  if (!/(\$|fair value)/i.test(tableHtml)) return [];

  const rows = getRows(tableHtml);
  if (rows.length < 3) return [];

  const results: any[] = [];
  let col = { company: -1, type: -1, industry: -1, interest: -1, maturity: -1, par: -1, cost: -1, fair: -1 };
  let dataStart = 0;

  for (let i = 0; i < Math.min(8, rows.length); i++) {
    const cells = getCells(rows[i]);
    cells.forEach((t, idx) => {
      const lt = t.toLowerCase();
      if ((lt.includes('company') || lt.includes('issuer')) && !lt.includes('type') && col.company === -1) col.company = idx;
      if (lt.includes('industry') && col.industry === -1) col.industry = idx;
      if (lt.includes('type') && col.type === -1) col.type = idx;
      if (lt.includes('interest') && col.interest === -1) col.interest = idx;
      if (lt.includes('maturity') && col.maturity === -1) col.maturity = idx;
      if ((lt.includes('principal') || lt.includes('par')) && col.par === -1) col.par = idx;
      if (lt.includes('cost') && col.cost === -1) col.cost = idx;
      if (lt.includes('fair') && col.fair === -1) col.fair = idx;
    });
    if (col.company > -1 || col.fair > -1) { dataStart = i + 1; break; }
  }
  if (col.company === -1) col.company = 0;

  let currentCompany = null;
  let currentIndustry = null;

  for (let i = dataStart; i < rows.length; i++) {
    const cells = getCells(rows[i]);
    if (cells.length < 3) continue;

    // ARCC Industry Header Detection
    const firstText = cells[0];
    const hasNumbers = cells.slice(1).some(x => /\d/.test(x));
    if (firstText && firstText.length > 3 && !hasNumbers && !/(company|issuer|total)/i.test(firstText)) {
      if (!hasCompanySuffix(firstText)) {
         currentIndustry = firstText;
         continue;
      }
    }

    let comp = cleanCompanyName(cells[col.company] || cells[0]);
    if (/(total|subtotal|balance)/i.test(comp)) continue;

    let effectiveComp = comp;
    if (hasCompanySuffix(comp)) currentCompany = comp;
    else if (currentCompany && (!comp || comp.length < 5)) effectiveComp = currentCompany;
    else if (!comp) continue;

    const fairVal = col.fair > -1 ? cleanNumeric(cells[col.fair]) : cleanNumeric(cells[cells.length - 1]);
    const costVal = col.cost > -1 ? cleanNumeric(cells[col.cost]) : cleanNumeric(cells[cells.length - 2]);

    if (fairVal === null && costVal === null) continue;

    results.push({
      company_name: effectiveComp,
      investment_type: col.type > -1 ? cells[col.type] : null,
      industry: currentIndustry,
      interest_rate: col.interest > -1 ? cells[col.interest] : null,
      maturity_date: col.maturity > -1 ? parseDate(cells[col.maturity]) : null,
      par_amount: col.par > -1 ? toMillions(cleanNumeric(cells[col.par]), scale) : null,
      cost: toMillions(costVal, scale),
      fair_value: toMillions(fairVal, scale),
    });
  }
  return results;
}

// ======================================================================
// 4. STREAMING ENGINE
// ======================================================================

async function processStream(
  response: Response, 
  filingId: string, 
  supabaseClient: any, 
  parserType: string
) {
  if (!response.body) throw new Error("No response body");

  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  
  let buffer = "";
  let scale = 0.001; 
  let scaleDetected = false;
  let inSOI = false; 
  let inPriorPeriod = false; 
  
  let insertedCount = 0;
  let pendingRows: any[] = [];
  const seenKeys = new Set<string>();

  await supabaseClient.from("holdings").delete().eq("filing_id", filingId);

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      
      buffer += decoder.decode(value, { stream: true });

      if (!scaleDetected && buffer.length > 50000) {
        if (/\(in millions\)/i.test(buffer)) scale = 1;
        scaleDetected = true;
      }

      if (!inSOI) {
        const soiIdx = buffer.toLowerCase().indexOf("schedule of investments");
        if (soiIdx !== -1) {
          inSOI = true;
          buffer = buffer.slice(soiIdx);
        } else {
          if (buffer.length > 100000) buffer = buffer.slice(-1000);
          continue;
        }
      }

      if (inSOI && !inPriorPeriod) {
        const lowerBuf = buffer.toLowerCase();
        
        if (lowerBuf.includes("notes to consolidated") || lowerBuf.includes("notes to financial")) {
           console.log("🛑 Found Notes. Stopping.");
           break; 
        }

        if (insertedCount > 50) {
           const nextSoi = lowerBuf.indexOf("schedule of investments");
           if (nextSoi !== -1 && (
               lowerBuf.includes("december 31, 2024") || 
               lowerBuf.includes("december 31, 2023")
           )) {
               console.log("🛑 Found Prior Period Date. Stopping.");
               break;
           }
        }
      }

      while (true) {
        const tableStart = buffer.indexOf("<table");
        if (tableStart === -1) break;

        const tableEnd = buffer.indexOf("</table>", tableStart);
        if (tableEnd === -1) break; 

        const tableHtml = buffer.slice(tableStart, tableEnd + 8);
        buffer = buffer.slice(tableEnd + 8);

        let newRows: any[] = [];
        if (parserType === 'GBDC') newRows = processTable_GBDC(tableHtml, scale);
        else if (parserType === 'BXSL') newRows = processTable_BXSL(tableHtml, scale);
        else newRows = processTable_ARCC(tableHtml, scale);

        if (newRows.length > 0) {
          for (const row of newRows) {
            const key = `${row.company_name}-${row.fair_value}-${row.cost}`;
            if (!seenKeys.has(key)) {
              seenKeys.add(key);
              pendingRows.push({
                ...row,
                filing_id: filingId,
                row_number: insertedCount + pendingRows.length + 1
              });
            }
          }
        }

        if (pendingRows.length >= 200) {
          await supabaseClient.from("holdings").insert(pendingRows.splice(0, pendingRows.length));
          insertedCount += 200;
        }
      }

      if (buffer.length > 15_000_000) {
        console.warn("⚠️ Buffer > 15MB. Trimming safely.");
        buffer = buffer.slice(-2000); 
      }
    }
  } catch (err) {
    console.error("Stream Error:", err);
  }

  if (pendingRows.length > 0) {
    await supabaseClient.from("holdings").insert(pendingRows);
    insertedCount += pendingRows.length;
  }

  return { count: insertedCount, scale: scale === 1 ? 'millions' : 'thousands' };
}

// ======================================================================
// MAIN HANDLER
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

    const { data: filing, error: fError } = await supabaseClient
      .from("filings")
      .select(`*, bdcs (cik, bdc_name, ticker)`)
      .eq("id", filingId)
      .single();

    if (fError || !filing) throw new Error("Filing not found");

    const { cik, bdc_name, ticker } = filing.bdcs;
    const accessionNo = filing.sec_accession_no;
    const parserType = determineParserType(ticker, bdc_name);
    console.log(`Processing ${accessionNo} (${parserType}) via STREAM`);

    const paddedCik = cik.replace(/^0+/, "");
    const accNoClean = accessionNo.replace(/-/g, "");
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accNoClean}/index.json`;
    const indexRes = await fetch(indexUrl, { headers: { "User-Agent": SEC_USER_AGENT } });
    const indexJson = await indexRes.json();
    const docs = indexJson.directory?.item || [];
    
    const htmDocs = docs.filter((d: any) => d.name.endsWith(".htm") || d.name.endsWith(".html"));
    if (htmDocs.length === 0) throw new Error("No HTML found");

    const targetDoc = htmDocs.sort((a: any, b: any) => parseInt(b.size) - parseInt(a.size))[0];
    const docUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accNoClean}/${targetDoc.name}`;
    
    const htmlRes = await fetch(docUrl, { headers: { "User-Agent": SEC_USER_AGENT } });
    const result = await processStream(htmlRes, filingId, supabaseClient, parserType);

    if (result.count > 0) {
      await supabaseClient.from("filings")
        .update({ parsed_successfully: true, value_scale: result.scale })
        .eq("id", filingId);
    }

    return new Response(JSON.stringify({ success: true, count: result.count }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });

  } catch (error) {
    console.error("Critical Error:", error);
    return new Response(JSON.stringify({ error: error instanceof Error ? error.message : "Unknown" }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });
  }
});
