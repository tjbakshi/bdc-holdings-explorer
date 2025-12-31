import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { DOMParser, Element } from "https://deno.land/x/deno_dom@v0.1.38/deno-dom-wasm.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const SEC_USER_AGENT = "BDCTrackerApp/1.0 (contact@bdctracker.com)";

// ======================================================================
// HELPERS
// ======================================================================

function detectScale(html: string): number {
  const headerText = html.slice(0, 50000).toLowerCase();
  if (/\(in millions\)/.test(headerText) || /amounts?\s+in\s+millions/.test(headerText)) {
    return 1;
  }
  return 0.001; // Default to thousands
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

function hasCompanySuffix(name: string): boolean {
  return /(llc|inc|corp|l\.p\.|limited|co\.|holdings|group)/i.test(name);
}

// ======================================================================
// 1. GBDC SPECIFIC PARSER (Chunked DOM)
// Preserves GBDC-specific column mapping logic
// ======================================================================
async function parseGBDC(html: string, filingId: string, supabaseClient: any, scale: number) {
  console.log("📘 Running GBDC Specific Parser...");
  
  const soiMatch = /(consolidated schedule of investments|schedule of investments)/i.exec(html);
  if (!soiMatch) return 0;
  const content = html.slice(soiMatch.index);
  const tableRe = /<table\b[^>]*>([\s\S]*?)<\/table>/gi;
  
  await supabaseClient.from("holdings").delete().eq("filing_id", filingId);

  let insertedCount = 0;
  let tableMatch;
  const pendingRows: any[] = [];
  const seenKeys = new Set<string>();

  while ((tableMatch = tableRe.exec(content)) !== null) {
    const tableHtml = tableMatch[0];
    
    // GBDC Filter: Must have typical GBDC keywords or company suffixes
    if (!/(llc|inc\.|corp\.|l\.p\.|limited|\$|fair value)/i.test(tableHtml)) continue;

    const doc = new DOMParser().parseFromString(tableHtml, "text/html");
    if (!doc) continue;
    const rows = Array.from(doc.querySelectorAll("tr"));
    if (rows.length < 3) continue;

    // --- GBDC LOGIC START ---
    let col = { company: -1, type: -1, industry: -1, interest: -1, maturity: -1, par: -1, cost: -1, fair: -1 };
    let dataStart = 0;

    for (let i = 0; i < Math.min(8, rows.length); i++) {
      const cells = Array.from(rows[i].querySelectorAll("td, th"));
      let cIdx = 0;
      for (const cell of cells) {
        const txt = cell.textContent.toLowerCase();
        const span = parseInt(cell.getAttribute("colspan") || "1");
        
        // GBDC Specific Headers
        if (txt.includes('company') && !txt.includes('type') && col.company === -1) col.company = cIdx;
        if (txt.includes('type') && col.type === -1) col.type = cIdx;
        if (txt.includes('industry') && col.industry === -1) col.industry = cIdx;
        if (txt.includes('interest') && col.interest === -1) col.interest = cIdx;
        if ((txt.includes('spread') || txt.includes('floor')) && col.interest === -1) col.interest = cIdx; // GBDC often puts spread next to rate
        if (txt.includes('maturity') && col.maturity === -1) col.maturity = cIdx;
        if ((txt.includes('principal') || txt.includes('par')) && col.par === -1) col.par = cIdx;
        if (txt.includes('cost') && col.cost === -1) col.cost = cIdx;
        if (txt.includes('fair') && !txt.includes('un') && col.fair === -1) col.fair = cIdx;
        
        cIdx += span;
      }
      if (col.company > -1 || col.fair > -1) { dataStart = i + 1; if (col.fair > -1) break; }
    }
    
    // GBDC Fallback
    if (col.company === -1) col.company = 0;

    let currentCompany = null;
    let currentIndustry = null;

    for (let i = dataStart; i < rows.length; i++) {
      const cells = Array.from(rows[i].querySelectorAll("td, th"));
      const flatCells: string[] = [];
      cells.forEach(c => {
          const txt = c.textContent.trim();
          const span = parseInt(c.getAttribute("colspan") || "1");
          flatCells.push(txt);
          for(let k=1; k<span; k++) flatCells.push("");
      });

      if (flatCells.length < 3) continue;

      let comp = flatCells[col.company] || flatCells[0];
      comp = cleanCompanyName(comp);
      if (/(total|subtotal|balance)/i.test(comp)) continue;

      let effectiveComp = comp;
      if (hasCompanySuffix(comp)) currentCompany = comp;
      else if (currentCompany && (!comp || comp.length < 5)) effectiveComp = currentCompany;
      else if (!comp) continue;

      // GBDC sometimes lists industry in a column
      if (col.industry > -1 && flatCells[col.industry] && flatCells[col.industry].length > 3) currentIndustry = flatCells[col.industry];

      const fairVal = col.fair > -1 ? cleanNumeric(flatCells[col.fair]) : cleanNumeric(flatCells[flatCells.length - 1]);
      const costVal = col.cost > -1 ? cleanNumeric(flatCells[col.cost]) : cleanNumeric(flatCells[flatCells.length - 2]);

      if (fairVal === null && costVal === null) continue;

      const key = `${effectiveComp}-${fairVal}-${costVal}`;
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);

      pendingRows.push({
        filing_id: filingId,
        company_name: effectiveComp,
        investment_type: col.type > -1 ? flatCells[col.type] : null,
        industry: col.industry > -1 ? flatCells[col.industry] : currentIndustry,
        interest_rate: col.interest > -1 ? flatCells[col.interest] : null,
        maturity_date: col.maturity > -1 ? parseDate(flatCells[col.maturity]) : null,
        par_amount: col.par > -1 ? toMillions(cleanNumeric(flatCells[col.par]), scale) : null,
        cost: toMillions(costVal, scale),
        fair_value: toMillions(fairVal, scale),
        row_number: insertedCount + pendingRows.length + 1
      });
    }
    // --- GBDC LOGIC END ---

    if (pendingRows.length >= 200) {
      await supabaseClient.from("holdings").insert(pendingRows.splice(0, pendingRows.length));
      insertedCount += 200;
    }
  }
  if (pendingRows.length > 0) {
    await supabaseClient.from("holdings").insert(pendingRows);
    insertedCount += pendingRows.length;
  }
  return insertedCount;
}

// ======================================================================
// 2. BXSL SPECIFIC PARSER (Chunked DOM)
// Preserves BXSL-specific headers (Reference Rate, Spread)
// ======================================================================
async function parseBXSL(html: string, filingId: string, supabaseClient: any, scale: number) {
  console.log("🟣 Running BXSL Specific Parser...");

  const soiMatch = /(consolidated schedule of investments|schedule of investments)/i.exec(html);
  if (!soiMatch) return 0;
  const content = html.slice(soiMatch.index);
  const tableRe = /<table\b[^>]*>([\s\S]*?)<\/table>/gi;

  await supabaseClient.from("holdings").delete().eq("filing_id", filingId);

  let insertedCount = 0;
  let tableMatch;
  const pendingRows: any[] = [];
  const seenKeys = new Set<string>();

  while ((tableMatch = tableRe.exec(content)) !== null) {
    const tableHtml = tableMatch[0];
    if (!/(llc|inc\.|corp\.|limited|\$|fair value)/i.test(tableHtml)) continue;

    const doc = new DOMParser().parseFromString(tableHtml, "text/html");
    if (!doc) continue;
    const rows = Array.from(doc.querySelectorAll("tr"));
    
    // --- BXSL LOGIC START ---
    let col = { company: -1, industry: -1, type: -1, interest: -1, spread: -1, maturity: -1, par: -1, cost: -1, fair: -1 };
    let dataStart = 0;

    for (let i = 0; i < Math.min(8, rows.length); i++) {
      const cells = Array.from(rows[i].querySelectorAll("td, th"));
      let cIdx = 0;
      for (const cell of cells) {
        const txt = cell.textContent.toLowerCase();
        const span = parseInt(cell.getAttribute("colspan") || "1");

        // BXSL Specific Headers
        if ((txt.includes('company') || txt.includes('investments')) && !txt.includes('type') && col.company === -1) col.company = cIdx;
        if ((txt.includes('industry') || txt.includes('sector')) && col.industry === -1) col.industry = cIdx;
        if (txt.includes('type') && col.type === -1) col.type = cIdx;
        if (txt.includes('interest') && !txt.includes('spread') && col.interest === -1) col.interest = cIdx;
        if ((txt.includes('spread') || txt.includes('reference')) && col.spread === -1) col.spread = cIdx;
        if (txt.includes('maturity') && col.maturity === -1) col.maturity = cIdx;
        if ((txt.includes('principal') || txt.includes('par')) && col.par === -1) col.par = cIdx;
        if (txt.includes('cost') && col.cost === -1) col.cost = cIdx;
        if (txt.includes('fair') && col.fair === -1) col.fair = cIdx;
        cIdx += span;
      }
      if (col.company > -1 || col.fair > -1) { dataStart = i + 1; if (col.fair > -1) break; }
    }
    
    if (col.company === -1) col.company = 0;
    let currentCompany = null;
    let currentIndustry = null;

    for (let i = dataStart; i < rows.length; i++) {
      const cells = Array.from(rows[i].querySelectorAll("td, th"));
      const flatCells: string[] = [];
      cells.forEach(c => {
          const txt = c.textContent.trim();
          const span = parseInt(c.getAttribute("colspan") || "1");
          flatCells.push(txt);
          for(let k=1; k<span; k++) flatCells.push("");
      });

      if (flatCells.length < 3) continue;

      let comp = flatCells[col.company] || flatCells[0];
      comp = cleanCompanyName(comp);
      if (/(total|subtotal)/i.test(comp)) continue;

      let effectiveComp = comp;
      if (hasCompanySuffix(comp)) currentCompany = comp;
      else if (currentCompany && (!comp || comp.length < 5)) effectiveComp = currentCompany;
      else if (!comp) continue;

      // BXSL: Industry is usually a dedicated column
      if (col.industry > -1 && flatCells[col.industry] && flatCells[col.industry].length > 3) currentIndustry = flatCells[col.industry];

      const fairVal = col.fair > -1 ? cleanNumeric(flatCells[col.fair]) : cleanNumeric(flatCells[flatCells.length - 1]);
      const costVal = col.cost > -1 ? cleanNumeric(flatCells[col.cost]) : cleanNumeric(flatCells[flatCells.length - 2]);

      if (fairVal === null && costVal === null) continue;

      const key = `${effectiveComp}-${fairVal}-${costVal}`;
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);

      let refRate = null;
      if (col.spread > -1 && flatCells[col.spread]) refRate = flatCells[col.spread];

      pendingRows.push({
        filing_id: filingId,
        company_name: effectiveComp,
        investment_type: col.type > -1 ? flatCells[col.type] : null,
        industry: currentIndustry,
        interest_rate: col.interest > -1 ? flatCells[col.interest] : null,
        reference_rate: refRate,
        maturity_date: col.maturity > -1 ? parseDate(flatCells[col.maturity]) : null,
        par_amount: col.par > -1 ? toMillions(cleanNumeric(flatCells[col.par]), scale) : null,
        cost: toMillions(costVal, scale),
        fair_value: toMillions(fairVal, scale),
        row_number: insertedCount + pendingRows.length + 1
      });
    }
    // --- BXSL LOGIC END ---

    if (pendingRows.length >= 200) {
      await supabaseClient.from("holdings").insert(pendingRows.splice(0, pendingRows.length));
      insertedCount += 200;
    }
  }

  if (pendingRows.length > 0) {
    await supabaseClient.from("holdings").insert(pendingRows);
    insertedCount += pendingRows.length;
  }
  return insertedCount;
}

// ======================================================================
// 3. ARCC / GENERIC PARSER (Chunked DOM)
// Handles ARCC's "Industry Header Rows" logic
// ======================================================================
async function parseARCC(html: string, filingId: string, supabaseClient: any, scale: number) {
  console.log("📘 Running ARCC Specific Parser...");
  
  const soiMatch = /(consolidated schedule of investments|schedule of investments)/i.exec(html);
  if (!soiMatch) return 0;
  const content = html.slice(soiMatch.index);
  const tableRe = /<table\b[^>]*>([\s\S]*?)<\/table>/gi;

  await supabaseClient.from("holdings").delete().eq("filing_id", filingId);

  let insertedCount = 0;
  let tableMatch;
  const pendingRows: any[] = [];
  const seenKeys = new Set<string>();

  while ((tableMatch = tableRe.exec(content)) !== null) {
    const tableHtml = tableMatch[0];
    if (!/(llc|inc|corp|l\.p\.|limited|\$|fair value)/i.test(tableHtml)) continue;

    const doc = new DOMParser().parseFromString(tableHtml, "text/html");
    if (!doc) continue;
    const rows = Array.from(doc.querySelectorAll("tr"));

    // --- ARCC LOGIC START ---
    let col = { company: -1, type: -1, industry: -1, interest: -1, maturity: -1, par: -1, cost: -1, fair: -1 };
    let dataStart = 0;

    for (let i = 0; i < Math.min(8, rows.length); i++) {
      const cells = Array.from(rows[i].querySelectorAll("td, th"));
      let cIdx = 0;
      for (const cell of cells) {
        const txt = cell.textContent.toLowerCase();
        const span = parseInt(cell.getAttribute("colspan") || "1");
        
        if ((txt.includes('company') || txt.includes('issuer')) && !txt.includes('type') && col.company === -1) col.company = cIdx;
        if (txt.includes('industry') && col.industry === -1) col.industry = cIdx;
        if (txt.includes('type') && col.type === -1) col.type = cIdx;
        if (txt.includes('interest') && col.interest === -1) col.interest = cIdx;
        if (txt.includes('maturity') && col.maturity === -1) col.maturity = cIdx;
        if ((txt.includes('principal') || txt.includes('par')) && col.par === -1) col.par = cIdx;
        if (txt.includes('cost') && col.cost === -1) col.cost = cIdx;
        if (txt.includes('fair') && col.fair === -1) col.fair = cIdx;
        cIdx += span;
      }
      if (col.company > -1 || col.fair > -1) { dataStart = i + 1; if (col.fair > -1) break; }
    }
    
    if (col.company === -1) col.company = 0;
    let currentCompany = null;
    let currentIndustry = null;

    for (let i = dataStart; i < rows.length; i++) {
      const cells = Array.from(rows[i].querySelectorAll("td, th"));
      const flatCells: string[] = [];
      cells.forEach(c => {
          const txt = c.textContent.trim();
          const span = parseInt(c.getAttribute("colspan") || "1");
          flatCells.push(txt);
          for(let k=1; k<span; k++) flatCells.push("");
      });

      if (flatCells.length < 3) continue;

      // ARCC Specific: Industry is often a row header
      if (cells.length === 1 || (flatCells[0] && flatCells.slice(1).every(x => !x))) {
          const header = flatCells[0];
          if (header.length > 3 && !hasCompanySuffix(header) && !/(total|subtotal)/i.test(header)) {
              currentIndustry = header;
              continue;
          }
      }

      let comp = flatCells[col.company] || flatCells[0];
      comp = cleanCompanyName(comp);
      if (/(total|subtotal|balance)/i.test(comp)) continue;

      let effectiveComp = comp;
      if (hasCompanySuffix(comp)) currentCompany = comp;
      else if (currentCompany && (!comp || comp.length < 5)) effectiveComp = currentCompany;
      else if (!comp) continue;

      const fairVal = col.fair > -1 ? cleanNumeric(flatCells[col.fair]) : cleanNumeric(flatCells[flatCells.length - 1]);
      const costVal = col.cost > -1 ? cleanNumeric(flatCells[col.cost]) : cleanNumeric(flatCells[flatCells.length - 2]);

      if (fairVal === null && costVal === null) continue;

      const key = `${effectiveComp}-${fairVal}-${costVal}`;
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);

      pendingRows.push({
        filing_id: filingId,
        company_name: effectiveComp,
        investment_type: col.type > -1 ? flatCells[col.type] : null,
        industry: col.industry > -1 ? flatCells[col.industry] : currentIndustry,
        interest_rate: col.interest > -1 ? flatCells[col.interest] : null,
        maturity_date: col.maturity > -1 ? parseDate(flatCells[col.maturity]) : null,
        par_amount: col.par > -1 ? toMillions(cleanNumeric(flatCells[col.par]), scale) : null,
        cost: toMillions(costVal, scale),
        fair_value: toMillions(fairVal, scale),
        row_number: insertedCount + pendingRows.length + 1
      });
    }
    // --- ARCC LOGIC END ---

    if (pendingRows.length >= 200) {
      await supabaseClient.from("holdings").insert(pendingRows.splice(0, pendingRows.length));
      insertedCount += 200;
    }
  }
  if (pendingRows.length > 0) {
    await supabaseClient.from("holdings").insert(pendingRows);
    insertedCount += pendingRows.length;
  }
  return insertedCount;
}

// ======================================================================
// MAIN SWITCHBOARD
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
    console.log(`Processing ${accessionNo} using ${parserType} parser`);

    const paddedCik = cik.replace(/^0+/, "");
    const accNoClean = accessionNo.replace(/-/g, "");
    
    // Fetch Index
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accNoClean}/index.json`;
    const indexRes = await fetch(indexUrl, { headers: { "User-Agent": SEC_USER_AGENT } });
    if (!indexRes.ok) throw new Error("Failed to fetch index.json");
    
    const indexJson = await indexRes.json();
    const docs = indexJson.directory?.item || [];
    
    const htmDocs = docs.filter((d: any) => d.name.endsWith(".htm") || d.name.endsWith(".html"));
    if (htmDocs.length === 0) throw new Error("No HTML found");

    // Process largest file (most likely to contain schedule)
    const targetDoc = htmDocs.sort((a: any, b: any) => parseInt(b.size) - parseInt(a.size))[0];
    const docUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accNoClean}/${targetDoc.name}`;
    
    const htmlRes = await fetch(docUrl, { headers: { "User-Agent": SEC_USER_AGENT } });
    const html = await htmlRes.text();
    const scaleRes = detectScale(html);
    
    let totalInserted = 0;

    if (parserType === 'GBDC') {
      totalInserted = await parseGBDC(html, filingId, supabaseClient, scaleRes);
    } else if (parserType === 'BXSL') {
      totalInserted = await parseBXSL(html, filingId, supabaseClient, scaleRes);
    } else {
      totalInserted = await parseARCC(html, filingId, supabaseClient, scaleRes);
    }

    if (totalInserted > 0) {
      await supabaseClient.from("filings")
        .update({ parsed_successfully: true, value_scale: scaleRes === 1 ? 'millions' : 'thousands' })
        .eq("id", filingId);
    }

    return new Response(JSON.stringify({ success: true, count: totalInserted }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });

  } catch (error) {
    console.error("Error:", error);
    return new Response(JSON.stringify({ error: error instanceof Error ? error.message : "Unknown" }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });
  }
});
