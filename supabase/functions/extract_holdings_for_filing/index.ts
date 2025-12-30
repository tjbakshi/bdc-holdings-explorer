import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { DOMParser, Element } from "https://deno.land/x/deno_dom@v0.1.38/deno-dom-wasm.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const SEC_USER_AGENT = "BDCTrackerApp/1.0 (contact@bdctracker.com)";

interface Holding {
  company_name: string;
  investment_type?: string | null;
  industry?: string | null;
  description?: string | null;
  interest_rate?: string | null;
  reference_rate?: string | null;
  maturity_date?: string | null;
  par_amount?: number | null;
  cost?: number | null;
  fair_value?: number | null;
}

// Helper to fetch SEC files with retry
async function fetchSecFile(url: string, retries = 2): Promise<string> {
  for (let i = 0; i <= retries; i++) {
    try {
      console.log(`Fetching: ${url}`);
      const response = await fetch(url, {
        headers: { "User-Agent": SEC_USER_AGENT },
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      return await response.text();
    } catch (error) {
      if (i === retries) throw error;
      console.log(`Retry ${i + 1}/${retries} for ${url}`);
      await new Promise((resolve) => setTimeout(resolve, 1000 * (i + 1)));
    }
  }
  throw new Error("Failed to fetch after retries");
}

// Parse numeric value from string (handles $, commas, parentheses for negatives)
function parseNumeric(value: string | null | undefined): number | null {
  if (!value) return null;
  
  const cleaned = value.replace(/[$,\s]/g, "").trim();
  if (!cleaned || cleaned === "-" || cleaned === "—") return null;
  
  // Handle parentheses as negative
  const isNegative = cleaned.startsWith("(") && cleaned.endsWith(")");
  const numStr = isNegative ? cleaned.slice(1, -1) : cleaned;
  
  const parsed = parseFloat(numStr);
  if (isNaN(parsed)) return null;
  
  return isNegative ? -parsed : parsed;
}

// Parse date from various formats
function parseDate(value: string | null | undefined): string | null {
  if (!value) return null;
  
  const cleaned = value.trim();
  if (!cleaned || cleaned === "-" || cleaned === "—") return null;
  
  // Try parsing common date formats
  const date = new Date(cleaned);
  if (!isNaN(date.getTime())) {
    return date.toISOString().split("T")[0];
  }
  
  return null;
}

// Extract interest rate info
function extractInterestRate(text: string): { rate: string | null; reference: string | null } {
  if (!text) return { rate: null, reference: null };
  
  const lowerText = text.toLowerCase();
  
  // Common reference rates
  const referenceRates = ["sofr", "libor", "prime", "euribor"];
  let reference = null;
  
  for (const ref of referenceRates) {
    if (lowerText.includes(ref)) {
      reference = ref.toUpperCase();
      break;
    }
  }
  
  // If we found a reference rate, return the full text as rate
  if (reference) {
    return { rate: text.trim(), reference };
  }
  
  // Check for fixed rate (e.g., "8.5%", "10.00%")
  const fixedRateMatch = text.match(/(\d+\.?\d*)\s*%/);
  if (fixedRateMatch) {
    return { rate: text.trim(), reference: null };
  }
  
  return { rate: text.trim(), reference: null };
}

// Extract candidate HTML snippets containing Schedule of Investments
function extractCandidateTableHtml(html: string): string[] {
  // Normalize HTML to handle &nbsp; and extra whitespace
  const normalized = html.replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ');
  const lower = normalized.toLowerCase();
  
  // Broaden keywords to catch more variations
  const keywords = [
    "consolidated schedule of investments",
    "schedule of investments (unaudited)",
    "schedule of investments (continued)",
    "schedule of investments",
    "portfolio investments",
  ];
  
  const snippets: string[] = [];
  
  for (const keyword of keywords) {
    let searchStart = 0;
    while (true) {
      const idx = lower.indexOf(keyword, searchStart);
      if (idx === -1) break;
      
      // Take a window around the keyword (±150kb)
      const start = Math.max(0, idx - 150_000);
      const end = Math.min(html.length, idx + 150_000);
      snippets.push(html.slice(start, end));
      
      searchStart = idx + keyword.length;
      // Only find first occurrence per keyword to avoid too many snippets
      break;
    }
  }
  
  // If we didn't find any keyword, fall back to the first ~300kb of the file
  if (snippets.length === 0) {
    console.log("No SOI keywords found, using first 300KB");
    snippets.push(html.slice(0, 300_000));
  } else {
    console.log(`Found ${snippets.length} candidate SOI regions`);
  }
  
  return snippets;
}

// Helper to find header row in a table
function findHeaderRow(table: Element, debugMode = false): Element | null {
  const rows = (table as Element).querySelectorAll("tr");
  
  // Scan first ~5 rows to find the header
  const maxHeaderScan = Math.min(5, rows.length);
  
  for (let i = 0; i < maxHeaderScan; i++) {
    const row = rows[i] as Element;
    const rowText = row.textContent?.toLowerCase() || "";
    
    // Header must contain fair value/market value (relaxed: cost is optional)
    const hasFairValue = rowText.includes("fair value") || 
                        rowText.includes("market value") ||
                        rowText.includes("fair");
    const hasCost = rowText.includes("cost") || rowText.includes("amortized");
    const hasCompany = rowText.includes("company") || 
                       rowText.includes("portfolio") || 
                       rowText.includes("name") ||
                       rowText.includes("issuer") ||
                       rowText.includes("investment");
    
    // Accept header if it has fair value + company, cost is optional
    if (hasFairValue && hasCompany) {
      if (debugMode) {
        const headerCells = Array.from(row.querySelectorAll("th, td"));
        const headers = headerCells.map(h => (h as Element).textContent?.trim() || "");
        console.log("✓ Found candidate header row:", headers);
      }
      return row;
    }
  }
  
  return null;
}

// Parse tables looking for Schedule of Investments
function parseTables(tables: Iterable<Element>, maxRowsPerTable: number, maxHoldings: number, debugMode = false): Holding[] {
  const holdings: Holding[] = [];
  
  let tableIndex = 0;
  for (const table of tables) {
    tableIndex++;
    
    // Find the header row (don't assume it's the first row)
    const headerRow = findHeaderRow(table as Element, debugMode);
    if (!headerRow) {
      if (debugMode && tableIndex <= 10) {
        console.log(`⊗ Table ${tableIndex}: No valid header row found`);
      }
      continue;
    }
    
    // Find column indices
    const headerCells = Array.from(headerRow.querySelectorAll("th, td"));
    const headers = headerCells.map(
      (h) => (h as Element).textContent?.toLowerCase().trim() || ""
    );
    
    if (debugMode) {
      console.log(`\n=== Table ${tableIndex} Headers ===`);
      console.log("Raw headers:", headers);
    }
    
    const colIndices = {
      company: headers.findIndex((h) => 
        h.includes("company") || 
        h.includes("portfolio") || 
        h.includes("name") || 
        h.includes("issuer") ||
        h.includes("borrower") ||
        h.includes("investment")
      ),
      investmentType: headers.findIndex((h) => 
        h.includes("type") || h.includes("instrument") || h.includes("class")
      ),
      industry: headers.findIndex((h) => 
        h.includes("industry") || h.includes("sector")
      ),
      description: headers.findIndex((h) => 
        h.includes("description") || h.includes("notes")
      ),
      interestRate: headers.findIndex((h) => 
        h.includes("interest") || h.includes("rate") || h.includes("coupon") || h.includes("spread")
      ),
      maturity: headers.findIndex((h) => 
        h.includes("maturity") || h.includes("expiration") || h.includes("due")
      ),
      par: headers.findIndex((h) => 
        h.includes("par") || 
        h.includes("principal") || 
        h.includes("face") ||
        (h.includes("amount") && !h.includes("fair") && !h.includes("cost"))
      ),
      cost: headers.findIndex((h) => 
        h.includes("cost") || h.includes("amortized")
      ),
      fairValue: headers.findIndex((h) => 
        h.includes("fair") || 
        h.includes("market") ||
        (h.includes("value") && !h.includes("par"))
      ),
    };
    
    if (debugMode) {
      console.log("Column indices:", colIndices);
    }
    
    // Must have at least company and fair value columns
    if (colIndices.company === -1 || colIndices.fairValue === -1) {
      if (debugMode) {
        console.log(`⊗ Table ${tableIndex}: Missing required columns (company: ${colIndices.company}, fairValue: ${colIndices.fairValue})`);
      }
      continue;
    }
    
    if (debugMode) {
      console.log(`✓ Table ${tableIndex}: Valid structure, attempting to parse rows...`);
    }
    
    // Parse data rows
    const rows = (table as Element).querySelectorAll("tr");
    
    // Find the index of the header row
    let headerRowIndex = 0;
    for (let idx = 0; idx < rows.length; idx++) {
      if (rows[idx] === headerRow) {
        headerRowIndex = idx;
        break;
      }
    }
    
    // Cap the number of rows we process per table to prevent blowup
    const rowsToProcess = Math.min(rows.length, headerRowIndex + maxRowsPerTable + 1);
    
    for (let i = headerRowIndex + 1; i < rowsToProcess; i++) {
      const row = rows[i] as Element;
      const cellNodes = Array.from(row.querySelectorAll("td"));
      const cells = cellNodes.map(c => c as Element);
      
      if (cells.length === 0) continue;
      
      const companyName = cells[colIndices.company]?.textContent?.trim();
      if (!companyName || companyName.length < 2) continue;
      
      // Skip subtotal/total rows
      if (
        companyName.toLowerCase().includes("total") ||
        companyName.toLowerCase().includes("subtotal")
      ) {
        continue;
      }
      
      const interestRateText = 
        colIndices.interestRate >= 0 
          ? cells[colIndices.interestRate]?.textContent?.trim() || ""
          : "";
      
      const { rate, reference } = extractInterestRate(interestRateText);
      
      const holding: Holding = {
        company_name: companyName,
        investment_type: 
          colIndices.investmentType >= 0 
            ? cells[colIndices.investmentType]?.textContent?.trim() || null
            : null,
        industry: 
          colIndices.industry >= 0 
            ? cells[colIndices.industry]?.textContent?.trim() || null
            : null,
        description: 
          colIndices.description >= 0 
            ? cells[colIndices.description]?.textContent?.trim() || null
            : null,
        interest_rate: rate,
        reference_rate: reference,
        maturity_date: parseDate(
          colIndices.maturity >= 0 
            ? cells[colIndices.maturity]?.textContent?.trim()
            : null
        ),
        par_amount: parseNumeric(
          colIndices.par >= 0 
            ? cells[colIndices.par]?.textContent?.trim()
            : null
        ),
        cost: parseNumeric(
          colIndices.cost >= 0 
            ? cells[colIndices.cost]?.textContent?.trim()
            : null
        ),
        fair_value: parseNumeric(
          cells[colIndices.fairValue]?.textContent?.trim()
        ),
      };
      
      // Only add if we have fair value (required field)
      if (holding.fair_value !== null) {
        holdings.push(holding);
      }
      
      // Cap total holdings to prevent excessive memory usage
      if (holdings.length >= maxHoldings) {
        console.log(`Reached max holdings cap (${maxHoldings}), stopping parse`);
        return holdings;
      }
    }
    
    // If we found holdings in this table, stop searching
    if (holdings.length > 0) {
      return holdings;
    }
  }
  
  return holdings;
}

// Parse HTML Schedule of Investments table from snippets
function parseHtmlScheduleOfInvestments(html: string, debugMode = false): Holding[] {
  const maxRowsPerTable = 500;
  const maxHoldings = 1000;
  
  try {
    // First try: Extract only relevant HTML snippets
    const snippets = extractCandidateTableHtml(html);
    
    for (const snippet of snippets) {
      const doc = new DOMParser().parseFromString(snippet, "text/html");
      if (!doc) continue;
      
      const tables = Array.from(doc.querySelectorAll("table")) as Element[];
      const holdings = parseTables(tables, maxRowsPerTable, maxHoldings, debugMode);
      
      if (holdings.length > 0) {
        console.log(`Found ${holdings.length} holdings in snippet`);
        return holdings;
      }
    }
    
    // No full-document fallback to avoid WORKER_LIMIT on large filings
    console.log("No holdings found in snippets; returning empty result without full-document fallback");
  }
  catch (error) {
    console.error("Error parsing HTML:", error);
  }
  
  return [];
}

// Main serve function
serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { filingId, mode } = await req.json();

    if (!filingId) {
      throw new Error("filingId is required");
    }

    const supabaseClient = createClient(
      Deno.env.get("SUPABASE_URL") ?? "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
    );

    // Fetch filing details
    const { data: filing, error: filingError } = await supabaseClient
      .from("filings")
      .select(`
        *,
        bdcs (cik, bdc_name)
      `)
      .eq("id", filingId)
      .single();

    if (filingError) {
      throw new Error(`Filing not found: ${filingError.message}`);
    }

    const cik = filing.bdcs.cik;
    const accessionNo = filing.sec_accession_no;
    const bdcName = filing.bdcs.bdc_name;

    console.log(`Extracting holdings for filing ${accessionNo} (CIK: ${cik}, BDC: ${bdcName})`);
    console.log(`Using local parser`);

    // Build filing document URL
    const accessionNoNoDashes = accessionNo.replace(/-/g, "");
    const paddedCik = cik.replace(/^0+/, ""); // Remove leading zeros for URL
    
    // Try to fetch the primary filing document (usually the .htm file)
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accessionNoNoDashes}/index.json`;
    console.log(`Index URL: ${indexUrl}`);
    
    let holdings: Holding[] = [];
    const warnings: string[] = [];
    let docUrl = "";
    
    // Enable debug mode for specific test filing
    const debugMode = accessionNo === "0001104659-25-108820";
    if (debugMode) {
      console.log("\n🔍 DEBUG MODE ENABLED for test filing 0001104659-25-108820\n");
    }
    
    try {
      // Fetch the filing index to find the primary document
      const indexJson = await fetchSecFile(indexUrl);
      const index = JSON.parse(indexJson);
      
      // Log all available documents for debugging
      if (debugMode && index.directory?.item) {
        console.log("\n=== Available documents in index.json ===");
        index.directory.item.forEach((item: any, idx: number) => {
          console.log(`${idx + 1}. ${item.name} (type: ${item.type || 'N/A'})`);
        });
        console.log("=====================================\n");
      }
      
      // Find all .htm documents (not just primary)
      const htmDocs = (index.directory?.item || []).filter(
        (item: any) => item.name.endsWith(".htm") || item.name.endsWith(".html")
      );
      
      // Prioritize documents that might contain schedules
      const prioritizedDocs = [...htmDocs].sort((a: any, b: any) => {
        const aName = a.name.toLowerCase();
        const bName = b.name.toLowerCase();
        
        // Highest priority: documents with "schedule", "soi", "portfolio" in name
        const aIsSchedule = aName.includes("schedule") || aName.includes("soi") || aName.includes("portfolio");
        const bIsSchedule = bName.includes("schedule") || bName.includes("soi") || bName.includes("portfolio");
        if (aIsSchedule && !bIsSchedule) return -1;
        if (!aIsSchedule && bIsSchedule) return 1;
        
        // Next priority: primary document
        if (a.type === "primary" && b.type !== "primary") return -1;
        if (a.type !== "primary" && b.type === "primary") return 1;
        
        // Deprioritize documents with underscores (usually graphics/exhibits)
        const aHasUnderscore = aName.includes("_");
        const bHasUnderscore = bName.includes("_");
        if (!aHasUnderscore && bHasUnderscore) return -1;
        if (aHasUnderscore && !bHasUnderscore) return 1;
        
        return 0;
      });
      
      if (debugMode) {
        console.log("\n=== Document processing order ===");
        prioritizedDocs.forEach((doc: any, idx: number) => {
          console.log(`${idx + 1}. ${doc.name} (type: ${doc.type || 'N/A'})`);
        });
        console.log("=================================\n");
      }
      
      // Try parsing each document until we find holdings
      for (const doc of prioritizedDocs) {
        docUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accessionNoNoDashes}/${doc.name}`;
        console.log(`\n📄 Trying document: ${doc.name}`);
        console.log(`   URL: ${docUrl}`);
        
        try {
          const html = await fetchSecFile(docUrl);
          console.log(`   Size: ${(html.length / 1024).toFixed(0)} KB`);
          
          // Hard limit: Skip documents over 1MB to avoid WORKER_LIMIT
          if (html.length > 1_000_000) {
            warnings.push(`Document ${doc.name} too large to safely parse (${(html.length / 1024).toFixed(0)} KB > 1MB). Skipping.`);
            console.log(`   ⚠️ Document too large, skipping`);
            continue;
          }
          
          holdings = parseHtmlScheduleOfInvestments(html, debugMode);
          
          console.log(`   Result: ${holdings.length} holdings found`);
          
          if (holdings.length > 0) {
            console.log(`✅ Successfully extracted ${holdings.length} holdings from ${doc.name}`);
            break; // Stop trying more documents
          }
        } catch (docError) {
          console.error(`   Error parsing ${doc.name}:`, docError);
          continue; // Try next document
        }
      }
      
      if (holdings.length === 0 && prioritizedDocs.length === 0) {
        warnings.push("Could not locate any HTML documents in filing");
      }
    } catch (error) {
      console.error("Error fetching/parsing filing:", error);
      warnings.push(`Parsing error: ${error instanceof Error ? error.message : "Unknown error"}`);
    }
    
    // If we found holdings, insert them
    if (holdings.length > 0) {
      const holdingsToInsert = holdings.map((h) => ({
        filing_id: filingId,
        ...h,
      }));

      const { error: insertError } = await supabaseClient
        .from("holdings")
        .insert(holdingsToInsert);

      if (insertError) {
        throw new Error(`Error inserting holdings: ${insertError.message}`);
      }

      // Mark filing as parsed successfully
      const { error: updateError } = await supabaseClient
        .from("filings")
        .update({ parsed_successfully: true })
        .eq("id", filingId);

      if (updateError) {
        console.error("Error updating filing status:", updateError);
      }

      console.log(`Inserted ${holdingsToInsert.length} holdings for filing ${accessionNo}`);

      return new Response(
        JSON.stringify({
          filingId,
          holdingsInserted: holdingsToInsert.length,
          warnings: warnings.length > 0 ? warnings : undefined,
        }),
        {
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        }
      );
    } else {
      // No holdings found - return 200 with warnings
      warnings.push("No holdings found in filing (snippet-based parsing only)");
      
      console.log(`No holdings found. Index URL: ${indexUrl}`);
      console.log(`Doc URL: ${docUrl}`);
      
      return new Response(
        JSON.stringify({
          filingId,
          holdingsInserted: 0,
          warnings,
        }),
        {
          headers: { ...corsHeaders, "Content-Type": "application/json" },
          status: 200,
        }
      );
    }
  } catch (error) {
    console.error("Error in extract_holdings_for_filing:", error);
    const errorMessage = error instanceof Error ? error.message : "Unknown error";
    return new Response(
      JSON.stringify({ error: errorMessage }),
      {
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      }
    );
  }
});
