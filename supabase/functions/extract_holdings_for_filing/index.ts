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
  const lower = html.toLowerCase();
  
  // Prefer regions around "schedule of investments"
  const keywords = [
    "consolidated schedule of investments",
    "schedule of investments",
    "schedule of investments (continued)",
  ];
  
  const snippets: string[] = [];
  
  for (const keyword of keywords) {
    const idx = lower.indexOf(keyword);
    if (idx !== -1) {
      // Take a window around the keyword (±150kb)
      const start = Math.max(0, idx - 150_000);
      const end = Math.min(html.length, idx + 150_000);
      snippets.push(html.slice(start, end));
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

// Parse HTML Schedule of Investments table from snippets
function parseHtmlScheduleOfInvestments(html: string): Holding[] {
  const holdings: Holding[] = [];
  
  try {
    // Extract only relevant HTML snippets instead of parsing entire document
    const snippets = extractCandidateTableHtml(html);
    
    for (const snippet of snippets) {
      // Parse only this snippet
      const doc = new DOMParser().parseFromString(snippet, "text/html");
      if (!doc) continue;
      
      // Find tables that might contain Schedule of Investments
      const tables = doc.querySelectorAll("table");
    
    for (const table of tables) {
      // Look for table headers that indicate this is a Schedule of Investments
      const headerRow = (table as Element).querySelector("tr");
      if (!headerRow) continue;
      
      const headerText = headerRow.textContent?.toLowerCase() || "";
      
      // Check if this table looks like a Schedule of Investments
      const isSOI = 
        headerText.includes("portfolio") ||
        headerText.includes("investment") ||
        headerText.includes("company") ||
        (headerText.includes("fair value") && headerText.includes("cost"));
      
      if (!isSOI) continue;
      
      // Find column indices
      const headerCells = Array.from(headerRow.querySelectorAll("th, td"));
      const headers = headerCells.map(
        (h) => (h as Element).textContent?.toLowerCase().trim() || ""
      );
      
      const colIndices = {
        company: headers.findIndex((h) => 
          h.includes("company") || h.includes("portfolio") || h.includes("name") || h.includes("issuer")
        ),
        investmentType: headers.findIndex((h) => 
          h.includes("type") || h.includes("instrument")
        ),
        industry: headers.findIndex((h) => 
          h.includes("industry") || h.includes("sector")
        ),
        description: headers.findIndex((h) => 
          h.includes("description") || h.includes("investment")
        ),
        interestRate: headers.findIndex((h) => 
          h.includes("interest") || h.includes("rate") || h.includes("coupon")
        ),
        maturity: headers.findIndex((h) => 
          h.includes("maturity") || h.includes("expiration")
        ),
        par: headers.findIndex((h) => 
          h.includes("par") || h.includes("principal") || h.includes("face")
        ),
        cost: headers.findIndex((h) => 
          h.includes("cost") || h.includes("amortized")
        ),
        fairValue: headers.findIndex((h) => 
          h.includes("fair value") || h.includes("market value")
        ),
      };
      
      // Must have at least company and fair value columns
      if (colIndices.company === -1 || colIndices.fairValue === -1) continue;
      
      // Parse data rows
      const rows = (table as Element).querySelectorAll("tr");
      
      // Cap the number of rows we process per table to prevent blowup
      const maxRowsPerTable = 1000;
      const rowsToProcess = Math.min(rows.length, maxRowsPerTable + 1);
      
      for (let i = 1; i < rowsToProcess; i++) {
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
        if (holdings.length >= 2000) {
          console.log("Reached max holdings cap (2000), stopping parse");
          break;
        }
      }
      
      // If we found holdings in this snippet, no need to check others
      if (holdings.length > 0) {
        console.log(`Found ${holdings.length} holdings in snippet, stopping search`);
        break;
      }
    }
    
    // If we found holdings in this snippet, no need to check other snippets
    if (holdings.length > 0) {
      break;
    }
  }
  } catch (error) {
    console.error("Error parsing HTML:", error);
  }
  
  return holdings;
}

// Main serve function
serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { filingId } = await req.json();

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

    console.log(`Extracting holdings for filing ${accessionNo} (CIK: ${cik})`);

    // Build filing document URL
    const accessionNoNoDashes = accessionNo.replace(/-/g, "");
    const paddedCik = cik.replace(/^0+/, ""); // Remove leading zeros for URL
    
    // Try to fetch the primary filing document (usually the .htm file)
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accessionNoNoDashes}/index.json`;
    
    let holdings: Holding[] = [];
    const warnings: string[] = [];
    
    try {
      // Fetch the filing index to find the primary document
      const indexJson = await fetchSecFile(indexUrl);
      const index = JSON.parse(indexJson);
      
      // Find the primary document (usually the main .htm file)
      const primaryDoc = index.directory?.item?.find(
        (item: any) => 
          item.name.endsWith(".htm") && 
          !item.name.includes("_") && 
          item.type === "primary"
      ) || index.directory?.item?.find(
        (item: any) => item.name.endsWith(".htm")
      );
      
      if (primaryDoc) {
        const docUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accessionNoNoDashes}/${primaryDoc.name}`;
        console.log(`Fetching primary document: ${docUrl}`);
        
        const html = await fetchSecFile(docUrl);
        holdings = parseHtmlScheduleOfInvestments(html);
        
        console.log(`Parsed ${holdings.length} holdings from HTML`);
      } else {
        warnings.push("Could not locate primary filing document");
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
      // No holdings found - mark as failed parse
      warnings.push("No holdings found in filing");
      
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
