import { Handler } from 'aws-lambda';

// Types
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

interface LambdaEvent {
  body: string;
}

interface RequestBody {
  filingId: string;
}

const SEC_USER_AGENT = process.env.SEC_USER_AGENT || "BDCTrackerApp/1.0 (contact@bdctracker.com)";
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

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

// Parse numeric value from string
function parseNumeric(value: string | null | undefined): number | null {
  if (!value) return null;
  
  const cleaned = value.replace(/[$,\s]/g, "").trim();
  if (!cleaned || cleaned === "-" || cleaned === "—") return null;
  
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
  
  const referenceRates = ["sofr", "libor", "prime", "euribor"];
  let reference = null;
  
  for (const ref of referenceRates) {
    if (lowerText.includes(ref)) {
      reference = ref.toUpperCase();
      break;
    }
  }
  
  if (reference) {
    return { rate: text.trim(), reference };
  }
  
  const fixedRateMatch = text.match(/(\d+\.?\d*)\s*%/);
  if (fixedRateMatch) {
    return { rate: text.trim(), reference: null };
  }
  
  return { rate: text.trim(), reference: null };
}

// Extract candidate HTML snippets containing Schedule of Investments
function extractCandidateTableHtml(html: string): string[] {
  const normalized = html.replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ');
  const lower = normalized.toLowerCase();
  
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
      
      const start = Math.max(0, idx - 150_000);
      const end = Math.min(html.length, idx + 150_000);
      snippets.push(html.slice(start, end));
      
      searchStart = idx + keyword.length;
      break;
    }
  }
  
  if (snippets.length === 0) {
    console.log("No SOI keywords found, using first 300KB");
    snippets.push(html.slice(0, 300_000));
  } else {
    console.log(`Found ${snippets.length} candidate SOI regions`);
  }
  
  return snippets;
}

// Helper to find header row in a table
function findHeaderRow(table: any, debugMode = false): any | null {
  const rows = table.querySelectorAll("tr");
  
  const maxHeaderScan = Math.min(5, rows.length);
  
  for (let i = 0; i < maxHeaderScan; i++) {
    const row = rows[i];
    const rowText = row.textContent?.toLowerCase() || "";
    
    const hasFairValue = rowText.includes("fair value") || 
                        rowText.includes("market value") ||
                        rowText.includes("fair");
    const hasCompany = rowText.includes("company") || 
                       rowText.includes("portfolio") || 
                       rowText.includes("name") ||
                       rowText.includes("issuer") ||
                       rowText.includes("investment");
    
    if (hasFairValue && hasCompany) {
      if (debugMode) {
        const headerCells = Array.from(row.querySelectorAll("th, td"));
        const headers = headerCells.map((h: any) => h.textContent?.trim() || "");
        console.log("✓ Found candidate header row:", headers);
      }
      return row;
    }
  }
  
  return null;
}

// Parse tables looking for Schedule of Investments
function parseTables(tables: any[], maxRowsPerTable: number, maxHoldings: number, debugMode = false): Holding[] {
  const holdings: Holding[] = [];
  
  let tableIndex = 0;
  for (const table of tables) {
    tableIndex++;
    
    const headerRow = findHeaderRow(table, debugMode);
    if (!headerRow) {
      if (debugMode && tableIndex <= 10) {
        console.log(`⊗ Table ${tableIndex}: No valid header row found`);
      }
      continue;
    }
    
    const headerCells = Array.from(headerRow.querySelectorAll("th, td"));
    const headers = headerCells.map(
      (h: any) => h.textContent?.toLowerCase().trim() || ""
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
    
    if (colIndices.company === -1 || colIndices.fairValue === -1) {
      if (debugMode) {
        console.log(`⊗ Table ${tableIndex}: Missing required columns`);
      }
      continue;
    }
    
    if (debugMode) {
      console.log(`✓ Table ${tableIndex}: Valid structure, attempting to parse rows...`);
    }
    
    const rows = table.querySelectorAll("tr");
    
    let headerRowIndex = 0;
    for (let idx = 0; idx < rows.length; idx++) {
      if (rows[idx] === headerRow) {
        headerRowIndex = idx;
        break;
      }
    }
    
    const rowsToProcess = Math.min(rows.length, headerRowIndex + maxRowsPerTable + 1);
    
    for (let i = headerRowIndex + 1; i < rowsToProcess; i++) {
      const row = rows[i];
      const cellNodes = Array.from(row.querySelectorAll("td"));
      const cells = cellNodes.map((c: any) => c);
      
      if (cells.length === 0) continue;
      
      const companyName = cells[colIndices.company]?.textContent?.trim();
      if (!companyName || companyName.length < 2) continue;
      
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
      
      if (holding.fair_value !== null) {
        holdings.push(holding);
      }
      
      if (holdings.length >= maxHoldings) {
        console.log(`Reached max holdings cap (${maxHoldings}), stopping parse`);
        return holdings;
      }
    }
    
    if (holdings.length > 0) {
      return holdings;
    }
  }
  
  return holdings;
}

// Parse HTML Schedule of Investments table from snippets
function parseHtmlScheduleOfInvestments(html: string, debugMode = false): Holding[] {
  const maxRowsPerTable = 1000;
  const maxHoldings = 5000;
  
  // Import JSDOM for Lambda environment
  const { JSDOM } = require('jsdom');
  
  try {
    const snippets = extractCandidateTableHtml(html);
    
    for (const snippet of snippets) {
      const dom = new JSDOM(snippet);
      const doc = dom.window.document;
      
      const tables = Array.from(doc.querySelectorAll("table"));
      const holdings = parseTables(tables, maxRowsPerTable, maxHoldings, debugMode);
      
      if (holdings.length > 0) {
        console.log(`Found ${holdings.length} holdings in snippet`);
        return holdings;
      }
    }
    
    console.log("No holdings found in snippets");
  }
  catch (error) {
    console.error("Error parsing HTML:", error);
  }
  
  return [];
}

export const handler: Handler = async (event: LambdaEvent) => {
  try {
    const body: RequestBody = JSON.parse(event.body);
    const { filingId } = body;

    if (!filingId) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "filingId is required" }),
      };
    }

    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing Supabase configuration" }),
      };
    }

    console.log(`Processing filing: ${filingId}`);

    // Fetch filing details from Supabase
    const filingResponse = await fetch(
      `${SUPABASE_URL}/rest/v1/filings?id=eq.${filingId}&select=*,bdcs(cik,bdc_name)`,
      {
        headers: {
          'apikey': SUPABASE_SERVICE_ROLE_KEY,
          'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          'Content-Type': 'application/json',
        },
      }
    );

    if (!filingResponse.ok) {
      throw new Error(`Failed to fetch filing: ${filingResponse.statusText}`);
    }

    const filings = await filingResponse.json();
    if (!filings || filings.length === 0) {
      return {
        statusCode: 404,
        body: JSON.stringify({ error: "Filing not found" }),
      };
    }

    const filing = filings[0];
    const cik = filing.bdcs.cik;
    const accessionNo = filing.sec_accession_no;
    const bdcName = filing.bdcs.bdc_name;

    console.log(`Extracting holdings for filing ${accessionNo} (CIK: ${cik}, BDC: ${bdcName})`);

    // Build filing document URL
    const accessionNoNoDashes = accessionNo.replace(/-/g, "");
    const paddedCik = cik.replace(/^0+/, "");
    
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accessionNoNoDashes}/index.json`;
    console.log(`Index URL: ${indexUrl}`);
    
    let holdings: Holding[] = [];
    const warnings: string[] = [];
    let docUrl = "";
    
    try {
      const indexJson = await fetchSecFile(indexUrl);
      const index = JSON.parse(indexJson);
      
      const htmDocs = (index.directory?.item || []).filter(
        (item: any) => item.name.endsWith(".htm") || item.name.endsWith(".html")
      );
      
      const prioritizedDocs = [...htmDocs].sort((a: any, b: any) => {
        const aName = a.name.toLowerCase();
        const bName = b.name.toLowerCase();
        
        const aIsSchedule = aName.includes("schedule") || aName.includes("soi") || aName.includes("portfolio");
        const bIsSchedule = bName.includes("schedule") || bName.includes("soi") || bName.includes("portfolio");
        if (aIsSchedule && !bIsSchedule) return -1;
        if (!aIsSchedule && bIsSchedule) return 1;
        
        if (a.type === "primary" && b.type !== "primary") return -1;
        if (a.type !== "primary" && b.type === "primary") return 1;
        
        const aHasUnderscore = aName.includes("_");
        const bHasUnderscore = bName.includes("_");
        if (!aHasUnderscore && bHasUnderscore) return -1;
        if (aHasUnderscore && !bHasUnderscore) return 1;
        
        return 0;
      });
      
      console.log(`Processing ${prioritizedDocs.length} documents`);
      
      for (const doc of prioritizedDocs) {
        docUrl = `https://www.sec.gov/Archives/edgar/data/${paddedCik}/${accessionNoNoDashes}/${doc.name}`;
        console.log(`\n📄 Trying document: ${doc.name}`);
        
        try {
          const html = await fetchSecFile(docUrl);
          console.log(`   Size: ${(html.length / 1024).toFixed(0)} KB`);
          
          holdings = parseHtmlScheduleOfInvestments(html, false);
          
          console.log(`   Result: ${holdings.length} holdings found`);
          
          if (holdings.length > 0) {
            console.log(`✅ Successfully extracted ${holdings.length} holdings from ${doc.name}`);
            break;
          }
        } catch (docError) {
          console.error(`   Error parsing ${doc.name}:`, docError);
          continue;
        }
      }
      
      if (holdings.length === 0 && prioritizedDocs.length === 0) {
        warnings.push("Could not locate any HTML documents in filing");
      }
    } catch (error) {
      console.error("Error fetching/parsing filing:", error);
      warnings.push(`Parsing error: ${error instanceof Error ? error.message : "Unknown error"}`);
    }
    
    // Insert holdings into Supabase
    if (holdings.length > 0) {
      const holdingsToInsert = holdings.map((h) => ({
        filing_id: filingId,
        ...h,
      }));

      const insertResponse = await fetch(
        `${SUPABASE_URL}/rest/v1/holdings`,
        {
          method: 'POST',
          headers: {
            'apikey': SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal',
          },
          body: JSON.stringify(holdingsToInsert),
        }
      );

      if (!insertResponse.ok) {
        const errorText = await insertResponse.text();
        throw new Error(`Error inserting holdings: ${errorText}`);
      }

      // Mark filing as parsed successfully
      const updateResponse = await fetch(
        `${SUPABASE_URL}/rest/v1/filings?id=eq.${filingId}`,
        {
          method: 'PATCH',
          headers: {
            'apikey': SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ parsed_successfully: true }),
        }
      );

      if (!updateResponse.ok) {
        console.error("Error updating filing status:", await updateResponse.text());
      }

      console.log(`Inserted ${holdingsToInsert.length} holdings for filing ${accessionNo}`);

      return {
        statusCode: 200,
        body: JSON.stringify({
          filingId,
          holdingsInserted: holdingsToInsert.length,
          warnings: warnings.length > 0 ? warnings : undefined,
        }),
      };
    } else {
      warnings.push("No holdings found in filing");
      
      console.log(`No holdings found. Index URL: ${indexUrl}`);
      console.log(`Doc URL: ${docUrl}`);
      
      return {
        statusCode: 200,
        body: JSON.stringify({
          filingId,
          holdingsInserted: 0,
          warnings,
        }),
      };
    }
  } catch (error) {
    console.error("Error in Lambda handler:", error);
    const errorMessage = error instanceof Error ? error.message : "Unknown error";
    return {
      statusCode: 500,
      body: JSON.stringify({ error: errorMessage }),
    };
  }
};
