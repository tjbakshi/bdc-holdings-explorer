import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

// Zero-pad CIK to 10 digits (for SEC API)
function padCik(cik: string): string {
  const cleaned = cik.replace(/\D/g, "");
  return cleaned.padStart(10, "0");
}

// Normalize CIK by stripping leading zeros (for database)
function normalizeCik(cik: string): string {
  const cleaned = cik.replace(/\D/g, "");
  const stripped = cleaned.replace(/^0+/, "");
  return stripped || "0";
}

// Fetch with retry on 429
async function fetchWithRetry(
  url: string,
  options: RequestInit,
  maxRetries = 3
): Promise<Response> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const response = await fetch(url, options);
    
    if (response.status === 429) {
      console.log(`Rate limited (attempt ${attempt}/${maxRetries}), waiting...`);
      if (attempt < maxRetries) {
        await new Promise((r) => setTimeout(r, 2000 * attempt));
        continue;
      }
    }
    
    return response;
  }
  throw new Error("Max retries exceeded");
}

Deno.serve(async (req) => {
  // Handle CORS
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  // Only allow POST
  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "Method not allowed" }), {
      status: 405,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const userAgent = Deno.env.get("SEC_USER_AGENT") || "BDC-Analytics admin@example.com";

  const supabase = createClient(supabaseUrl, serviceRoleKey);

  let runId: string | null = null;
  let cik: string;

  try {
    const body = await req.json();
    cik = body.cik;

    if (!cik) {
      return new Response(JSON.stringify({ error: "Missing cik parameter" }), {
        status: 400,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    const paddedCik = padCik(cik);
    const normalizedCik = normalizeCik(cik);
    console.log(`Starting SEC ingest for CIK: ${paddedCik} (normalized: ${normalizedCik})`);

    // Create ingestion run record (uses normalized CIK)
    const { data: runData, error: runError } = await supabase
      .from("ingestion_runs")
      .insert({ cik: normalizedCik, status: "running" })
      .select("id")
      .single();

    if (runError) {
      console.error("Failed to create ingestion run:", runError);
      throw new Error(`Failed to create ingestion run record: ${runError.message}`);
    }
    runId = runData.id;
    console.log(`Created ingestion run: ${runId}`);

    // Fetch SEC submissions data (uses padded CIK for SEC API)
    const secUrl = `https://data.sec.gov/submissions/CIK${paddedCik}.json`;
    console.log(`Fetching SEC data: ${secUrl}`);

    const secResponse = await fetchWithRetry(secUrl, {
      headers: {
        "User-Agent": userAgent,
        Accept: "application/json",
      },
    });

    if (!secResponse.ok) {
      throw new Error(`SEC API returned ${secResponse.status}: ${secResponse.statusText}`);
    }

    const secData = await secResponse.json();
    const companyName = secData.name || `CIK ${paddedCik}`;
    console.log(`Company: ${companyName}`);

    // Ensure BDC exists (uses normalized CIK for database)
    console.log(`Looking up BDC with normalized CIK: ${normalizedCik}`);
    const { data: existingBdc, error: lookupError } = await supabase
      .from("bdcs")
      .select("id")
      .eq("cik", normalizedCik)
      .maybeSingle();

    if (lookupError) {
      console.error("BDC lookup error:", lookupError);
    }

    let bdcId: string;
    if (existingBdc) {
      bdcId = existingBdc.id;
      console.log(`Found existing BDC: ${bdcId}`);
    } else {
      // Create new BDC record (CIK will be normalized by trigger)
      console.log(`Creating new BDC with CIK: ${normalizedCik}`);
      const { data: newBdc, error: bdcError } = await supabase
        .from("bdcs")
        .insert({
          cik: normalizedCik,
          bdc_name: companyName,
          ticker: secData.tickers?.[0] || null,
          fiscal_year_end_month: 12,
          fiscal_year_end_day: 31,
        })
        .select("id")
        .single();

      if (bdcError) {
        console.error("BDC insert error:", bdcError);
        throw new Error(`Failed to create BDC: ${bdcError.message}`);
      }
      bdcId = newBdc.id;
      console.log(`Created new BDC: ${bdcId}`);
    }

    // Parse filings from SEC response
    const recent = secData.filings?.recent;
    if (!recent || !recent.accessionNumber) {
      throw new Error("No recent filings found in SEC response");
    }

    const filings: Array<{
      bdc_id: string;
      sec_accession_no: string;
      filing_type: string;
      period_end: string;
      filing_url: string;
      data_source: string;
    }> = [];

    // Filter for 10-K and 10-Q filings only
    const targetForms = ["10-K", "10-Q", "10-K/A", "10-Q/A"];

    for (let i = 0; i < recent.accessionNumber.length; i++) {
      const form = recent.form[i];
      if (!targetForms.includes(form)) continue;

      const accessionNo = recent.accessionNumber[i];
      const filingDate = recent.filingDate[i];
      const reportDate = recent.reportDate?.[i] || filingDate;
      const primaryDoc = recent.primaryDocument?.[i] || "";

      // Build SEC document URL
      const accessionNoClean = accessionNo.replace(/-/g, "");
      const filingUrl = `https://www.sec.gov/Archives/edgar/data/${parseInt(paddedCik)}/${accessionNoClean}/${primaryDoc}`;

      filings.push({
        bdc_id: bdcId,
        sec_accession_no: accessionNo,
        filing_type: form,
        period_end: reportDate,
        filing_url: filingUrl,
        data_source: "sec-ingest",
      });
    }

    console.log(`Found ${filings.length} 10-K/10-Q filings to insert`);

    // Insert filings (skip duplicates with ON CONFLICT)
    let insertedCount = 0;
    for (const filing of filings) {
      const { error: insertError } = await supabase
        .from("filings")
        .upsert(filing, {
          onConflict: "bdc_id,sec_accession_no",
          ignoreDuplicates: true,
        });

      if (!insertError) {
        insertedCount++;
      }
    }

    console.log(`Inserted ${insertedCount} new filings`);

    // Update ingestion run as success
    await supabase
      .from("ingestion_runs")
      .update({
        status: "success",
        finished_at: new Date().toISOString(),
        inserted_count: insertedCount,
      })
      .eq("id", runId);

    return new Response(
      JSON.stringify({
        success: true,
        cik: normalizedCik,
        companyName,
        bdcId,
        filingsFound: filings.length,
        filingsInserted: insertedCount,
        runId,
      }),
      {
        status: 200,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      }
    );
  } catch (error) {
    console.error("SEC ingest error:", error);

    // Update ingestion run as error if we have a runId
    if (runId) {
      await supabase
        .from("ingestion_runs")
        .update({
          status: "error",
          finished_at: new Date().toISOString(),
          error_message: error instanceof Error ? error.message : String(error),
        })
        .eq("id", runId);
    }

    return new Response(
      JSON.stringify({
        error: error instanceof Error ? error.message : "Unknown error",
      }),
      {
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      }
    );
  }
});
