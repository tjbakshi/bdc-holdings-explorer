import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const SEC_USER_AGENT = "BDCTrackerApp/1.0 (contact@bdctracker.com)";

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { bdcId, cik: providedCik } = await req.json();

    const supabaseClient = createClient(
      Deno.env.get("SUPABASE_URL") ?? "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
    );

    let cik = providedCik;
    let bdcDbId = bdcId;

    // Look up BDC if only ID provided
    if (bdcId && !cik) {
      const { data: bdc, error: bdcError } = await supabaseClient
        .from("bdcs")
        .select("cik")
        .eq("id", bdcId)
        .single();

      if (bdcError) throw new Error(`BDC not found: ${bdcError.message}`);
      cik = bdc.cik;
    } else if (cik && !bdcId) {
      const { data: bdc, error: bdcError } = await supabaseClient
        .from("bdcs")
        .select("id")
        .eq("cik", cik)
        .single();

      if (bdcError) throw new Error(`BDC not found for CIK ${cik}: ${bdcError.message}`);
      bdcDbId = bdc.id;
    }

    if (!cik || !bdcDbId) {
      throw new Error("Must provide either bdcId or cik");
    }

    console.log(`Fetching filings for CIK: ${cik}`);

    // Fetch filings from SEC EDGAR
    const secUrl = `https://data.sec.gov/submissions/CIK${cik.padStart(10, "0")}.json`;
    const secResponse = await fetch(secUrl, {
      headers: { "User-Agent": SEC_USER_AGENT },
    });

    if (!secResponse.ok) {
      throw new Error(`SEC API error: ${secResponse.status} ${secResponse.statusText}`);
    }

    const secData = await secResponse.json();
    const recentFilings = secData.filings?.recent;

    if (!recentFilings) {
      return new Response(
        JSON.stringify({ cik, filingsFound: 0, filingsInserted: 0, newFilingIds: [] }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Filter to 10-K and 10-Q since 2022-01-01
    const targetForms = ["10-K", "10-Q"];
    const sinceDate = new Date("2022-01-01");
    const filings: Array<{
      accessionNumber: string;
      filingDate: string;
      reportDate: string;
      form: string;
    }> = [];

    for (let i = 0; i < recentFilings.form.length; i++) {
      const form = recentFilings.form[i];
      const filingDate = new Date(recentFilings.filingDate[i]);
      const reportDate = recentFilings.reportDate[i];
      const accessionNumber = recentFilings.accessionNumber[i];

      if (targetForms.includes(form) && filingDate >= sinceDate) {
        filings.push({
          accessionNumber,
          filingDate: recentFilings.filingDate[i],
          reportDate,
          form,
        });
      }
    }

    console.log(`Found ${filings.length} filings for CIK ${cik}`);

    // Insert new filings
    let filingsInserted = 0;
    const newFilingIds: string[] = [];

    for (const filing of filings) {
      // Check if already exists
      const { data: existing } = await supabaseClient
        .from("filings")
        .select("id")
        .eq("bdc_id", bdcDbId)
        .eq("sec_accession_no", filing.accessionNumber)
        .maybeSingle();

      if (existing) {
        console.log(`Filing ${filing.accessionNumber} already exists, skipping`);
        continue;
      }

      // Build filing URL
      const accessionNoNoDashes = filing.accessionNumber.replace(/-/g, "");
      const filingUrl = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cik}&type=${filing.form}&dateb=&owner=exclude&count=100&search_text=`;

      const { data: inserted, error: insertError } = await supabaseClient
        .from("filings")
        .insert({
          bdc_id: bdcDbId,
          filing_type: filing.form,
          period_end: filing.reportDate,
          sec_accession_no: filing.accessionNumber,
          filing_url: filingUrl,
          parsed_successfully: false,
        })
        .select("id")
        .single();

      if (insertError) {
        console.error(`Error inserting filing ${filing.accessionNumber}:`, insertError);
      } else {
        filingsInserted++;
        newFilingIds.push(inserted.id);
        console.log(`Inserted filing ${filing.accessionNumber}`);
      }
    }

    return new Response(
      JSON.stringify({
        cik,
        filingsFound: filings.length,
        filingsInserted,
        newFilingIds,
      }),
      {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      }
    );
  } catch (error) {
    console.error("Error in fetch_new_filings_for_bdc:", error);
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
