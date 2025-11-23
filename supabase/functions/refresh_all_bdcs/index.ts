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
    const supabaseClient = createClient(
      Deno.env.get("SUPABASE_URL") ?? "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
    );

    // Parse request body for offset and limit
    const { offset = 0, limit = 10 } = await req.json().catch(() => ({}));

    // Fetch all BDCs
    const { data: bdcs, error: bdcsError } = await supabaseClient
      .from("bdcs")
      .select("*")
      .order("bdc_name");

    if (bdcsError) {
      throw new Error(`Error fetching BDCs: ${bdcsError.message}`);
    }

    let totalFilingsInserted = 0;
    let totalHoldingsInserted = 0;
    const errors: string[] = [];

    // Process BDCs in batches using offset and limit
    const bdcsToProcess = bdcs.slice(offset, offset + limit);
    console.log(`Processing ${bdcsToProcess.length} BDCs (offset ${offset}, limit ${limit}) of ${bdcs.length} total`);

    for (const bdc of bdcsToProcess) {
      try {
        console.log(`Processing BDC: ${bdc.bdc_name} (CIK: ${bdc.cik})`);

        // Fetch filings (inline logic from fetch_new_filings_for_bdc)
        const secUrl = `https://data.sec.gov/submissions/CIK${bdc.cik.padStart(10, "0")}.json`;
        const secResponse = await fetch(secUrl, {
          headers: { "User-Agent": SEC_USER_AGENT },
        });

        if (!secResponse.ok) {
          errors.push(`${bdc.bdc_name}: SEC API error ${secResponse.status}`);
          continue;
        }

        const secData = await secResponse.json();
        const recentFilings = secData.filings?.recent;

        if (!recentFilings) {
          continue;
        }

        const targetForms = ["10-K", "10-Q"];
        const sinceDate = new Date("2022-01-01");

        for (let i = 0; i < recentFilings.form.length; i++) {
          const form = recentFilings.form[i];
          const filingDate = new Date(recentFilings.filingDate[i]);
          const reportDate = recentFilings.reportDate[i];
          const accessionNumber = recentFilings.accessionNumber[i];

          if (!targetForms.includes(form) || filingDate < sinceDate) {
            continue;
          }

          // Check if exists
          const { data: existing } = await supabaseClient
            .from("filings")
            .select("id")
            .eq("bdc_id", bdc.id)
            .eq("sec_accession_no", accessionNumber)
            .maybeSingle();

          if (existing) {
            continue;
          }

          // Insert filing
          const filingUrl = `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${bdc.cik}&type=${form}&dateb=&owner=exclude&count=100&search_text=`;

          const { data: inserted, error: insertError } = await supabaseClient
            .from("filings")
            .insert({
              bdc_id: bdc.id,
              filing_type: form,
              period_end: reportDate,
              sec_accession_no: accessionNumber,
              filing_url: filingUrl,
              parsed_successfully: false,
            })
            .select("id")
            .single();

          if (insertError) {
            errors.push(`${bdc.bdc_name} - ${accessionNumber}: ${insertError.message}`);
            continue;
          }

          totalFilingsInserted++;

          // Extract holdings (simplified for now)
          // In production, you might queue this or do it asynchronously
          // For MVP, we'll skip auto-extraction in bulk refresh to keep it fast
        }

        // Rate limiting: wait 100ms between BDCs
        await new Promise((resolve) => setTimeout(resolve, 100));
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : "Unknown error";
        errors.push(`${bdc.bdc_name}: ${errorMessage}`);
      }
    }

    // Calculate nextOffset
    const nextOffset = (offset + bdcsToProcess.length < bdcs.length) 
      ? offset + bdcsToProcess.length 
      : null;

    return new Response(
      JSON.stringify({
        totalBdcs: bdcs.length,
        bdcCount: bdcsToProcess.length,
        totalFilingsInserted,
        totalHoldingsInserted,
        nextOffset,
        errors,
      }),
      {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      }
    );
  } catch (error) {
    console.error("Error in refresh_all_bdcs:", error);
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
