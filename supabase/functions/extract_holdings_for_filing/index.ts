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

    // Build XBRL document URL
    const accessionNoNoDashes = accessionNo.replace(/-/g, "");
    const xbrlIndexUrl = `https://www.sec.gov/cgi-bin/viewer?action=view&cik=${cik}&accession_number=${accessionNo}&xbrl_type=v`;
    
    // For BDC Schedule of Investments, we need to fetch the structured data
    // The SEC provides BDC data in a specific JSON format at:
    const bdcDataUrl = `https://data.sec.gov/api/xbrl/companyfacts/CIK${cik.padStart(10, "0")}.json`;
    
    console.log(`Fetching BDC data from: ${bdcDataUrl}`);

    // For now, we'll use a simplified approach: fetch the filing index and parse the Schedule of Investments
    // In production, you'd parse the actual XBRL/XML files
    // For this MVP, we'll create sample holdings to demonstrate the flow

    // TODO: Implement actual XBRL parsing
    // For now, insert sample holdings to demonstrate the system works
    const sampleHoldings = [
      {
        company_name: "Sample Portfolio Company A",
        investment_type: "Senior Secured First Lien",
        industry: "Software & Services",
        description: "First Lien Term Loan",
        interest_rate: "SOFR + 5.50%",
        reference_rate: "SOFR",
        maturity_date: "2027-12-31",
        par_amount: 10000000,
        cost: 9800000,
        fair_value: 9900000,
      },
      {
        company_name: "Sample Portfolio Company B",
        investment_type: "Senior Secured Second Lien",
        industry: "Healthcare Services",
        description: "Second Lien Term Loan",
        interest_rate: "SOFR + 8.00%",
        reference_rate: "SOFR",
        maturity_date: "2028-06-30",
        par_amount: 5000000,
        cost: 4900000,
        fair_value: 4850000,
      },
      {
        company_name: "Sample Portfolio Company C",
        investment_type: "Equity",
        industry: "Technology",
        description: "Preferred Stock",
        interest_rate: null,
        reference_rate: null,
        maturity_date: null,
        par_amount: null,
        cost: 2000000,
        fair_value: 2500000,
      },
    ];

    const holdingsToInsert = sampleHoldings.map((h) => ({
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
        warnings: ["Using sample holdings data - XBRL parsing not yet implemented"],
      }),
      {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      }
    );
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
