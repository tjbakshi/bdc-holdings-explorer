import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

Deno.serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    const { action, filingId, bdcId } = await req.json();
    console.log(`Manage data action: ${action}, filingId: ${filingId}, bdcId: ${bdcId}`);

    if (action === "reset_filing" && filingId) {
      // Delete all holdings for this filing
      const { error: deleteError } = await supabase
        .from("holdings")
        .delete()
        .eq("filing_id", filingId);

      if (deleteError) {
        console.error("Error deleting holdings:", deleteError);
        throw new Error(`Failed to delete holdings: ${deleteError.message}`);
      }

      // Reset the filing status
      const { error: updateError } = await supabase
        .from("filings")
        .update({ 
          parsed_successfully: false, 
          value_scale: null,
          data_source: null 
        })
        .eq("id", filingId);

      if (updateError) {
        console.error("Error updating filing:", updateError);
        throw new Error(`Failed to update filing: ${updateError.message}`);
      }

      console.log(`Reset filing ${filingId}: deleted holdings, reset status`);
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: `Filing reset successfully. Holdings deleted.`
        }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    if (action === "clear_bdc" && bdcId) {
      // First, get all filings for this BDC
      const { data: filings, error: filingsError } = await supabase
        .from("filings")
        .select("id")
        .eq("bdc_id", bdcId);

      if (filingsError) {
        console.error("Error fetching filings:", filingsError);
        throw new Error(`Failed to fetch filings: ${filingsError.message}`);
      }

      const filingIds = filings?.map(f => f.id) || [];
      console.log(`Found ${filingIds.length} filings for BDC ${bdcId}`);

      if (filingIds.length > 0) {
        // Delete all holdings for all filings of this BDC
        const { error: deleteError } = await supabase
          .from("holdings")
          .delete()
          .in("filing_id", filingIds);

        if (deleteError) {
          console.error("Error deleting holdings:", deleteError);
          throw new Error(`Failed to delete holdings: ${deleteError.message}`);
        }

        // Reset all filings for this BDC
        const { error: updateError } = await supabase
          .from("filings")
          .update({ 
            parsed_successfully: false, 
            value_scale: null,
            data_source: null 
          })
          .eq("bdc_id", bdcId);

        if (updateError) {
          console.error("Error updating filings:", updateError);
          throw new Error(`Failed to update filings: ${updateError.message}`);
        }
      }

      console.log(`Cleared BDC ${bdcId}: reset ${filingIds.length} filings`);
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: `BDC data cleared. ${filingIds.length} filings reset.`,
          resetFilings: filingIds.length
        }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    return new Response(
      JSON.stringify({ success: false, error: "Invalid action or missing parameters" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );

  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : "Unknown error";
    console.error("Error in manage-data function:", errorMessage);
    return new Response(
      JSON.stringify({ success: false, error: errorMessage }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }
});
