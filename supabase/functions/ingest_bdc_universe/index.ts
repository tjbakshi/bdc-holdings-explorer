import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { csvContent } = await req.json();

    if (!csvContent) {
      throw new Error("No CSV content provided");
    }

    const supabaseClient = createClient(
      Deno.env.get("SUPABASE_URL") ?? "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
    );

    // Parse CSV
    const lines = csvContent.split("\n").filter((line: string) => line.trim());
    const headers = lines[0].toLowerCase().split(",").map((h: string) => h.trim());

    const bdcNameIdx = headers.indexOf("bdc_name");
    const tickerIdx = headers.indexOf("ticker");
    const cikIdx = headers.indexOf("cik");
    const fiscalMonthIdx = headers.indexOf("fiscal_year_end_month");
    const fiscalDayIdx = headers.indexOf("fiscal_year_end_day");

    if (bdcNameIdx === -1 || cikIdx === -1 || fiscalMonthIdx === -1 || fiscalDayIdx === -1) {
      throw new Error("CSV must have columns: bdc_name, cik, fiscal_year_end_month, fiscal_year_end_day");
    }

    const errors: string[] = [];
    let upserts = 0;

    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(",").map((c: string) => c.trim());
      
      if (cols.length < Math.max(bdcNameIdx, cikIdx, fiscalMonthIdx, fiscalDayIdx) + 1) {
        errors.push(`Row ${i + 1}: Insufficient columns`);
        continue;
      }

      const bdcName = cols[bdcNameIdx];
      const cik = cols[cikIdx].padStart(10, "0");
      const ticker = tickerIdx !== -1 ? cols[tickerIdx] : null;
      const fiscalMonth = parseInt(cols[fiscalMonthIdx], 10);
      const fiscalDay = parseInt(cols[fiscalDayIdx], 10);

      if (!bdcName || !cik || isNaN(fiscalMonth) || isNaN(fiscalDay)) {
        errors.push(`Row ${i + 1}: Missing or invalid required fields`);
        continue;
      }

      const { error } = await supabaseClient
        .from("bdcs")
        .upsert(
          {
            cik,
            bdc_name: bdcName,
            ticker,
            fiscal_year_end_month: fiscalMonth,
            fiscal_year_end_day: fiscalDay,
          },
          { onConflict: "cik" }
        );

      if (error) {
        errors.push(`Row ${i + 1}: ${error.message}`);
      } else {
        upserts++;
      }
    }

    return new Response(
      JSON.stringify({
        totalRows: lines.length - 1,
        upserts,
        errors,
      }),
      {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      }
    );
  } catch (error) {
    console.error("Error in ingest_bdc_universe:", error);
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
