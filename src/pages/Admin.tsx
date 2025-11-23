import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Loader2, Upload, RefreshCw, Database } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

interface IngestLog {
  timestamp: string;
  operation: string;
  bdcCik?: string;
  summary: string;
}

export default function Admin() {
  const { toast } = useToast();
  const [selectedBdcId, setSelectedBdcId] = useState<string>("");
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState<string | null>(null);
  const [logs, setLogs] = useState<IngestLog[]>([]);
  const [parsingFilingIds, setParsingFilingIds] = useState<Set<string>>(new Set());

  const { data: bdcs } = useQuery({
    queryKey: ["bdcs-admin"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("bdcs")
        .select("*")
        .order("bdc_name");
      if (error) throw error;
      return data;
    },
  });

  const { data: latestFilings, refetch: refetchFilings } = useQuery({
    queryKey: ["latest-filings-admin"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("filings")
        .select(`
          *,
          bdcs (bdc_name, cik)
        `)
        .order("period_end", { ascending: false })
        .limit(50);
      if (error) throw error;
      return data;
    },
  });

  const addLog = (operation: string, summary: string, bdcCik?: string) => {
    setLogs((prev) => [
      {
        timestamp: new Date().toLocaleTimeString(),
        operation,
        bdcCik,
        summary,
      },
      ...prev.slice(0, 19),
    ]);
  };

  const handleUploadUniverse = async () => {
    if (!file) {
      toast({ title: "Error", description: "Please select a file", variant: "destructive" });
      return;
    }

    setLoading("upload");
    try {
      const reader = new FileReader();
      reader.onload = async (e) => {
        const content = e.target?.result as string;
        
        const { data, error } = await supabase.functions.invoke("ingest_bdc_universe", {
          body: { csvContent: content },
        });

        if (error) throw error;

        addLog("ingest_bdc_universe", `Uploaded ${data.totalRows} rows, ${data.upserts} upserts`);
        toast({
          title: "Success",
          description: `Processed ${data.totalRows} rows, created/updated ${data.upserts} BDCs`,
        });
        setFile(null);
      };
      reader.readAsText(file);
    } catch (error: any) {
      toast({ title: "Error", description: error.message, variant: "destructive" });
      addLog("ingest_bdc_universe", `Error: ${error.message}`);
    } finally {
      setLoading(null);
    }
  };

  const handleRefreshSingleBdc = async () => {
    if (!selectedBdcId) {
      toast({ title: "Error", description: "Please select a BDC", variant: "destructive" });
      return;
    }

    setLoading("single");
    try {
      // Fetch filings
      const { data: filingsData, error: filingsError } = await supabase.functions.invoke(
        "fetch_new_filings_for_bdc",
        { body: { bdcId: selectedBdcId } }
      );

      if (filingsError) throw filingsError;

      addLog(
        "fetch_new_filings_for_bdc",
        `Found ${filingsData.filingsFound} filings, inserted ${filingsData.filingsInserted}`,
        filingsData.cik
      );

      // Query for existing unparsed filings for this BDC
      const { data: unparsedFilings, error: unparsedError } = await supabase
        .from("filings")
        .select("id, sec_accession_no")
        .eq("bdc_id", selectedBdcId)
        .eq("parsed_successfully", false)
        .order("period_end", { ascending: false })
        .limit(5);

      if (unparsedError) throw unparsedError;

      // Parse unparsed filings
      let totalHoldingsInserted = 0;
      let filingsParsed = 0;

      if (unparsedFilings && unparsedFilings.length > 0) {
        for (const filing of unparsedFilings) {
          try {
            const { data: holdingsData, error: holdingsError } = await supabase.functions.invoke(
              "extract_holdings_for_filing",
              { body: { filingId: filing.id } }
            );

            if (holdingsError) {
              console.error(`Error parsing filing ${filing.sec_accession_no}:`, holdingsError);
              addLog(
                "extract_holdings_for_filing",
                `Error for filing ${filing.sec_accession_no}: ${holdingsError.message}`,
                filingsData.cik
              );
              continue;
            }

            totalHoldingsInserted += holdingsData.holdingsInserted || 0;
            filingsParsed += 1;

            const warningsStr = holdingsData?.warnings?.length > 0 ? ` (Warnings: ${holdingsData.warnings.join(", ")})` : "";
            addLog(
              "extract_holdings_for_filing",
              `Filing ${filing.sec_accession_no}: ${holdingsData.holdingsInserted} holdings${warningsStr}`,
              filingsData.cik
            );
          } catch (error: any) {
            console.error(`Error parsing filing ${filing.sec_accession_no}:`, error);
            addLog(
              "extract_holdings_for_filing",
              `Error for filing ${filing.sec_accession_no}: ${error.message}`,
              filingsData.cik
            );
          }
        }
      }

      // Show summary toast
      if (filingsParsed > 0) {
        toast({
          title: "Success",
          description: `Inserted ${filingsData.filingsInserted} new filing(s). Parsed ${filingsParsed} filing(s) with ${totalHoldingsInserted} total holdings.`,
        });
      } else {
        toast({
          title: "Complete",
          description: `Found ${filingsData.filingsFound} filings, inserted ${filingsData.filingsInserted}. No unparsed filings found.`,
        });
      }

      refetchFilings();
    } catch (error: any) {
      toast({ title: "Error", description: error.message, variant: "destructive" });
      addLog("refresh_single_bdc", `Error: ${error.message}`);
    } finally {
      setLoading(null);
    }
  };

  const handleParseFiling = async (filingId: string, accessionNo: string) => {
    setParsingFilingIds((prev) => new Set(prev).add(filingId));
    
    try {
      const { data, error } = await supabase.functions.invoke(
        "extract_holdings_for_filing",
        { body: { filingId } }
      );

      if (error) throw error;

      addLog(
        "extract_holdings_for_filing",
        `Filing ${accessionNo}: ${data.holdingsInserted} holdings inserted${data?.warnings?.length > 0 ? ` (Warnings: ${data.warnings.join(", ")})` : ""}`
      );

      toast({
        title: "Success",
        description: `Parsed filing ${accessionNo}. Inserted ${data.holdingsInserted} holdings${data?.warnings?.length > 0 ? ". " + data.warnings.join(", ") : ""}.`,
      });

      refetchFilings();
    } catch (error: any) {
      console.error(`Error parsing filing ${accessionNo}:`, error);
      toast({ 
        title: "Error", 
        description: `Failed to parse filing: ${error.message}`, 
        variant: "destructive" 
      });
      addLog("extract_holdings_for_filing", `Error for ${accessionNo}: ${error.message}`);
    } finally {
      setParsingFilingIds((prev) => {
        const next = new Set(prev);
        next.delete(filingId);
        return next;
      });
    }
  };

  const handleRefreshAll = async () => {
    setLoading("all");
    
    let offset = 0;
    const limit = 10;
    let totalFilings = 0;
    let totalHoldings = 0;
    let totalBdcsProcessed = 0;
    let allErrors: string[] = [];
    const maxBatches = 10; // Safety limit to avoid infinite loops
    let batchCount = 0;

    try {
      while (batchCount < maxBatches) {
        batchCount++;
        
        const { data, error } = await supabase.functions.invoke("refresh_all_bdcs", {
          body: { offset, limit },
        });

        if (error) {
          console.error("Edge function error:", error);
          throw error;
        }

        totalFilings += data.totalFilingsInserted || 0;
        totalHoldings += data.totalHoldingsInserted || 0;
        totalBdcsProcessed += data.bdcCount || 0;
        allErrors = [...allErrors, ...(data.errors || [])];

        addLog(
          "refresh_all_bdcs",
          `Batch processed: offset ${offset} to ${offset + data.bdcCount} – inserted ${data.totalFilingsInserted} filings, ${data.totalHoldingsInserted} holdings`
        );

        // Check if there are more BDCs to process
        if (data.nextOffset === null) {
          break;
        }

        offset = data.nextOffset;
      }

      addLog(
        "refresh_all_bdcs",
        `All batches complete: Processed ${totalBdcsProcessed} BDCs, inserted ${totalFilings} total filings, ${totalHoldings} total holdings${allErrors.length > 0 ? `, ${allErrors.length} errors` : ""}`
      );

      toast({
        title: "Success",
        description: `Refreshed all ${totalBdcsProcessed} BDCs with ${totalFilings} new filings and ${totalHoldings} holdings`,
      });

      refetchFilings();
    } catch (error: any) {
      console.error("Full error:", error);
      const errorMsg = error?.message || "Failed to send a request to the Edge Function";
      toast({ title: "Error", description: errorMsg, variant: "destructive" });
      addLog("refresh_all_bdcs", `Error: ${errorMsg}`);
    } finally {
      setLoading(null);
    }
  };

  return (
    <div className="container mx-auto py-8 space-y-8">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Admin / Data Updates</h1>
          <p className="text-muted-foreground mt-1">
            Manage BDC universe and trigger SEC data ingestion
          </p>
        </div>
      </div>

      {/* Actions Panel */}
      <div className="grid gap-6 md:grid-cols-3">
        {/* Upload BDC Universe */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Upload className="h-5 w-5" />
              Upload BDC Universe
            </CardTitle>
            <CardDescription>
              Upload a CSV file with columns: bdc_name, ticker, cik, fiscal_year_end_month, fiscal_year_end_day
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <Input
              type="file"
              accept=".csv"
              onChange={(e) => setFile(e.target.files?.[0] || null)}
              disabled={loading === "upload"}
            />
            <Button
              onClick={handleUploadUniverse}
              disabled={!file || loading === "upload"}
              className="w-full"
            >
              {loading === "upload" ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Uploading...
                </>
              ) : (
                "Upload BDC Universe"
              )}
            </Button>
          </CardContent>
        </Card>

        {/* Refresh Single BDC */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <RefreshCw className="h-5 w-5" />
              Refresh Single BDC
            </CardTitle>
            <CardDescription>
              Fetch new filings and parse holdings for a specific BDC
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <Select value={selectedBdcId} onValueChange={setSelectedBdcId}>
              <SelectTrigger>
                <SelectValue placeholder="Select a BDC" />
              </SelectTrigger>
              <SelectContent>
                {bdcs?.map((bdc) => (
                  <SelectItem key={bdc.id} value={bdc.id}>
                    {bdc.bdc_name} ({bdc.ticker || bdc.cik})
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              onClick={handleRefreshSingleBdc}
              disabled={!selectedBdcId || loading === "single"}
              className="w-full"
            >
              {loading === "single" ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Refreshing...
                </>
              ) : (
                "Refresh This BDC"
              )}
            </Button>
          </CardContent>
        </Card>

        {/* Refresh All BDCs */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Database className="h-5 w-5" />
              Refresh All BDCs
            </CardTitle>
            <CardDescription>
              Fetch new filings and parse holdings for all BDCs in the universe
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <Alert>
              <AlertDescription>
                This may take several minutes for large universes
              </AlertDescription>
            </Alert>
            <Button
              onClick={handleRefreshAll}
              disabled={loading === "all"}
              variant="secondary"
              className="w-full"
            >
              {loading === "all" ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Refreshing...
                </>
              ) : (
                "Refresh All BDCs"
              )}
            </Button>
          </CardContent>
        </Card>
      </div>

      {/* Ingestion Activity Logs */}
      {logs.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Recent Activity</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2 max-h-60 overflow-y-auto">
              {logs.map((log, i) => (
                <div key={i} className="text-sm border-l-2 border-primary pl-3 py-1">
                  <div className="font-mono text-xs text-muted-foreground">
                    {log.timestamp} - {log.operation}
                    {log.bdcCik && ` (CIK: ${log.bdcCik})`}
                  </div>
                  <div>{log.summary}</div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Latest Filings Table */}
      <Card>
        <CardHeader>
          <CardTitle>Latest Filings</CardTitle>
          <CardDescription>Recent SEC filings and their parsing status</CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>BDC Name</TableHead>
                <TableHead>CIK</TableHead>
                <TableHead>Filing Type</TableHead>
                <TableHead>Period End</TableHead>
                <TableHead>Accession No</TableHead>
                <TableHead>Parsed</TableHead>
                <TableHead>Created</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {latestFilings?.map((filing) => (
                <TableRow key={filing.id}>
                  <TableCell>{filing.bdcs?.bdc_name}</TableCell>
                  <TableCell className="font-mono text-xs">{filing.bdcs?.cik}</TableCell>
                  <TableCell>{filing.filing_type}</TableCell>
                  <TableCell>{new Date(filing.period_end).toLocaleDateString()}</TableCell>
                  <TableCell className="font-mono text-xs">{filing.sec_accession_no}</TableCell>
                  <TableCell>
                    {filing.parsed_successfully ? (
                      <span className="text-green-600">✓</span>
                    ) : (
                      <span className="text-muted-foreground">Pending</span>
                    )}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {new Date(filing.created_at!).toLocaleDateString()}
                  </TableCell>
                  <TableCell>
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => handleParseFiling(filing.id, filing.sec_accession_no || filing.id)}
                      disabled={parsingFilingIds.has(filing.id)}
                    >
                      {parsingFilingIds.has(filing.id) ? (
                        <>
                          <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                          Parsing...
                        </>
                      ) : (
                        "Parse holdings"
                      )}
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
