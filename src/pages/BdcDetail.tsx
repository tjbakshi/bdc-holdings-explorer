import { useState, useMemo } from "react";
import { useParams, Link } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { ArrowLeft, Download, Search, RotateCcw, Trash2, AlertTriangle } from "lucide-react";
import { Input } from "@/components/ui/input";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { useToast } from "@/hooks/use-toast";
import * as XLSX from "xlsx";

const BdcDetail = () => {
  const { bdcId } = useParams<{ bdcId: string }>();
  const [selectedFilingId, setSelectedFilingId] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [isResetting, setIsResetting] = useState<string | null>(null);
  const [isClearing, setIsClearing] = useState(false);
  const queryClient = useQueryClient();
  const { toast } = useToast();

  // Fetch BDC info
  const { data: bdc, isLoading: bdcLoading } = useQuery({
    queryKey: ["bdc", bdcId],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("bdcs")
        .select("*")
        .eq("id", bdcId)
        .single();
      
      if (error) throw error;
      return data;
    },
    enabled: !!bdcId,
  });

  // Fetch filings for this BDC
  const { data: filings, isLoading: filingsLoading } = useQuery({
    queryKey: ["filings", bdcId],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("filings")
        .select("*")
        .eq("bdc_id", bdcId)
        .order("period_end", { ascending: false });
      
      if (error) throw error;
      
      // Auto-select the most recent filing
      if (data && data.length > 0 && !selectedFilingId) {
        setSelectedFilingId(data[0].id);
      }
      
      return data;
    },
    enabled: !!bdcId,
  });

  // Fetch holdings for selected filing - sorted by industry then alphabetically by company
  const { data: holdings, isLoading: holdingsLoading } = useQuery({
    queryKey: ["holdings", selectedFilingId],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("holdings")
        .select("*")
        .eq("filing_id", selectedFilingId);
      
      if (error) throw error;
      
      // Sort client-side: by industry (alphabetically), then by company name within each industry
      // This ensures consistent visual ordering regardless of HTML source position
      if (data) {
        data.sort((a, b) => {
          // First sort by industry (nulls/unknown last)
          const industryA = a.industry || 'zzz_Unknown';
          const industryB = b.industry || 'zzz_Unknown';
          const industryCompare = industryA.localeCompare(industryB);
          if (industryCompare !== 0) return industryCompare;
          
          // Within same industry, sort alphabetically by company name
          return a.company_name.localeCompare(b.company_name);
        });
      }
      
      return data;
    },
    enabled: !!selectedFilingId,
  });

  const calculateFmvPar = (fairValue: number | null, parAmount: number | null) => {
    if (!fairValue || !parAmount || parAmount === 0) return "—";
    return ((fairValue / parAmount) * 100).toFixed(2) + "%";
  };

  const calculateFmvCost = (fairValue: number | null, cost: number | null) => {
    if (!fairValue || !cost || cost === 0) return "—";
    return ((fairValue / cost) * 100).toFixed(2) + "%";
  };

  // Format currency values in millions with "M" suffix
  const formatCurrencyMillions = (value: number | null) => {
    if (value === null || value === undefined) return "—";
    // Values are already in millions, format with 1 decimal and M suffix
    if (Math.abs(value) >= 1000) {
      // If >= 1000M, show as $X.XB
      return `$${(value / 1000).toFixed(1)}B`;
    }
    return `$${value.toFixed(1)}M`;
  };

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return "—";
    return new Date(dateStr).toLocaleDateString();
  };

  // Calculate portfolio summary totals (use cost as fallback for par if blank)
  const portfolioSummary = holdings ? {
    totalPar: holdings.reduce((sum, h) => sum + (h.par_amount ?? h.cost ?? 0), 0),
    totalCost: holdings.reduce((sum, h) => sum + (h.cost || 0), 0),
    totalFairValue: holdings.reduce((sum, h) => sum + (h.fair_value || 0), 0),
  } : null;

  // Calculate industry-level summary
  const industrySummary = useMemo(() => {
    if (!holdings) return [];
    const industryMap = new Map<string, { par: number; cost: number; fairValue: number; count: number }>();
    
    holdings.forEach(h => {
      const industry = h.industry || "Unknown";
      const existing = industryMap.get(industry) || { par: 0, cost: 0, fairValue: 0, count: 0 };
      industryMap.set(industry, {
        par: existing.par + (h.par_amount ?? h.cost ?? 0),
        cost: existing.cost + (h.cost || 0),
        fairValue: existing.fairValue + (h.fair_value || 0),
        count: existing.count + 1,
      });
    });

    return Array.from(industryMap.entries())
      .map(([industry, data]) => ({
        industry,
        ...data,
        fmvPar: data.par > 0 ? ((data.fairValue / data.par) * 100).toFixed(2) + "%" : "—",
        fmvCost: data.cost > 0 ? ((data.fairValue / data.cost) * 100).toFixed(2) + "%" : "—",
        allocation: portfolioSummary && portfolioSummary.totalFairValue > 0 
          ? ((data.fairValue / portfolioSummary.totalFairValue) * 100).toFixed(1) + "%"
          : "—",
      }))
      .sort((a, b) => b.fairValue - a.fairValue);
  }, [holdings, portfolioSummary]);

  const summaryFmvPar = portfolioSummary && portfolioSummary.totalPar > 0 
    ? ((portfolioSummary.totalFairValue / portfolioSummary.totalPar) * 100).toFixed(2) + "%" 
    : "—";
  
  const summaryFmvCost = portfolioSummary && portfolioSummary.totalCost > 0 
    ? ((portfolioSummary.totalFairValue / portfolioSummary.totalCost) * 100).toFixed(2) + "%" 
    : "—";

  // Filter holdings based on search query
  const filteredHoldings = useMemo(() => {
    if (!holdings || !searchQuery.trim()) return holdings;
    const query = searchQuery.toLowerCase();
    return holdings.filter(h => 
      h.company_name.toLowerCase().includes(query) ||
      (h.industry?.toLowerCase().includes(query)) ||
      (h.investment_type?.toLowerCase().includes(query)) ||
      (h.description?.toLowerCase().includes(query))
    );
  }, [holdings, searchQuery]);

  const exportToExcel = () => {
    if (!holdings || !bdc) return;
    
    const selectedFiling = filings?.find(f => f.id === selectedFilingId);
    const periodEnd = selectedFiling ? formatDate(selectedFiling.period_end) : "Unknown";
    
    const exportData = holdings.map((h) => ({
      "Portfolio Company": h.company_name,
      "Investment Type": h.investment_type || "",
      "Industry": h.industry || "",
      "Description": h.description || "",
      "Interest Rate": h.interest_rate || "",
      "Reference Rate": h.reference_rate || "",
      "Maturity Date": h.maturity_date || "",
      "Par Amount ($M)": h.par_amount ?? h.cost ?? "",
      "Cost ($M)": h.cost ?? "",
      "Fair Value ($M)": h.fair_value ?? "",
      "FMV % Par": h.fair_value && (h.par_amount ?? h.cost) 
        ? ((h.fair_value / (h.par_amount ?? h.cost!)) * 100).toFixed(2) + "%" 
        : "",
      "FMV % Cost": h.fair_value && h.cost 
        ? ((h.fair_value / h.cost) * 100).toFixed(2) + "%" 
        : "",
    }));

    const worksheet = XLSX.utils.json_to_sheet(exportData);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Holdings");
    
    const fileName = `${bdc.ticker || bdc.bdc_name}_Holdings_${periodEnd.replace(/\//g, "-")}.xlsx`;
    XLSX.writeFile(workbook, fileName);
  };

  const handleResetFiling = async (filingId: string) => {
    setIsResetting(filingId);
    try {
      // Step 1: Reset the filing (delete holdings and reset status)
      const { data, error } = await supabase.functions.invoke("manage-data", {
        body: { action: "reset_filing", filingId },
      });

      if (error) throw error;
      if (!data.success) throw new Error(data.error);

      toast({
        title: "Filing Reset",
        description: "Holdings deleted. Now re-extracting data...",
      });

      // Step 2: Re-extract holdings for the filing
      const { data: extractData, error: extractError } = await supabase.functions.invoke("extract_holdings_for_filing", {
        body: { filing_id: filingId },
      });

      if (extractError) {
        console.error("Extract error:", extractError);
        toast({
          title: "Extraction Started",
          description: "Reset complete. Extraction may still be processing in the background.",
        });
      } else {
        toast({
          title: "Filing Re-Parsed",
          description: extractData?.message || "Holdings have been re-extracted successfully.",
        });
      }

      // Invalidate queries to refresh the UI
      queryClient.invalidateQueries({ queryKey: ["filings", bdcId] });
      queryClient.invalidateQueries({ queryKey: ["holdings", filingId] });
    } catch (error) {
      console.error("Error resetting filing:", error);
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to reset filing",
        variant: "destructive",
      });
    } finally {
      setIsResetting(null);
    }
  };

  const handleClearBDC = async () => {
    setIsClearing(true);
    try {
      const { data, error } = await supabase.functions.invoke("manage-data", {
        body: { action: "clear_bdc", bdcId },
      });

      if (error) throw error;
      if (!data.success) throw new Error(data.error);

      toast({
        title: "BDC Data Cleared",
        description: data.message,
      });

      // Invalidate all related queries
      queryClient.invalidateQueries({ queryKey: ["filings", bdcId] });
      queryClient.invalidateQueries({ queryKey: ["holdings"] });
      setSelectedFilingId(null);
    } catch (error) {
      console.error("Error clearing BDC data:", error);
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to clear BDC data",
        variant: "destructive",
      });
    } finally {
      setIsClearing(false);
    }
  };

  if (bdcLoading) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <p className="text-muted-foreground">Loading BDC details...</p>
      </div>
    );
  }

  if (!bdc) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <p className="text-muted-foreground mb-4">BDC not found</p>
          <Link to="/">
            <Button variant="outline">
              <ArrowLeft className="mr-2 h-4 w-4" />
              Back to Home
            </Button>
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto px-4 py-8">
        <Link to="/">
          <Button variant="ghost" className="mb-6">
            <ArrowLeft className="mr-2 h-4 w-4" />
            Back to BDC List
          </Button>
        </Link>

        <Card className="mb-6">
          <CardHeader>
            <CardTitle className="text-3xl">{bdc.bdc_name}</CardTitle>
            <CardDescription>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
                <div>
                  <span className="text-muted-foreground">Ticker:</span>{" "}
                  <span className="font-medium">{bdc.ticker || "—"}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">CIK:</span>{" "}
                  <span className="font-mono font-medium">{bdc.cik}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Fiscal Year End:</span>{" "}
                  <span className="font-medium">
                    {bdc.fiscal_year_end_month}/{bdc.fiscal_year_end_day}
                  </span>
                </div>
              </div>
            </CardDescription>
          </CardHeader>
        </Card>

        {filingsLoading ? (
          <div className="text-center py-8">
            <p className="text-muted-foreground">Loading filings...</p>
          </div>
        ) : !filings || filings.length === 0 ? (
          <Card>
            <CardContent className="py-12 text-center">
              <p className="text-muted-foreground">
                No filings available for this BDC yet.
              </p>
            </CardContent>
          </Card>
        ) : (
          <>
            <div className="mb-6">
              <label className="text-sm font-medium mb-2 block">Select Filing Period</label>
              <div className="flex items-center gap-2">
                <Select
                  value={selectedFilingId || undefined}
                  onValueChange={setSelectedFilingId}
                >
                  <SelectTrigger className="w-full md:w-96 bg-card">
                    <SelectValue placeholder="Select a filing period" />
                  </SelectTrigger>
                  <SelectContent className="bg-card">
                    {filings.map((filing) => (
                      <SelectItem key={filing.id} value={filing.id}>
                        {formatDate(filing.period_end)} – {filing.filing_type}
                        {filing.parsed_successfully ? " ✓" : " (Pending)"}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                
                {selectedFilingId && (
                  <AlertDialog>
                    <AlertDialogTrigger asChild>
                      <Button 
                        variant="outline" 
                        size="sm" 
                        className="text-destructive border-destructive hover:bg-destructive hover:text-destructive-foreground"
                        disabled={isResetting === selectedFilingId}
                      >
                        <RotateCcw className="mr-1 h-4 w-4" />
                        {isResetting === selectedFilingId ? "Resetting..." : "Reset Filing"}
                      </Button>
                    </AlertDialogTrigger>
                    <AlertDialogContent>
                      <AlertDialogHeader>
                        <AlertDialogTitle className="flex items-center gap-2">
                          <AlertTriangle className="h-5 w-5 text-destructive" />
                          Reset Filing Data
                        </AlertDialogTitle>
                        <AlertDialogDescription>
                          This will permanently delete all holdings for this filing and reset its parsing status. 
                          You will need to re-parse the filing to recover the data.
                          <br /><br />
                          <strong>This action cannot be undone.</strong>
                        </AlertDialogDescription>
                      </AlertDialogHeader>
                      <AlertDialogFooter>
                        <AlertDialogCancel>Cancel</AlertDialogCancel>
                        <AlertDialogAction 
                          onClick={() => handleResetFiling(selectedFilingId)}
                          className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                        >
                          Reset Filing
                        </AlertDialogAction>
                      </AlertDialogFooter>
                    </AlertDialogContent>
                  </AlertDialog>
                )}
              </div>
            </div>

            {holdingsLoading ? (
              <div className="text-center py-8">
                <p className="text-muted-foreground">Loading holdings...</p>
              </div>
            ) : !holdings || holdings.length === 0 ? (
              <Card>
                <CardContent className="py-12 text-center">
                  <p className="text-muted-foreground">
                    No holdings found for this filing.
                  </p>
                </CardContent>
              </Card>
            ) : (
              <Card>
                <CardHeader className="flex flex-col gap-4">
                  <div className="flex flex-row items-center justify-between">
                    <div>
                      <CardTitle>Portfolio Holdings</CardTitle>
                      <CardDescription>
                        Showing {filteredHoldings?.length || 0} of {holdings.length} holdings for the selected filing period
                        <span className="ml-2 text-xs text-muted-foreground">(Values in millions USD)</span>
                      </CardDescription>
                    </div>
                    <Button onClick={exportToExcel} variant="outline" size="sm">
                      <Download className="mr-2 h-4 w-4" />
                      Export to Excel
                    </Button>
                  </div>
                  <div className="relative max-w-sm">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <Input
                      placeholder="Search by company, industry, type..."
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      className="pl-9"
                    />
                  </div>
                </CardHeader>
                <CardContent>
                  {/* Portfolio Summary */}
                  {portfolioSummary && (
                    <div className="mb-6 p-4 bg-muted/50 rounded-lg border">
                      <h3 className="text-sm font-semibold mb-3 text-muted-foreground uppercase tracking-wide">Portfolio Summary</h3>
                      <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
                        <div>
                          <p className="text-xs text-muted-foreground">Total Par</p>
                          <p className="text-lg font-semibold font-mono">{formatCurrencyMillions(portfolioSummary.totalPar)}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Total Cost</p>
                          <p className="text-lg font-semibold font-mono">{formatCurrencyMillions(portfolioSummary.totalCost)}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Total Fair Value</p>
                          <p className="text-lg font-semibold font-mono">{formatCurrencyMillions(portfolioSummary.totalFairValue)}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">FMV % Par</p>
                          <p className="text-lg font-semibold">{summaryFmvPar}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">FMV % Cost</p>
                          <p className="text-lg font-semibold">{summaryFmvCost}</p>
                        </div>
                      </div>

                      {/* Industry Breakdown */}
                      {industrySummary.length > 0 && (
                        <>
                          <h4 className="text-sm font-semibold mb-3 text-muted-foreground uppercase tracking-wide">By Industry</h4>
                          <div className="rounded-md border overflow-x-auto">
                            <Table>
                              <TableHeader>
                                <TableRow>
                                  <TableHead>Industry</TableHead>
                                  <TableHead className="text-center"># Holdings</TableHead>
                                  <TableHead className="text-right">Par</TableHead>
                                  <TableHead className="text-right">Cost</TableHead>
                                  <TableHead className="text-right">Fair Value</TableHead>
                                  <TableHead className="text-right">FMV % Par</TableHead>
                                  <TableHead className="text-right">FMV % Cost</TableHead>
                                  <TableHead className="text-right">Allocation</TableHead>
                                </TableRow>
                              </TableHeader>
                              <TableBody>
                                {industrySummary.map((ind) => (
                                  <TableRow key={ind.industry}>
                                    <TableCell className="font-medium">{ind.industry}</TableCell>
                                    <TableCell className="text-center">{ind.count}</TableCell>
                                    <TableCell className="text-right font-mono">{formatCurrencyMillions(ind.par)}</TableCell>
                                    <TableCell className="text-right font-mono">{formatCurrencyMillions(ind.cost)}</TableCell>
                                    <TableCell className="text-right font-mono">{formatCurrencyMillions(ind.fairValue)}</TableCell>
                                    <TableCell className="text-right">{ind.fmvPar}</TableCell>
                                    <TableCell className="text-right">{ind.fmvCost}</TableCell>
                                    <TableCell className="text-right font-semibold">{ind.allocation}</TableCell>
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </div>
                        </>
                      )}
                    </div>
                  )}
                  <TooltipProvider>
                    <div className="rounded-lg border overflow-x-auto">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead className="min-w-[200px]">Portfolio Company</TableHead>
                            <TableHead>Investment Type</TableHead>
                            <TableHead>Industry</TableHead>
                            <TableHead className="min-w-[250px]">Description</TableHead>
                            <TableHead>Interest Rate</TableHead>
                            <TableHead>Reference Rate</TableHead>
                            <TableHead>Maturity</TableHead>
                            <TableHead className="text-right">
                              <Tooltip>
                                <TooltipTrigger>Par Amount</TooltipTrigger>
                                <TooltipContent>Values in millions USD</TooltipContent>
                              </Tooltip>
                            </TableHead>
                            <TableHead className="text-right">
                              <Tooltip>
                                <TooltipTrigger>Cost</TooltipTrigger>
                                <TooltipContent>Values in millions USD</TooltipContent>
                              </Tooltip>
                            </TableHead>
                            <TableHead className="text-right">
                              <Tooltip>
                                <TooltipTrigger>Fair Value</TooltipTrigger>
                                <TooltipContent>Values in millions USD</TooltipContent>
                              </Tooltip>
                            </TableHead>
                            <TableHead className="text-right">FMV % Par</TableHead>
                            <TableHead className="text-right">FMV % Cost</TableHead>
                            <TableHead>Actions</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {filteredHoldings?.map((holding) => (
                            <TableRow key={holding.id}>
                              <TableCell className="font-medium">
                                <Link 
                                  to={`/company/${encodeURIComponent(holding.company_name)}`}
                                  className="hover:underline text-primary"
                                >
                                  {holding.company_name}
                                </Link>
                              </TableCell>
                              <TableCell>{holding.investment_type || "—"}</TableCell>
                              <TableCell>{holding.industry || "—"}</TableCell>
                              <TableCell className="text-sm">
                                {holding.description || "—"}
                              </TableCell>
                              <TableCell>{holding.interest_rate || "—"}</TableCell>
                              <TableCell>{holding.reference_rate || "—"}</TableCell>
                              <TableCell>{formatDate(holding.maturity_date)}</TableCell>
                              <TableCell className="text-right font-mono">
                                {formatCurrencyMillions(holding.par_amount ?? holding.cost)}
                              </TableCell>
                              <TableCell className="text-right font-mono">
                                {formatCurrencyMillions(holding.cost)}
                              </TableCell>
                              <TableCell className="text-right font-mono">
                                {formatCurrencyMillions(holding.fair_value)}
                              </TableCell>
                              <TableCell className="text-right font-medium">
                                {calculateFmvPar(holding.fair_value, holding.par_amount ?? holding.cost)}
                              </TableCell>
                              <TableCell className="text-right font-medium">
                                {calculateFmvCost(holding.fair_value, holding.cost)}
                              </TableCell>
                              <TableCell>
                                <Link to={`/holding/${holding.id}`}>
                                  <Button variant="outline" size="sm">
                                    View Details
                                  </Button>
                                </Link>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </TooltipProvider>
                </CardContent>
              </Card>
            )}
          </>
        )}

        {/* Danger Zone */}
        <Card className="mt-8 border-destructive/50">
          <CardHeader>
            <CardTitle className="text-destructive flex items-center gap-2">
              <AlertTriangle className="h-5 w-5" />
              Danger Zone
            </CardTitle>
            <CardDescription>
              These actions are destructive and cannot be undone.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between p-4 border border-destructive/30 rounded-lg bg-destructive/5">
              <div>
                <p className="font-medium">Clear All Data for {bdc.ticker || bdc.bdc_name}</p>
                <p className="text-sm text-muted-foreground">
                  Delete all holdings and reset all filings for this BDC. You will need to re-parse all filings.
                </p>
              </div>
              <AlertDialog>
                <AlertDialogTrigger asChild>
                  <Button 
                    variant="destructive" 
                    disabled={isClearing}
                  >
                    <Trash2 className="mr-2 h-4 w-4" />
                    {isClearing ? "Clearing..." : `Clear All ${bdc.ticker || "BDC"} Data`}
                  </Button>
                </AlertDialogTrigger>
                <AlertDialogContent>
                  <AlertDialogHeader>
                    <AlertDialogTitle className="flex items-center gap-2">
                      <AlertTriangle className="h-5 w-5 text-destructive" />
                      Clear All BDC Data
                    </AlertDialogTitle>
                    <AlertDialogDescription>
                      This will permanently delete <strong>all holdings</strong> for <strong>{bdc.bdc_name}</strong> across 
                      all filing periods, and reset the parsing status of all filings.
                      <br /><br />
                      You will need to re-run the entire parsing history to recover this data.
                      <br /><br />
                      <strong className="text-destructive">This action cannot be undone.</strong>
                    </AlertDialogDescription>
                  </AlertDialogHeader>
                  <AlertDialogFooter>
                    <AlertDialogCancel>Cancel</AlertDialogCancel>
                    <AlertDialogAction 
                      onClick={handleClearBDC}
                      className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                    >
                      Yes, Clear All Data
                    </AlertDialogAction>
                  </AlertDialogFooter>
                </AlertDialogContent>
              </AlertDialog>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default BdcDetail;
