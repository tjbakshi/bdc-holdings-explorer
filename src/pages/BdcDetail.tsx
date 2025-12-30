import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
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
import { ArrowLeft, Download } from "lucide-react";
import * as XLSX from "xlsx";

const BdcDetail = () => {
  const { bdcId } = useParams<{ bdcId: string }>();
  const [selectedFilingId, setSelectedFilingId] = useState<string | null>(null);

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

  const summaryFmvPar = portfolioSummary && portfolioSummary.totalPar > 0 
    ? ((portfolioSummary.totalFairValue / portfolioSummary.totalPar) * 100).toFixed(2) + "%" 
    : "—";
  
  const summaryFmvCost = portfolioSummary && portfolioSummary.totalCost > 0 
    ? ((portfolioSummary.totalFairValue / portfolioSummary.totalCost) * 100).toFixed(2) + "%" 
    : "—";

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
                <CardHeader className="flex flex-row items-center justify-between">
                  <div>
                    <CardTitle>Portfolio Holdings</CardTitle>
                    <CardDescription>
                      Showing {holdings.length} holdings for the selected filing period
                      <span className="ml-2 text-xs text-muted-foreground">(Values in millions USD)</span>
                    </CardDescription>
                  </div>
                  <Button onClick={exportToExcel} variant="outline" size="sm">
                    <Download className="mr-2 h-4 w-4" />
                    Export to Excel
                  </Button>
                </CardHeader>
                <CardContent>
                  {/* Portfolio Summary */}
                  {portfolioSummary && (
                    <div className="mb-6 p-4 bg-muted/50 rounded-lg border">
                      <h3 className="text-sm font-semibold mb-3 text-muted-foreground uppercase tracking-wide">Portfolio Summary</h3>
                      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
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
                          {holdings.map((holding) => (
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
      </div>
    </div>
  );
};

export default BdcDetail;
