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
import { ArrowLeft } from "lucide-react";

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

  // Fetch holdings for selected filing
  const { data: holdings, isLoading: holdingsLoading } = useQuery({
    queryKey: ["holdings", selectedFilingId],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("holdings")
        .select("*")
        .eq("filing_id", selectedFilingId)
        .order("company_name", { ascending: true });
      
      if (error) throw error;
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

  const formatCurrency = (value: number | null) => {
    if (value === null || value === undefined) return "—";
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return "—";
    return new Date(dateStr).toLocaleDateString();
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
                <CardHeader>
                  <CardTitle>Portfolio Holdings</CardTitle>
                  <CardDescription>
                    Showing {holdings.length} holdings for the selected filing period
                  </CardDescription>
                </CardHeader>
                <CardContent>
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
                          <TableHead className="text-right">Par Amount</TableHead>
                          <TableHead className="text-right">Cost</TableHead>
                          <TableHead className="text-right">Fair Value</TableHead>
                          <TableHead className="text-right">FMV % Par</TableHead>
                          <TableHead className="text-right">FMV % Cost</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {holdings.map((holding) => (
                          <TableRow key={holding.id}>
                            <TableCell className="font-medium">
                              {holding.company_name}
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
                              {formatCurrency(holding.par_amount)}
                            </TableCell>
                            <TableCell className="text-right font-mono">
                              {formatCurrency(holding.cost)}
                            </TableCell>
                            <TableCell className="text-right font-mono">
                              {formatCurrency(holding.fair_value)}
                            </TableCell>
                            <TableCell className="text-right font-medium">
                              {calculateFmvPar(holding.fair_value, holding.par_amount)}
                            </TableCell>
                            <TableCell className="text-right font-medium">
                              {calculateFmvCost(holding.fair_value, holding.cost)}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
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
