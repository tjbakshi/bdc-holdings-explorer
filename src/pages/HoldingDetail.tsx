import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ArrowLeft, TrendingUp } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";

const HoldingDetail = () => {
  const { holdingId } = useParams<{ holdingId: string }>();

  // Fetch the base holding with its filing and BDC info
  const { data: baseHolding, isLoading: baseLoading } = useQuery({
    queryKey: ["holding", holdingId],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("holdings")
        .select(`
          *,
          filing:filings (
            id,
            period_end,
            filing_type,
            bdc_id,
            bdc:bdcs (
              id,
              bdc_name,
              ticker,
              cik
            )
          )
        `)
        .eq("id", holdingId)
        .single();
      
      if (error) throw error;
      return data;
    },
    enabled: !!holdingId,
  });

  // Fetch historical holdings for the same company and BDC
  const { data: historicalHoldings, isLoading: historyLoading } = useQuery({
    queryKey: ["holding-history", baseHolding?.filing?.bdc_id, baseHolding?.company_name],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("holdings")
        .select(`
          *,
          filing:filings (
            id,
            period_end,
            filing_type,
            bdc_id
          )
        `)
        .eq("company_name", baseHolding?.company_name)
        .eq("filing.bdc_id", baseHolding?.filing?.bdc_id)
        .order("filing(period_end)", { ascending: true });
      
      if (error) throw error;
      return data;
    },
    enabled: !!baseHolding?.company_name && !!baseHolding?.filing?.bdc_id,
  });

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
    // Avoid timezone shifting for YYYY-MM-DD (treated as UTC by Date constructor)
    const d = /^\d{4}-\d{2}-\d{2}$/.test(dateStr)
      ? new Date(`${dateStr}T00:00:00`)
      : new Date(dateStr);
    return d.toLocaleDateString();
  };

  const formatShortDate = (dateStr: string) => {
    const date = /^\d{4}-\d{2}-\d{2}$/.test(dateStr)
      ? new Date(`${dateStr}T00:00:00`)
      : new Date(dateStr);
    return date.toLocaleDateString(undefined, { month: "short", year: "2-digit" });
  };

  const calculateFmvPar = (fairValue: number | null, parAmount: number | null) => {
    if (!fairValue || !parAmount || parAmount === 0) return null;
    return (fairValue / parAmount) * 100;
  };

  const calculateFmvCost = (fairValue: number | null, cost: number | null) => {
    if (!fairValue || !cost || cost === 0) return null;
    return (fairValue / cost) * 100;
  };

  // Prepare chart data for values over time
  const valueChartData = historicalHoldings?.map((holding: any) => ({
    period: holding.filing?.period_end,
    parAmount: holding.par_amount || 0,
    cost: holding.cost || 0,
    fairValue: holding.fair_value || 0,
  })) || [];

  // Prepare chart data for FMV % over time
  const fmvChartData = historicalHoldings?.map((holding: any) => ({
    period: holding.filing?.period_end,
    fmvPar: calculateFmvPar(holding.fair_value, holding.par_amount),
    fmvCost: calculateFmvCost(holding.fair_value, holding.cost),
  })).filter((item) => item.fmvPar !== null || item.fmvCost !== null) || [];

  if (baseLoading) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <p className="text-muted-foreground">Loading holding details...</p>
      </div>
    );
  }

  if (!baseHolding) {
    return (
      <div className="min-h-screen bg-background">
        <div className="container mx-auto px-4 py-8">
          <Link to="/">
            <Button variant="ghost" className="mb-6">
              <ArrowLeft className="mr-2 h-4 w-4" />
              Back to Home
            </Button>
          </Link>
          <Card>
            <CardContent className="py-12 text-center">
              <p className="text-muted-foreground">Holding not found</p>
            </CardContent>
          </Card>
        </div>
      </div>
    );
  }

  const bdc = baseHolding.filing?.bdc;
  const fmvPar = calculateFmvPar(baseHolding.fair_value, baseHolding.par_amount);
  const fmvCost = calculateFmvCost(baseHolding.fair_value, baseHolding.cost);

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto px-4 py-8">
        <div className="flex items-center gap-4 mb-6">
          <Link to={`/bdc/${bdc?.id}`}>
            <Button variant="ghost">
              <ArrowLeft className="mr-2 h-4 w-4" />
              Back to BDC
            </Button>
          </Link>
          <Link to={`/company/${encodeURIComponent(baseHolding.company_name)}`}>
            <Button variant="ghost">
              View All {baseHolding.company_name} Holdings
            </Button>
          </Link>
        </div>

        {/* Header / Summary Card */}
        <Card className="mb-6">
          <CardHeader>
            <div className="flex items-start justify-between">
              <div className="flex-1">
                <CardTitle className="text-3xl mb-2">{baseHolding.company_name}</CardTitle>
                <CardDescription className="text-base">
                  Held by{" "}
                  <Link 
                    to={`/bdc/${bdc?.id}`}
                    className="font-medium hover:underline"
                  >
                    {bdc?.bdc_name} ({bdc?.ticker || bdc?.cik})
                  </Link>
                </CardDescription>
              </div>
              <div className="text-right">
                <div className="text-sm text-muted-foreground">Filing Period</div>
                <div className="font-medium">{formatDate(baseHolding.filing?.period_end)}</div>
                <div className="text-sm text-muted-foreground">{baseHolding.filing?.filing_type}</div>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <div>
                <div className="text-sm text-muted-foreground mb-1">Investment Type</div>
                <div className="font-medium">{baseHolding.investment_type || "—"}</div>
              </div>
              <div>
                <div className="text-sm text-muted-foreground mb-1">Industry</div>
                <div className="font-medium">{baseHolding.industry || "—"}</div>
              </div>
              <div>
                <div className="text-sm text-muted-foreground mb-1">Interest Rate</div>
                <div className="font-medium">{baseHolding.interest_rate || "—"}</div>
              </div>
              <div>
                <div className="text-sm text-muted-foreground mb-1">Reference Rate</div>
                <div className="font-medium">{baseHolding.reference_rate || "—"}</div>
              </div>
            </div>
            {baseHolding.description && (
              <div className="mt-4">
                <div className="text-sm text-muted-foreground mb-1">Description</div>
                <div className="text-sm">{baseHolding.description}</div>
              </div>
            )}
            {baseHolding.maturity_date && (
              <div className="mt-4">
                <div className="text-sm text-muted-foreground mb-1">Maturity Date</div>
                <div className="font-medium">{formatDate(baseHolding.maturity_date)}</div>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Current Snapshot */}
        <div className="grid grid-cols-1 md:grid-cols-5 gap-4 mb-6">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground">Par Amount</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{formatCurrency(baseHolding.par_amount)}</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground">Cost</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{formatCurrency(baseHolding.cost)}</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground">Fair Value</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{formatCurrency(baseHolding.fair_value)}</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground">FMV % Par</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {fmvPar ? `${fmvPar.toFixed(2)}%` : "—"}
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground">FMV % Cost</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {fmvCost ? `${fmvCost.toFixed(2)}%` : "—"}
              </div>
            </CardContent>
          </Card>
        </div>

        {historyLoading ? (
          <div className="text-center py-8">
            <p className="text-muted-foreground">Loading historical data...</p>
          </div>
        ) : (
          <>
            {/* Historical Charts */}
            {valueChartData.length > 0 && (
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <TrendingUp className="h-5 w-5" />
                      Value Over Time
                    </CardTitle>
                    <CardDescription>Par amount, cost, and fair value trends</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <ResponsiveContainer width="100%" height={300}>
                      <LineChart data={valueChartData}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis 
                          dataKey="period" 
                          tickFormatter={formatShortDate}
                        />
                        <YAxis 
                          tickFormatter={(value) => `$${(value / 1000000).toFixed(1)}M`}
                        />
                        <Tooltip 
                          formatter={(value: any) => formatCurrency(value)}
                          labelFormatter={(label) => formatDate(label)}
                        />
                        <Legend />
                        <Line
                          type="monotone"
                          dataKey="parAmount"
                          stroke="#8884d8"
                          strokeWidth={2}
                          dot={{ r: 4 }}
                          name="Par Amount"
                        />
                        <Line
                          type="monotone"
                          dataKey="cost"
                          stroke="#82ca9d"
                          strokeWidth={2}
                          dot={{ r: 4 }}
                          name="Cost"
                        />
                        <Line
                          type="monotone"
                          dataKey="fairValue"
                          stroke="#ffc658"
                          strokeWidth={2}
                          dot={{ r: 4 }}
                          name="Fair Value"
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <TrendingUp className="h-5 w-5" />
                      FMV % Over Time
                    </CardTitle>
                    <CardDescription>Fair value as percentage of par and cost</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <ResponsiveContainer width="100%" height={300}>
                      <LineChart data={fmvChartData}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis 
                          dataKey="period" 
                          tickFormatter={formatShortDate}
                        />
                        <YAxis 
                          tickFormatter={(value) => `${value}%`}
                          domain={[80, 120]}
                        />
                        <Tooltip 
                          formatter={(value: any) => value ? `${value.toFixed(2)}%` : "—"}
                          labelFormatter={(label) => formatDate(label)}
                        />
                        <Legend />
                        <Line
                          type="monotone"
                          dataKey="fmvPar"
                          stroke="#8884d8"
                          strokeWidth={2}
                          dot={{ r: 4 }}
                          name="FMV % Par"
                        />
                        <Line
                          type="monotone"
                          dataKey="fmvCost"
                          stroke="#82ca9d"
                          strokeWidth={2}
                          dot={{ r: 4 }}
                          name="FMV % Cost"
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
              </div>
            )}

            {/* Historical Table */}
            {historicalHoldings && historicalHoldings.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle>Historical Data</CardTitle>
                  <CardDescription>
                    Complete history of this holding across {historicalHoldings.length} filing{historicalHoldings.length !== 1 ? "s" : ""}
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="rounded-lg border overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Period</TableHead>
                          <TableHead>Filing Type</TableHead>
                          <TableHead className="text-right">Par Amount</TableHead>
                          <TableHead className="text-right">Cost</TableHead>
                          <TableHead className="text-right">Fair Value</TableHead>
                          <TableHead className="text-right">FMV % Par</TableHead>
                          <TableHead className="text-right">FMV % Cost</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {historicalHoldings.map((holding: any) => {
                          const holdingFmvPar = calculateFmvPar(holding.fair_value, holding.par_amount);
                          const holdingFmvCost = calculateFmvCost(holding.fair_value, holding.cost);
                          
                          return (
                            <TableRow 
                              key={holding.id}
                              className={holding.id === holdingId ? "bg-muted/50" : ""}
                            >
                              <TableCell className="font-medium">
                                {formatDate(holding.filing?.period_end)}
                              </TableCell>
                              <TableCell>{holding.filing?.filing_type || "—"}</TableCell>
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
                                {holdingFmvPar ? `${holdingFmvPar.toFixed(2)}%` : "—"}
                              </TableCell>
                              <TableCell className="text-right font-medium">
                                {holdingFmvCost ? `${holdingFmvCost.toFixed(2)}%` : "—"}
                              </TableCell>
                            </TableRow>
                          );
                        })}
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

export default HoldingDetail;
