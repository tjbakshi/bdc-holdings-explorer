import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
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
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";

const CompanyDetail = () => {
  const { companyName } = useParams<{ companyName: string }>();
  const decodedCompanyName = decodeURIComponent(companyName || "");

  // Fetch all holdings for this company across all BDCs and filings
  const { data: holdings, isLoading: holdingsLoading } = useQuery({
    queryKey: ["company-holdings", decodedCompanyName],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("holdings")
        .select(`
          *,
          filing:filings (
            id,
            period_end,
            filing_type,
            bdc:bdcs (
              id,
              bdc_name,
              ticker
            )
          )
        `)
        .eq("company_name", decodedCompanyName)
        .order("filing(period_end)", { ascending: false });
      
      if (error) throw error;
      return data;
    },
    enabled: !!decodedCompanyName,
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

  const calculateFmvPar = (fairValue: number | null, parAmount: number | null) => {
    if (!fairValue || !parAmount || parAmount === 0) return null;
    return (fairValue / parAmount) * 100;
  };

  const calculateFmvCost = (fairValue: number | null, cost: number | null) => {
    if (!fairValue || !cost || cost === 0) return null;
    return (fairValue / cost) * 100;
  };

  // Prepare chart data - group by period and BDC
  const chartData = holdings?.reduce((acc: any[], holding: any) => {
    const periodEnd = holding.filing?.period_end;
    const bdcName = holding.filing?.bdc?.bdc_name || "Unknown";
    const fairValue = holding.fair_value;

    if (!periodEnd || !fairValue) return acc;

    const existingPeriod = acc.find((item) => item.period === periodEnd);
    if (existingPeriod) {
      existingPeriod[bdcName] = (existingPeriod[bdcName] || 0) + fairValue;
    } else {
      acc.push({
        period: periodEnd,
        [bdcName]: fairValue,
      });
    }

    return acc;
  }, []).sort((a, b) => new Date(a.period).getTime() - new Date(b.period).getTime()) || [];

  // Prepare FMV % data
  const fmvParData = holdings?.map((holding: any) => ({
    period: holding.filing?.period_end,
    bdcName: holding.filing?.bdc?.bdc_name,
    fmvPar: calculateFmvPar(holding.fair_value, holding.par_amount),
    fmvCost: calculateFmvCost(holding.fair_value, holding.cost),
  })).filter((item) => item.period && (item.fmvPar !== null || item.fmvCost !== null))
    .sort((a, b) => new Date(a.period).getTime() - new Date(b.period).getTime()) || [];

  // Get unique BDCs for color coding
  const uniqueBdcs = [...new Set(holdings?.map((h: any) => h.filing?.bdc?.bdc_name).filter(Boolean))] as string[];
  const colors = ["#8884d8", "#82ca9d", "#ffc658", "#ff7c7c", "#a28dff", "#ff9f40"];

  if (holdingsLoading) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <p className="text-muted-foreground">Loading company details...</p>
      </div>
    );
  }

  if (!holdings || holdings.length === 0) {
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
              <p className="text-muted-foreground">
                No holdings found for "{decodedCompanyName}"
              </p>
            </CardContent>
          </Card>
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
            Back to Home
          </Button>
        </Link>

        <Card className="mb-6">
          <CardHeader>
            <CardTitle className="text-3xl">{decodedCompanyName}</CardTitle>
            <CardDescription>
              Portfolio company held across {uniqueBdcs.length} BDC{uniqueBdcs.length !== 1 ? "s" : ""} 
              {" "}with {holdings.length} total holding{holdings.length !== 1 ? "s" : ""} across all filings
            </CardDescription>
          </CardHeader>
        </Card>

        {chartData.length > 0 && (
          <Card className="mb-6">
            <CardHeader>
              <CardTitle>Fair Value Over Time by BDC</CardTitle>
              <CardDescription>Historical fair value trends across all BDCs</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="period" 
                    tickFormatter={(value) => {
                      const d = /^\d{4}-\d{2}-\d{2}$/.test(value) ? new Date(`${value}T00:00:00`) : new Date(value);
                      return d.toLocaleDateString(undefined, { month: "short", year: "2-digit" });
                    }}
                  />
                  <YAxis 
                    tickFormatter={(value) => `$${(value / 1000000).toFixed(1)}M`}
                  />
                  <Tooltip 
                    formatter={(value: any) => formatCurrency(value)}
                    labelFormatter={(label) => formatDate(label)}
                  />
                  <Legend />
                  {uniqueBdcs.map((bdc, index) => (
                    <Line
                      key={bdc}
                      type="monotone"
                      dataKey={bdc}
                      stroke={colors[index % colors.length]}
                      strokeWidth={2}
                      dot={{ r: 4 }}
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        )}

        {fmvParData.length > 0 && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
            <Card>
              <CardHeader>
                <CardTitle>FMV % Par Over Time</CardTitle>
                <CardDescription>Fair value as percentage of par amount</CardDescription>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={fmvParData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                      dataKey="period" 
                      tickFormatter={(value) => {
                        const d = /^\d{4}-\d{2}-\d{2}$/.test(value) ? new Date(`${value}T00:00:00`) : new Date(value);
                        return d.toLocaleDateString(undefined, { month: "short", year: "2-digit" });
                      }}
                    />
                    <YAxis 
                      tickFormatter={(value) => `${value}%`}
                      domain={[80, 120]}
                    />
                    <Tooltip 
                      formatter={(value: any) => `${value?.toFixed(2)}%`}
                      labelFormatter={(label) => formatDate(label)}
                    />
                    <Line
                      type="monotone"
                      dataKey="fmvPar"
                      stroke="#8884d8"
                      strokeWidth={2}
                      dot={{ r: 4 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>FMV % Cost Over Time</CardTitle>
                <CardDescription>Fair value as percentage of cost basis</CardDescription>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={fmvParData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                      dataKey="period" 
                      tickFormatter={(value) => new Date(value).toLocaleDateString(undefined, { month: 'short', year: '2-digit' })}
                    />
                    <YAxis 
                      tickFormatter={(value) => `${value}%`}
                      domain={[80, 120]}
                    />
                    <Tooltip 
                      formatter={(value: any) => `${value?.toFixed(2)}%`}
                      labelFormatter={(label) => formatDate(label)}
                    />
                    <Line
                      type="monotone"
                      dataKey="fmvCost"
                      stroke="#82ca9d"
                      strokeWidth={2}
                      dot={{ r: 4 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          </div>
        )}

        <Card>
          <CardHeader>
            <CardTitle>All Holdings</CardTitle>
            <CardDescription>Complete history of holdings across all BDCs and filings</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="rounded-lg border overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>BDC</TableHead>
                    <TableHead>Period</TableHead>
                    <TableHead>Filing Type</TableHead>
                    <TableHead>Investment Type</TableHead>
                    <TableHead>Industry</TableHead>
                    <TableHead className="min-w-[200px]">Description</TableHead>
                    <TableHead>Interest Rate</TableHead>
                    <TableHead>Reference Rate</TableHead>
                    <TableHead>Maturity</TableHead>
                    <TableHead className="text-right">Par Amount</TableHead>
                    <TableHead className="text-right">Cost</TableHead>
                    <TableHead className="text-right">Fair Value</TableHead>
                    <TableHead className="text-right">FMV % Par</TableHead>
                    <TableHead className="text-right">FMV % Cost</TableHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {holdings.map((holding: any) => (
                    <TableRow key={holding.id}>
                      <TableCell>
                        <Link 
                          to={`/bdc/${holding.filing?.bdc?.id}`}
                          className="font-medium hover:underline"
                        >
                          {holding.filing?.bdc?.bdc_name || "Unknown"}
                        </Link>
                      </TableCell>
                      <TableCell>{formatDate(holding.filing?.period_end)}</TableCell>
                      <TableCell>{holding.filing?.filing_type || "—"}</TableCell>
                      <TableCell>{holding.investment_type || "—"}</TableCell>
                      <TableCell>{holding.industry || "—"}</TableCell>
                      <TableCell className="text-sm">{holding.description || "—"}</TableCell>
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
                        {calculateFmvPar(holding.fair_value, holding.par_amount)?.toFixed(2) + "%" || "—"}
                      </TableCell>
                      <TableCell className="text-right font-medium">
                        {calculateFmvCost(holding.fair_value, holding.cost)?.toFixed(2) + "%" || "—"}
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
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default CompanyDetail;
