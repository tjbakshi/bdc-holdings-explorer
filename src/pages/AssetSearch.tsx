import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Search, ArrowLeft } from "lucide-react";
import { Link } from "react-router-dom";

const AssetSearch = () => {
  const [searchQuery, setSearchQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");

  // Debounce search to avoid too many queries
  const handleSearch = (value: string) => {
    setSearchQuery(value);
    // Only search after 2+ characters
    if (value.length >= 2) {
      setDebouncedQuery(value);
    } else {
      setDebouncedQuery("");
    }
  };

  const { data: results, isLoading } = useQuery({
    queryKey: ["asset-search", debouncedQuery],
    queryFn: async () => {
      if (!debouncedQuery) return [];
      
      const { data, error } = await supabase
        .from("holdings")
        .select(`
          id,
          company_name,
          investment_type,
          industry,
          fair_value,
          cost,
          interest_rate,
          maturity_date,
          period_date,
          filing_id
        `)
        .ilike("company_name", `%${debouncedQuery}%`)
        .order("company_name")
        .limit(200);
      
      if (error) throw error;
      
      // Get filing and BDC info for each holding
      const filingIds = [...new Set(data.map(h => h.filing_id))];
      
      const { data: filings, error: filingsError } = await supabase
        .from("filings")
        .select("id, bdc_id, period_end")
        .in("id", filingIds);
      
      if (filingsError) throw filingsError;
      
      const bdcIds = [...new Set(filings?.map(f => f.bdc_id) || [])];
      
      const { data: bdcs, error: bdcsError } = await supabase
        .from("bdcs")
        .select("id, bdc_name, ticker")
        .in("id", bdcIds);
      
      if (bdcsError) throw bdcsError;
      
      // Join the data
      return data.map(holding => {
        const filing = filings?.find(f => f.id === holding.filing_id);
        const bdc = bdcs?.find(b => b.id === filing?.bdc_id);
        return {
          ...holding,
          bdc_name: bdc?.bdc_name || "Unknown",
          bdc_ticker: bdc?.ticker || "",
          bdc_id: bdc?.id,
          filing_period: filing?.period_end,
        };
      });
    },
    enabled: debouncedQuery.length >= 2,
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

  const formatDate = (date: string | null) => {
    if (!date) return "—";
    return new Date(date).toLocaleDateString();
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto px-4 py-8">
        <header className="mb-8">
          <Link to="/" className="inline-flex items-center gap-2 text-muted-foreground hover:text-foreground mb-4">
            <ArrowLeft className="h-4 w-4" />
            Back to BDC List
          </Link>
          <h1 className="text-4xl font-bold mb-2">Asset Search</h1>
          <p className="text-muted-foreground">
            Search for portfolio companies across all BDCs
          </p>
        </header>

        <div className="mb-6 relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
          <Input
            type="text"
            placeholder="Search by company name (min 2 characters)..."
            value={searchQuery}
            onChange={(e) => handleSearch(e.target.value)}
            className="pl-10 text-lg py-6"
            autoFocus
          />
        </div>

        {debouncedQuery.length < 2 ? (
          <div className="text-center py-12">
            <p className="text-muted-foreground">
              Enter at least 2 characters to search for assets
            </p>
          </div>
        ) : isLoading ? (
          <div className="text-center py-12">
            <p className="text-muted-foreground">Searching...</p>
          </div>
        ) : (
          <>
            <div className="rounded-lg border bg-card overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Company Name</TableHead>
                    <TableHead>BDC</TableHead>
                    <TableHead>Investment Type</TableHead>
                    <TableHead>Industry</TableHead>
                    <TableHead className="text-right">Fair Value</TableHead>
                    <TableHead>Rate</TableHead>
                    <TableHead>Period</TableHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {results?.map((holding) => (
                    <TableRow key={holding.id}>
                      <TableCell className="font-medium max-w-xs truncate">
                        {holding.company_name}
                      </TableCell>
                      <TableCell>
                        <Link 
                          to={`/bdc/${holding.bdc_id}`}
                          className="text-primary hover:underline"
                        >
                          {holding.bdc_ticker || holding.bdc_name}
                        </Link>
                      </TableCell>
                      <TableCell>{holding.investment_type || "—"}</TableCell>
                      <TableCell className="max-w-[150px] truncate">
                        {holding.industry || "—"}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatCurrency(holding.fair_value)}
                      </TableCell>
                      <TableCell>{holding.interest_rate || "—"}</TableCell>
                      <TableCell>{formatDate(holding.filing_period)}</TableCell>
                      <TableCell>
                        <Link to={`/holding/${holding.id}`}>
                          <Button variant="outline" size="sm">
                            Details
                          </Button>
                        </Link>
                      </TableCell>
                    </TableRow>
                  ))}
                  {results?.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={8} className="text-center py-8 text-muted-foreground">
                        No assets found matching "{debouncedQuery}"
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>

            <div className="mt-4 text-sm text-muted-foreground">
              Found {results?.length || 0} holdings {results?.length === 200 && "(showing first 200)"}
            </div>
          </>
        )}
      </div>
    </div>
  );
};

export default AssetSearch;
