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
import { Search, ArrowUpDown } from "lucide-react";
import { Link } from "react-router-dom";

type SortField = "bdc_name" | "ticker" | "cik" | "fiscal_year_end_month";
type SortDirection = "asc" | "desc";

const Index = () => {
  const [searchQuery, setSearchQuery] = useState("");
  const [sortField, setSortField] = useState<SortField>("bdc_name");
  const [sortDirection, setSortDirection] = useState<SortDirection>("asc");

  const { data: bdcs, isLoading } = useQuery({
    queryKey: ["bdcs"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("bdcs")
        .select("*")
        .order(sortField, { ascending: sortDirection === "asc" });
      
      if (error) throw error;
      return data;
    },
  });

  const { data: latestFilings } = useQuery({
    queryKey: ["latest-filings"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("latest_filings")
        .select("*");
      
      if (error) throw error;
      return data;
    },
  });

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("asc");
    }
  };

  const filteredBdcs = bdcs?.filter((bdc) => {
    const query = searchQuery.toLowerCase();
    return (
      bdc.bdc_name.toLowerCase().includes(query) ||
      bdc.ticker?.toLowerCase().includes(query) ||
      bdc.cik.toLowerCase().includes(query)
    );
  });

  const getLatestFilingDate = (bdcId: string) => {
    const filing = latestFilings?.find((f) => f.bdc_id === bdcId);
    return filing?.period_end ? new Date(filing.period_end).toLocaleDateString() : "N/A";
  };

  const SortButton = ({ field, label }: { field: SortField; label: string }) => (
    <Button
      variant="ghost"
      onClick={() => handleSort(field)}
      className="flex items-center gap-1 hover:bg-muted/50"
    >
      {label}
      <ArrowUpDown className="h-4 w-4" />
    </Button>
  );

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto px-4 py-8">
        <header className="mb-8">
          <h1 className="text-4xl font-bold mb-2">BDC Tracker</h1>
          <p className="text-muted-foreground">
            Track and analyze Business Development Company holdings from SEC filings
          </p>
        </header>

        <div className="mb-6 relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
          <Input
            type="text"
            placeholder="Search by BDC name, ticker, or CIK..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10"
          />
        </div>

        {isLoading ? (
          <div className="text-center py-12">
            <p className="text-muted-foreground">Loading BDCs...</p>
          </div>
        ) : (
          <div className="rounded-lg border bg-card">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>
                    <SortButton field="bdc_name" label="BDC Name" />
                  </TableHead>
                  <TableHead>
                    <SortButton field="ticker" label="Ticker" />
                  </TableHead>
                  <TableHead>
                    <SortButton field="cik" label="CIK" />
                  </TableHead>
                  <TableHead>
                    <SortButton field="fiscal_year_end_month" label="Fiscal Year End" />
                  </TableHead>
                  <TableHead>Last Update</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredBdcs?.map((bdc) => (
                  <TableRow key={bdc.id}>
                    <TableCell className="font-medium">{bdc.bdc_name}</TableCell>
                    <TableCell>{bdc.ticker || "—"}</TableCell>
                    <TableCell className="font-mono text-sm">{bdc.cik}</TableCell>
                    <TableCell>
                      {bdc.fiscal_year_end_month}/{bdc.fiscal_year_end_day}
                    </TableCell>
                    <TableCell>{getLatestFilingDate(bdc.id)}</TableCell>
                    <TableCell>
                      <Link to={`/bdc/${bdc.id}`}>
                        <Button variant="outline" size="sm">
                          View Details
                        </Button>
                      </Link>
                    </TableCell>
                  </TableRow>
                ))}
                {filteredBdcs?.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center py-8 text-muted-foreground">
                      No BDCs found matching your search.
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        )}

        <div className="mt-4 text-sm text-muted-foreground">
          Showing {filteredBdcs?.length || 0} of {bdcs?.length || 0} BDCs
        </div>

        <div className="mt-12 pt-6 border-t border-border text-center">
          <Link to="/admin" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
            Admin / Data Updates (Internal)
          </Link>
        </div>
      </div>
    </div>
  );
};

export default Index;
