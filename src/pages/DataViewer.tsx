import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { Link } from "react-router-dom";
import { ArrowLeft } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { format } from "date-fns";

const DataViewer = () => {
  const { data: bdcs, isLoading: bdcsLoading } = useQuery({
    queryKey: ["all-bdcs"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("bdcs")
        .select("*")
        .order("bdc_name");
      if (error) throw error;
      return data;
    },
  });

  const { data: filings, isLoading: filingsLoading } = useQuery({
    queryKey: ["all-filings"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("filings")
        .select("*, bdcs(bdc_name, ticker)")
        .order("created_at", { ascending: false });
      if (error) throw error;
      return data;
    },
  });

  const { data: ingestionRuns, isLoading: runsLoading } = useQuery({
    queryKey: ["ingestion-runs"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ingestion_runs")
        .select("*")
        .order("started_at", { ascending: false });
      if (error) throw error;
      return data;
    },
  });

  const formatDate = (date: string | null) => {
    if (!date) return "-";
    return format(new Date(date), "MMM d, yyyy HH:mm");
  };

  return (
    <div className="container mx-auto py-8 px-4">
      <div className="mb-6">
        <Link
          to="/admin"
          className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground mb-4"
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to Admin
        </Link>
        <h1 className="text-3xl font-bold">Data Viewer</h1>
        <p className="text-muted-foreground">
          View all data stored in the database
        </p>
      </div>

      <Tabs defaultValue="bdcs" className="space-y-4">
        <TabsList>
          <TabsTrigger value="bdcs">
            BDCs ({bdcs?.length ?? 0})
          </TabsTrigger>
          <TabsTrigger value="filings">
            Filings ({filings?.length ?? 0})
          </TabsTrigger>
          <TabsTrigger value="runs">
            Ingestion Runs ({ingestionRuns?.length ?? 0})
          </TabsTrigger>
        </TabsList>

        <TabsContent value="bdcs">
          <Card>
            <CardHeader>
              <CardTitle>All BDCs</CardTitle>
            </CardHeader>
            <CardContent>
              {bdcsLoading ? (
                <p className="text-muted-foreground">Loading...</p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Name</TableHead>
                      <TableHead>Ticker</TableHead>
                      <TableHead>CIK</TableHead>
                      <TableHead>Fiscal Year End</TableHead>
                      <TableHead>Created At</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {bdcs?.map((bdc) => (
                      <TableRow key={bdc.id}>
                        <TableCell className="font-medium">
                          {bdc.bdc_name}
                        </TableCell>
                        <TableCell>{bdc.ticker || "-"}</TableCell>
                        <TableCell className="font-mono text-sm">
                          {bdc.cik}
                        </TableCell>
                        <TableCell>
                          {bdc.fiscal_year_end_month}/{bdc.fiscal_year_end_day}
                        </TableCell>
                        <TableCell>{formatDate(bdc.created_at)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="filings">
          <Card>
            <CardHeader>
              <CardTitle>All Filings</CardTitle>
            </CardHeader>
            <CardContent>
              {filingsLoading ? (
                <p className="text-muted-foreground">Loading...</p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>BDC</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Period End</TableHead>
                      <TableHead>Accession No</TableHead>
                      <TableHead>Parsed</TableHead>
                      <TableHead>Source</TableHead>
                      <TableHead>Created At</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filings?.map((filing) => (
                      <TableRow key={filing.id}>
                        <TableCell className="font-medium">
                          {(filing.bdcs as any)?.bdc_name || "-"}
                          {(filing.bdcs as any)?.ticker && (
                            <span className="text-muted-foreground ml-1">
                              ({(filing.bdcs as any).ticker})
                            </span>
                          )}
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline">{filing.filing_type}</Badge>
                        </TableCell>
                        <TableCell>{filing.period_end}</TableCell>
                        <TableCell className="font-mono text-xs">
                          {filing.sec_accession_no || "-"}
                        </TableCell>
                        <TableCell>
                          <Badge
                            variant={
                              filing.parsed_successfully
                                ? "default"
                                : "secondary"
                            }
                          >
                            {filing.parsed_successfully ? "Yes" : "No"}
                          </Badge>
                        </TableCell>
                        <TableCell>{filing.data_source || "-"}</TableCell>
                        <TableCell>{formatDate(filing.created_at)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="runs">
          <Card>
            <CardHeader>
              <CardTitle>Ingestion Runs</CardTitle>
            </CardHeader>
            <CardContent>
              {runsLoading ? (
                <p className="text-muted-foreground">Loading...</p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>CIK</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Inserted</TableHead>
                      <TableHead>Started At</TableHead>
                      <TableHead>Finished At</TableHead>
                      <TableHead>Error</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {ingestionRuns?.map((run) => (
                      <TableRow key={run.id}>
                        <TableCell className="font-mono text-sm">
                          {run.cik}
                        </TableCell>
                        <TableCell>
                          <Badge
                            variant={
                              run.status === "completed"
                                ? "default"
                                : run.status === "failed"
                                ? "destructive"
                                : "secondary"
                            }
                          >
                            {run.status}
                          </Badge>
                        </TableCell>
                        <TableCell>{run.inserted_count ?? 0}</TableCell>
                        <TableCell>{formatDate(run.started_at)}</TableCell>
                        <TableCell>{formatDate(run.finished_at)}</TableCell>
                        <TableCell className="max-w-xs truncate text-destructive">
                          {run.error_message || "-"}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
};

export default DataViewer;
