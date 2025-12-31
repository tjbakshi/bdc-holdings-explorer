import React, { useState, useEffect, useMemo } from 'react';
import { useParams, Link } from 'react-router-dom';
import { supabase } from '@/integrations/supabase/client';
import { 
  ArrowLeft, 
  RefreshCw, 
  Search, 
  Download, 
  AlertCircle, 
  Trash2,
  ChevronRight,
  TrendingUp,
  DollarSign,
  Briefcase
} from 'lucide-react';
import * as XLSX from 'xlsx';

interface Filing {
  id: string;
  period_end: string;
  sec_accession_no: string;
  filing_url: string | null;
  parsed_successfully: boolean | null;
  value_scale: string | null;
  current_byte_offset: number | null;
  total_file_size: number | null;
}

interface Holding {
  id: string;
  company_name: string;
  investment_type: string;
  industry: string;
  interest_rate: string;
  maturity_date: string;
  par_amount: number;
  cost: number;
  fair_value: number;
}

const BdcDetail = () => {
  const { ticker } = useParams<{ ticker: string }>();
  const [bdc, setBdc] = useState<any>(null);
  const [filings, setFilings] = useState<Filing[]>([]);
  const [selectedFiling, setSelectedFiling] = useState<Filing | null>(null);
  const [holdings, setHoldings] = useState<Holding[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isParsing, setIsParsing] = useState(false);
  const [progress, setProgress] = useState(0);
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    fetchBdcData();
  }, [ticker]);

  const fetchBdcData = async () => {
    setIsLoading(true);
    try {
      const { data: bdcData } = await supabase
        .from('bdcs')
        .select('*')
        .eq('ticker', ticker)
        .single();
      
      setBdc(bdcData);

      if (bdcData) {
        const { data: filingData } = await supabase
          .from('filings')
          .select('*')
          .eq('bdc_id', bdcData.id)
          .order('period_end_date', { ascending: false });
        
        setFilings(filingData || []);
        if (filingData && filingData.length > 0) {
          setSelectedFiling(filingData[0]);
          fetchHoldings(filingData[0].id);
        }
      }
    } catch (error) {
      console.error('Error fetching BDC data:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const fetchHoldings = async (filingId: string) => {
    const { data } = await supabase
      .from('holdings')
      .select('*')
      .eq('filing_id', filingId)
      .order('industry', { ascending: true })
      .order('company_name', { ascending: true });
    
    setHoldings(data || []);
  };

  // --- RECURSIVE RELAY PARSING LOGIC ---
  const handleSync = async (filingId: string, startOffset: number = 0) => {
    setIsParsing(true);
    
    try {
      const { data, error } = await supabase.functions.invoke('extract_holdings_for_filing', {
        body: { filingId, resumeFromOffset: startOffset },
      });

      if (error) throw error;

      if (data.percentage_complete !== undefined) {
        setProgress(data.percentage_complete);
      }

      if (data.status === "PARTIAL") {
        // Continue the relay
        await handleSync(filingId, data.next_offset);
      } else {
        // Complete
        setIsParsing(false);
        setProgress(100);
        await fetchHoldings(filingId);
        // Refresh filings to show success status
        const { data: updatedFilings } = await supabase
          .from('filings')
          .select('*')
          .eq('bdc_id', bdc.id)
          .order('period_end_date', { ascending: false });
        setFilings(updatedFilings || []);
      }
    } catch (err) {
      console.error("Parsing failed:", err);
      setIsParsing(false);
      alert("Parsing stopped. You can click 'Reset' and try again.");
    }
  };

  const handleResetFiling = async (filingId: string) => {
    if (!window.confirm("Are you sure you want to clear all data for this filing and start over?")) return;

    try {
      // 1. Delete holdings
      await supabase.from('holdings').delete().eq('filing_id', filingId);
      // 2. Reset filing status
      await supabase.from('filings').update({
        parsed_successfully: false,
        current_byte_offset: 0,
        current_industry_state: null,
        value_scale: null
      }).eq('id', filingId);

      // Refresh UI
      fetchBdcData();
    } catch (error) {
      console.error("Reset failed:", error);
    }
  };

  const handleExportExcel = () => {
    const worksheet = XLSX.utils.json_to_sheet(filteredHoldings);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Holdings");
    XLSX.writeFile(workbook, `${ticker}_Holdings_${selectedFiling?.period_end}.xlsx`);
  };

  // Calculations for Summary
  const totals = useMemo(() => {
    return holdings.reduce((acc, curr) => {
      // Fallback par to cost if par is blank
      const par = curr.par_amount || curr.cost || 0;
      acc.par += par;
      acc.cost += curr.cost || 0;
      acc.fv += curr.fair_value || 0;
      return acc;
    }, { par: 0, cost: 0, fv: 0 });
  }, [holdings]);

  const filteredHoldings = useMemo(() => {
    return holdings.filter(h => 
      h.company_name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      h.industry?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      h.investment_type?.toLowerCase().includes(searchQuery.toLowerCase())
    );
  }, [holdings, searchQuery]);

  if (isLoading) return <div className="p-8 text-center">Loading BDC Data...</div>;

  return (
    <div className="min-h-screen bg-gray-50 p-4 md:p-8">
      {/* Header */}
      <div className="max-w-7xl mx-auto mb-8">
        <Link to="/" className="inline-flex items-center text-blue-600 hover:text-blue-800 mb-4">
          <ArrowLeft className="w-4 h-4 mr-1" /> Back to Dashboard
        </Link>
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">{bdc?.name} ({bdc?.ticker})</h1>
            <p className="text-gray-500">CIK: {bdc?.cik}</p>
          </div>
          <div className="flex gap-2">
            <button 
              onClick={handleExportExcel}
              className="inline-flex items-center px-4 py-2 bg-white border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50"
            >
              <Download className="w-4 h-4 mr-2" /> Export Excel
            </button>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-4 gap-8">
        {/* Sidebar: Filings List */}
        <div className="lg:col-span-1 space-y-4">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Filings</h2>
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
            {filings.map((filing) => (
              <div 
                key={filing.id}
                onClick={() => { setSelectedFiling(filing); fetchHoldings(filing.id); }}
                className={`p-4 border-b border-gray-100 cursor-pointer transition-colors ${selectedFiling?.id === filing.id ? 'bg-blue-50' : 'hover:bg-gray-50'}`}
              >
                <div className="flex justify-between items-start mb-2">
                  <span className="font-medium">{filing.period_end}</span>
                  {filing.parsed_successfully ? (
                    <span className="bg-green-100 text-green-700 text-xs px-2 py-1 rounded">Parsed</span>
                  ) : (
                    <span className="bg-yellow-100 text-yellow-700 text-xs px-2 py-1 rounded">Pending</span>
                  )}
                </div>
                <div className="flex justify-between items-center">
                  {filing.filing_url && (
                    <a 
                      href={filing.filing_url} 
                      target="_blank" 
                      rel="noreferrer" 
                      className="text-xs text-blue-500 hover:underline"
                      onClick={(e) => e.stopPropagation()}
                    >
                      View SEC Source
                    </a>
                  )}
                  <button 
                    onClick={(e) => { e.stopPropagation(); handleResetFiling(filing.id); }}
                    className="p-1 text-gray-400 hover:text-red-500"
                    title="Reset Data"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Main Content: Summary & Holdings */}
        <div className="lg:col-span-3 space-y-6">
          {/* Summary Cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-200">
              <div className="flex items-center text-gray-500 mb-2">
                <DollarSign className="w-4 h-4 mr-2" /> Total Fair Value
              </div>
              <div className="text-2xl font-bold text-gray-900">${(totals.fv / 1000).toFixed(2)}B</div>
              <div className="text-sm text-gray-500 mt-1">
                {(totals.fv / totals.cost * 100).toFixed(1)}% of Amortized Cost
              </div>
            </div>
            <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-200">
              <div className="flex items-center text-gray-500 mb-2">
                <TrendingUp className="w-4 h-4 mr-2" /> FMV % of Par
              </div>
              <div className="text-2xl font-bold text-gray-900">
                {totals.par > 0 ? ((totals.fv / totals.par) * 100).toFixed(1) : 0}%
              </div>
              <div className="text-sm text-gray-500 mt-1">Weighted Portfolio Average</div>
            </div>
            <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-200">
              <div className="flex items-center text-gray-500 mb-2">
                <Briefcase className="w-4 h-4 mr-2" /> Total Positions
              </div>
              <div className="text-2xl font-bold text-gray-900">{holdings.length}</div>
              <div className="text-sm text-gray-500 mt-1">Extracted holdings</div>
            </div>
          </div>

          {/* Progress Bar (Only visible when parsing) */}
          {isParsing && (
            <div className="bg-blue-50 border border-blue-200 rounded-xl p-4">
              <div className="flex justify-between items-center mb-2">
                <span className="text-sm font-medium text-blue-700 flex items-center">
                  <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> Parsing Large Filing...
                </span>
                <span className="text-sm font-bold text-blue-700">{progress}%</span>
              </div>
              <div className="w-full bg-blue-200 rounded-full h-2.5">
                <div className="bg-blue-600 h-2.5 rounded-full transition-all duration-500" style={{ width: `${progress}%` }}></div>
              </div>
              <p className="text-xs text-blue-600 mt-2">
                Step-by-step extraction is active to prevent timeouts. Please keep this tab open.
              </p>
            </div>
          )}

          {/* Action Row & Search */}
          <div className="flex flex-col md:flex-row gap-4 items-center justify-between">
            <div className="relative w-full md:w-96">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input 
                type="text"
                placeholder="Search holdings, industry, or type..."
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
            </div>
            {!selectedFiling?.parsed_successfully && !isParsing && (
              <button 
                onClick={() => handleSync(selectedFiling!.id)}
                className="w-full md:w-auto px-6 py-2 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700 flex items-center justify-center"
              >
                <RefreshCw className="w-4 h-4 mr-2" /> Start Parsing
              </button>
            )}
          </div>

          {/* Holdings Table */}
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="bg-gray-50 border-b border-gray-200 text-gray-600 font-medium">
                  <tr>
                    <th className="px-4 py-3">Company</th>
                    <th className="px-4 py-3">Industry</th>
                    <th className="px-4 py-3">Type</th>
                    <th className="px-4 py-3 text-right">Maturity</th>
                    <th className="px-4 py-3 text-right">Cost ($M)</th>
                    <th className="px-4 py-3 text-right">Fair Value ($M)</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {filteredHoldings.map((holding) => (
                    <tr key={holding.id} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-medium text-gray-900">{holding.company_name}</td>
                      <td className="px-4 py-3 text-gray-500">{holding.industry || '—'}</td>
                      <td className="px-4 py-3 text-gray-500">{holding.investment_type || '—'}</td>
                      <td className="px-4 py-3 text-right text-gray-500">{holding.maturity_date || '—'}</td>
                      <td className="px-4 py-3 text-right text-gray-900 font-mono">{(holding.cost || 0).toLocaleString()}</td>
                      <td className="px-4 py-3 text-right text-gray-900 font-bold font-mono">{(holding.fair_value || 0).toLocaleString()}</td>
                    </tr>
                  ))}
                  {filteredHoldings.length === 0 && (
                    <tr>
                      <td colSpan={6} className="px-4 py-12 text-center text-gray-500">
                        {isParsing ? "Data is being extracted..." : "No holdings found for this filing."}
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default BdcDetail;
