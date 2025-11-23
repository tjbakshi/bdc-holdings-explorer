export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  // Allows to automatically instantiate createClient with right options
  // instead of createClient<Database, { PostgrestVersion: 'XX' }>(URL, KEY)
  __InternalSupabase: {
    PostgrestVersion: "13.0.5"
  }
  public: {
    Tables: {
      bdcs: {
        Row: {
          bdc_name: string
          cik: string
          created_at: string | null
          fiscal_year_end_day: number
          fiscal_year_end_month: number
          id: string
          ticker: string | null
        }
        Insert: {
          bdc_name: string
          cik: string
          created_at?: string | null
          fiscal_year_end_day: number
          fiscal_year_end_month: number
          id?: string
          ticker?: string | null
        }
        Update: {
          bdc_name?: string
          cik?: string
          created_at?: string | null
          fiscal_year_end_day?: number
          fiscal_year_end_month?: number
          id?: string
          ticker?: string | null
        }
        Relationships: []
      }
      filings: {
        Row: {
          bdc_id: string
          created_at: string | null
          data_source: string | null
          filing_type: string
          filing_url: string | null
          id: string
          parsed_successfully: boolean | null
          period_end: string
          sec_accession_no: string | null
        }
        Insert: {
          bdc_id: string
          created_at?: string | null
          data_source?: string | null
          filing_type: string
          filing_url?: string | null
          id?: string
          parsed_successfully?: boolean | null
          period_end: string
          sec_accession_no?: string | null
        }
        Update: {
          bdc_id?: string
          created_at?: string | null
          data_source?: string | null
          filing_type?: string
          filing_url?: string | null
          id?: string
          parsed_successfully?: boolean | null
          period_end?: string
          sec_accession_no?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "filings_bdc_id_fkey"
            columns: ["bdc_id"]
            isOneToOne: false
            referencedRelation: "bdcs"
            referencedColumns: ["id"]
          },
        ]
      }
      holdings: {
        Row: {
          company_name: string
          cost: number | null
          created_at: string | null
          description: string | null
          fair_value: number | null
          filing_id: string
          id: string
          industry: string | null
          interest_rate: string | null
          investment_type: string | null
          maturity_date: string | null
          par_amount: number | null
          reference_rate: string | null
        }
        Insert: {
          company_name: string
          cost?: number | null
          created_at?: string | null
          description?: string | null
          fair_value?: number | null
          filing_id: string
          id?: string
          industry?: string | null
          interest_rate?: string | null
          investment_type?: string | null
          maturity_date?: string | null
          par_amount?: number | null
          reference_rate?: string | null
        }
        Update: {
          company_name?: string
          cost?: number | null
          created_at?: string | null
          description?: string | null
          fair_value?: number | null
          filing_id?: string
          id?: string
          industry?: string | null
          interest_rate?: string | null
          investment_type?: string | null
          maturity_date?: string | null
          par_amount?: number | null
          reference_rate?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "holdings_filing_id_fkey"
            columns: ["filing_id"]
            isOneToOne: false
            referencedRelation: "filings"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "holdings_filing_id_fkey"
            columns: ["filing_id"]
            isOneToOne: false
            referencedRelation: "latest_filings"
            referencedColumns: ["id"]
          },
        ]
      }
    }
    Views: {
      latest_filings: {
        Row: {
          bdc_id: string | null
          created_at: string | null
          data_source: string | null
          filing_type: string | null
          filing_url: string | null
          id: string | null
          parsed_successfully: boolean | null
          period_end: string | null
          sec_accession_no: string | null
        }
        Relationships: [
          {
            foreignKeyName: "filings_bdc_id_fkey"
            columns: ["bdc_id"]
            isOneToOne: false
            referencedRelation: "bdcs"
            referencedColumns: ["id"]
          },
        ]
      }
    }
    Functions: {
      [_ in never]: never
    }
    Enums: {
      [_ in never]: never
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

type DatabaseWithoutInternals = Omit<Database, "__InternalSupabase">

type DefaultSchema = DatabaseWithoutInternals[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] &
        DefaultSchema["Views"])
    ? (DefaultSchema["Tables"] &
        DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
        Row: infer R
      }
      ? R
      : never
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Insert: infer I
      }
      ? I
      : never
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Update: infer U
      }
      ? U
      : never
    : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof DatabaseWithoutInternals },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never,
> = DefaultSchemaEnumNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
    ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
    : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof DatabaseWithoutInternals },
  CompositeTypeName extends PublicCompositeTypeNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never = never,
> = PublicCompositeTypeNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
    ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
    : never

export const Constants = {
  public: {
    Enums: {},
  },
} as const
