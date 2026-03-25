export type Gateway = {
  id: number;
  enabled: boolean;
  addr: string;
  pubkey: string;
  connectedness?: string;
  fee_pool_ready?: boolean;
  is_master?: boolean;
  last_error?: string;
  last_runtime_error?: string;
  last_runtime_error_stage?: string;
};

export type GatewaysResp = {
  total: number;
  items: Gateway[];
};

export type Arbiter = {
  id: number;
  enabled: boolean;
  addr: string;
  pubkey: string;
};

export type ArbitersResp = {
  total: number;
  items: Arbiter[];
};

export type Workspace = {
  id: number;
  path: string;
  max_bytes: number;
  used_bytes: number;
  enabled: boolean;
  created_at_unix: number;
  updated_at_unix: number;
};

export type WorkspacesResp = {
  total: number;
  items: Workspace[];
};

export type StaticItem = {
  name: string;
  path: string;
  type: "dir" | "file";
  size: number;
  mtime_unix: number;
  seed_hash?: string;
  floor_unit_price_sat_per_64k?: number;
  resale_discount_bps?: number;
};

export type StaticTreeResp = {
  root_path: string;
  current_path: string;
  parent_path: string;
  total: number;
  items: StaticItem[];
};

export type WalletSummary = {
  wallet_address?: string;
  onchain_balance_satoshi?: number;
  balance_source?: string;
  wallet_chain_type?: string;
  wallet_chain_base_url?: string;
  ledger_net_satoshi?: number;
  total_in_satoshi?: number;
  total_out_satoshi?: number;
  total_used_satoshi?: number;
  total_returned_satoshi?: number;
  wallet_fund_flow_count?: number;
};

export type WalletFundFlowItem = {
  id: number;
  created_at_unix: number;
  visit_id: string;
  visit_locator: string;
  flow_id: string;
  flow_type: string;
  ref_id: string;
  stage: string;
  direction: string;
  purpose: string;
  amount_satoshi: number;
  used_satoshi: number;
  returned_satoshi: number;
  related_txid: string;
  note: string;
  payload: unknown;
};

export type WalletFundFlowListResp = {
  total: number;
  limit: number;
  offset: number;
  items: WalletFundFlowItem[];
};
