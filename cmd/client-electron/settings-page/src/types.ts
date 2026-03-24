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
};
