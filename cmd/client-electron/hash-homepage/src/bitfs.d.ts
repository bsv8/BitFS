type BitfsWalletHistoryDirection = "in" | "out" | "unknown";

type BitfsClientInfo = {
  trusted_protocol: "bitfs://";
  pubkey_hex: string;
  started_at_unix: number;
  seller_enabled: boolean;
};

type BitfsClientStatus = {
  trusted_protocol: "bitfs://";
  current_url: string;
  current_root_seed_hash: string;
  wallet_ready: boolean;
  wallet_unlocked: boolean;
};

type BitfsWalletAddress = {
  address: string;
  encoding: "base58";
  purpose: "default_receive";
  pubkey_hex: string;
};

type BitfsWalletSummary = {
  trusted_protocol: "bitfs://";
  pubkey_hex: string;
  wallet_address: string;
  addresses: BitfsWalletAddress[];
  balance_satoshi: number;
};

type BitfsWalletHistoryItem = {
  id: number;
  txid: string;
  direction: BitfsWalletHistoryDirection;
  amount_satoshi: number;
  status: string;
  block_height: number;
  occurred_at_unix: number;
};

type BitfsWalletHistoryList = {
  total: number;
  limit: number;
  offset: number;
  items: BitfsWalletHistoryItem[];
};

type BitfsWalletHistoryQuery = {
  limit?: number;
  offset?: number;
  direction?: BitfsWalletHistoryDirection;
};

type BitfsWalletTokenStandard = "bsv20" | "bsv21";

type BitfsWalletTokenBalanceItem = {
  token_standard: BitfsWalletTokenStandard;
  asset_key: string;
  asset_symbol: string;
  quantity_text: string;
  output_count: number;
  source_name: string;
  updated_at_unix: number;
};

type BitfsWalletTokenBalanceList = {
  wallet_address: string;
  total: number;
  limit: number;
  offset: number;
  items: BitfsWalletTokenBalanceItem[];
};

type BitfsWalletTokenOutputItem = {
  utxo_id: string;
  wallet_address: string;
  txid: string;
  vout: number;
  value_satoshi: number;
  allocation_class: string;
  allocation_reason: string;
  token_standard: BitfsWalletTokenStandard;
  asset_key: string;
  asset_symbol: string;
  quantity_text: string;
  source_name: string;
  updated_at_unix: number;
  payload: unknown;
};

type BitfsWalletTokenOutputList = {
  wallet_address: string;
  total: number;
  limit: number;
  offset: number;
  items: BitfsWalletTokenOutputItem[];
};

type BitfsWalletAssetEventItem = {
  id: number;
  created_at_unix: number;
  utxo_id: string;
  wallet_address: string;
  asset_group: string;
  asset_standard: string;
  asset_key: string;
  asset_symbol: string;
  quantity_text: string;
  source_name: string;
  event_type: string;
  ref_txid: string;
  ref_business_id: string;
  note: string;
  payload: unknown;
};

type BitfsWalletAssetEventList = {
  wallet_address: string;
  total: number;
  limit: number;
  offset: number;
  items: BitfsWalletAssetEventItem[];
};

type BitfsWalletAssetPreviewChange = {
  owner_scope: string;
  asset_group: string;
  asset_standard: string;
  asset_key: string;
  asset_symbol: string;
  quantity_text: string;
  direction: string;
  note: string;
};

type BitfsWalletAssetPreview = {
  action: string;
  feasible: boolean;
  can_sign: boolean;
  summary: string;
  detail_lines: string[];
  warning_level: string;
  estimated_network_fee_bsv_sat: number;
  fee_funding_target_bsv_sat: number;
  selected_asset_utxo_ids: string[];
  selected_fee_utxo_ids: string[];
  txid: string;
  preview_hash: string;
  changes: BitfsWalletAssetPreviewChange[];
};

type BitfsWalletAssetPreviewResponse = {
  ok: boolean;
  code: string;
  message: string;
  preview: BitfsWalletAssetPreview;
};

type BitfsWalletAssetSignResponse = {
  ok: boolean;
  code: string;
  message: string;
  preview: BitfsWalletAssetPreview;
  signed_tx_hex: string;
  txid: string;
};

type BitfsWalletAssetSubmitResponse = {
  ok: boolean;
  code: string;
  message: string;
  txid: string;
};

type BitfsWalletTokenBalanceQuery = {
  limit?: number;
  offset?: number;
  standard?: BitfsWalletTokenStandard;
};

type BitfsWalletTokenOutputQuery = {
  limit?: number;
  offset?: number;
  standard?: BitfsWalletTokenStandard;
  assetKey?: string;
  asset_key?: string;
};

type BitfsWalletAssetEventQuery = {
  limit?: number;
  offset?: number;
  standard?: BitfsWalletTokenStandard;
  utxoID?: string;
  utxo_id?: string;
  assetKey?: string;
  asset_key?: string;
};

type BitfsWalletTokenSendPreviewRequest = {
  tokenStandard?: BitfsWalletTokenStandard;
  token_standard?: BitfsWalletTokenStandard;
  assetKey?: string;
  asset_key?: string;
  amountText?: string;
  amount_text?: string;
  toAddress?: string;
  to_address?: string;
};

type BitfsWalletTokenSendSignRequest = BitfsWalletTokenSendPreviewRequest & {
  expectedPreviewHash?: string;
  expected_preview_hash?: string;
};

type BitfsWalletTokenCreatePreviewRequest = {
  tokenStandard?: BitfsWalletTokenStandard;
  token_standard?: BitfsWalletTokenStandard;
  symbol?: string;
  maxSupply?: string;
  max_supply?: string;
  decimals?: number;
  icon?: string;
};

type BitfsWalletTokenCreateSignRequest = BitfsWalletTokenCreatePreviewRequest & {
  expectedPreviewHash?: string;
  expected_preview_hash?: string;
};

type BitfsWalletOrdinalItem = {
  utxo_id: string;
  wallet_address: string;
  txid: string;
  vout: number;
  value_satoshi: number;
  allocation_class: string;
  allocation_reason: string;
  asset_standard: "ordinal";
  asset_key: string;
  asset_symbol: string;
  source_name: string;
  updated_at_unix: number;
  payload: unknown;
};

type BitfsWalletOrdinalList = {
  wallet_address: string;
  total: number;
  limit: number;
  offset: number;
  items: BitfsWalletOrdinalItem[];
};

type BitfsWalletOrdinalQuery = {
  limit?: number;
  offset?: number;
};

type BitfsWalletOrdinalDetailQuery = {
  utxoID?: string;
  utxo_id?: string;
  assetKey?: string;
  asset_key?: string;
};

type BitfsWalletOrdinalTransferPreviewRequest = {
  utxoID?: string;
  utxo_id?: string;
  assetKey?: string;
  asset_key?: string;
  toAddress?: string;
  to_address?: string;
};

type BitfsWalletOrdinalTransferSignRequest = BitfsWalletOrdinalTransferPreviewRequest & {
  expectedPreviewHash?: string;
  expected_preview_hash?: string;
};

type BitfsWalletAssetSubmitRequest = {
  signedTxHex?: string;
  signed_tx_hex?: string;
};

type BitfsWalletBusinessRequest = {
  signerPubkeyHex?: string;
  signer_pubkey_hex?: string;
  signedEnvelope?: unknown;
  signed_envelope?: unknown;
};

type BitfsWalletBusinessPreview = {
  recognized: boolean;
  can_sign: boolean;
  template_id?: string;
  business_title?: string;
  summary: string;
  detail_lines?: string[];
  warning_level?: string;
  service_pubkey_hex?: string;
  pay_to_address?: string;
  business_amount_satoshi?: number;
  miner_fee_satoshi?: number;
  total_spend_satoshi?: number;
  change_amount_satoshi?: number;
  txid?: string;
  preview_hash?: string;
};

type BitfsWalletBusinessSignResponse = {
  ok?: boolean;
  code?: string;
  message?: string;
  preview?: BitfsWalletBusinessPreview | null;
  signed_tx_hex?: string;
  txid?: string;
};

type BitfsRuntimeEvent = {
  seq: number;
  runtime_epoch: string;
  topic: string;
  scope: "private" | "public";
  occurred_at_unix: number;
  producer: string;
  trace_id: string;
  payload: Record<string, unknown>;
};

type BitfsLocatorResolveResult = {
  kind: "bitfs" | "node" | "resolver";
  locator: string;
  route: string;
  seedHash: string;
  viewerURL: string;
  nodePubkeyHex: string;
  resolverPubkeyHex: string;
  name: string;
};

type BitfsPeerCallRequest = {
  to: string;
  route: string;
  contentType?: string;
  content_type?: string;
  body?: unknown;
};

type BitfsPeerQuoteDecision = {
  to: string;
  route: string;
  payment_scheme: string;
  payment_domain: string;
  quote_status: string;
  amount_satoshi: number;
  quantity: number;
  quantity_unit: string;
  quote: unknown;
  quoted_response: BitfsPeerCallResponse;
};

type BitfsPeerCallOptions = {
  onQuote?: (quote: BitfsPeerQuoteDecision) => boolean | Promise<boolean>;
};

type BitfsPeerCallResponse = {
  ok?: boolean;
  code?: string;
  message?: string;
  content_type?: string;
  body_json?: unknown;
  body_base64?: string;
  payment_options?: unknown[];
  payment_receipt_scheme?: string;
  payment_receipt?: unknown;
  payment_quote_scheme?: string;
  payment_quote?: unknown;
  payment_quote_base64?: string;
} & Record<string, unknown>;

type BitfsCapabilityItem = {
  id: string;
  version: number;
};

type BitfsCapabilitiesShowBody = {
  node_pubkey_hex: string;
  capabilities: BitfsCapabilityItem[];
};

type BitfsOwnedDomainItem = {
  name: string;
  owner_pubkey_hex: string;
  target_pubkey_hex: string;
  expire_at_unix: number;
  register_txid?: string;
  updated_at_unix?: number;
};

type BitfsDomainListOwnedResponse = {
  success: boolean;
  status: string;
  owner_pubkey_hex: string;
  items: BitfsOwnedDomainItem[];
  total: number;
  queried_at_unix?: number;
  error_message?: string;
};

type BitfsDomainPricing = {
  resolve_fee_satoshi?: number;
  query_fee_satoshi?: number;
  register_lock_fee_satoshi?: number;
  register_submit_fee_satoshi?: number;
  set_target_fee_satoshi?: number;
  register_price_satoshi?: number;
};

type BitfsDomainQueryResponse = {
  success: boolean;
  status: string;
  name: string;
  available: boolean;
  locked: boolean;
  registered: boolean;
  owner_pubkey_hex?: string;
  target_pubkey_hex?: string;
  expire_at_unix?: number;
  lock_expires_at_unix?: number;
  register_price_satoshi?: number;
  register_submit_fee_satoshi?: number;
  register_lock_fee_satoshi?: number;
  set_target_fee_satoshi?: number;
  resolve_fee_satoshi?: number;
  query_fee_satoshi?: number;
  charged_amount_satoshi?: number;
  updated_txid?: string;
  error_message?: string;
};

type BitfsDomainLockResponse = {
  success: boolean;
  status: string;
  name?: string;
  target_pubkey_hex?: string;
  lock_expires_at_unix?: number;
  charged_amount_satoshi?: number;
  updated_txid?: string;
  signed_quote_json?: unknown;
  error_message?: string;
};

type BitfsDomainRegisterSubmitResponse = {
  success: boolean;
  status: string;
  name?: string;
  owner_pubkey_hex?: string;
  target_pubkey_hex?: string;
  expire_at_unix?: number;
  register_txid?: string;
  signed_receipt_json?: unknown;
  error_message?: string;
};

type BitfsBridge = {
  trustedProtocol: "bitfs://";
  navigation: {
    open: (raw: string) => void;
    reload: () => void;
  };
  locator: {
    resolve: (locator: string) => Promise<BitfsLocatorResolveResult>;
  };
  peer: {
    call: (request: BitfsPeerCallRequest, options?: BitfsPeerCallOptions) => Promise<BitfsPeerCallResponse>;
  };
  client: {
    info: () => Promise<BitfsClientInfo>;
    getStatus: () => Promise<BitfsClientStatus>;
  };
  wallet: {
    summary: () => Promise<BitfsWalletSummary>;
    addresses: () => Promise<BitfsWalletAddress[]>;
    signBusinessRequest: (request: BitfsWalletBusinessRequest) => Promise<BitfsWalletBusinessSignResponse>;
    history: {
      list: (query?: BitfsWalletHistoryQuery) => Promise<BitfsWalletHistoryList>;
    };
    tokens: {
      balances: {
        list: (query?: BitfsWalletTokenBalanceQuery) => Promise<BitfsWalletTokenBalanceList>;
      };
      outputs: {
        list: (query?: BitfsWalletTokenOutputQuery) => Promise<BitfsWalletTokenOutputList>;
        get: (query: { standard?: BitfsWalletTokenStandard; utxoID?: string; utxo_id?: string; assetKey?: string; asset_key?: string }) => Promise<BitfsWalletTokenOutputItem>;
        events: {
          list: (query?: BitfsWalletAssetEventQuery) => Promise<BitfsWalletAssetEventList>;
        };
      };
      send: {
        preview: (input: BitfsWalletTokenSendPreviewRequest) => Promise<BitfsWalletAssetPreviewResponse>;
        sign: (input: BitfsWalletTokenSendSignRequest) => Promise<BitfsWalletAssetSignResponse>;
        submit: (input: BitfsWalletAssetSubmitRequest) => Promise<BitfsWalletAssetSubmitResponse>;
      };
      create: {
        preview: (input: BitfsWalletTokenCreatePreviewRequest) => Promise<BitfsWalletAssetPreviewResponse>;
        sign: (input: BitfsWalletTokenCreateSignRequest) => Promise<BitfsWalletAssetSignResponse>;
        submit: (input: BitfsWalletAssetSubmitRequest) => Promise<BitfsWalletAssetSubmitResponse>;
      };
    };
    ordinals: {
      list: (query?: BitfsWalletOrdinalQuery) => Promise<BitfsWalletOrdinalList>;
      get: (query: BitfsWalletOrdinalDetailQuery) => Promise<BitfsWalletOrdinalItem>;
      events: {
        list: (query?: Omit<BitfsWalletAssetEventQuery, "standard">) => Promise<BitfsWalletAssetEventList>;
      };
      transfer: {
        preview: (input: BitfsWalletOrdinalTransferPreviewRequest) => Promise<BitfsWalletAssetPreviewResponse>;
        sign: (input: BitfsWalletOrdinalTransferSignRequest) => Promise<BitfsWalletAssetSignResponse>;
        submit: (input: BitfsWalletAssetSubmitRequest) => Promise<BitfsWalletAssetSubmitResponse>;
      };
    };
  };
  events: {
    subscribe: (
      topics: string | string[],
      listener: (event: BitfsRuntimeEvent) => void
    ) => () => void;
  };
};

declare global {
  interface Window {
    bitfs?: BitfsBridge;
  }
}

export {};
