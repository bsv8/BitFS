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

type BitfsDomainResolveResponse = {
  success: boolean;
  status: string;
  name?: string;
  target_pubkey_hex?: string;
  expire_at_unix?: number;
  error?: string;
  message?: string;
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
