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

type BitfsBridge = {
  trustedProtocol: "bitfs://";
  navigation: {
    open: (raw: string) => void;
    reload: () => void;
  };
  client: {
    info: () => Promise<BitfsClientInfo>;
    getStatus: () => Promise<BitfsClientStatus>;
  };
  wallet: {
    summary: () => Promise<BitfsWalletSummary>;
    addresses: () => Promise<BitfsWalletAddress[]>;
    history: {
      list: (query?: BitfsWalletHistoryQuery) => Promise<BitfsWalletHistoryList>;
    };
  };
};

declare global {
  interface Window {
    bitfs?: BitfsBridge;
  }
}

export {};
