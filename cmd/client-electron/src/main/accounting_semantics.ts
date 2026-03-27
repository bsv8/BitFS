export type ShellVisitRollupBucket = "resolver" | "reachability" | "content" | "other";

type ShellAccountingSemantic = {
  key: string;
  label: string;
  visitBucket: ShellVisitRollupBucket;
  usedInVisitBucket: boolean;
};

// 设计说明：
// - 这里只收口壳层真正要消费的账务语义，不去反向定义业务协议；
// - key 必须保持真实 wire 值，壳层只负责“如何给人看”和“如何分桶”；
// - domain_query_fee / domain_resolve_fee 是当前真实域名链路扣费名，旧的 resolver_query_fee 不再保留。
const shellAccountingSemantics: ShellAccountingSemantic[] = [
  { key: "domain_query_fee", label: "域名查询费", visitBucket: "resolver", usedInVisitBucket: true },
  { key: "domain_resolve_fee", label: "域名解析费", visitBucket: "resolver", usedInVisitBucket: true },
  { key: "node_reachability_announce_fee", label: "地址广播费", visitBucket: "reachability", usedInVisitBucket: true },
  { key: "node_reachability_query_fee", label: "地址查询费", visitBucket: "reachability", usedInVisitBucket: true },
  { key: "direct_transfer_pool_open", label: "内容池开启", visitBucket: "content", usedInVisitBucket: true },
  { key: "prepare_exact_pool_amount", label: "内容池补足", visitBucket: "content", usedInVisitBucket: true },
  { key: "direct_transfer_chunk_pay", label: "内容传输费", visitBucket: "content", usedInVisitBucket: true },
  { key: "direct_transfer_pool_close", label: "内容池结算", visitBucket: "content", usedInVisitBucket: true },
  { key: "listen_cycle_fee", label: "监听周期费", visitBucket: "other", usedInVisitBucket: false },
  { key: "fee_pool_open", label: "费用池开启", visitBucket: "other", usedInVisitBucket: false },
  { key: "fee_pool_close", label: "费用池关闭", visitBucket: "other", usedInVisitBucket: false },
  { key: "fee_pool_suspicious_expiry_settle", label: "费用池异常结算", visitBucket: "other", usedInVisitBucket: false },
  { key: "peer_call_fee", label: "节点调用费", visitBucket: "other", usedInVisitBucket: false },
  { key: "domain_register_total_payment", label: "域名注册总支付", visitBucket: "other", usedInVisitBucket: false },
  { key: "demand_publish_fee", label: "需求发布费", visitBucket: "other", usedInVisitBucket: false },
  { key: "demand_publish_batch_fee", label: "批量需求发布费", visitBucket: "other", usedInVisitBucket: false },
  { key: "live_demand_publish_fee", label: "直播需求发布费", visitBucket: "other", usedInVisitBucket: false }
];

const shellAccountingSemanticMap = new Map(shellAccountingSemantics.map((item) => [item.key, item]));

export function shellVisitBucketForPurpose(purpose: string): ShellVisitRollupBucket {
  const normalized = String(purpose || "").trim();
  if (normalized === "") {
    return "other";
  }
  return shellAccountingSemanticMap.get(normalized)?.visitBucket || "other";
}

export function shellAccountingLabelForPurpose(purpose: string): string {
  const normalized = String(purpose || "").trim();
  if (normalized === "") {
    return "其他";
  }
  return shellAccountingSemanticMap.get(normalized)?.label || normalized;
}

export function shellPurposeUsedInVisitBucket(purpose: string): boolean {
  const normalized = String(purpose || "").trim();
  if (normalized === "") {
    return false;
  }
  return shellAccountingSemanticMap.get(normalized)?.usedInVisitBucket === true;
}
