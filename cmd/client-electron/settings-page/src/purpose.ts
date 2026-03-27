export const purposeLabels: Record<string, string> = {
  node_reachability_announce_fee: "地址广播费",
  node_reachability_query_fee: "地址查询费",
  resolver_query_fee: "解析费",
  listen_cycle_fee: "监听周期费",
  fee_pool_open: "费用池开启",
  fee_pool_close: "费用池关闭",
  demand_publish_fee: "需求发布费",
  demand_publish_batch_fee: "批量需求发布费",
  live_demand_publish_fee: "直播需求发布费",
  direct_transfer_pool_open: "内容池开启",
  prepare_exact_pool_amount: "内容池补足",
  direct_transfer_chunk_pay: "内容传输费",
  direct_transfer_pool_close: "内容池结算"
};

export const defaultPurposePlaceholder = "node_reachability_query_fee";

export function formatPurposeLabel(purpose: string): string {
  const normalized = String(purpose || "").trim();
  return purposeLabels[normalized] || normalized || "-";
}
