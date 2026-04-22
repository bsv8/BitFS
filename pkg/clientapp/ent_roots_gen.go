package clientapp

import (
	"database/sql"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
)

// queryBox 只给读壳用，避免把根 client 或写入口暴露出去。
// 设计说明：
// - 每次 Query 都生成一个新的 builder，避免上一次 Where/Order 的条件污染下一次查询。
type queryBox[T any] struct {
	new func() *T
}

func newQueryBox[T any](new func() *T) queryBox[T] {
	return queryBox[T]{new: new}
}

func (b queryBox[T]) Query() *T {
	if b.new == nil {
		return nil
	}
	return b.new()
}

// entReadRoot 只开放 query builder，不开放 Tx / Create / Update / Delete。
type entReadRoot struct {
	BizSeeds                                 queryBox[gen.BizSeedsQuery]
	BizSeedPricingPolicy                     queryBox[gen.BizSeedPricingPolicyQuery]
	BizSeedChunkSupply                       queryBox[gen.BizSeedChunkSupplyQuery]
	BizPricingAutopilotConfig                queryBox[gen.BizPricingAutopilotConfigQuery]
	BizPricingAutopilotState                 queryBox[gen.BizPricingAutopilotStateQuery]
	BizPricingAutopilotAudit                 queryBox[gen.BizPricingAutopilotAuditQuery]
	BizLiveQuotes                            queryBox[gen.BizLiveQuotesQuery]
	BizDemands                               queryBox[gen.BizDemandsQuery]
	BizPurchases                             queryBox[gen.BizPurchasesQuery]
	BizDemandQuotes                          queryBox[gen.BizDemandQuotesQuery]
	BizDemandQuoteArbiters                   queryBox[gen.BizDemandQuoteArbitersQuery]
	BizWorkspaces                            queryBox[gen.BizWorkspacesQuery]
	BizWorkspaceFiles                        queryBox[gen.BizWorkspaceFilesQuery]
	BizPool                                  queryBox[gen.BizPoolQuery]
	BizPoolAllocations                       queryBox[gen.BizPoolAllocationsQuery]
	Orders                                   queryBox[gen.OrdersQuery]
	OrderSettlements                         queryBox[gen.OrderSettlementsQuery]
	OrderSettlementEvents                    queryBox[gen.OrderSettlementEventsQuery]
	ProcCommandJournal                       queryBox[gen.ProcCommandJournalQuery]
	ProcGatewayEvents                        queryBox[gen.ProcGatewayEventsQuery]
	ProcDomainEvents                         queryBox[gen.ProcDomainEventsQuery]
	ProcStateSnapshots                       queryBox[gen.ProcStateSnapshotsQuery]
	ProcObservedGatewayStates                queryBox[gen.ProcObservedGatewayStatesQuery]
	ProcEffectLogs                           queryBox[gen.ProcEffectLogsQuery]
	ProcOrchestratorLogs                     queryBox[gen.ProcOrchestratorLogsQuery]
	ProcSchedulerTasks                       queryBox[gen.ProcSchedulerTasksQuery]
	ProcSchedulerTaskRuns                    queryBox[gen.ProcSchedulerTaskRunsQuery]
	ProcChainTipWorkerLogs                   queryBox[gen.ProcChainTipWorkerLogsQuery]
	ProcChainUtxoWorkerLogs                  queryBox[gen.ProcChainUtxoWorkerLogsQuery]
	ProcChainTipState                        queryBox[gen.ProcChainTipStateQuery]
	ProcDirectDeals                          queryBox[gen.ProcDirectDealsQuery]
	ProcDirectTransferPools                  queryBox[gen.ProcDirectTransferPoolsQuery]
	ProcFileDownloads                        queryBox[gen.ProcFileDownloadsQuery]
	ProcFileDownloadChunks                   queryBox[gen.ProcFileDownloadChunksQuery]
	ProcGetFileByHashChunks                  queryBox[gen.ProcGetFileByHashChunksQuery]
	ProcGetFileByHashJobs                    queryBox[gen.ProcGetFileByHashJobsQuery]
	ProcGetFileByHashQuotes                  queryBox[gen.ProcGetFileByHashQuotesQuery]
	ProcLiveFollows                          queryBox[gen.ProcLiveFollowsQuery]
	ProcNodeReachabilityCache                queryBox[gen.ProcNodeReachabilityCacheQuery]
	ProcSelfNodeReachabilityState            queryBox[gen.ProcSelfNodeReachabilityStateQuery]
	WalletUtxo                               queryBox[gen.WalletUtxoQuery]
	WalletUtxoSyncState                      queryBox[gen.WalletUtxoSyncStateQuery]
	WalletUtxoSyncCursor                     queryBox[gen.WalletUtxoSyncCursorQuery]
	WalletUtxoTokenVerification              queryBox[gen.WalletUtxoTokenVerificationQuery]
	FactPoolSessionEvents                    queryBox[gen.FactPoolSessionEventsQuery]
	FactBsvUtxos                             queryBox[gen.FactBsvUtxosQuery]
	FactTokenLots                            queryBox[gen.FactTokenLotsQuery]
	FactTokenCarrierLinks                    queryBox[gen.FactTokenCarrierLinksQuery]
	FactSettlementRecords                    queryBox[gen.FactSettlementRecordsQuery]
	FactSettlementPaymentAttempts            queryBox[gen.FactSettlementPaymentAttemptsQuery]
	FactSettlementChannelChainAssetCreate    queryBox[gen.FactSettlementChannelChainAssetCreateQuery]
	FactSettlementChannelChainDirectPay      queryBox[gen.FactSettlementChannelChainDirectPayQuery]
	FactSettlementChannelChainQuotePay       queryBox[gen.FactSettlementChannelChainQuotePayQuery]
	FactSettlementChannelPoolSessionQuotePay queryBox[gen.FactSettlementChannelPoolSessionQuotePayQuery]
}

// EntReadRoot 是读壳的对外类型。
type EntReadRoot = *entReadRoot

// entWriteRoot 只开放 entity client，不开放根 client 和 Tx。
type entWriteRoot struct {
	BizSeeds                                 *gen.BizSeedsClient
	BizSeedPricingPolicy                     *gen.BizSeedPricingPolicyClient
	BizSeedChunkSupply                       *gen.BizSeedChunkSupplyClient
	BizPricingAutopilotConfig                *gen.BizPricingAutopilotConfigClient
	BizPricingAutopilotState                 *gen.BizPricingAutopilotStateClient
	BizPricingAutopilotAudit                 *gen.BizPricingAutopilotAuditClient
	BizLiveQuotes                            *gen.BizLiveQuotesClient
	BizDemands                               *gen.BizDemandsClient
	BizPurchases                             *gen.BizPurchasesClient
	BizDemandQuotes                          *gen.BizDemandQuotesClient
	BizDemandQuoteArbiters                   *gen.BizDemandQuoteArbitersClient
	BizWorkspaces                            *gen.BizWorkspacesClient
	BizWorkspaceFiles                        *gen.BizWorkspaceFilesClient
	BizPool                                  *gen.BizPoolClient
	BizPoolAllocations                       *gen.BizPoolAllocationsClient
	Orders                                   *gen.OrdersClient
	OrderSettlements                         *gen.OrderSettlementsClient
	OrderSettlementEvents                    *gen.OrderSettlementEventsClient
	ProcCommandJournal                       *gen.ProcCommandJournalClient
	ProcGatewayEvents                        *gen.ProcGatewayEventsClient
	ProcDomainEvents                         *gen.ProcDomainEventsClient
	ProcStateSnapshots                       *gen.ProcStateSnapshotsClient
	ProcObservedGatewayStates                *gen.ProcObservedGatewayStatesClient
	ProcEffectLogs                           *gen.ProcEffectLogsClient
	ProcOrchestratorLogs                     *gen.ProcOrchestratorLogsClient
	ProcSchedulerTasks                       *gen.ProcSchedulerTasksClient
	ProcSchedulerTaskRuns                    *gen.ProcSchedulerTaskRunsClient
	ProcChainTipWorkerLogs                   *gen.ProcChainTipWorkerLogsClient
	ProcChainUtxoWorkerLogs                  *gen.ProcChainUtxoWorkerLogsClient
	ProcChainTipState                        *gen.ProcChainTipStateClient
	ProcDirectDeals                          *gen.ProcDirectDealsClient
	ProcDirectTransferPools                  *gen.ProcDirectTransferPoolsClient
	ProcFileDownloads                        *gen.ProcFileDownloadsClient
	ProcFileDownloadChunks                   *gen.ProcFileDownloadChunksClient
	ProcGetFileByHashChunks                  *gen.ProcGetFileByHashChunksClient
	ProcGetFileByHashJobs                    *gen.ProcGetFileByHashJobsClient
	ProcGetFileByHashQuotes                  *gen.ProcGetFileByHashQuotesClient
	ProcLiveFollows                          *gen.ProcLiveFollowsClient
	ProcNodeReachabilityCache                *gen.ProcNodeReachabilityCacheClient
	ProcSelfNodeReachabilityState            *gen.ProcSelfNodeReachabilityStateClient
	WalletUtxo                               *gen.WalletUtxoClient
	WalletUtxoSyncState                      *gen.WalletUtxoSyncStateClient
	WalletUtxoSyncCursor                     *gen.WalletUtxoSyncCursorClient
	WalletUtxoTokenVerification              *gen.WalletUtxoTokenVerificationClient
	FactPoolSessionEvents                    *gen.FactPoolSessionEventsClient
	FactBsvUtxos                             *gen.FactBsvUtxosClient
	FactTokenLots                            *gen.FactTokenLotsClient
	FactTokenCarrierLinks                    *gen.FactTokenCarrierLinksClient
	FactSettlementRecords                    *gen.FactSettlementRecordsClient
	FactSettlementPaymentAttempts            *gen.FactSettlementPaymentAttemptsClient
	FactSettlementChannelChainAssetCreate    *gen.FactSettlementChannelChainAssetCreateClient
	FactSettlementChannelChainDirectPay      *gen.FactSettlementChannelChainDirectPayClient
	FactSettlementChannelChainQuotePay       *gen.FactSettlementChannelChainQuotePayClient
	FactSettlementChannelPoolSessionQuotePay *gen.FactSettlementChannelPoolSessionQuotePayClient
}

// EntWriteRoot 是写壳的对外类型。
type EntWriteRoot = *entWriteRoot

func newEntReadRoot(client *gen.Client) EntReadRoot {
	return &entReadRoot{
		BizSeeds:                                 newQueryBox(client.BizSeeds.Query),
		BizSeedPricingPolicy:                     newQueryBox(client.BizSeedPricingPolicy.Query),
		BizSeedChunkSupply:                       newQueryBox(client.BizSeedChunkSupply.Query),
		BizPricingAutopilotConfig:                newQueryBox(client.BizPricingAutopilotConfig.Query),
		BizPricingAutopilotState:                 newQueryBox(client.BizPricingAutopilotState.Query),
		BizPricingAutopilotAudit:                 newQueryBox(client.BizPricingAutopilotAudit.Query),
		BizLiveQuotes:                            newQueryBox(client.BizLiveQuotes.Query),
		BizDemands:                               newQueryBox(client.BizDemands.Query),
		BizPurchases:                             newQueryBox(client.BizPurchases.Query),
		BizDemandQuotes:                          newQueryBox(client.BizDemandQuotes.Query),
		BizDemandQuoteArbiters:                   newQueryBox(client.BizDemandQuoteArbiters.Query),
		BizWorkspaces:                            newQueryBox(client.BizWorkspaces.Query),
		BizWorkspaceFiles:                        newQueryBox(client.BizWorkspaceFiles.Query),
		BizPool:                                  newQueryBox(client.BizPool.Query),
		BizPoolAllocations:                       newQueryBox(client.BizPoolAllocations.Query),
		Orders:                                   newQueryBox(client.Orders.Query),
		OrderSettlements:                         newQueryBox(client.OrderSettlements.Query),
		OrderSettlementEvents:                    newQueryBox(client.OrderSettlementEvents.Query),
		ProcCommandJournal:                       newQueryBox(client.ProcCommandJournal.Query),
		ProcGatewayEvents:                        newQueryBox(client.ProcGatewayEvents.Query),
		ProcDomainEvents:                         newQueryBox(client.ProcDomainEvents.Query),
		ProcStateSnapshots:                       newQueryBox(client.ProcStateSnapshots.Query),
		ProcObservedGatewayStates:                newQueryBox(client.ProcObservedGatewayStates.Query),
		ProcEffectLogs:                           newQueryBox(client.ProcEffectLogs.Query),
		ProcOrchestratorLogs:                     newQueryBox(client.ProcOrchestratorLogs.Query),
		ProcSchedulerTasks:                       newQueryBox(client.ProcSchedulerTasks.Query),
		ProcSchedulerTaskRuns:                    newQueryBox(client.ProcSchedulerTaskRuns.Query),
		ProcChainTipWorkerLogs:                   newQueryBox(client.ProcChainTipWorkerLogs.Query),
		ProcChainUtxoWorkerLogs:                  newQueryBox(client.ProcChainUtxoWorkerLogs.Query),
		ProcChainTipState:                        newQueryBox(client.ProcChainTipState.Query),
		ProcDirectDeals:                          newQueryBox(client.ProcDirectDeals.Query),
		ProcDirectTransferPools:                  newQueryBox(client.ProcDirectTransferPools.Query),
		ProcFileDownloads:                        newQueryBox(client.ProcFileDownloads.Query),
		ProcFileDownloadChunks:                   newQueryBox(client.ProcFileDownloadChunks.Query),
		ProcGetFileByHashChunks:                  newQueryBox(client.ProcGetFileByHashChunks.Query),
		ProcGetFileByHashJobs:                    newQueryBox(client.ProcGetFileByHashJobs.Query),
		ProcGetFileByHashQuotes:                  newQueryBox(client.ProcGetFileByHashQuotes.Query),
		ProcLiveFollows:                          newQueryBox(client.ProcLiveFollows.Query),
		ProcNodeReachabilityCache:                newQueryBox(client.ProcNodeReachabilityCache.Query),
		ProcSelfNodeReachabilityState:            newQueryBox(client.ProcSelfNodeReachabilityState.Query),
		WalletUtxo:                               newQueryBox(client.WalletUtxo.Query),
		WalletUtxoSyncState:                      newQueryBox(client.WalletUtxoSyncState.Query),
		WalletUtxoSyncCursor:                     newQueryBox(client.WalletUtxoSyncCursor.Query),
		WalletUtxoTokenVerification:              newQueryBox(client.WalletUtxoTokenVerification.Query),
		FactPoolSessionEvents:                    newQueryBox(client.FactPoolSessionEvents.Query),
		FactBsvUtxos:                             newQueryBox(client.FactBsvUtxos.Query),
		FactTokenLots:                            newQueryBox(client.FactTokenLots.Query),
		FactTokenCarrierLinks:                    newQueryBox(client.FactTokenCarrierLinks.Query),
		FactSettlementRecords:                    newQueryBox(client.FactSettlementRecords.Query),
		FactSettlementPaymentAttempts:            newQueryBox(client.FactSettlementPaymentAttempts.Query),
		FactSettlementChannelChainAssetCreate:    newQueryBox(client.FactSettlementChannelChainAssetCreate.Query),
		FactSettlementChannelChainDirectPay:      newQueryBox(client.FactSettlementChannelChainDirectPay.Query),
		FactSettlementChannelChainQuotePay:       newQueryBox(client.FactSettlementChannelChainQuotePay.Query),
		FactSettlementChannelPoolSessionQuotePay: newQueryBox(client.FactSettlementChannelPoolSessionQuotePay.Query),
	}
}

func newEntClientFromTx(tx *sql.Tx) *gen.Client {
	if tx == nil {
		return nil
	}
	client := gen.NewClient(gen.Driver(&boundEntTxDriver{
		base: entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx}),
	}))
	return client
}

func newEntWriteRoot(tx *sql.Tx) EntWriteRoot {
	client := newEntClientFromTx(tx)
	if client == nil {
		return nil
	}
	return &entWriteRoot{
		BizSeeds:                                 client.BizSeeds,
		BizSeedPricingPolicy:                     client.BizSeedPricingPolicy,
		BizSeedChunkSupply:                       client.BizSeedChunkSupply,
		BizPricingAutopilotConfig:                client.BizPricingAutopilotConfig,
		BizPricingAutopilotState:                 client.BizPricingAutopilotState,
		BizPricingAutopilotAudit:                 client.BizPricingAutopilotAudit,
		BizLiveQuotes:                            client.BizLiveQuotes,
		BizDemands:                               client.BizDemands,
		BizPurchases:                             client.BizPurchases,
		BizDemandQuotes:                          client.BizDemandQuotes,
		BizDemandQuoteArbiters:                   client.BizDemandQuoteArbiters,
		BizWorkspaces:                            client.BizWorkspaces,
		BizWorkspaceFiles:                        client.BizWorkspaceFiles,
		BizPool:                                  client.BizPool,
		BizPoolAllocations:                       client.BizPoolAllocations,
		Orders:                                   client.Orders,
		OrderSettlements:                         client.OrderSettlements,
		OrderSettlementEvents:                    client.OrderSettlementEvents,
		ProcCommandJournal:                       client.ProcCommandJournal,
		ProcGatewayEvents:                        client.ProcGatewayEvents,
		ProcDomainEvents:                         client.ProcDomainEvents,
		ProcStateSnapshots:                       client.ProcStateSnapshots,
		ProcObservedGatewayStates:                client.ProcObservedGatewayStates,
		ProcEffectLogs:                           client.ProcEffectLogs,
		ProcOrchestratorLogs:                     client.ProcOrchestratorLogs,
		ProcSchedulerTasks:                       client.ProcSchedulerTasks,
		ProcSchedulerTaskRuns:                    client.ProcSchedulerTaskRuns,
		ProcChainTipWorkerLogs:                   client.ProcChainTipWorkerLogs,
		ProcChainUtxoWorkerLogs:                  client.ProcChainUtxoWorkerLogs,
		ProcChainTipState:                        client.ProcChainTipState,
		ProcDirectDeals:                          client.ProcDirectDeals,
		ProcDirectTransferPools:                  client.ProcDirectTransferPools,
		ProcFileDownloads:                        client.ProcFileDownloads,
		ProcFileDownloadChunks:                   client.ProcFileDownloadChunks,
		ProcGetFileByHashChunks:                  client.ProcGetFileByHashChunks,
		ProcGetFileByHashJobs:                    client.ProcGetFileByHashJobs,
		ProcGetFileByHashQuotes:                  client.ProcGetFileByHashQuotes,
		ProcLiveFollows:                          client.ProcLiveFollows,
		ProcNodeReachabilityCache:                client.ProcNodeReachabilityCache,
		ProcSelfNodeReachabilityState:            client.ProcSelfNodeReachabilityState,
		WalletUtxo:                               client.WalletUtxo,
		WalletUtxoSyncState:                      client.WalletUtxoSyncState,
		WalletUtxoSyncCursor:                     client.WalletUtxoSyncCursor,
		WalletUtxoTokenVerification:              client.WalletUtxoTokenVerification,
		FactPoolSessionEvents:                    client.FactPoolSessionEvents,
		FactBsvUtxos:                             client.FactBsvUtxos,
		FactTokenLots:                            client.FactTokenLots,
		FactTokenCarrierLinks:                    client.FactTokenCarrierLinks,
		FactSettlementRecords:                    client.FactSettlementRecords,
		FactSettlementPaymentAttempts:            client.FactSettlementPaymentAttempts,
		FactSettlementChannelChainAssetCreate:    client.FactSettlementChannelChainAssetCreate,
		FactSettlementChannelChainDirectPay:      client.FactSettlementChannelChainDirectPay,
		FactSettlementChannelChainQuotePay:       client.FactSettlementChannelChainQuotePay,
		FactSettlementChannelPoolSessionQuotePay: client.FactSettlementChannelPoolSessionQuotePay,
	}
}

// 设计说明：
// - 这组访问器只给模块箱子里的 DB 入口用；
// - 它们只是把现有 root 的壳能力显式抛出去，不新增新边界。
func (r *entReadRoot) BizSeedsQuery() *gen.BizSeedsQuery { return r.BizSeeds.Query() }
func (r *entReadRoot) BizSeedPricingPolicyQuery() *gen.BizSeedPricingPolicyQuery {
	return r.BizSeedPricingPolicy.Query()
}
func (r *entReadRoot) BizSeedChunkSupplyQuery() *gen.BizSeedChunkSupplyQuery {
	return r.BizSeedChunkSupply.Query()
}
func (r *entReadRoot) BizWorkspacesQuery() *gen.BizWorkspacesQuery { return r.BizWorkspaces.Query() }
func (r *entReadRoot) BizWorkspaceFilesQuery() *gen.BizWorkspaceFilesQuery {
	return r.BizWorkspaceFiles.Query()
}
func (r *entReadRoot) ProcFileDownloadsQuery() *gen.ProcFileDownloadsQuery {
	return r.ProcFileDownloads.Query()
}
func (r *entReadRoot) ProcFileDownloadChunksQuery() *gen.ProcFileDownloadChunksQuery {
	return r.ProcFileDownloadChunks.Query()
}

func (r *entWriteRoot) BizSeedsClient() *gen.BizSeedsClient { return r.BizSeeds }
func (r *entWriteRoot) BizSeedPricingPolicyClient() *gen.BizSeedPricingPolicyClient {
	return r.BizSeedPricingPolicy
}
func (r *entWriteRoot) BizSeedChunkSupplyClient() *gen.BizSeedChunkSupplyClient {
	return r.BizSeedChunkSupply
}
func (r *entWriteRoot) BizWorkspacesClient() *gen.BizWorkspacesClient { return r.BizWorkspaces }
func (r *entWriteRoot) BizWorkspaceFilesClient() *gen.BizWorkspaceFilesClient {
	return r.BizWorkspaceFiles
}
func (r *entWriteRoot) ProcFileDownloadsClient() *gen.ProcFileDownloadsClient {
	return r.ProcFileDownloads
}
func (r *entWriteRoot) ProcFileDownloadChunksClient() *gen.ProcFileDownloadChunksClient {
	return r.ProcFileDownloadChunks
}
