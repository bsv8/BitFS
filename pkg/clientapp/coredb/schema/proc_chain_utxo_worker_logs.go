package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ProcChainUtxoWorkerLogs 对齐表 proc_chain_utxo_worker_logs。
type ProcChainUtxoWorkerLogs struct {
	ent.Schema
}

func (ProcChainUtxoWorkerLogs) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Annotation{Table: "proc_chain_utxo_worker_logs"}}
}

func (ProcChainUtxoWorkerLogs) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("triggered_at_unix"),
		field.Int64("started_at_unix"),
		field.Int64("ended_at_unix"),
		field.Int64("duration_ms"),
		field.String("trigger_source"),
		field.String("status"),
		field.String("error_message"),
		field.String("result_json"),
	}
}

func (ProcChainUtxoWorkerLogs) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("started_at_unix", "id"),
		index.Fields("status", "id"),
	}
}
