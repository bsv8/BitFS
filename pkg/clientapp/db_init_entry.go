package clientapp

// ensureClientDBSchema 是客户端数据库结构就绪的唯一入口。
// 设计说明：
// - 这里只负责调度分层，不承载手写 DDL；
// - 统一交给 contract 的 ent schema 产物做建表和建约束。

// ensureClientDBSchemaOnDB 从原始 sql.DB 初始化数据库结构。
// 设计说明：
// - 用于测试场景，测试代码直接持有 *sql.DB 而无 actor；
// - 生产代码应使用 ensureClientDBSchema(store *clientDB)；
// - 这里不再执行老的手写建表和补丁逻辑，只接受 contract 的 schema 真源。
