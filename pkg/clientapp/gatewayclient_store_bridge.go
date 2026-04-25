package clientapp

import (
	"fmt"

	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient"
)

// gatewayClientStoreFromDB 把已经准备好的主干 store 变成 gatewayclient 的窄能力。
//
// 设计说明：
// - 这里是唯一的转换口，root 侧不再散落地直接拼模块 store；
// - 上层只拿模块能力，不拿原始 DB，也不直接摸模块表；
// - 这个函数只做能力转换，不负责业务判断。
func gatewayClientStoreFromDB(store *clientDB) (gatewayclient.Store, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	gwAny := gatewayclient.NewGatewayClientStore(store)
	gw, ok := gwAny.(gatewayclient.Store)
	if !ok || gw == nil {
		return nil, fmt.Errorf("gatewayclient store not available")
	}
	return gw, nil
}
