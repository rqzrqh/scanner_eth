package mocknode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"net/http"
	"os/signal"
	"scanner_eth/fetch/node"
	"scanner_eth/filter"
	"scanner_eth/middleware"
	"scanner_eth/model"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gorm.io/gorm"
)

const (
	emptyHash      = "0x0000000000000000000000000000000000000000000000000000000000000000"
	emptyAddress   = "0x0000000000000000000000000000000000000000"
	emptyUncleHash = "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
)

var emptyLogsBloom = "0x" + strings.Repeat("00", 256)

type Config struct {
	AppName  string              `mapstructure:"app_name"`
	BaseAddr string              `mapstructure:"base_addr"`
	BasePort int                 `mapstructure:"base_port"`
	Nodes    []NodeConfig        `mapstructure:"nodes"`
	Database middleware.Database `mapstructure:"database"`
}

type NodeConfig struct {
	Latency       time.Duration `mapstructure:"latency"`
	LatencyJitter time.Duration `mapstructure:"latency_jitter"`
	DropRate      float64       `mapstructure:"drop_rate"`
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      json.RawMessage `json:"id"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type server struct {
	id          int
	chainID     int64
	genesisHash string
	db          *gorm.DB
	nodeConfig  NodeConfig
	randomMu    sync.Mutex
	random      *rand.Rand
}

type blockJSON struct {
	node.BlockHeaderJson
	LogsBloom string `json:"logsBloom"`
	MixHash   string `json:"mixHash"`
}

func RunNode(confPath string, nodeID int, addr string) error {
	conf, err := LoadConfig(confPath)
	if err != nil {
		return fmt.Errorf("load config failed: %w", err)
	}
	filter.InitBaseFilter()

	db, err := middleware.InitDB(conf.Database)
	if err != nil {
		return fmt.Errorf("connect database failed: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("get database handle failed: %w", err)
	}
	defer sqlDB.Close()

	scannerInfo, err := loadScannerInfo(context.Background(), db)
	if err != nil {
		return fmt.Errorf("load scanner info failed: %w", err)
	}
	if strings.TrimSpace(addr) == "" {
		addr = NodeAddr(conf, nodeID)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
	defer stop()

	nodeConfig := conf.NodeConfig(nodeID)
	handler := &server{
		id:          nodeID,
		chainID:     scannerInfo.ChainId,
		genesisHash: scannerInfo.GenesisBlockHash,
		db:          db,
		nodeConfig:  nodeConfig,
		random:      rand.New(rand.NewSource(time.Now().UnixNano() + int64(nodeID))),
	}
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		logrus.Infof("mock node started. id:%d chain_id:%d genesis_hash:%s url:http://%s latency:%s latency_jitter:%s drop_rate:%v",
			nodeID, scannerInfo.ChainId, scannerInfo.GenesisBlockHash, httpServer.Addr, nodeConfig.Latency, nodeConfig.LatencyJitter, nodeConfig.DropRate)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	select {
	case <-ctx.Done():
		logrus.Infof("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logrus.Warnf("shutdown server failed. addr:%s err:%v", httpServer.Addr, err)
		}
		return <-serverErr
	case err := <-serverErr:
		return err
	}
}

func LoadConfig(path string) (*Config, error) {
	conf := &Config{
		AppName:  "scanner_eth_mocknode",
		BaseAddr: "127.0.0.1",
		BasePort: 18545,
		Nodes:    []NodeConfig{{}},
		Database: middleware.Database{
			Host:      "127.0.0.1",
			Port:      3306,
			User:      "root",
			Password:  "123456",
			DBName:    "scanner_bsc_testnet",
			Charset:   "utf8mb4",
			ParseTime: true,
			Loc:       "Local",
		},
	}
	v := viper.New()
	v.SetConfigType("yaml")
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}
	if err := v.Unmarshal(conf); err != nil {
		return nil, err
	}
	if len(conf.Nodes) == 0 {
		return nil, fmt.Errorf("nodes must contain at least one node")
	}
	if conf.BasePort <= 0 {
		return nil, fmt.Errorf("base_port must be positive")
	}
	if err := validateNodeConfigs(conf); err != nil {
		return nil, err
	}
	return conf, nil
}

func NodeAddr(conf *Config, nodeID int) string {
	return fmt.Sprintf("%s:%d", conf.BaseAddr, conf.BasePort+nodeID)
}

func (conf *Config) NodeCount() int {
	if conf == nil {
		return 0
	}
	return len(conf.Nodes)
}

func (conf *Config) NodeConfig(nodeID int) NodeConfig {
	if conf == nil || nodeID < 0 || nodeID >= len(conf.Nodes) {
		return NodeConfig{}
	}
	return conf.Nodes[nodeID]
}

func validateNodeConfigs(conf *Config) error {
	for idx, nodeConfig := range conf.Nodes {
		if nodeConfig.Latency < 0 {
			return fmt.Errorf("node %d latency must not be negative", idx)
		}
		if nodeConfig.LatencyJitter < 0 {
			return fmt.Errorf("node %d latency_jitter must not be negative", idx)
		}
		if nodeConfig.DropRate < 0 || nodeConfig.DropRate > 1 {
			return fmt.Errorf("node %d drop_rate must be between 0 and 1", idx)
		}
	}
	return nil
}

func loadScannerInfo(ctx context.Context, db *gorm.DB) (*model.ScannerInfo, error) {
	var scannerInfo model.ScannerInfo
	if err := db.WithContext(ctx).First(&scannerInfo).Error; err != nil {
		return nil, err
	}
	return &scannerInfo, nil
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST is supported", http.StatusMethodNotAllowed)
		return
	}
	if !s.applyNetworkProfile(r.Context(), w) {
		return
	}
	defer r.Body.Close()

	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeJSON(w, rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: -32700, Message: err.Error()}})
		return
	}
	if len(raw) > 0 && raw[0] == '[' {
		var requests []rpcRequest
		if err := json.Unmarshal(raw, &requests); err != nil {
			writeJSON(w, rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: -32700, Message: err.Error()}})
			return
		}
		responses := make([]rpcResponse, 0, len(requests))
		for _, req := range requests {
			responses = append(responses, s.handle(r.Context(), req))
		}
		writeJSON(w, responses)
		return
	}

	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		writeJSON(w, rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: -32700, Message: err.Error()}})
		return
	}
	writeJSON(w, s.handle(r.Context(), req))
}

func (s *server) applyNetworkProfile(ctx context.Context, w http.ResponseWriter) bool {
	if delay := s.nextDelay(); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
	if !s.shouldDrop() {
		return true
	}
	logrus.Warnf("mock node dropping request. id:%d drop_rate:%v", s.id, s.nodeConfig.DropRate)
	if hijacker, ok := w.(http.Hijacker); ok {
		conn, _, err := hijacker.Hijack()
		if err == nil {
			_ = conn.Close()
			return false
		}
	}
	http.Error(w, "mock node dropped request", http.StatusServiceUnavailable)
	return false
}

func (s *server) nextDelay() time.Duration {
	delay := s.nodeConfig.Latency
	jitter := s.nodeConfig.LatencyJitter
	if jitter <= 0 {
		return delay
	}
	s.randomMu.Lock()
	extra := time.Duration(s.random.Int63n(int64(jitter) + 1))
	s.randomMu.Unlock()
	return delay + extra
}

func (s *server) shouldDrop() bool {
	if s.nodeConfig.DropRate <= 0 {
		return false
	}
	if s.nodeConfig.DropRate >= 1 {
		return true
	}
	s.randomMu.Lock()
	drop := s.random.Float64() < s.nodeConfig.DropRate
	s.randomMu.Unlock()
	return drop
}

func (s *server) handle(ctx context.Context, req rpcRequest) rpcResponse {
	result, err := s.call(ctx, req.Method, req.Params)
	if err != nil {
		return rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: -32000, Message: err.Error()}, ID: req.ID}
	}
	return rpcResponse{JSONRPC: "2.0", Result: result, ID: req.ID}
}

func (s *server) call(ctx context.Context, method string, params json.RawMessage) (interface{}, error) {
	switch method {
	case "eth_chainId":
		return hexutil.EncodeBig(big.NewInt(s.chainID)), nil
	case "eth_blockNumber":
		height, err := s.latestHeight(ctx)
		if err != nil {
			return nil, err
		}
		return hexutil.EncodeUint64(height), nil
	case "eth_getBlockByNumber":
		var p []interface{}
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		if len(p) < 1 {
			return nil, errors.New("eth_getBlockByNumber requires block number")
		}
		height, err := s.resolveBlockNumber(ctx, p[0])
		if err != nil {
			return nil, err
		}
		return s.blockByHeight(ctx, height)
	case "eth_getBlockByHash":
		var p []interface{}
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		if len(p) < 1 {
			return nil, errors.New("eth_getBlockByHash requires block hash")
		}
		hash, ok := p[0].(string)
		if !ok {
			return nil, errors.New("block hash must be a string")
		}
		return s.blockByHash(ctx, hash)
	case "eth_getTransactionByHash":
		var p []string
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		if len(p) < 1 {
			return nil, errors.New("eth_getTransactionByHash requires tx hash")
		}
		return s.transactionByHash(ctx, p[0])
	case "eth_getTransactionReceipt":
		var p []string
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		if len(p) < 1 {
			return nil, errors.New("eth_getTransactionReceipt requires tx hash")
		}
		return s.receiptByTxHash(ctx, p[0])
	case "debug_traceBlockByHash":
		var p []interface{}
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		if len(p) < 1 {
			return nil, errors.New("debug_traceBlockByHash requires block hash")
		}
		hash, ok := p[0].(string)
		if !ok {
			return nil, errors.New("block hash must be a string")
		}
		return s.traceBlockByHash(ctx, hash)
	case "eth_getBalance":
		var p []interface{}
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		if len(p) < 1 {
			return nil, errors.New("eth_getBalance requires address")
		}
		addr, ok := p[0].(string)
		if !ok {
			return nil, errors.New("address must be a string")
		}
		return s.nativeBalance(ctx, addr)
	case "eth_call":
		var p []json.RawMessage
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		if len(p) < 1 {
			return nil, errors.New("eth_call requires call object")
		}
		return s.ethCall(ctx, p[0])
	default:
		return nil, fmt.Errorf("unsupported method %s", method)
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) latestHeight(ctx context.Context) (uint64, error) {
	var height uint64
	err := s.db.WithContext(ctx).Model(&model.Block{}).Where("complete = ?", true).Select("COALESCE(MAX(height), 0)").Scan(&height).Error
	return height, err
}

func (s *server) resolveBlockNumber(ctx context.Context, raw interface{}) (uint64, error) {
	switch v := raw.(type) {
	case string:
		switch strings.ToLower(v) {
		case "latest", "safe", "finalized":
			return s.latestHeight(ctx)
		case "earliest":
			return 0, nil
		case "pending":
			return s.latestHeight(ctx)
		default:
			n, err := hexutil.DecodeUint64(v)
			if err != nil {
				return 0, err
			}
			return n, nil
		}
	case float64:
		if v < 0 {
			return 0, fmt.Errorf("negative block number %v", v)
		}
		return uint64(v), nil
	default:
		return 0, fmt.Errorf("unsupported block number %T", raw)
	}
}

func (s *server) blockByHeight(ctx context.Context, height uint64) (*blockJSON, error) {
	var block model.Block
	if err := s.db.WithContext(ctx).Where("height = ? AND complete = ?", height, true).First(&block).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if height == 0 {
				return s.genesisBlock(), nil
			}
			return nil, nil
		}
		return nil, err
	}
	return s.toBlockHeader(ctx, block)
}

func (s *server) blockByHash(ctx context.Context, hash string) (*blockJSON, error) {
	var block model.Block
	if err := s.db.WithContext(ctx).Where("hash = ? AND complete = ?", normalize(hash), true).First(&block).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if normalize(hash) == normalize(s.genesisHash) {
				return s.genesisBlock(), nil
			}
			return nil, nil
		}
		return nil, err
	}
	return s.toBlockHeader(ctx, block)
}

func (s *server) genesisBlock() *blockJSON {
	return &blockJSON{
		BlockHeaderJson: node.BlockHeaderJson{
			BaseFeePerGas:   "0x0",
			Difficulty:      "0x0",
			ExtraData:       "0x",
			GasLimit:        "0x0",
			GasUsed:         "0x0",
			Hash:            normalize(s.genesisHash),
			Miner:           emptyAddress,
			Nonce:           "0x0000000000000000",
			Number:          "0x0",
			ParentHash:      emptyHash,
			ReceiptsRoot:    emptyHash,
			Sha3Uncles:      emptyUncleHash,
			Size:            "0x0",
			StateRoot:       emptyHash,
			TimeStamp:       "0x0",
			TotalDifficulty: "0x0",
			TransactionRoot: emptyHash,
			Transactions:    []string{},
			Uncles:          []interface{}{},
		},
		LogsBloom: emptyLogsBloom,
		MixHash:   emptyHash,
	}
}

func (s *server) toBlockHeader(ctx context.Context, block model.Block) (*blockJSON, error) {
	var txs []model.Tx
	if err := s.db.WithContext(ctx).Where("height = ?", block.Height).Order("tx_index ASC").Find(&txs).Error; err != nil {
		return nil, err
	}
	txHashes := make([]string, 0, len(txs))
	for _, tx := range txs {
		txHashes = append(txHashes, normalize(tx.TxHash))
	}
	return &blockJSON{
		BlockHeaderJson: node.BlockHeaderJson{
			BaseFeePerGas:   decimalStringToHex(block.BaseFee),
			Difficulty:      decimalStringToHex(block.Difficulty),
			ExtraData:       bytesStringToHex(block.ExtraData),
			GasLimit:        hexutil.EncodeUint64(block.GasLimit),
			GasUsed:         hexutil.EncodeUint64(block.GasUsed),
			Hash:            normalize(block.Hash),
			Miner:           normalize(block.Miner),
			Nonce:           nonceOrDefault(block.Nonce),
			Number:          hexutil.EncodeUint64(block.Height),
			ParentHash:      normalize(block.ParentHash),
			ReceiptsRoot:    hashOrDefault(block.ReceiptRoot),
			Sha3Uncles:      emptyUncleHash,
			Size:            hexutil.EncodeUint64(uint64(block.Size)),
			StateRoot:       hashOrDefault(block.StateRoot),
			TimeStamp:       hexutil.EncodeUint64(uint64(block.Timestamp)),
			TotalDifficulty: decimalStringToHex(block.TotalDifficulty),
			TransactionRoot: hashOrDefault(block.TransactionRoot),
			Transactions:    txHashes,
			Uncles:          []interface{}{},
		},
		LogsBloom: emptyLogsBloom,
		MixHash:   emptyHash,
	}, nil
}

func (s *server) transactionByHash(ctx context.Context, hash string) (*node.TxJson, error) {
	var tx model.Tx
	if err := s.db.WithContext(ctx).Where("tx_hash = ?", normalize(hash)).First(&tx).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &node.TxJson{
		Hash:                 normalize(tx.TxHash),
		From:                 normalize(tx.From),
		To:                   normalize(tx.To),
		Gas:                  hexutil.EncodeUint64(tx.GasLimit),
		GasPrice:             decimalStringToHex(tx.GasPrice),
		Input:                bytesStringToHex(tx.Input),
		MaxFeePerGas:         decimalStringToHex(tx.MaxFeePerGas),
		MaxPriorityFeePerGas: decimalStringToHex(tx.MaxPriorityFeePerGas),
		Nonce:                hexutil.EncodeUint64(tx.Nonce),
		TransactionIndex:     hexutil.EncodeUint64(uint64(tx.TxIndex)),
		Type:                 hexutil.EncodeUint64(uint64(tx.TxType)),
		Value:                decimalStringToHex(tx.Value),
	}, nil
}

func (s *server) receiptByTxHash(ctx context.Context, hash string) (*ethTypes.Receipt, error) {
	var tx model.Tx
	if err := s.db.WithContext(ctx).Where("tx_hash = ?", normalize(hash)).First(&tx).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var block model.Block
	if err := s.db.WithContext(ctx).Where("height = ?", tx.Height).First(&block).Error; err != nil {
		return nil, err
	}
	var storedLogs []model.EventLog
	if err := s.db.WithContext(ctx).Where("tx_hash = ?", normalize(hash)).Order("index_in_block ASC").Find(&storedLogs).Error; err != nil {
		return nil, err
	}
	logs := make([]*ethTypes.Log, 0, len(storedLogs))
	for _, storedLog := range storedLogs {
		logs = append(logs, &ethTypes.Log{
			Address:     common.HexToAddress(storedLog.ContractAddr),
			Topics:      parseTopics(storedLog.Topic0, storedLog.Topic1, storedLog.Topic2, storedLog.Topic3),
			Data:        storedLog.Data,
			BlockNumber: tx.Height,
			TxHash:      common.HexToHash(tx.TxHash),
			TxIndex:     uint(tx.TxIndex),
			BlockHash:   common.HexToHash(block.Hash),
			Index:       storedLog.IndexInBlock,
		})
	}
	return &ethTypes.Receipt{
		Type:              uint8(tx.TxType),
		Status:            tx.ExecStatus,
		CumulativeGasUsed: tx.GasUsed,
		Logs:              logs,
		TxHash:            common.HexToHash(tx.TxHash),
		ContractAddress:   common.HexToAddress(s.contractAddress(ctx, tx)),
		GasUsed:           tx.GasUsed,
		BlockHash:         common.HexToHash(block.Hash),
		BlockNumber:       new(big.Int).SetUint64(tx.Height),
		TransactionIndex:  uint(tx.TxIndex),
	}, nil
}

func (s *server) traceBlockByHash(ctx context.Context, hash string) ([]*node.TxInternalTraceResultJson, error) {
	var block model.Block
	if err := s.db.WithContext(ctx).Where("hash = ?", normalize(hash)).First(&block).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var txs []model.Tx
	if err := s.db.WithContext(ctx).Where("height = ?", block.Height).Order("tx_index ASC").Find(&txs).Error; err != nil {
		return nil, err
	}
	results := make([]*node.TxInternalTraceResultJson, 0, len(txs))
	for _, tx := range txs {
		trace := &node.TxInternalJson{
			Type:    traceType(tx),
			From:    normalize(tx.From),
			To:      normalize(tx.To),
			Value:   (*hexutil.Big)(decimalStringToBig(tx.Value)),
			Gas:     hexutil.Uint64(tx.GasLimit),
			GasUsed: hexutil.Uint64(tx.GasUsed),
			Input:   bytesStringToHex(tx.Input),
		}
		if tx.ExecStatus == 0 {
			trace.Error = "execution reverted"
		}
		results = append(results, &node.TxInternalTraceResultJson{
			TxHash: normalize(tx.TxHash),
			Result: trace,
		})
	}
	return results, nil
}

func (s *server) nativeBalance(ctx context.Context, addr string) (string, error) {
	var balance model.BalanceNative
	if err := s.db.WithContext(ctx).Where("addr = ?", normalize(addr)).First(&balance).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "0x0", nil
		}
		return "", err
	}
	return decimalStringToHex(balance.Balance), nil
}

func (s *server) ethCall(ctx context.Context, raw json.RawMessage) (string, error) {
	var call struct {
		To   string `json:"to"`
		Data string `json:"data"`
	}
	if err := json.Unmarshal(raw, &call); err != nil {
		return "", err
	}
	input, err := hexutil.Decode(call.Data)
	if err != nil {
		return "", err
	}
	if len(input) < 4 {
		return "0x", nil
	}
	to := normalize(call.To)

	if method, err := filter.Erc20ABI.MethodById(input[:4]); err == nil {
		result, err := s.handleErc20Call(ctx, to, method.Name, input[4:])
		if err != nil || result != "0x" {
			return result, err
		}
	}
	if method, err := filter.Erc721ABI.MethodById(input[:4]); err == nil {
		result, err := s.handleErc721Call(ctx, to, method.Name, input[4:])
		if err != nil || result != "0x" {
			return result, err
		}
	}
	if method, err := filter.Erc1155ABI.MethodById(input[:4]); err == nil {
		return s.handleErc1155Call(ctx, to, method.Name, input[4:])
	}
	return "0x", nil
}

func (s *server) handleErc20Call(ctx context.Context, contractAddr, method string, args []byte) (string, error) {
	switch method {
	case "name", "symbol", "decimals", "totalSupply":
		var contract model.ContractErc20
		if err := s.db.WithContext(ctx).Where("contract_addr = ?", contractAddr).First(&contract).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return "0x", nil
			}
			return "", err
		}
		switch method {
		case "name":
			return packCallOutput(filter.Erc20ABI, method, contract.Name)
		case "symbol":
			return packCallOutput(filter.Erc20ABI, method, contract.Symbol)
		case "decimals":
			return packCallOutput(filter.Erc20ABI, method, uint8(contract.Decimals))
		case "totalSupply":
			return packCallOutput(filter.Erc20ABI, method, decimalStringToBig(contract.TotalSupply))
		}
	case "balanceOf":
		values, err := filter.Erc20ABI.Methods[method].Inputs.Unpack(args)
		if err != nil {
			return "", err
		}
		addr := normalize(values[0].(common.Address).Hex())
		var balance model.BalanceErc20
		err = s.db.WithContext(ctx).Where("addr = ? AND contract_addr = ?", addr, contractAddr).First(&balance).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return "", err
		}
		return packCallOutput(filter.Erc20ABI, method, decimalStringToBig(balance.Balance))
	}
	return "0x", nil
}

func (s *server) handleErc721Call(ctx context.Context, contractAddr, method string, args []byte) (string, error) {
	switch method {
	case "name", "symbol":
		var contract model.ContractErc721
		if err := s.db.WithContext(ctx).Where("contract_addr = ?", contractAddr).First(&contract).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return "0x", nil
			}
			return "", err
		}
		if method == "name" {
			return packCallOutput(filter.Erc721ABI, method, contract.Name)
		}
		return packCallOutput(filter.Erc721ABI, method, contract.Symbol)
	case "ownerOf", "tokenURI":
		values, err := filter.Erc721ABI.Methods[method].Inputs.Unpack(args)
		if err != nil {
			return "", err
		}
		tokenID := values[0].(*big.Int).String()
		var token model.TokenErc721
		err = s.db.WithContext(ctx).Where("contract_addr = ? AND token_id = ?", contractAddr, tokenID).First(&token).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return "0x", nil
			}
			return "", err
		}
		if method == "ownerOf" {
			return packCallOutput(filter.Erc721ABI, method, common.HexToAddress(token.OwnerAddr))
		}
		return packCallOutput(filter.Erc721ABI, method, token.TokenUri)
	}
	return "0x", nil
}

func (s *server) handleErc1155Call(ctx context.Context, contractAddr, method string, args []byte) (string, error) {
	if method != "balanceOf" {
		return "0x", nil
	}
	values, err := filter.Erc1155ABI.Methods[method].Inputs.Unpack(args)
	if err != nil {
		return "", err
	}
	addr := normalize(values[0].(common.Address).Hex())
	tokenID := values[1].(*big.Int).String()
	var balance model.BalanceErc1155
	err = s.db.WithContext(ctx).Where("addr = ? AND contract_addr = ? AND token_id = ?", addr, contractAddr, tokenID).First(&balance).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", err
	}
	return packCallOutput(filter.Erc1155ABI, method, decimalStringToBig(balance.Balance))
}

func packCallOutput(contractABI abi.ABI, method string, values ...interface{}) (string, error) {
	m, ok := contractABI.Methods[method]
	if !ok {
		return "", fmt.Errorf("unknown abi method %s", method)
	}
	out, err := m.Outputs.Pack(values...)
	if err != nil {
		return "", err
	}
	return hexutil.Encode(out), nil
}

func normalize(v string) string {
	if strings.TrimSpace(v) == "" {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(v))
}

func hashOrDefault(v string) string {
	if strings.TrimSpace(v) == "" {
		return emptyHash
	}
	return normalize(v)
}

func nonceOrDefault(v string) string {
	if strings.TrimSpace(v) == "" {
		return "0x0000000000000000"
	}
	return v
}

func decimalStringToBig(v string) *big.Int {
	n := new(big.Int)
	v = strings.TrimSpace(v)
	if v == "" {
		return n
	}
	if strings.HasPrefix(strings.ToLower(v), "0x") {
		if parsed, err := hexutil.DecodeBig(v); err == nil {
			return parsed
		}
		return n
	}
	if _, ok := n.SetString(v, 10); ok {
		return n
	}
	return n
}

func decimalStringToHex(v string) string {
	return hexutil.EncodeBig(decimalStringToBig(v))
}

func bytesStringToHex(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "0x"
	}
	if strings.HasPrefix(strings.ToLower(v), "0x") {
		return v
	}
	return hexutil.Encode([]byte(v))
}

func parseTopics(values ...string) []common.Hash {
	topics := make([]common.Hash, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		topics = append(topics, common.HexToHash(value))
	}
	return topics
}

func (s *server) contractAddress(ctx context.Context, tx model.Tx) string {
	if !tx.IsCreateContract {
		return ""
	}
	var contract model.Contract
	if err := s.db.WithContext(ctx).Where("tx_hash = ?", normalize(tx.TxHash)).First(&contract).Error; err == nil {
		return contract.ContractAddr
	}
	return ""
}

func traceType(tx model.Tx) string {
	if tx.IsCreateContract {
		return "CREATE"
	}
	return "CALL"
}
