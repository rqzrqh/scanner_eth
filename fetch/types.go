package fetch

import (
	fetcherpkg "scanner_eth/fetch/fetcher"
	fetchstore "scanner_eth/fetch/store"
)

// EventBlockData carries converted block payloads for storage and Redis export.
type EventBlockData struct {
	StorageFullBlock *StorageFullBlock
}

type BlockHeaderJson = fetcherpkg.BlockHeaderJson
type TxJson = fetcherpkg.TxJson
type TxInternalTraceResultJson = fetcherpkg.TxInternalTraceResultJson
type TxInternalJson = fetcherpkg.TxInternalJson
type TokenErc721KeyValue = fetcherpkg.TokenErc721KeyValue
type FetchResult = fetcherpkg.FetchResult

type StorageFullBlock = fetchstore.StorageFullBlock
