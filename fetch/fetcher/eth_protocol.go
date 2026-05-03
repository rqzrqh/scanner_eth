package fetcher

import nodepkg "scanner_eth/fetch/node"

// Keep compatibility aliases here while the ownership of node protocol types
// lives in `fetch/node`.
type BlockHeaderJson = nodepkg.BlockHeaderJson
type TxJson = nodepkg.TxJson
type TxInternalTraceResultJson = nodepkg.TxInternalTraceResultJson
type TxInternalJson = nodepkg.TxInternalJson
