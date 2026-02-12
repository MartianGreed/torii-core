import type { MessageSchema } from "@toriijs/sdk";

export const schemas: Record<string, MessageSchema> = {};

function reg(schema: MessageSchema): MessageSchema {
  schemas[schema.name] = schema;
  schemas[schema.fullName] = schema;
  return schema;
}

// ===== Torii Core =====

export const ToriiTopicUpdate = reg({
  name: "ToriiTopicUpdate",
  fullName: "torii.TopicUpdate",
  fields: {
    topic: { number: 1, type: "string", repeated: false },
    updateType: { number: 2, type: "enum", repeated: false, enumType: "UpdateType" },
    timestamp: { number: 3, type: "int64", repeated: false },
    typeId: { number: 4, type: "string", repeated: false },
    data: { number: 5, type: "message", repeated: false, messageType: "Any" },
  },
});

// ===== ERC20 =====

export const Erc20Transfer = reg({
  name: "Transfer",
  fullName: "torii.sinks.erc20.Transfer",
  fields: {
    token: { number: 1, type: "bytes", repeated: false },
    from: { number: 2, type: "bytes", repeated: false },
    to: { number: 3, type: "bytes", repeated: false },
    amount: { number: 4, type: "bytes", repeated: false },
    blockNumber: { number: 5, type: "uint64", repeated: false },
    txHash: { number: 6, type: "bytes", repeated: false },
    timestamp: { number: 7, type: "int64", repeated: false },
  },
});

export const Erc20TransferFilter = reg({
  name: "Erc20TransferFilter",
  fullName: "torii.sinks.erc20.TransferFilter",
  fields: {
    wallet: { number: 1, type: "bytes", repeated: false, optional: true },
    from: { number: 2, type: "bytes", repeated: false, optional: true },
    to: { number: 3, type: "bytes", repeated: false, optional: true },
    tokens: { number: 4, type: "bytes", repeated: true },
    direction: { number: 5, type: "enum", repeated: false, enumType: "TransferDirection" },
    blockFrom: { number: 6, type: "uint64", repeated: false, optional: true },
    blockTo: { number: 7, type: "uint64", repeated: false, optional: true },
  },
});

export const Erc20Cursor = reg({
  name: "Erc20Cursor",
  fullName: "torii.sinks.erc20.Cursor",
  fields: {
    blockNumber: { number: 1, type: "uint64", repeated: false },
    id: { number: 2, type: "int64", repeated: false },
  },
});

export const Erc20GetTransfersRequest = reg({
  name: "Erc20GetTransfersRequest",
  fullName: "torii.sinks.erc20.GetTransfersRequest",
  fields: {
    filter: { number: 1, type: "message", repeated: false, messageType: "Erc20TransferFilter" },
    cursor: { number: 2, type: "message", repeated: false, optional: true, messageType: "Erc20Cursor" },
    limit: { number: 3, type: "uint32", repeated: false },
  },
});

export const Erc20GetTransfersResponse = reg({
  name: "Erc20GetTransfersResponse",
  fullName: "torii.sinks.erc20.GetTransfersResponse",
  fields: {
    transfers: { number: 1, type: "message", repeated: true, messageType: "Transfer" },
    nextCursor: { number: 2, type: "message", repeated: false, optional: true, messageType: "Erc20Cursor" },
  },
});

export const Erc20GetTokenMetadataRequest = reg({
  name: "Erc20GetTokenMetadataRequest",
  fullName: "torii.sinks.erc20.GetTokenMetadataRequest",
  fields: {
    token: { number: 1, type: "bytes", repeated: false, optional: true },
    cursor: { number: 2, type: "bytes", repeated: false, optional: true },
    limit: { number: 3, type: "uint32", repeated: false },
  },
});

export const Erc20GetTokenMetadataResponse = reg({
  name: "Erc20GetTokenMetadataResponse",
  fullName: "torii.sinks.erc20.GetTokenMetadataResponse",
  fields: {
    tokens: { number: 1, type: "message", repeated: true, messageType: "Erc20TokenMetadataEntry" },
    nextCursor: { number: 2, type: "bytes", repeated: false, optional: true },
  },
});

export const Erc20GetBalanceRequest = reg({
  name: "Erc20GetBalanceRequest",
  fullName: "torii.sinks.erc20.GetBalanceRequest",
  fields: {
    token: { number: 1, type: "bytes", repeated: false },
    wallet: { number: 2, type: "bytes", repeated: false },
  },
});

export const Erc20GetBalanceResponse = reg({
  name: "Erc20GetBalanceResponse",
  fullName: "torii.sinks.erc20.GetBalanceResponse",
  fields: {
    balance: { number: 1, type: "bytes", repeated: false },
    lastBlock: { number: 2, type: "uint64", repeated: false },
  },
});

export const Erc20BalanceEntry = reg({
  name: "Erc20BalanceEntry",
  fullName: "torii.sinks.erc20.BalanceEntry",
  fields: {
    token: { number: 1, type: "bytes", repeated: false },
    wallet: { number: 2, type: "bytes", repeated: false },
    balance: { number: 3, type: "bytes", repeated: false },
    lastBlock: { number: 4, type: "uint64", repeated: false },
  },
});

export const Erc20GetBalancesRequest = reg({
  name: "Erc20GetBalancesRequest",
  fullName: "torii.sinks.erc20.GetBalancesRequest",
  fields: {
    token: { number: 1, type: "bytes", repeated: false, optional: true },
    wallet: { number: 2, type: "bytes", repeated: false, optional: true },
    cursor: { number: 3, type: "int64", repeated: false, optional: true },
    limit: { number: 4, type: "uint32", repeated: false },
  },
});

export const Erc20GetBalancesResponse = reg({
  name: "Erc20GetBalancesResponse",
  fullName: "torii.sinks.erc20.GetBalancesResponse",
  fields: {
    balances: { number: 1, type: "message", repeated: true, messageType: "Erc20BalanceEntry" },
    nextCursor: { number: 2, type: "int64", repeated: false, optional: true },
  },
});

export const Erc20GetStatsRequest = reg({
  name: "Erc20GetStatsRequest",
  fullName: "torii.sinks.erc20.GetStatsRequest",
  fields: {},
});

export const Erc20GetStatsResponse = reg({
  name: "Erc20GetStatsResponse",
  fullName: "torii.sinks.erc20.GetStatsResponse",
  fields: {
    totalTransfers: { number: 1, type: "uint64", repeated: false },
    totalApprovals: { number: 2, type: "uint64", repeated: false },
    uniqueTokens: { number: 3, type: "uint64", repeated: false },
    latestBlock: { number: 4, type: "uint64", repeated: false },
  },
});

export const Erc20TokenMetadataEntry = reg({
  name: "Erc20TokenMetadataEntry",
  fullName: "torii.sinks.erc20.TokenMetadataEntry",
  fields: {
    token: { number: 1, type: "bytes", repeated: false },
    name: { number: 2, type: "string", repeated: false, optional: true },
    symbol: { number: 3, type: "string", repeated: false, optional: true },
    decimals: { number: 4, type: "uint32", repeated: false, optional: true },
  },
});

// ===== ERC721 =====

export const Erc721NftTransfer = reg({
  name: "NftTransfer",
  fullName: "torii.sinks.erc721.NftTransfer",
  fields: {
    token: { number: 1, type: "bytes", repeated: false },
    tokenId: { number: 2, type: "bytes", repeated: false },
    from: { number: 3, type: "bytes", repeated: false },
    to: { number: 4, type: "bytes", repeated: false },
    blockNumber: { number: 5, type: "uint64", repeated: false },
    txHash: { number: 6, type: "bytes", repeated: false },
    timestamp: { number: 7, type: "int64", repeated: false },
  },
});

export const Erc721TransferFilter = reg({
  name: "Erc721TransferFilter",
  fullName: "torii.sinks.erc721.TransferFilter",
  fields: {
    wallet: { number: 1, type: "bytes", repeated: false, optional: true },
    from: { number: 2, type: "bytes", repeated: false, optional: true },
    to: { number: 3, type: "bytes", repeated: false, optional: true },
    tokens: { number: 4, type: "bytes", repeated: true },
    tokenIds: { number: 5, type: "bytes", repeated: true },
    blockFrom: { number: 6, type: "uint64", repeated: false, optional: true },
    blockTo: { number: 7, type: "uint64", repeated: false, optional: true },
  },
});

export const Erc721Cursor = reg({
  name: "Erc721Cursor",
  fullName: "torii.sinks.erc721.Cursor",
  fields: {
    blockNumber: { number: 1, type: "uint64", repeated: false },
    id: { number: 2, type: "int64", repeated: false },
  },
});

export const Erc721GetTransfersRequest = reg({
  name: "Erc721GetTransfersRequest",
  fullName: "torii.sinks.erc721.GetTransfersRequest",
  fields: {
    filter: { number: 1, type: "message", repeated: false, messageType: "Erc721TransferFilter" },
    cursor: { number: 2, type: "message", repeated: false, optional: true, messageType: "Erc721Cursor" },
    limit: { number: 3, type: "uint32", repeated: false },
  },
});

export const Erc721GetTransfersResponse = reg({
  name: "Erc721GetTransfersResponse",
  fullName: "torii.sinks.erc721.GetTransfersResponse",
  fields: {
    transfers: { number: 1, type: "message", repeated: true, messageType: "NftTransfer" },
    nextCursor: { number: 2, type: "message", repeated: false, optional: true, messageType: "Erc721Cursor" },
  },
});

export const Erc721TokenMetadataEntry = reg({
  name: "Erc721TokenMetadataEntry",
  fullName: "torii.sinks.erc721.TokenMetadataEntry",
  fields: {
    token: { number: 1, type: "bytes", repeated: false },
    name: { number: 2, type: "string", repeated: false, optional: true },
    symbol: { number: 3, type: "string", repeated: false, optional: true },
    totalSupply: { number: 4, type: "bytes", repeated: false, optional: true },
  },
});

export const Erc721GetTokenMetadataRequest = reg({
  name: "Erc721GetTokenMetadataRequest",
  fullName: "torii.sinks.erc721.GetTokenMetadataRequest",
  fields: {
    token: { number: 1, type: "bytes", repeated: false, optional: true },
    cursor: { number: 2, type: "bytes", repeated: false, optional: true },
    limit: { number: 3, type: "uint32", repeated: false },
  },
});

export const Erc721GetTokenMetadataResponse = reg({
  name: "Erc721GetTokenMetadataResponse",
  fullName: "torii.sinks.erc721.GetTokenMetadataResponse",
  fields: {
    tokens: { number: 1, type: "message", repeated: true, messageType: "Erc721TokenMetadataEntry" },
    nextCursor: { number: 2, type: "bytes", repeated: false, optional: true },
  },
});

export const Erc721GetStatsRequest = reg({
  name: "Erc721GetStatsRequest",
  fullName: "torii.sinks.erc721.GetStatsRequest",
  fields: {},
});

export const Erc721GetStatsResponse = reg({
  name: "Erc721GetStatsResponse",
  fullName: "torii.sinks.erc721.GetStatsResponse",
  fields: {
    totalTransfers: { number: 1, type: "uint64", repeated: false },
    uniqueTokens: { number: 2, type: "uint64", repeated: false },
    uniqueNfts: { number: 3, type: "uint64", repeated: false },
    latestBlock: { number: 4, type: "uint64", repeated: false },
  },
});

// ===== ERC1155 =====

export const Erc1155TokenTransfer = reg({
  name: "TokenTransfer",
  fullName: "torii.sinks.erc1155.TokenTransfer",
  fields: {
    token: { number: 1, type: "bytes", repeated: false },
    operator: { number: 2, type: "bytes", repeated: false },
    from: { number: 3, type: "bytes", repeated: false },
    to: { number: 4, type: "bytes", repeated: false },
    tokenId: { number: 5, type: "bytes", repeated: false },
    amount: { number: 6, type: "bytes", repeated: false },
    blockNumber: { number: 7, type: "uint64", repeated: false },
    txHash: { number: 8, type: "bytes", repeated: false },
    timestamp: { number: 9, type: "int64", repeated: false },
    isBatch: { number: 10, type: "bool", repeated: false },
    batchIndex: { number: 11, type: "uint32", repeated: false },
  },
});

export const Erc1155TransferFilter = reg({
  name: "Erc1155TransferFilter",
  fullName: "torii.sinks.erc1155.TransferFilter",
  fields: {
    wallet: { number: 1, type: "bytes", repeated: false, optional: true },
    from: { number: 2, type: "bytes", repeated: false, optional: true },
    to: { number: 3, type: "bytes", repeated: false, optional: true },
    operator: { number: 4, type: "bytes", repeated: false, optional: true },
    tokens: { number: 5, type: "bytes", repeated: true },
    tokenIds: { number: 6, type: "bytes", repeated: true },
    blockFrom: { number: 7, type: "uint64", repeated: false, optional: true },
    blockTo: { number: 8, type: "uint64", repeated: false, optional: true },
  },
});

export const Erc1155Cursor = reg({
  name: "Erc1155Cursor",
  fullName: "torii.sinks.erc1155.Cursor",
  fields: {
    blockNumber: { number: 1, type: "uint64", repeated: false },
    id: { number: 2, type: "int64", repeated: false },
  },
});

export const Erc1155GetTransfersRequest = reg({
  name: "Erc1155GetTransfersRequest",
  fullName: "torii.sinks.erc1155.GetTransfersRequest",
  fields: {
    filter: { number: 1, type: "message", repeated: false, messageType: "Erc1155TransferFilter" },
    cursor: { number: 2, type: "message", repeated: false, optional: true, messageType: "Erc1155Cursor" },
    limit: { number: 3, type: "uint32", repeated: false },
  },
});

export const Erc1155GetTransfersResponse = reg({
  name: "Erc1155GetTransfersResponse",
  fullName: "torii.sinks.erc1155.GetTransfersResponse",
  fields: {
    transfers: { number: 1, type: "message", repeated: true, messageType: "TokenTransfer" },
    nextCursor: { number: 2, type: "message", repeated: false, optional: true, messageType: "Erc1155Cursor" },
  },
});

export const Erc1155TokenMetadataEntry = reg({
  name: "Erc1155TokenMetadataEntry",
  fullName: "torii.sinks.erc1155.TokenMetadataEntry",
  fields: {
    token: { number: 1, type: "bytes", repeated: false },
    name: { number: 2, type: "string", repeated: false, optional: true },
    symbol: { number: 3, type: "string", repeated: false, optional: true },
    totalSupply: { number: 4, type: "bytes", repeated: false, optional: true },
  },
});

export const Erc1155GetTokenMetadataRequest = reg({
  name: "Erc1155GetTokenMetadataRequest",
  fullName: "torii.sinks.erc1155.GetTokenMetadataRequest",
  fields: {
    token: { number: 1, type: "bytes", repeated: false, optional: true },
    cursor: { number: 2, type: "bytes", repeated: false, optional: true },
    limit: { number: 3, type: "uint32", repeated: false },
  },
});

export const Erc1155GetTokenMetadataResponse = reg({
  name: "Erc1155GetTokenMetadataResponse",
  fullName: "torii.sinks.erc1155.GetTokenMetadataResponse",
  fields: {
    tokens: { number: 1, type: "message", repeated: true, messageType: "Erc1155TokenMetadataEntry" },
    nextCursor: { number: 2, type: "bytes", repeated: false, optional: true },
  },
});

export const Erc1155GetBalanceRequest = reg({
  name: "Erc1155GetBalanceRequest",
  fullName: "torii.sinks.erc1155.GetBalanceRequest",
  fields: {
    contract: { number: 1, type: "bytes", repeated: false },
    wallet: { number: 2, type: "bytes", repeated: false },
    tokenId: { number: 3, type: "bytes", repeated: false },
  },
});

export const Erc1155GetBalanceResponse = reg({
  name: "Erc1155GetBalanceResponse",
  fullName: "torii.sinks.erc1155.GetBalanceResponse",
  fields: {
    balance: { number: 1, type: "bytes", repeated: false },
    lastBlock: { number: 2, type: "uint64", repeated: false },
  },
});

export const Erc1155GetStatsRequest = reg({
  name: "Erc1155GetStatsRequest",
  fullName: "torii.sinks.erc1155.GetStatsRequest",
  fields: {},
});

export const Erc1155GetStatsResponse = reg({
  name: "Erc1155GetStatsResponse",
  fullName: "torii.sinks.erc1155.GetStatsResponse",
  fields: {
    totalTransfers: { number: 1, type: "uint64", repeated: false },
    uniqueTokens: { number: 2, type: "uint64", repeated: false },
    uniqueTokenIds: { number: 3, type: "uint64", repeated: false },
    latestBlock: { number: 4, type: "uint64", repeated: false },
  },
});
