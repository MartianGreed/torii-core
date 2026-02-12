import { hexToBytes, bytesToHex, formatU256 } from "./utils";
import type { TokensClient } from "./client";
import {
  Erc20GetStatsRequest, Erc20GetStatsResponse,
  Erc20GetTransfersRequest, Erc20GetTransfersResponse,
  Erc20GetBalanceRequest, Erc20GetBalanceResponse,
  Erc20GetBalancesRequest, Erc20GetBalancesResponse,
  Erc20GetTokenMetadataRequest, Erc20GetTokenMetadataResponse,
  Erc721GetStatsRequest, Erc721GetStatsResponse,
  Erc721GetTransfersRequest, Erc721GetTransfersResponse,
  Erc721GetTokenMetadataRequest, Erc721GetTokenMetadataResponse,
  Erc1155GetStatsRequest, Erc1155GetStatsResponse,
  Erc1155GetTransfersRequest, Erc1155GetTransfersResponse,
  Erc1155GetBalanceRequest, Erc1155GetBalanceResponse,
  Erc1155GetTokenMetadataRequest, Erc1155GetTokenMetadataResponse,
} from "./schemas";

export interface TokenQuery {
  contractAddress: string;
  wallet: string;
}

export interface Erc1155TokenQuery extends TokenQuery {
  tokenId: string;
}

export interface TokenMetadataResult {
  token: string;
  name?: string;
  symbol?: string;
  decimals?: number;
  totalSupply?: string;
}

export interface BalanceResult {
  balance: string;
  balanceRaw: string;
  lastBlock: number;
}

export interface TokenBalanceResult {
  token: string;
  wallet?: string;
  symbol?: string;
  balance: string;
  balanceRaw: string;
  lastBlock: number;
}

export interface TransferResult {
  token: string;
  from: string;
  to: string;
  value?: string;
  amount?: string;
  blockNumber: number;
  txHash: string;
  timestamp: number;
}

export interface Erc1155TransferResult extends TransferResult {
  tokenId: string;
  operator: string;
  isBatch: boolean;
}

export interface StatsResult {
  totalTransfers: number;
  totalApprovals?: number;
  uniqueTokens: number;
  uniqueNfts?: number;
  uniqueTokenIds?: number;
  latestBlock: number;
}

export interface TransferCursorResult {
  blockNumber: number;
  id: number;
}

export interface PageResult<TItem, TCursor> {
  items: TItem[];
  nextCursor?: TCursor;
}

const DEFAULT_PAGE_LIMIT = 100;

async function getErc20BalanceWithDecimals(
  client: TokensClient,
  contractAddress: string,
  wallet: string,
  decimals?: number
): Promise<BalanceResult> {
  const response = await client.call(
    "/torii.sinks.erc20.Erc20/GetBalance",
    {
      token: hexToBytes(contractAddress),
      wallet: hexToBytes(wallet),
    },
    Erc20GetBalanceRequest,
    Erc20GetBalanceResponse
  );

  return {
    balance: formatU256(response.balance as string | Uint8Array | undefined, decimals),
    balanceRaw: typeof response.balance === "string" ? response.balance : "",
    lastBlock: Number(response.lastBlock) || 0,
  };
}

export async function getErc20Stats(
  client: TokensClient
): Promise<StatsResult> {
  const response = await client.call(
    "/torii.sinks.erc20.Erc20/GetStats",
    {},
    Erc20GetStatsRequest,
    Erc20GetStatsResponse
  );

  return {
    totalTransfers: Number(response.totalTransfers ?? 0),
    totalApprovals: Number(response.totalApprovals ?? 0),
    uniqueTokens: Number(response.uniqueTokens ?? 0),
    latestBlock: Number(response.latestBlock ?? 0),
  };
}

export async function getErc721Stats(
  client: TokensClient
): Promise<StatsResult> {
  const response = await client.call(
    "/torii.sinks.erc721.Erc721/GetStats",
    {},
    Erc721GetStatsRequest,
    Erc721GetStatsResponse
  );

  return {
    totalTransfers: Number(response.totalTransfers ?? 0),
    uniqueTokens: Number(response.uniqueTokens ?? 0),
    uniqueNfts: Number(response.uniqueNfts ?? 0),
    latestBlock: Number(response.latestBlock ?? 0),
  };
}

export async function getErc1155Stats(
  client: TokensClient
): Promise<StatsResult> {
  const response = await client.call(
    "/torii.sinks.erc1155.Erc1155/GetStats",
    {},
    Erc1155GetStatsRequest,
    Erc1155GetStatsResponse
  );

  return {
    totalTransfers: Number(response.totalTransfers ?? 0),
    uniqueTokens: Number(response.uniqueTokens ?? 0),
    uniqueTokenIds: Number(response.uniqueTokenIds ?? 0),
    latestBlock: Number(response.latestBlock ?? 0),
  };
}

export async function getErc20TokenMetadataPage(
  client: TokensClient,
  options?: { contractAddress?: string; cursor?: string; limit?: number }
): Promise<PageResult<TokenMetadataResult, string>> {
  const contractAddress = options?.contractAddress;
  const cursor = options?.cursor;
  const limit = options?.limit ?? DEFAULT_PAGE_LIMIT;
  const request: Record<string, unknown> = {};
  if (contractAddress) {
    request.token = hexToBytes(contractAddress);
  }
  if (cursor) {
    request.cursor = hexToBytes(cursor);
  }
  request.limit = limit;

  const response = await client.call(
    "/torii.sinks.erc20.Erc20/GetTokenMetadata",
    request,
    Erc20GetTokenMetadataRequest,
    Erc20GetTokenMetadataResponse
  );

  const tokens = response.tokens;
  const list = Array.isArray(tokens) ? tokens : tokens ? [tokens] : [];

  const items = list.map((t: Record<string, unknown>) => ({
    token: bytesToHex(t.token as string | Uint8Array | undefined),
    name: t.name as string | undefined,
    symbol: t.symbol as string | undefined,
    decimals: t.decimals != null ? Number(t.decimals) : undefined,
  }));

  const nextCursor = response.nextCursor
    ? bytesToHex(response.nextCursor as string | Uint8Array | undefined)
    : undefined;

  return { items, nextCursor };
}

export async function getErc20TokenMetadata(
  client: TokensClient,
  contractAddress?: string
): Promise<TokenMetadataResult[]> {
  const page = await getErc20TokenMetadataPage(client, {
    contractAddress,
    limit: DEFAULT_PAGE_LIMIT,
  });
  return page.items;
}

export async function getErc20Balance(
  client: TokensClient,
  query: TokenQuery
): Promise<BalanceResult | null> {
  if (!query.contractAddress || !query.wallet) {
    return null;
  }

  // Fetch decimals from token metadata
  const metadata = await getErc20TokenMetadata(client, query.contractAddress);
  const decimals = metadata[0]?.decimals;

  return getErc20BalanceWithDecimals(
    client,
    query.contractAddress,
    query.wallet,
    decimals
  );
}

export async function getErc20BalancesForWallet(
  client: TokensClient,
  wallet: string,
  contractAddress?: string
): Promise<TokenBalanceResult[]> {
  return getErc20Balances(client, {
    contractAddress: contractAddress ?? "",
    wallet,
  });
}

export async function getErc20Balances(
  client: TokensClient,
  query: TokenQuery
): Promise<TokenBalanceResult[]> {
  const page = await getErc20BalancesPage(client, { ...query, limit: DEFAULT_PAGE_LIMIT });
  return page.items;
}

export async function getErc20BalancesPage(
  client: TokensClient,
  query: TokenQuery & { cursor?: number; limit?: number }
): Promise<PageResult<TokenBalanceResult, number>> {
  const hasContract = !!query.contractAddress;
  const hasWallet = !!query.wallet;

  if (!hasContract && !hasWallet) {
    return { items: [] };
  }

  let metaByToken = new Map<string, TokenMetadataResult>();
  if (hasContract) {
    const metadata = await getErc20TokenMetadata(client, query.contractAddress);
    metaByToken = new Map(metadata.map((m) => [m.token, m]));
  }

  const request: Record<string, unknown> = { limit: query.limit ?? DEFAULT_PAGE_LIMIT };
  if (hasContract) request.token = hexToBytes(query.contractAddress);
  if (hasWallet) request.wallet = hexToBytes(query.wallet);
  if (query.cursor !== undefined) request.cursor = query.cursor;

  const response = await client.call(
    "/torii.sinks.erc20.Erc20/GetBalances",
    request,
    Erc20GetBalancesRequest,
    Erc20GetBalancesResponse
  );

  const balances = response.balances;
  const rows = Array.isArray(balances) ? balances : balances ? [balances] : [];

  const items = rows.map((row) => {
    const r = row as Record<string, unknown>;
    const token = bytesToHex(r.token as string | Uint8Array | undefined);
    const wallet = bytesToHex(r.wallet as string | Uint8Array | undefined);
    const meta = metaByToken.get(token);
    return {
      token,
      wallet,
      symbol: meta?.symbol,
      balance: formatU256(r.balance as string | Uint8Array | undefined, meta?.decimals),
      balanceRaw: typeof r.balance === "string" ? r.balance : "",
      lastBlock: Number(r.lastBlock ?? 0),
    };
  });

  return {
    items,
    nextCursor: response.nextCursor != null ? Number(response.nextCursor) : undefined,
  };
}

export async function getErc20Transfers(
  client: TokensClient,
  query: TokenQuery,
  limit = 50
): Promise<TransferResult[]> {
  const page = await getErc20TransfersPage(client, { ...query, limit });
  return page.items;
}

export async function getErc20TransfersPage(
  client: TokensClient,
  query: TokenQuery & { cursor?: TransferCursorResult; limit?: number }
): Promise<PageResult<TransferResult, TransferCursorResult>> {
  const filter: Record<string, unknown> = {};
  if (query.wallet) filter.wallet = hexToBytes(query.wallet);
  if (query.contractAddress) filter.tokens = [hexToBytes(query.contractAddress)];

  // Fetch all token metadata to build a decimals lookup
  const allMetadata = await getErc20TokenMetadata(client, query.contractAddress || undefined);
  const decimalsMap = new Map<string, number>();
  for (const m of allMetadata) {
    if (m.decimals != null) {
      decimalsMap.set(m.token, m.decimals);
    }
  }

  const response = await client.call(
    "/torii.sinks.erc20.Erc20/GetTransfers",
    {
      filter,
      limit: query.limit ?? DEFAULT_PAGE_LIMIT,
      ...(query.cursor
        ? { cursor: { blockNumber: query.cursor.blockNumber, id: query.cursor.id } }
        : {}),
    },
    Erc20GetTransfersRequest,
    Erc20GetTransfersResponse
  );

  const transfers = response.transfers;
  const list = Array.isArray(transfers) ? transfers : transfers ? [transfers] : [];

  const items = list.map((t: Record<string, unknown>) => {
    const token = bytesToHex(t.token as string | Uint8Array | undefined);
    const decimals = decimalsMap.get(token);
    return {
      token,
      from: bytesToHex(t.from as string | Uint8Array | undefined),
      to: bytesToHex(t.to as string | Uint8Array | undefined),
      amount: formatU256(t.amount as string | Uint8Array | undefined, decimals),
      blockNumber: Number(t.blockNumber ?? 0),
      txHash: bytesToHex(t.txHash as string | Uint8Array | undefined),
      timestamp: Number(t.timestamp ?? 0),
    };
  });

  const next = response.nextCursor as Record<string, unknown> | undefined;
  const nextCursor = next
    ? {
        blockNumber: Number(next.blockNumber ?? 0),
        id: Number(next.id ?? 0),
      }
    : undefined;

  return { items, nextCursor };
}

export async function getErc721Transfers(
  client: TokensClient,
  query: TokenQuery,
  limit = 50
): Promise<TransferResult[]> {
  const page = await getErc721TransfersPage(client, { ...query, limit });
  return page.items;
}

export async function getErc721TransfersPage(
  client: TokensClient,
  query: TokenQuery & { cursor?: TransferCursorResult; limit?: number }
): Promise<PageResult<TransferResult, TransferCursorResult>> {
  const filter: Record<string, unknown> = {};
  if (query.wallet) filter.wallet = hexToBytes(query.wallet);
  if (query.contractAddress) filter.tokens = [hexToBytes(query.contractAddress)];

  const response = await client.call(
    "/torii.sinks.erc721.Erc721/GetTransfers",
    {
      filter,
      limit: query.limit ?? DEFAULT_PAGE_LIMIT,
      ...(query.cursor
        ? { cursor: { blockNumber: query.cursor.blockNumber, id: query.cursor.id } }
        : {}),
    },
    Erc721GetTransfersRequest,
    Erc721GetTransfersResponse
  );

  const transfers = response.transfers;
  const list = Array.isArray(transfers) ? transfers : transfers ? [transfers] : [];

  const items = list.map((t: Record<string, unknown>) => ({
    token: bytesToHex(t.token as string | Uint8Array | undefined),
    tokenId: bytesToHex(t.tokenId as string | Uint8Array | undefined),
    from: bytesToHex(t.from as string | Uint8Array | undefined),
    to: bytesToHex(t.to as string | Uint8Array | undefined),
    blockNumber: Number(t.blockNumber ?? 0),
    txHash: bytesToHex(t.txHash as string | Uint8Array | undefined),
    timestamp: Number(t.timestamp ?? 0),
  }));

  const next = response.nextCursor as Record<string, unknown> | undefined;
  const nextCursor = next
    ? {
        blockNumber: Number(next.blockNumber ?? 0),
        id: Number(next.id ?? 0),
      }
    : undefined;

  return { items, nextCursor };
}

export async function getErc721TokenMetadataPage(
  client: TokensClient,
  options?: { contractAddress?: string; cursor?: string; limit?: number }
): Promise<PageResult<TokenMetadataResult, string>> {
  const contractAddress = options?.contractAddress;
  const cursor = options?.cursor;
  const limit = options?.limit ?? DEFAULT_PAGE_LIMIT;
  const request: Record<string, unknown> = {};
  if (contractAddress) {
    request.token = hexToBytes(contractAddress);
  }
  if (cursor) {
    request.cursor = hexToBytes(cursor);
  }
  request.limit = limit;

  const response = await client.call(
    "/torii.sinks.erc721.Erc721/GetTokenMetadata",
    request,
    Erc721GetTokenMetadataRequest,
    Erc721GetTokenMetadataResponse
  );

  const tokens = response.tokens;
  const list = Array.isArray(tokens) ? tokens : tokens ? [tokens] : [];

  const items = list.map((t: Record<string, unknown>) => ({
    token: bytesToHex(t.token as string | Uint8Array | undefined),
    name: t.name as string | undefined,
    symbol: t.symbol as string | undefined,
    totalSupply: t.totalSupply ? formatU256(t.totalSupply as string | Uint8Array | undefined) : undefined,
  }));

  const nextCursor = response.nextCursor
    ? bytesToHex(response.nextCursor as string | Uint8Array | undefined)
    : undefined;

  return { items, nextCursor };
}

export async function getErc721TokenMetadata(
  client: TokensClient,
  contractAddress?: string
): Promise<TokenMetadataResult[]> {
  const page = await getErc721TokenMetadataPage(client, {
    contractAddress,
    limit: DEFAULT_PAGE_LIMIT,
  });
  return page.items;
}

export async function getErc1155TokenMetadataPage(
  client: TokensClient,
  options?: { contractAddress?: string; cursor?: string; limit?: number }
): Promise<PageResult<TokenMetadataResult, string>> {
  const contractAddress = options?.contractAddress;
  const cursor = options?.cursor;
  const limit = options?.limit ?? DEFAULT_PAGE_LIMIT;
  const request: Record<string, unknown> = {};
  if (contractAddress) {
    request.token = hexToBytes(contractAddress);
  }
  if (cursor) {
    request.cursor = hexToBytes(cursor);
  }
  request.limit = limit;

  const response = await client.call(
    "/torii.sinks.erc1155.Erc1155/GetTokenMetadata",
    request,
    Erc1155GetTokenMetadataRequest,
    Erc1155GetTokenMetadataResponse
  );

  const tokens = response.tokens;
  const list = Array.isArray(tokens) ? tokens : tokens ? [tokens] : [];

  const items = list.map((t: Record<string, unknown>) => ({
    token: bytesToHex(t.token as string | Uint8Array | undefined),
    name: t.name as string | undefined,
    symbol: t.symbol as string | undefined,
    totalSupply: t.totalSupply ? formatU256(t.totalSupply as string | Uint8Array | undefined) : undefined,
  }));

  const nextCursor = response.nextCursor
    ? bytesToHex(response.nextCursor as string | Uint8Array | undefined)
    : undefined;

  return { items, nextCursor };
}

export async function getErc1155TokenMetadata(
  client: TokensClient,
  contractAddress?: string
): Promise<TokenMetadataResult[]> {
  const page = await getErc1155TokenMetadataPage(client, {
    contractAddress,
    limit: DEFAULT_PAGE_LIMIT,
  });
  return page.items;
}

export async function getErc1155Balance(
  client: TokensClient,
  query: Erc1155TokenQuery
): Promise<BalanceResult> {
  const response = await client.call(
    "/torii.sinks.erc1155.Erc1155/GetBalance",
    {
      contract: hexToBytes(query.contractAddress),
      wallet: hexToBytes(query.wallet),
      tokenId: hexToBytes(query.tokenId),
    },
    Erc1155GetBalanceRequest,
    Erc1155GetBalanceResponse
  );

  return {
    balance: formatU256(response.balance as string | Uint8Array | undefined),
    balanceRaw: typeof response.balance === "string" ? response.balance : "",
    lastBlock: Number(response.lastBlock) || 0,
  };
}

export async function getErc1155Transfers(
  client: TokensClient,
  query: TokenQuery,
  limit = 50
): Promise<Erc1155TransferResult[]> {
  const page = await getErc1155TransfersPage(client, { ...query, limit });
  return page.items;
}

export async function getErc1155TransfersPage(
  client: TokensClient,
  query: TokenQuery & { cursor?: TransferCursorResult; limit?: number }
): Promise<PageResult<Erc1155TransferResult, TransferCursorResult>> {
  const filter: Record<string, unknown> = {};
  if (query.wallet) filter.wallet = hexToBytes(query.wallet);
  if (query.contractAddress) filter.tokens = [hexToBytes(query.contractAddress)];

  const response = await client.call(
    "/torii.sinks.erc1155.Erc1155/GetTransfers",
    {
      filter,
      limit: query.limit ?? DEFAULT_PAGE_LIMIT,
      ...(query.cursor
        ? { cursor: { blockNumber: query.cursor.blockNumber, id: query.cursor.id } }
        : {}),
    },
    Erc1155GetTransfersRequest,
    Erc1155GetTransfersResponse
  );

  const transfers = response.transfers;
  const list = Array.isArray(transfers) ? transfers : transfers ? [transfers] : [];

  const items = list.map((t: Record<string, unknown>) => {
    const rawValue = (t.value ?? t.amount) as string | Uint8Array | undefined;
    const formattedValue = formatU256(rawValue);

    return {
      token: bytesToHex(t.token as string | Uint8Array | undefined),
      operator: bytesToHex(t.operator as string | Uint8Array | undefined),
      from: bytesToHex(t.from as string | Uint8Array | undefined),
      to: bytesToHex(t.to as string | Uint8Array | undefined),
      tokenId: bytesToHex(t.tokenId as string | Uint8Array | undefined),
      value: formattedValue,
      amount: formattedValue,
      blockNumber: Number(t.blockNumber ?? 0),
      txHash: bytesToHex(t.txHash as string | Uint8Array | undefined),
      timestamp: Number(t.timestamp ?? 0),
      isBatch: Boolean(t.isBatch),
    };
  });

  const next = response.nextCursor as Record<string, unknown> | undefined;
  const nextCursor = next
    ? {
        blockNumber: Number(next.blockNumber ?? 0),
        id: Number(next.id ?? 0),
      }
    : undefined;

  return { items, nextCursor };
}
