import { useEffect, useState, useCallback, useRef } from "react";
import {
  createTokensClient,
  SERVER_URL,
  generateClientId,
  getErc20BalancesPage,
  getErc20TransfersPage,
  getErc20Stats,
  getErc20TokenMetadataPage,
  getErc721Stats,
  getErc721TransfersPage,
  getErc721TokenMetadataPage,
  getErc1155Stats,
  getErc1155TransfersPage,
  getErc1155TokenMetadataPage,
  type TransferCursorResult,
  type TokenBalanceResult,
  type TransferResult,
  type TokenMetadataResult,
} from "@torii-tokens/shared";
import StatusPanel from "./components/StatusPanel";
import TokenPanel from "./components/TokenPanel";
import UpdatesFeed from "./components/UpdatesFeed";
import QueryFilters from "./components/QueryFilters";
import QueryResults from "./components/QueryResults";

interface Stats {
  totalTransfers: number;
  totalApprovals?: number;
  uniqueTokens: number;
  uniqueAccounts: number;
}

interface Transfer {
  id: string;
  token: string;
  from: string;
  to: string;
  value?: string;
  amount?: string;
  tokenId?: string;
  blockNumber: number;
  timestamp: number;
}

interface Update {
  topic: string;
  updateType: number;
  timestamp: number;
  typeId: string;
  data?: unknown;
}

const client = createTokensClient(SERVER_URL);
const clientId = generateClientId();
const PAGE_SIZE = 100;

export default function App() {
  const [connected, setConnected] = useState(false);
  const [updates, setUpdates] = useState<Update[]>([]);
  const unsubscribeRef = useRef<(() => void) | null>(null);

  const [erc20Stats, setErc20Stats] = useState<Stats | null>(null);
  const [erc20Transfers, setErc20Transfers] = useState<Transfer[]>([]);
  const [erc721Stats, setErc721Stats] = useState<Stats | null>(null);
  const [erc721Transfers, setErc721Transfers] = useState<Transfer[]>([]);
  const [erc1155Stats, setErc1155Stats] = useState<Stats | null>(null);
  const [erc1155Transfers, setErc1155Transfers] = useState<Transfer[]>([]);

  const [erc20Metadata, setErc20Metadata] = useState<TokenMetadataResult[]>([]);
  const [erc721Metadata, setErc721Metadata] = useState<TokenMetadataResult[]>([]);
  const [erc1155Metadata, setErc1155Metadata] = useState<TokenMetadataResult[]>([]);

  const [queryContractAddress, setQueryContractAddress] = useState("");
  const [queryWallet, setQueryWallet] = useState("");
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryBalancesLoading, setQueryBalancesLoading] = useState(false);
  const [queryTransfersLoading, setQueryTransfersLoading] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [queryErc20Balances, setQueryErc20Balances] = useState<TokenBalanceResult[]>([]);
  const [queryErc20Transfers, setQueryErc20Transfers] = useState<TransferResult[]>([]);
  const [erc20MetadataLoading, setErc20MetadataLoading] = useState(false);
  const [erc721MetadataLoading, setErc721MetadataLoading] = useState(false);
  const [erc1155MetadataLoading, setErc1155MetadataLoading] = useState(false);
  const [erc20TransfersLoading, setErc20TransfersLoading] = useState(false);
  const [erc721TransfersLoading, setErc721TransfersLoading] = useState(false);
  const [erc1155TransfersLoading, setErc1155TransfersLoading] = useState(false);
  const [erc20TransfersHistory, setErc20TransfersHistory] = useState<(TransferCursorResult | undefined)[]>([]);
  const [erc20TransfersCursor, setErc20TransfersCursor] = useState<TransferCursorResult | undefined>(undefined);
  const [erc20TransfersNext, setErc20TransfersNext] = useState<TransferCursorResult | undefined>(undefined);
  const [erc721TransfersHistory, setErc721TransfersHistory] = useState<(TransferCursorResult | undefined)[]>([]);
  const [erc721TransfersCursor, setErc721TransfersCursor] = useState<TransferCursorResult | undefined>(undefined);
  const [erc721TransfersNext, setErc721TransfersNext] = useState<TransferCursorResult | undefined>(undefined);
  const [erc1155TransfersHistory, setErc1155TransfersHistory] = useState<(TransferCursorResult | undefined)[]>([]);
  const [erc1155TransfersCursor, setErc1155TransfersCursor] = useState<TransferCursorResult | undefined>(undefined);
  const [erc1155TransfersNext, setErc1155TransfersNext] = useState<TransferCursorResult | undefined>(undefined);
  const [erc20MetadataHistory, setErc20MetadataHistory] = useState<(string | undefined)[]>([]);
  const [erc20MetadataCursor, setErc20MetadataCursor] = useState<string | undefined>(undefined);
  const [erc20MetadataNext, setErc20MetadataNext] = useState<string | undefined>(undefined);
  const [erc721MetadataHistory, setErc721MetadataHistory] = useState<(string | undefined)[]>([]);
  const [erc721MetadataCursor, setErc721MetadataCursor] = useState<string | undefined>(undefined);
  const [erc721MetadataNext, setErc721MetadataNext] = useState<string | undefined>(undefined);
  const [erc1155MetadataHistory, setErc1155MetadataHistory] = useState<(string | undefined)[]>([]);
  const [erc1155MetadataCursor, setErc1155MetadataCursor] = useState<string | undefined>(undefined);
  const [erc1155MetadataNext, setErc1155MetadataNext] = useState<string | undefined>(undefined);
  const [queryBalancesHistory, setQueryBalancesHistory] = useState<(number | undefined)[]>([]);
  const [queryBalancesCursor, setQueryBalancesCursor] = useState<number | undefined>(undefined);
  const [queryBalancesNext, setQueryBalancesNext] = useState<number | undefined>(undefined);
  const [queryTransfersHistory, setQueryTransfersHistory] = useState<(TransferCursorResult | undefined)[]>([]);
  const [queryTransfersCursor, setQueryTransfersCursor] = useState<TransferCursorResult | undefined>(undefined);
  const [queryTransfersNext, setQueryTransfersNext] = useState<TransferCursorResult | undefined>(undefined);

  const loadStats = useCallback(async () => {
    try {
      const [s20, s721, s1155] = await Promise.all([
        getErc20Stats(client),
        getErc721Stats(client),
        getErc1155Stats(client),
      ]);
      setErc20Stats({
        totalTransfers: s20.totalTransfers,
        totalApprovals: s20.totalApprovals,
        uniqueTokens: s20.uniqueTokens,
        uniqueAccounts: 0,
      });
      setErc721Stats({
        totalTransfers: s721.totalTransfers,
        uniqueTokens: s721.uniqueTokens,
        uniqueAccounts: s721.uniqueNfts ?? 0,
      });
      setErc1155Stats({
        totalTransfers: s1155.totalTransfers,
        uniqueTokens: s1155.uniqueTokens,
        uniqueAccounts: s1155.uniqueTokenIds ?? 0,
      });
    } catch (err) {
      console.error("Failed to load stats:", err);
      setErc20Stats({ totalTransfers: 0, totalApprovals: 0, uniqueTokens: 0, uniqueAccounts: 0 });
      setErc721Stats({ totalTransfers: 0, uniqueTokens: 0, uniqueAccounts: 0 });
      setErc1155Stats({ totalTransfers: 0, uniqueTokens: 0, uniqueAccounts: 0 });
    }
  }, []);

  const loadMetadata = useCallback(async (
    tokenType: "erc20" | "erc721" | "erc1155",
    cursor?: string,
  ) => {
    try {
      if (tokenType === "erc20") {
        setErc20MetadataLoading(true);
        const page = await getErc20TokenMetadataPage(client, { cursor, limit: PAGE_SIZE });
        setErc20Metadata(page.items);
        setErc20MetadataNext(page.nextCursor);
        return;
      }
      if (tokenType === "erc721") {
        setErc721MetadataLoading(true);
        const page = await getErc721TokenMetadataPage(client, { cursor, limit: PAGE_SIZE });
        setErc721Metadata(page.items);
        setErc721MetadataNext(page.nextCursor);
        return;
      }
      setErc1155MetadataLoading(true);
      const page = await getErc1155TokenMetadataPage(client, { cursor, limit: PAGE_SIZE });
      setErc1155Metadata(page.items);
      setErc1155MetadataNext(page.nextCursor);
    } catch (err) {
      console.error("Failed to load metadata:", err);
    } finally {
      if (tokenType === "erc20") setErc20MetadataLoading(false);
      if (tokenType === "erc721") setErc721MetadataLoading(false);
      if (tokenType === "erc1155") setErc1155MetadataLoading(false);
    }
  }, []);

  const loadTransfers = useCallback(async (
    tokenType: "erc20" | "erc721" | "erc1155",
    cursor?: TransferCursorResult,
  ) => {
    const emptyQuery = { contractAddress: "", wallet: "" };
    try {
      if (tokenType === "erc20") {
        setErc20TransfersLoading(true);
        const page = await getErc20TransfersPage(client, { ...emptyQuery, cursor, limit: PAGE_SIZE });
        setErc20TransfersNext(page.nextCursor);
        setErc20Transfers(page.items.map((t, i) => ({
          id: `${t.txHash}-${i}`,
          token: t.token,
          from: t.from,
          to: t.to,
          amount: t.amount,
          blockNumber: t.blockNumber,
          timestamp: t.timestamp,
        })));
        return;
      }
      if (tokenType === "erc721") {
        setErc721TransfersLoading(true);
        const page = await getErc721TransfersPage(client, { ...emptyQuery, cursor, limit: PAGE_SIZE });
        setErc721TransfersNext(page.nextCursor);
        setErc721Transfers(page.items.map((t, i) => ({
          id: `${t.txHash}-${i}`,
          token: t.token,
          from: t.from,
          to: t.to,
          tokenId: (t as unknown as Record<string, unknown>).tokenId as string | undefined,
          blockNumber: t.blockNumber,
          timestamp: t.timestamp,
        })));
        return;
      }
      setErc1155TransfersLoading(true);
      const page = await getErc1155TransfersPage(client, { ...emptyQuery, cursor, limit: PAGE_SIZE });
      setErc1155TransfersNext(page.nextCursor);
      setErc1155Transfers(page.items.map((t, i) => ({
        id: `${t.txHash}-${i}`,
        token: t.token,
        from: t.from,
        to: t.to,
        value: t.value,
        amount: t.amount,
        tokenId: t.tokenId,
        blockNumber: t.blockNumber,
        timestamp: t.timestamp,
      })));
    } catch (err) {
      console.error("Failed to load transfers:", err);
    } finally {
      if (tokenType === "erc20") setErc20TransfersLoading(false);
      if (tokenType === "erc721") setErc721TransfersLoading(false);
      if (tokenType === "erc1155") setErc1155TransfersLoading(false);
    }
  }, []);

  const handleQuery = useCallback(async (contractAddress: string, wallet: string) => {
    setQueryContractAddress(contractAddress);
    setQueryWallet(wallet);
    setQueryBalancesHistory([]);
    setQueryTransfersHistory([]);
    setQueryBalancesCursor(undefined);
    setQueryTransfersCursor(undefined);
    setQueryLoading(true);
    setQueryBalancesLoading(true);
    setQueryTransfersLoading(true);
    setQueryError(null);

    try {
      const [balances, transfers] = await Promise.all([
        getErc20BalancesPage(client, { contractAddress, wallet, limit: PAGE_SIZE }),
        getErc20TransfersPage(client, { contractAddress, wallet, limit: PAGE_SIZE }),
      ]);
      setQueryErc20Balances(balances.items);
      setQueryBalancesNext(balances.nextCursor);
      setQueryErc20Transfers(transfers.items);
      setQueryTransfersNext(transfers.nextCursor);
    } catch (err) {
      console.error("Query failed:", err);
      setQueryError(err instanceof Error ? err.message : "Query failed");
    } finally {
      setQueryBalancesLoading(false);
      setQueryTransfersLoading(false);
      setQueryLoading(false);
    }
  }, []);

  const navigateDashboardTransfers = useCallback(async (
    tokenType: "erc20" | "erc721" | "erc1155",
    dir: "next" | "prev"
  ) => {
    if (tokenType === "erc20") {
      const target = dir === "next" ? erc20TransfersNext : erc20TransfersHistory[erc20TransfersHistory.length - 1];
      if (!target && dir === "next") return;
      const nextHistory = dir === "next"
        ? [...erc20TransfersHistory, erc20TransfersCursor]
        : erc20TransfersHistory.slice(0, -1);
      setErc20TransfersHistory(nextHistory);
      setErc20TransfersCursor(target);
      await loadTransfers("erc20", target);
      return;
    }
    if (tokenType === "erc721") {
      const target = dir === "next" ? erc721TransfersNext : erc721TransfersHistory[erc721TransfersHistory.length - 1];
      if (!target && dir === "next") return;
      const nextHistory = dir === "next"
        ? [...erc721TransfersHistory, erc721TransfersCursor]
        : erc721TransfersHistory.slice(0, -1);
      setErc721TransfersHistory(nextHistory);
      setErc721TransfersCursor(target);
      await loadTransfers("erc721", target);
      return;
    }
    const target = dir === "next" ? erc1155TransfersNext : erc1155TransfersHistory[erc1155TransfersHistory.length - 1];
    if (!target && dir === "next") return;
    const nextHistory = dir === "next"
      ? [...erc1155TransfersHistory, erc1155TransfersCursor]
      : erc1155TransfersHistory.slice(0, -1);
    setErc1155TransfersHistory(nextHistory);
    setErc1155TransfersCursor(target);
    await loadTransfers("erc1155", target);
  }, [erc20TransfersNext, erc20TransfersHistory, erc20TransfersCursor, erc721TransfersNext, erc721TransfersHistory, erc1155TransfersNext, erc1155TransfersHistory, erc1155TransfersCursor, loadTransfers]);

  const navigateDashboardMetadata = useCallback(async (
    tokenType: "erc20" | "erc721" | "erc1155",
    dir: "next" | "prev"
  ) => {
    if (tokenType === "erc20") {
      const target = dir === "next" ? erc20MetadataNext : erc20MetadataHistory[erc20MetadataHistory.length - 1];
      if (!target && dir === "next") return;
      setErc20MetadataHistory(dir === "next" ? [...erc20MetadataHistory, erc20MetadataCursor] : erc20MetadataHistory.slice(0, -1));
      setErc20MetadataCursor(target);
      await loadMetadata("erc20", target);
      return;
    }
    if (tokenType === "erc721") {
      const target = dir === "next" ? erc721MetadataNext : erc721MetadataHistory[erc721MetadataHistory.length - 1];
      if (!target && dir === "next") return;
      setErc721MetadataHistory(dir === "next" ? [...erc721MetadataHistory, erc721MetadataCursor] : erc721MetadataHistory.slice(0, -1));
      setErc721MetadataCursor(target);
      await loadMetadata("erc721", target);
      return;
    }
    const target = dir === "next" ? erc1155MetadataNext : erc1155MetadataHistory[erc1155MetadataHistory.length - 1];
    if (!target && dir === "next") return;
    setErc1155MetadataHistory(dir === "next" ? [...erc1155MetadataHistory, erc1155MetadataCursor] : erc1155MetadataHistory.slice(0, -1));
    setErc1155MetadataCursor(target);
    await loadMetadata("erc1155", target);
  }, [erc20MetadataNext, erc20MetadataHistory, erc20MetadataCursor, erc721MetadataNext, erc721MetadataHistory, erc1155MetadataNext, erc1155MetadataHistory, erc1155MetadataCursor, loadMetadata]);

  const navigateQueryBalances = useCallback(async (dir: "next" | "prev") => {
    const target = dir === "next" ? queryBalancesNext : queryBalancesHistory[queryBalancesHistory.length - 1];
    if (target == null && dir === "next") return;
    setQueryBalancesLoading(true);
    setQueryError(null);
    try {
      setQueryBalancesHistory(dir === "next" ? [...queryBalancesHistory, queryBalancesCursor] : queryBalancesHistory.slice(0, -1));
      setQueryBalancesCursor(target);
      const page = await getErc20BalancesPage(client, {
        contractAddress: queryContractAddress,
        wallet: queryWallet,
        cursor: target,
        limit: PAGE_SIZE,
      });
      setQueryErc20Balances(page.items);
      setQueryBalancesNext(page.nextCursor);
    } catch (err) {
      console.error("Balance pagination failed:", err);
      setQueryError(err instanceof Error ? err.message : "Balance pagination failed");
    } finally {
      setQueryBalancesLoading(false);
    }
  }, [queryBalancesNext, queryBalancesHistory, queryBalancesCursor, queryContractAddress, queryWallet]);

  const navigateQueryTransfers = useCallback(async (dir: "next" | "prev") => {
    const target = dir === "next" ? queryTransfersNext : queryTransfersHistory[queryTransfersHistory.length - 1];
    if (!target && dir === "next") return;
    setQueryTransfersLoading(true);
    setQueryError(null);
    try {
      setQueryTransfersHistory(dir === "next" ? [...queryTransfersHistory, queryTransfersCursor] : queryTransfersHistory.slice(0, -1));
      setQueryTransfersCursor(target);
      const page = await getErc20TransfersPage(client, {
        contractAddress: queryContractAddress,
        wallet: queryWallet,
        cursor: target,
        limit: PAGE_SIZE,
      });
      setQueryErc20Transfers(page.items);
      setQueryTransfersNext(page.nextCursor);
    } catch (err) {
      console.error("Transfer pagination failed:", err);
      setQueryError(err instanceof Error ? err.message : "Transfer pagination failed");
    } finally {
      setQueryTransfersLoading(false);
    }
  }, [queryTransfersNext, queryTransfersHistory, queryTransfersCursor, queryContractAddress, queryWallet]);

  const subscribe = useCallback(async () => {
    try {
      unsubscribeRef.current = await client.subscribeTopics(
        clientId,
        [
          { topic: "erc20.transfer" },
          { topic: "erc20.metadata" },
          { topic: "erc721.transfer" },
          { topic: "erc721.metadata" },
          { topic: "erc1155.transfer" },
          { topic: "erc1155.metadata" },
          { topic: "erc1155.uri" },
        ],
        (update) => {
          setUpdates((prev) => [update as Update, ...prev].slice(0, 50));
        },
        (err) => {
          console.error("Subscription error:", err);
          setConnected(false);
        },
        () => {
          setConnected(true);
        }
      );
    } catch (err) {
      console.error("Failed to subscribe:", err);
    }
  }, []);

  const disconnect = useCallback(() => {
    if (unsubscribeRef.current) {
      unsubscribeRef.current();
      unsubscribeRef.current = null;
    }
    setConnected(false);
  }, []);

  const clearUpdates = useCallback(() => {
    setUpdates([]);
  }, []);

  useEffect(() => {
    loadStats();
    void Promise.all([
      loadTransfers("erc20"),
      loadTransfers("erc721"),
      loadTransfers("erc1155"),
      loadMetadata("erc20"),
      loadMetadata("erc721"),
      loadMetadata("erc1155"),
    ]);
    return () => {
      disconnect();
    };
  }, [loadStats, loadTransfers, loadMetadata, disconnect]);

  return (
    <div className="container">
      <header>
        <h1>Torii Tokens - React</h1>
        <p className="subtitle">ERC20 / ERC721 / ERC1155 Token Indexer</p>
      </header>

      <StatusPanel
        connected={connected}
        clientId={clientId}
        serverUrl={SERVER_URL}
        updateCount={updates.length}
        onSubscribe={subscribe}
        onDisconnect={disconnect}
      />

      <QueryFilters onQuery={handleQuery} loading={queryLoading} />

      <QueryResults
        contractAddress={queryContractAddress}
        wallet={queryWallet}
        erc20Balances={queryErc20Balances}
        erc20Transfers={queryErc20Transfers}
        loading={queryLoading}
        error={queryError}
        onBalancesPrev={() => navigateQueryBalances("prev")}
        onBalancesNext={() => navigateQueryBalances("next")}
        balancesCanPrev={queryBalancesHistory.length > 0}
        balancesCanNext={queryBalancesNext != null}
        onTransfersPrev={() => navigateQueryTransfers("prev")}
        onTransfersNext={() => navigateQueryTransfers("next")}
        transfersCanPrev={queryTransfersHistory.length > 0}
        transfersCanNext={queryTransfersNext != null}
        balancesLoading={queryBalancesLoading}
        transfersLoading={queryTransfersLoading}
      />

      <div className="panels">
        <TokenPanel
          title="ERC20 Tokens"
          tokenType="erc20"
          stats={erc20Stats}
          transfers={erc20Transfers}
          metadata={erc20Metadata}
          showAmount={true}
          onMetadataPrev={() => navigateDashboardMetadata("erc20", "prev")}
          onMetadataNext={() => navigateDashboardMetadata("erc20", "next")}
          metadataCanPrev={erc20MetadataHistory.length > 0}
          metadataCanNext={erc20MetadataNext != null}
          onTransfersPrev={() => navigateDashboardTransfers("erc20", "prev")}
          onTransfersNext={() => navigateDashboardTransfers("erc20", "next")}
          transfersCanPrev={erc20TransfersHistory.length > 0}
          transfersCanNext={erc20TransfersNext != null}
          metadataLoading={erc20MetadataLoading}
          transfersLoading={erc20TransfersLoading}
        />

        <TokenPanel
          title="ERC721 NFTs"
          tokenType="erc721"
          stats={erc721Stats}
          transfers={erc721Transfers}
          metadata={erc721Metadata}
          showAmount={false}
          onMetadataPrev={() => navigateDashboardMetadata("erc721", "prev")}
          onMetadataNext={() => navigateDashboardMetadata("erc721", "next")}
          metadataCanPrev={erc721MetadataHistory.length > 0}
          metadataCanNext={erc721MetadataNext != null}
          onTransfersPrev={() => navigateDashboardTransfers("erc721", "prev")}
          onTransfersNext={() => navigateDashboardTransfers("erc721", "next")}
          transfersCanPrev={erc721TransfersHistory.length > 0}
          transfersCanNext={erc721TransfersNext != null}
          metadataLoading={erc721MetadataLoading}
          transfersLoading={erc721TransfersLoading}
        />

        <TokenPanel
          title="ERC1155 Multi-Tokens"
          tokenType="erc1155"
          stats={erc1155Stats}
          transfers={erc1155Transfers}
          metadata={erc1155Metadata}
          showAmount={true}
          onMetadataPrev={() => navigateDashboardMetadata("erc1155", "prev")}
          onMetadataNext={() => navigateDashboardMetadata("erc1155", "next")}
          metadataCanPrev={erc1155MetadataHistory.length > 0}
          metadataCanNext={erc1155MetadataNext != null}
          onTransfersPrev={() => navigateDashboardTransfers("erc1155", "prev")}
          onTransfersNext={() => navigateDashboardTransfers("erc1155", "next")}
          transfersCanPrev={erc1155TransfersHistory.length > 0}
          transfersCanNext={erc1155TransfersNext != null}
          metadataLoading={erc1155MetadataLoading}
          transfersLoading={erc1155TransfersLoading}
        />

        <UpdatesFeed
          updates={updates}
          connected={connected}
          onClear={clearUpdates}
        />
      </div>
    </div>
  );
}
