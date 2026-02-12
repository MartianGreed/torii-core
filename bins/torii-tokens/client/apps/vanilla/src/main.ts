import "../../../shared/styles/shared.css";
import {
  createTokensClient,
  SERVER_URL,
  formatTimestamp,
  truncateAddress,
  getContractExplorerUrl,
  getUpdateTypeName,
  generateClientId,
  getErc20Stats,
  getErc721Stats,
  getErc1155Stats,
  getErc20TransfersPage,
  getErc721TransfersPage,
  getErc1155TransfersPage,
  getErc20BalancesPage,
  getErc20TokenMetadataPage,
  getErc721TokenMetadataPage,
  getErc1155TokenMetadataPage,
  type TransferCursorResult,
  type TokenBalanceResult,
  type TransferResult,
  type TokenMetadataResult,
} from "@torii-tokens/shared";

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

class TokensApp {
  private readonly pageSize = 100;
  private client = createTokensClient(SERVER_URL);
  private clientId = generateClientId();
  private connected = false;
  private updates: Update[] = [];
  private unsubscribe: (() => void) | null = null;

  private erc20Stats: Stats | null = null;
  private erc20Transfers: Transfer[] = [];
  private erc721Stats: Stats | null = null;
  private erc721Transfers: Transfer[] = [];
  private erc1155Stats: Stats | null = null;
  private erc1155Transfers: Transfer[] = [];

  private erc20Metadata: TokenMetadataResult[] = [];
  private erc721Metadata: TokenMetadataResult[] = [];
  private erc1155Metadata: TokenMetadataResult[] = [];

  private queryContractAddress = "";
  private queryWallet = "";
  private queryLoading = false;
  private queryBalancesLoading = false;
  private queryTransfersLoading = false;
  private queryError: string | null = null;
  private queryErc20Balances: TokenBalanceResult[] = [];
  private queryErc20Transfers: TransferResult[] = [];
  private erc20MetadataLoading = false;
  private erc721MetadataLoading = false;
  private erc1155MetadataLoading = false;
  private erc20TransfersLoading = false;
  private erc721TransfersLoading = false;
  private erc1155TransfersLoading = false;
  private erc20TransfersHistory: (TransferCursorResult | undefined)[] = [];
  private erc20TransfersCursor?: TransferCursorResult;
  private erc20TransfersNext?: TransferCursorResult;
  private erc721TransfersHistory: (TransferCursorResult | undefined)[] = [];
  private erc721TransfersCursor?: TransferCursorResult;
  private erc721TransfersNext?: TransferCursorResult;
  private erc1155TransfersHistory: (TransferCursorResult | undefined)[] = [];
  private erc1155TransfersCursor?: TransferCursorResult;
  private erc1155TransfersNext?: TransferCursorResult;
  private erc20MetadataHistory: (string | undefined)[] = [];
  private erc20MetadataCursor?: string;
  private erc20MetadataNext?: string;
  private erc721MetadataHistory: (string | undefined)[] = [];
  private erc721MetadataCursor?: string;
  private erc721MetadataNext?: string;
  private erc1155MetadataHistory: (string | undefined)[] = [];
  private erc1155MetadataCursor?: string;
  private erc1155MetadataNext?: string;
  private queryBalancesHistory: (number | undefined)[] = [];
  private queryBalancesCursor?: number;
  private queryBalancesNext?: number;
  private queryTransfersHistory: (TransferCursorResult | undefined)[] = [];
  private queryTransfersCursor?: TransferCursorResult;
  private queryTransfersNext?: TransferCursorResult;

  constructor() {
    this.render();
    this.init();
  }

  private async init() {
    await this.checkHealth();
    await this.loadAllStats();
    await Promise.all([
      this.loadTransfers("erc20"),
      this.loadTransfers("erc721"),
      this.loadTransfers("erc1155"),
      this.loadMetadata("erc20"),
      this.loadMetadata("erc721"),
      this.loadMetadata("erc1155"),
    ]);
    this.render();
  }

  private async checkHealth() {
    try {
      await this.client.getVersion();
      this.render();
    } catch (err) {
      console.error("Health check failed:", err);
    }
  }

  private async loadAllStats() {
    try {
      const [s20, s721, s1155] = await Promise.all([
        getErc20Stats(this.client),
        getErc721Stats(this.client),
        getErc1155Stats(this.client),
      ]);
      this.erc20Stats = {
        totalTransfers: s20.totalTransfers,
        totalApprovals: s20.totalApprovals,
        uniqueTokens: s20.uniqueTokens,
        uniqueAccounts: 0,
      };
      this.erc721Stats = {
        totalTransfers: s721.totalTransfers,
        uniqueTokens: s721.uniqueTokens,
        uniqueAccounts: s721.uniqueNfts ?? 0,
      };
      this.erc1155Stats = {
        totalTransfers: s1155.totalTransfers,
        uniqueTokens: s1155.uniqueTokens,
        uniqueAccounts: s1155.uniqueTokenIds ?? 0,
      };
    } catch (err) {
      console.error("Failed to load stats:", err);
      this.erc20Stats = { totalTransfers: 0, totalApprovals: 0, uniqueTokens: 0, uniqueAccounts: 0 };
      this.erc721Stats = { totalTransfers: 0, uniqueTokens: 0, uniqueAccounts: 0 };
      this.erc1155Stats = { totalTransfers: 0, uniqueTokens: 0, uniqueAccounts: 0 };
    }
  }

  private async loadTransfers(tokenType: "erc20" | "erc721" | "erc1155", cursor?: TransferCursorResult) {
    const emptyQuery = { contractAddress: "", wallet: "" };
    try {
      if (tokenType === "erc20") {
        this.erc20TransfersLoading = true;
        const page = await getErc20TransfersPage(this.client, { ...emptyQuery, cursor, limit: this.pageSize });
        this.erc20TransfersNext = page.nextCursor;
        this.erc20Transfers = page.items.map((t, i) => ({
          id: `${t.txHash}-${i}`,
          token: t.token,
          from: t.from,
          to: t.to,
          amount: t.amount,
          blockNumber: t.blockNumber,
          timestamp: t.timestamp,
        }));
        return;
      }
      if (tokenType === "erc721") {
        this.erc721TransfersLoading = true;
        const page = await getErc721TransfersPage(this.client, { ...emptyQuery, cursor, limit: this.pageSize });
        this.erc721TransfersNext = page.nextCursor;
        this.erc721Transfers = page.items.map((t, i) => ({
          id: `${t.txHash}-${i}`,
          token: t.token,
          from: t.from,
          to: t.to,
          tokenId: (t as unknown as Record<string, unknown>).tokenId as string | undefined,
          blockNumber: t.blockNumber,
          timestamp: t.timestamp,
        }));
        return;
      }
      this.erc1155TransfersLoading = true;
      const page = await getErc1155TransfersPage(this.client, { ...emptyQuery, cursor, limit: this.pageSize });
      this.erc1155TransfersNext = page.nextCursor;
      this.erc1155Transfers = page.items.map((t, i) => ({
        id: `${t.txHash}-${i}`,
        token: t.token,
        from: t.from,
        to: t.to,
        value: t.value,
        amount: t.amount,
        tokenId: t.tokenId,
        blockNumber: t.blockNumber,
        timestamp: t.timestamp,
      }));
    } catch (err) {
      console.error("Failed to load transfers:", err);
    } finally {
      if (tokenType === "erc20") this.erc20TransfersLoading = false;
      if (tokenType === "erc721") this.erc721TransfersLoading = false;
      if (tokenType === "erc1155") this.erc1155TransfersLoading = false;
    }
  }

  private async loadMetadata(tokenType: "erc20" | "erc721" | "erc1155", cursor?: string) {
    try {
      if (tokenType === "erc20") {
        this.erc20MetadataLoading = true;
        const page = await getErc20TokenMetadataPage(this.client, { cursor, limit: this.pageSize });
        this.erc20Metadata = page.items;
        this.erc20MetadataNext = page.nextCursor;
        return;
      }
      if (tokenType === "erc721") {
        this.erc721MetadataLoading = true;
        const page = await getErc721TokenMetadataPage(this.client, { cursor, limit: this.pageSize });
        this.erc721Metadata = page.items;
        this.erc721MetadataNext = page.nextCursor;
        return;
      }
      this.erc1155MetadataLoading = true;
      const page = await getErc1155TokenMetadataPage(this.client, { cursor, limit: this.pageSize });
      this.erc1155Metadata = page.items;
      this.erc1155MetadataNext = page.nextCursor;
    } catch (err) {
      console.error("Failed to load metadata:", err);
    } finally {
      if (tokenType === "erc20") this.erc20MetadataLoading = false;
      if (tokenType === "erc721") this.erc721MetadataLoading = false;
      if (tokenType === "erc1155") this.erc1155MetadataLoading = false;
    }
  }

  private async handleQuery(contractAddress: string, wallet: string) {
    this.queryContractAddress = contractAddress;
    this.queryWallet = wallet;
    this.queryBalancesHistory = [];
    this.queryTransfersHistory = [];
    this.queryBalancesCursor = undefined;
    this.queryTransfersCursor = undefined;
    this.queryLoading = true;
    this.queryBalancesLoading = true;
    this.queryTransfersLoading = true;
    this.queryError = null;
    this.render();

    try {
      const [balances, transfers] = await Promise.all([
        getErc20BalancesPage(this.client, { contractAddress, wallet, limit: this.pageSize }),
        getErc20TransfersPage(this.client, { contractAddress, wallet, limit: this.pageSize }),
      ]);
      this.queryErc20Balances = balances.items;
      this.queryBalancesNext = balances.nextCursor;
      this.queryErc20Transfers = transfers.items;
      this.queryTransfersNext = transfers.nextCursor;
    } catch (err) {
      console.error("Query failed:", err);
      this.queryError = err instanceof Error ? err.message : "Query failed";
    } finally {
      this.queryBalancesLoading = false;
      this.queryTransfersLoading = false;
      this.queryLoading = false;
      this.render();
    }
  }

  private async subscribe() {
    try {
      this.unsubscribe = await this.client.subscribeTopics(
        this.clientId,
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
          this.updates = [update as Update, ...this.updates].slice(0, 50);
          this.render();
        },
        (err) => {
          console.error("Subscription error:", err);
          this.connected = false;
          this.render();
        },
        () => {
          this.connected = true;
          this.render();
        }
      );
    } catch (err) {
      console.error("Failed to subscribe:", err);
    }
  }

  private disconnect() {
    if (this.unsubscribe) {
      this.unsubscribe();
      this.unsubscribe = null;
    }
    this.connected = false;
    this.render();
  }

  private clearUpdates() {
    this.updates = [];
    this.render();
  }

  private async navigateDashboardTransfers(tokenType: "erc20" | "erc721" | "erc1155", dir: "next" | "prev") {
    if (tokenType === "erc20") {
      const target = dir === "next" ? this.erc20TransfersNext : this.erc20TransfersHistory[this.erc20TransfersHistory.length - 1];
      if (!target && dir === "next") return;
      this.erc20TransfersHistory = dir === "next"
        ? [...this.erc20TransfersHistory, this.erc20TransfersCursor]
        : this.erc20TransfersHistory.slice(0, -1);
      this.erc20TransfersCursor = target;
      this.render();
      await this.loadTransfers("erc20", target);
      this.render();
      return;
    }
    if (tokenType === "erc721") {
      const target = dir === "next" ? this.erc721TransfersNext : this.erc721TransfersHistory[this.erc721TransfersHistory.length - 1];
      if (!target && dir === "next") return;
      this.erc721TransfersHistory = dir === "next"
        ? [...this.erc721TransfersHistory, this.erc721TransfersCursor]
        : this.erc721TransfersHistory.slice(0, -1);
      this.erc721TransfersCursor = target;
      this.render();
      await this.loadTransfers("erc721", target);
      this.render();
      return;
    }
    const target = dir === "next" ? this.erc1155TransfersNext : this.erc1155TransfersHistory[this.erc1155TransfersHistory.length - 1];
    if (!target && dir === "next") return;
    this.erc1155TransfersHistory = dir === "next"
      ? [...this.erc1155TransfersHistory, this.erc1155TransfersCursor]
      : this.erc1155TransfersHistory.slice(0, -1);
    this.erc1155TransfersCursor = target;
    this.render();
    await this.loadTransfers("erc1155", target);
    this.render();
  }

  private async navigateDashboardMetadata(tokenType: "erc20" | "erc721" | "erc1155", dir: "next" | "prev") {
    if (tokenType === "erc20") {
      const target = dir === "next" ? this.erc20MetadataNext : this.erc20MetadataHistory[this.erc20MetadataHistory.length - 1];
      if (!target && dir === "next") return;
      this.erc20MetadataHistory = dir === "next"
        ? [...this.erc20MetadataHistory, this.erc20MetadataCursor]
        : this.erc20MetadataHistory.slice(0, -1);
      this.erc20MetadataCursor = target;
      this.render();
      await this.loadMetadata("erc20", target);
      this.render();
      return;
    }
    if (tokenType === "erc721") {
      const target = dir === "next" ? this.erc721MetadataNext : this.erc721MetadataHistory[this.erc721MetadataHistory.length - 1];
      if (!target && dir === "next") return;
      this.erc721MetadataHistory = dir === "next"
        ? [...this.erc721MetadataHistory, this.erc721MetadataCursor]
        : this.erc721MetadataHistory.slice(0, -1);
      this.erc721MetadataCursor = target;
      this.render();
      await this.loadMetadata("erc721", target);
      this.render();
      return;
    }
    const target = dir === "next" ? this.erc1155MetadataNext : this.erc1155MetadataHistory[this.erc1155MetadataHistory.length - 1];
    if (!target && dir === "next") return;
    this.erc1155MetadataHistory = dir === "next"
      ? [...this.erc1155MetadataHistory, this.erc1155MetadataCursor]
      : this.erc1155MetadataHistory.slice(0, -1);
    this.erc1155MetadataCursor = target;
    this.render();
    await this.loadMetadata("erc1155", target);
    this.render();
  }

  private async navigateQueryBalances(dir: "next" | "prev") {
    const target = dir === "next" ? this.queryBalancesNext : this.queryBalancesHistory[this.queryBalancesHistory.length - 1];
    if (target == null && dir === "next") return;
    this.queryBalancesLoading = true;
    this.queryError = null;
    this.render();
    try {
      this.queryBalancesHistory = dir === "next"
        ? [...this.queryBalancesHistory, this.queryBalancesCursor]
        : this.queryBalancesHistory.slice(0, -1);
      this.queryBalancesCursor = target;
      const page = await getErc20BalancesPage(this.client, {
        contractAddress: this.queryContractAddress,
        wallet: this.queryWallet,
        cursor: target,
        limit: this.pageSize,
      });
      this.queryErc20Balances = page.items;
      this.queryBalancesNext = page.nextCursor;
    } catch (err) {
      console.error("Balance pagination failed:", err);
      this.queryError = err instanceof Error ? err.message : "Balance pagination failed";
    } finally {
      this.queryBalancesLoading = false;
      this.render();
    }
  }

  private async navigateQueryTransfers(dir: "next" | "prev") {
    const target = dir === "next" ? this.queryTransfersNext : this.queryTransfersHistory[this.queryTransfersHistory.length - 1];
    if (!target && dir === "next") return;
    this.queryTransfersLoading = true;
    this.queryError = null;
    this.render();
    try {
      this.queryTransfersHistory = dir === "next"
        ? [...this.queryTransfersHistory, this.queryTransfersCursor]
        : this.queryTransfersHistory.slice(0, -1);
      this.queryTransfersCursor = target;
      const page = await getErc20TransfersPage(this.client, {
        contractAddress: this.queryContractAddress,
        wallet: this.queryWallet,
        cursor: target,
        limit: this.pageSize,
      });
      this.queryErc20Transfers = page.items;
      this.queryTransfersNext = page.nextCursor;
    } catch (err) {
      console.error("Transfer pagination failed:", err);
      this.queryError = err instanceof Error ? err.message : "Transfer pagination failed";
    } finally {
      this.queryTransfersLoading = false;
      this.render();
    }
  }

  private render() {
    const app = document.getElementById("app")!;
    app.innerHTML = `
      <div class="container">
        <header>
          <h1>Torii Tokens - Vanilla</h1>
          <p class="subtitle">ERC20 / ERC721 / ERC1155 Token Indexer</p>
        </header>

        ${this.renderStatusPanel()}

        ${this.renderQueryPanel()}

        ${this.renderQueryResults()}

        <div class="panels">
          ${this.renderErc20Panel()}
          ${this.renderErc721Panel()}
          ${this.renderErc1155Panel()}
          ${this.renderUpdatesPanel()}
        </div>
      </div>
    `;

    this.attachEventListeners();
  }

  private renderQueryPanel(): string {
    return `
      <section class="panel query-panel full-width">
        <h2>Query Balances & Transfers</h2>
        <form id="query-form">
          <div class="form-grid">
            <div class="form-group">
              <label for="contractAddress">Contract Address</label>
              <input
                type="text"
                id="contractAddress"
                value="${this.queryContractAddress}"
                placeholder="0x..."
                class="input"
              />
            </div>
            <div class="form-group">
              <label for="wallet">Wallet Address</label>
              <input
                type="text"
                id="wallet"
                value="${this.queryWallet}"
                placeholder="0x..."
                class="input"
              />
            </div>
          </div>
          <div class="btn-group" style="margin-top: 1rem; justify-content: center;">
            <button
              type="submit"
              class="btn btn-primary"
              id="query-btn"
              ${this.queryLoading ? "disabled" : ""}
            >
              ${this.queryLoading ? "Loading..." : "Query"}
            </button>
          </div>
        </form>
      </section>
    `;
  }

  private renderQueryResults(): string {
    if (!this.queryContractAddress && !this.queryWallet) {
      return `
        <section class="panel results-panel full-width">
          <h2>Query Results</h2>
          <div class="empty-state">
            Enter a contract address or wallet to query balances and transfers
          </div>
        </section>
      `;
    }

    if (this.queryLoading && this.queryErc20Balances.length === 0 && this.queryErc20Transfers.length === 0) {
      return `
        <section class="panel results-panel full-width">
          <h2>Query Results</h2>
          <div class="empty-state">Loading...</div>
        </section>
      `;
    }

    if (this.queryError) {
      return `
        <section class="panel results-panel full-width">
          <h2>Query Results</h2>
          <div class="error-state">${this.queryError}</div>
        </section>
      `;
    }

    const balances = this.queryErc20Balances;
    const transfers = this.queryErc20Transfers;

    return `
      <section class="panel results-panel full-width">
        <h2>Query Results</h2>

        <div class="results-section">
          <h3>ERC20 Balances</h3>
          ${
            balances.length === 0
              ? `<div class="empty-state">No token balances found</div>`
              : `
            <div class="table-shell" aria-busy="${this.queryBalancesLoading}">
              <div class="table-container">
                <table>
                  <thead>
                    <tr>
                      <th>Token</th>
                      <th>Wallet</th>
                      <th>Balance</th>
                      <th>Last Updated Block</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${balances
                      .map(
                        (b) => `
                      <tr>
                        <td class="address"><a href="${getContractExplorerUrl(b.token)}" target="_blank" rel="noopener noreferrer">${b.symbol ? `${b.symbol} (${truncateAddress(b.token)})` : truncateAddress(b.token)}</a></td>
                        <td class="address">${b.wallet ? `<a href="${getContractExplorerUrl(b.wallet)}" target="_blank" rel="noopener noreferrer">${truncateAddress(b.wallet)}</a>` : "—"}</td>
                        <td class="amount">${b.balance}</td>
                        <td>${b.lastBlock}</td>
                      </tr>
                    `
                      )
                      .join("")}
                  </tbody>
                </table>
              </div>
              ${this.queryBalancesLoading ? `<div class="table-overlay"><div class="loading-spinner table-overlay-spinner"></div><span class="table-overlay-text">Loading balances...</span></div>` : ""}
            </div>
            <div class="btn-group" style="margin-top: 0.5rem; justify-content: flex-end;">
              <button class="btn btn-sm" id="query-balances-prev" ${this.queryBalancesHistory.length > 0 && !this.queryBalancesLoading ? "" : "disabled"}>Prev</button>
              <button class="btn btn-sm" id="query-balances-next" ${this.queryBalancesNext != null && !this.queryBalancesLoading ? "" : "disabled"}>Next</button>
            </div>
          `
          }
        </div>

        <div class="results-section" style="margin-top: 1.5rem;">
          <h3>Transfer History (${transfers.length})</h3>
          ${
            transfers.length === 0
              ? `<div class="empty-state">No transfers found</div>`
              : `
            <div class="table-shell" aria-busy="${this.queryTransfersLoading}">
              <div class="table-container">
                <table>
                  <thead>
                    <tr>
                      <th>From</th>
                      <th>To</th>
                      <th>Amount</th>
                      <th>Block</th>
                      <th>Time</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${transfers
                      .map(
                        (t) => `
                      <tr>
                        <td class="address"><a href="${getContractExplorerUrl(t.from)}" target="_blank" rel="noopener noreferrer">${truncateAddress(t.from)}</a></td>
                        <td class="address"><a href="${getContractExplorerUrl(t.to)}" target="_blank" rel="noopener noreferrer">${truncateAddress(t.to)}</a></td>
                        <td class="amount">${t.amount}</td>
                        <td>${t.blockNumber}</td>
                        <td class="timestamp">${formatTimestamp(t.timestamp)}</td>
                      </tr>
                    `
                      )
                      .join("")}
                  </tbody>
                </table>
              </div>
              ${this.queryTransfersLoading ? `<div class="table-overlay"><div class="loading-spinner table-overlay-spinner"></div><span class="table-overlay-text">Loading transfers...</span></div>` : ""}
            </div>
            <div class="btn-group" style="margin-top: 0.5rem; justify-content: flex-end;">
              <button class="btn btn-sm" id="query-transfers-prev" ${this.queryTransfersHistory.length > 0 && !this.queryTransfersLoading ? "" : "disabled"}>Prev</button>
              <button class="btn btn-sm" id="query-transfers-next" ${this.queryTransfersNext != null && !this.queryTransfersLoading ? "" : "disabled"}>Next</button>
            </div>
          `
          }
        </div>
      </section>
    `;
  }

  private renderStatusPanel(): string {
    return `
      <section class="panel status-panel full-width">
        <h2>Status</h2>
        <div class="status-grid">
          <div class="stat">
            <div class="stat-label">Connection</div>
            <div class="stat-value ${this.connected ? "success" : ""}">
              ${this.connected ? "Connected" : "Disconnected"}
            </div>
          </div>
          <div class="stat">
            <div class="stat-label">Server</div>
            <div class="stat-value">${SERVER_URL}</div>
          </div>
          <div class="stat">
            <div class="stat-label">Client ID</div>
            <div class="stat-value mono">${this.clientId}</div>
          </div>
          <div class="stat">
            <div class="stat-label">Updates</div>
            <div class="stat-value">${this.updates.length}</div>
          </div>
        </div>
        <div class="btn-group" style="margin-top: 1rem; justify-content: center;">
          ${
            this.connected
              ? `<button class="btn btn-danger" id="disconnect-btn">Disconnect</button>`
              : `<button class="btn btn-primary" id="subscribe-btn">Subscribe</button>`
          }
        </div>
      </section>
    `;
  }

  private renderErc20Panel(): string {
    const stats = this.erc20Stats;
    return `
      <section class="panel token-panel erc20">
        <h2>ERC20 Tokens</h2>
        ${
          stats
            ? `
          <div class="status-grid" style="margin-bottom: 1rem;">
            <div class="stat">
              <div class="stat-label">Transfers</div>
              <div class="stat-value">${stats.totalTransfers}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Approvals</div>
              <div class="stat-value">${stats.totalApprovals || 0}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Tokens</div>
              <div class="stat-value">${stats.uniqueTokens}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Accounts</div>
              <div class="stat-value">${stats.uniqueAccounts}</div>
            </div>
          </div>
        `
            : ""
        }
        ${this.renderMetadataTable(this.erc20Metadata, true, "erc20", this.erc20MetadataHistory.length > 0, this.erc20MetadataNext != null, this.erc20MetadataLoading)}
        ${this.renderTransfersTable(this.erc20Transfers, true, "erc20", this.erc20TransfersHistory.length > 0, this.erc20TransfersNext != null, this.erc20TransfersLoading)}
      </section>
    `;
  }

  private renderErc721Panel(): string {
    const stats = this.erc721Stats;
    return `
      <section class="panel token-panel erc721">
        <h2>ERC721 NFTs</h2>
        ${
          stats
            ? `
          <div class="status-grid" style="margin-bottom: 1rem;">
            <div class="stat">
              <div class="stat-label">Transfers</div>
              <div class="stat-value">${stats.totalTransfers}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Collections</div>
              <div class="stat-value">${stats.uniqueTokens}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Owners</div>
              <div class="stat-value">${stats.uniqueAccounts}</div>
            </div>
          </div>
        `
            : ""
        }
        ${this.renderMetadataTable(this.erc721Metadata, false, "erc721", this.erc721MetadataHistory.length > 0, this.erc721MetadataNext != null, this.erc721MetadataLoading)}
        ${this.renderTransfersTable(this.erc721Transfers, false, "erc721", this.erc721TransfersHistory.length > 0, this.erc721TransfersNext != null, this.erc721TransfersLoading)}
      </section>
    `;
  }

  private renderErc1155Panel(): string {
    const stats = this.erc1155Stats;
    return `
      <section class="panel token-panel erc1155">
        <h2>ERC1155 Multi-Tokens</h2>
        ${
          stats
            ? `
          <div class="status-grid" style="margin-bottom: 1rem;">
            <div class="stat">
              <div class="stat-label">Transfers</div>
              <div class="stat-value">${stats.totalTransfers}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Tokens</div>
              <div class="stat-value">${stats.uniqueTokens}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Accounts</div>
              <div class="stat-value">${stats.uniqueAccounts}</div>
            </div>
          </div>
        `
            : ""
        }
        ${this.renderMetadataTable(this.erc1155Metadata, false, "erc1155", this.erc1155MetadataHistory.length > 0, this.erc1155MetadataNext != null, this.erc1155MetadataLoading)}
        ${this.renderTransfersTable(this.erc1155Transfers, true, "erc1155", this.erc1155TransfersHistory.length > 0, this.erc1155TransfersNext != null, this.erc1155TransfersLoading)}
      </section>
    `;
  }

  private renderMetadataTable(
    metadata: TokenMetadataResult[],
    showDecimals: boolean,
    tokenType: "erc20" | "erc721" | "erc1155",
    canPrev: boolean,
    canNext: boolean,
    loading: boolean
  ): string {
    if (metadata.length === 0) return "";

    return `
      <div class="metadata-list" style="margin-bottom: 1rem;">
        <h3 style="font-size: 0.9rem; margin-bottom: 0.5rem;">Token Metadata</h3>
        <div class="table-shell" aria-busy="${loading}">
          <div class="table-container">
            <table>
              <thead>
                <tr>
                  <th>Contract</th>
                  <th>Name</th>
                  <th>Symbol</th>
                  ${showDecimals ? "<th>Decimals</th>" : ""}
                </tr>
              </thead>
              <tbody>
                ${metadata.map((m) => `
                  <tr>
                    <td class="address"><a href="${getContractExplorerUrl(m.token)}" target="_blank" rel="noopener noreferrer">${truncateAddress(m.token)}</a></td>
                    <td>${m.name ?? "—"}</td>
                    <td>${m.symbol ?? "—"}</td>
                    ${showDecimals ? `<td>${m.decimals ?? "—"}</td>` : ""}
                  </tr>
                `).join("")}
              </tbody>
            </table>
          </div>
          ${loading ? `<div class="table-overlay"><div class="loading-spinner table-overlay-spinner"></div><span class="table-overlay-text">Loading metadata...</span></div>` : ""}
        </div>
      </div>
      <div class="btn-group" style="margin-top: 0.5rem; justify-content: flex-end;">
        <button class="btn btn-sm" id="${tokenType}-metadata-prev" ${canPrev && !loading ? "" : "disabled"}>Prev</button>
        <button class="btn btn-sm" id="${tokenType}-metadata-next" ${canNext && !loading ? "" : "disabled"}>Next</button>
      </div>
    `;
  }

  private renderTransfersTable(
    transfers: Transfer[],
    showAmount: boolean,
    tokenType: "erc20" | "erc721" | "erc1155",
    canPrev: boolean,
    canNext: boolean,
    loading: boolean
  ): string {
    if (transfers.length === 0) {
      return `<div class="empty-state">No transfers yet</div>`;
    }

    return `
      <div class="table-shell" aria-busy="${loading}">
        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th>From</th>
                <th>To</th>
                ${showAmount ? "<th>Value</th>" : "<th>Token ID</th>"}
                <th>Block</th>
                <th>Time</th>
              </tr>
            </thead>
            <tbody>
              ${transfers
                .map(
                  (t) => `
                <tr>
                  <td class="address"><a href="${getContractExplorerUrl(t.from)}" target="_blank" rel="noopener noreferrer">${truncateAddress(t.from)}</a></td>
                  <td class="address"><a href="${getContractExplorerUrl(t.to)}" target="_blank" rel="noopener noreferrer">${truncateAddress(t.to)}</a></td>
                  <td class="amount">${showAmount ? (t.value ?? t.amount) : t.tokenId}</td>
                  <td>${t.blockNumber}</td>
                  <td class="timestamp">${formatTimestamp(t.timestamp)}</td>
                </tr>
              `
                )
                .join("")}
            </tbody>
          </table>
        </div>
        ${loading ? `<div class="table-overlay"><div class="loading-spinner table-overlay-spinner"></div><span class="table-overlay-text">Loading transfers...</span></div>` : ""}
      </div>
      <div class="btn-group" style="margin-top: 0.5rem; justify-content: flex-end;">
        <button class="btn btn-sm" id="${tokenType}-transfers-prev" ${canPrev && !loading ? "" : "disabled"}>Prev</button>
        <button class="btn btn-sm" id="${tokenType}-transfers-next" ${canNext && !loading ? "" : "disabled"}>Next</button>
      </div>
    `;
  }

  private renderUpdatesPanel(): string {
    return `
      <section class="panel full-width">
        <div class="panel-header">
          <h2>Real-time Updates (${this.updates.length})</h2>
          <button class="btn btn-sm" id="clear-updates-btn">Clear</button>
        </div>
        ${
          this.updates.length === 0
            ? `
          <div class="empty-state">
            ${this.connected ? "Waiting for updates..." : "Subscribe to start receiving updates"}
          </div>
        `
            : `
          <div class="updates-list">
            ${this.updates
              .map(
                (u) => `
              <div class="update-item">
                <div class="update-header">
                  <span class="badge badge-${getUpdateTypeName(u.updateType).toLowerCase()}">${getUpdateTypeName(u.updateType)}</span>
                  <span class="update-topic">${u.topic}</span>
                  <span class="update-time">${formatTimestamp(u.timestamp)}</span>
                </div>
                <div class="update-body">
                  <pre style="font-size: 0.85rem; overflow-x: auto;">${JSON.stringify(u.data, null, 2)}</pre>
                </div>
              </div>
            `
              )
              .join("")}
          </div>
        `
        }
      </section>
    `;
  }

  private attachEventListeners() {
    document.getElementById("subscribe-btn")?.addEventListener("click", () => this.subscribe());
    document.getElementById("disconnect-btn")?.addEventListener("click", () => this.disconnect());
    document.getElementById("clear-updates-btn")?.addEventListener("click", () => this.clearUpdates());
    document.getElementById("query-balances-prev")?.addEventListener("click", () => void this.navigateQueryBalances("prev"));
    document.getElementById("query-balances-next")?.addEventListener("click", () => void this.navigateQueryBalances("next"));
    document.getElementById("query-transfers-prev")?.addEventListener("click", () => void this.navigateQueryTransfers("prev"));
    document.getElementById("query-transfers-next")?.addEventListener("click", () => void this.navigateQueryTransfers("next"));
    document.getElementById("erc20-metadata-prev")?.addEventListener("click", () => void this.navigateDashboardMetadata("erc20", "prev"));
    document.getElementById("erc20-metadata-next")?.addEventListener("click", () => void this.navigateDashboardMetadata("erc20", "next"));
    document.getElementById("erc721-metadata-prev")?.addEventListener("click", () => void this.navigateDashboardMetadata("erc721", "prev"));
    document.getElementById("erc721-metadata-next")?.addEventListener("click", () => void this.navigateDashboardMetadata("erc721", "next"));
    document.getElementById("erc1155-metadata-prev")?.addEventListener("click", () => void this.navigateDashboardMetadata("erc1155", "prev"));
    document.getElementById("erc1155-metadata-next")?.addEventListener("click", () => void this.navigateDashboardMetadata("erc1155", "next"));
    document.getElementById("erc20-transfers-prev")?.addEventListener("click", () => void this.navigateDashboardTransfers("erc20", "prev"));
    document.getElementById("erc20-transfers-next")?.addEventListener("click", () => void this.navigateDashboardTransfers("erc20", "next"));
    document.getElementById("erc721-transfers-prev")?.addEventListener("click", () => void this.navigateDashboardTransfers("erc721", "prev"));
    document.getElementById("erc721-transfers-next")?.addEventListener("click", () => void this.navigateDashboardTransfers("erc721", "next"));
    document.getElementById("erc1155-transfers-prev")?.addEventListener("click", () => void this.navigateDashboardTransfers("erc1155", "prev"));
    document.getElementById("erc1155-transfers-next")?.addEventListener("click", () => void this.navigateDashboardTransfers("erc1155", "next"));
    document.getElementById("query-form")?.addEventListener("submit", (e) => {
      e.preventDefault();
      const contractAddress = (document.getElementById("contractAddress") as HTMLInputElement)?.value ?? "";
      const wallet = (document.getElementById("wallet") as HTMLInputElement)?.value ?? "";
      if (contractAddress || wallet) {
        this.handleQuery(contractAddress, wallet);
      }
    });
  }
}

new TokensApp();
