<script lang="ts">
  import {
    formatTimestamp,
    truncateAddress,
    getContractExplorerUrl,
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

  interface Props {
    title: string;
    tokenType: "erc20" | "erc721" | "erc1155";
    stats: Stats | null;
    transfers: Transfer[];
    metadata?: TokenMetadataResult[];
    showAmount: boolean;
    onMetadataPrev?: () => void;
    onMetadataNext?: () => void;
    metadataCanPrev?: boolean;
    metadataCanNext?: boolean;
    onTransfersPrev?: () => void;
    onTransfersNext?: () => void;
    transfersCanPrev?: boolean;
    transfersCanNext?: boolean;
    metadataLoading?: boolean;
    transfersLoading?: boolean;
  }

  let {
    title,
    tokenType,
    stats,
    transfers,
    metadata = [],
    showAmount,
    onMetadataPrev,
    onMetadataNext,
    metadataCanPrev = false,
    metadataCanNext = false,
    onTransfersPrev,
    onTransfersNext,
    transfersCanPrev = false,
    transfersCanNext = false,
    metadataLoading = false,
    transfersLoading = false,
  }: Props = $props();
</script>

<section class="panel token-panel {tokenType}">
  <h2>{title}</h2>

  {#if stats}
    <div class="status-grid" style="margin-bottom: 1rem;">
      <div class="stat">
        <div class="stat-label">Transfers</div>
        <div class="stat-value">{stats.totalTransfers}</div>
      </div>
      {#if stats.totalApprovals !== undefined}
        <div class="stat">
          <div class="stat-label">Approvals</div>
          <div class="stat-value">{stats.totalApprovals}</div>
        </div>
      {/if}
      <div class="stat">
        <div class="stat-label">{tokenType === "erc721" ? "Collections" : "Tokens"}</div>
        <div class="stat-value">{stats.uniqueTokens}</div>
      </div>
      <div class="stat">
        <div class="stat-label">{tokenType === "erc721" ? "Owners" : "Accounts"}</div>
        <div class="stat-value">{stats.uniqueAccounts}</div>
      </div>
      <div class="btn-group" style="margin-top: 0.5rem; justify-content: flex-end;">
        <button class="btn btn-sm" onclick={onMetadataPrev} disabled={!metadataCanPrev || metadataLoading}>Prev</button>
        <button class="btn btn-sm" onclick={onMetadataNext} disabled={!metadataCanNext || metadataLoading}>Next</button>
      </div>
    </div>
  {/if}

  {#if metadata.length > 0}
    <div class="metadata-list" style="margin-bottom: 1rem;">
      <h3 style="font-size: 0.9rem; margin-bottom: 0.5rem;">Token Metadata</h3>
      <div class="table-shell" aria-busy={metadataLoading}>
        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th>Contract</th>
                <th>Name</th>
                <th>Symbol</th>
                {#if tokenType === "erc20"}
                  <th>Decimals</th>
                {/if}
              </tr>
            </thead>
            <tbody>
              {#each metadata as m (m.token)}
                <tr>
                  <td class="address">
                    <a href={getContractExplorerUrl(m.token)} target="_blank" rel="noopener noreferrer">
                      {truncateAddress(m.token)}
                    </a>
                  </td>
                  <td>{m.name ?? "—"}</td>
                  <td>{m.symbol ?? "—"}</td>
                  {#if tokenType === "erc20"}
                    <td>{m.decimals ?? "—"}</td>
                  {/if}
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
        {#if metadataLoading}
          <div class="table-overlay">
            <div class="loading-spinner table-overlay-spinner"></div>
            <span class="table-overlay-text">Loading metadata...</span>
          </div>
        {/if}
      </div>
    </div>
  {/if}

  {#if transfers.length === 0}
    <div class="empty-state">No transfers yet</div>
  {:else}
    <div class="table-shell" aria-busy={transfersLoading}>
      <div class="table-container">
        <table>
          <thead>
            <tr>
              <th>From</th>
              <th>To</th>
              <th>{showAmount ? (tokenType === "erc1155" ? "Value" : "Amount") : "Token ID"}</th>
              <th>Block</th>
              <th>Time</th>
            </tr>
          </thead>
          <tbody>
            {#each transfers as t (t.id)}
              <tr>
                <td class="address">
                  <a href={getContractExplorerUrl(t.from)} target="_blank" rel="noopener noreferrer">
                    {truncateAddress(t.from)}
                  </a>
                </td>
                <td class="address">
                  <a href={getContractExplorerUrl(t.to)} target="_blank" rel="noopener noreferrer">
                    {truncateAddress(t.to)}
                  </a>
                </td>
                <td class="amount">{showAmount ? (t.value ?? t.amount) : t.tokenId}</td>
                <td>{t.blockNumber}</td>
                <td class="timestamp">{formatTimestamp(t.timestamp)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
      {#if transfersLoading}
        <div class="table-overlay">
          <div class="loading-spinner table-overlay-spinner"></div>
          <span class="table-overlay-text">Loading transfers...</span>
        </div>
      {/if}
    </div>
  {/if}
  {#if transfers.length > 0}
    <div class="btn-group" style="margin-top: 0.5rem; justify-content: flex-end;">
      <button class="btn btn-sm" onclick={onTransfersPrev} disabled={!transfersCanPrev || transfersLoading}>Prev</button>
      <button class="btn btn-sm" onclick={onTransfersNext} disabled={!transfersCanNext || transfersLoading}>Next</button>
    </div>
  {/if}
</section>
