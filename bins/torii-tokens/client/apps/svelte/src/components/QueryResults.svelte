<script lang="ts">
  import {
    formatTimestamp,
    truncateAddress,
    getContractExplorerUrl,
    type TokenBalanceResult,
    type TransferResult,
  } from "@torii-tokens/shared";

  interface Props {
    contractAddress: string;
    wallet: string;
    erc20Balances: TokenBalanceResult[];
    erc20Transfers: TransferResult[];
    loading: boolean;
    error: string | null;
    onBalancesPrev?: () => void;
    onBalancesNext?: () => void;
    balancesCanPrev?: boolean;
    balancesCanNext?: boolean;
    onTransfersPrev?: () => void;
    onTransfersNext?: () => void;
    transfersCanPrev?: boolean;
    transfersCanNext?: boolean;
    balancesLoading?: boolean;
    transfersLoading?: boolean;
  }

  let {
    contractAddress,
    wallet,
    erc20Balances,
    erc20Transfers,
    loading,
    error,
    onBalancesPrev,
    onBalancesNext,
    balancesCanPrev = false,
    balancesCanNext = false,
    onTransfersPrev,
    onTransfersNext,
    transfersCanPrev = false,
    transfersCanNext = false,
    balancesLoading = false,
    transfersLoading = false,
  }: Props = $props();
</script>

{#if !contractAddress && !wallet}
  <section class="panel results-panel full-width">
    <h2>Query Results</h2>
    <div class="empty-state">
      Enter a contract address or wallet to query balances and transfers
    </div>
  </section>
{:else if loading && erc20Balances.length === 0 && erc20Transfers.length === 0}
  <section class="panel results-panel full-width">
    <h2>Query Results</h2>
    <div class="empty-state">Loading...</div>
  </section>
{:else if error}
  <section class="panel results-panel full-width">
    <h2>Query Results</h2>
    <div class="error-state">{error}</div>
  </section>
{:else}
  <section class="panel results-panel full-width">
    <h2>Query Results</h2>

    <div class="results-section">
      <h3>ERC20 Balances</h3>
      {#if erc20Balances.length === 0}
        <div class="empty-state">No token balances found</div>
      {:else}
        <div class="table-shell" aria-busy={balancesLoading}>
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
                {#each erc20Balances as b (`${b.token}-${b.wallet ?? "none"}`)}
                  <tr>
                    <td class="address">
                      <a href={getContractExplorerUrl(b.token)} target="_blank" rel="noopener noreferrer">
                        {b.symbol ? `${b.symbol} (${truncateAddress(b.token)})` : truncateAddress(b.token)}
                      </a>
                    </td>
                    <td class="address">
                      {#if b.wallet}
                        <a href={getContractExplorerUrl(b.wallet)} target="_blank" rel="noopener noreferrer">
                          {truncateAddress(b.wallet)}
                        </a>
                      {:else}
                        —
                      {/if}
                    </td>
                    <td class="amount">{b.balance}</td>
                    <td>{b.lastBlock}</td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
          {#if balancesLoading}
            <div class="table-overlay">
              <div class="loading-spinner table-overlay-spinner"></div>
              <span class="table-overlay-text">Loading balances...</span>
            </div>
          {/if}
        </div>
      {/if}
      {#if erc20Balances.length > 0}
        <div class="btn-group" style="margin-top: 0.5rem; justify-content: flex-end;">
          <button class="btn btn-sm" onclick={onBalancesPrev} disabled={!balancesCanPrev || balancesLoading}>Prev</button>
          <button class="btn btn-sm" onclick={onBalancesNext} disabled={!balancesCanNext || balancesLoading}>Next</button>
        </div>
      {/if}
    </div>

    <div class="results-section" style="margin-top: 1.5rem;">
      <h3>Transfer History ({erc20Transfers.length})</h3>
      {#if erc20Transfers.length === 0}
        <div class="empty-state">No transfers found</div>
      {:else}
        <div class="table-shell" aria-busy={transfersLoading}>
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
                {#each erc20Transfers as t, idx (`${t.txHash}-${idx}`)}
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
                    <td class="amount">{t.amount}</td>
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
      {#if erc20Transfers.length > 0}
        <div class="btn-group" style="margin-top: 0.5rem; justify-content: flex-end;">
          <button class="btn btn-sm" onclick={onTransfersPrev} disabled={!transfersCanPrev || transfersLoading}>Prev</button>
          <button class="btn btn-sm" onclick={onTransfersNext} disabled={!transfersCanNext || transfersLoading}>Next</button>
        </div>
      {/if}
    </div>
  </section>
{/if}
