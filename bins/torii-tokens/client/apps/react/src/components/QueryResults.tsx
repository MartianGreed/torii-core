import {
  formatTimestamp,
  truncateAddress,
  getContractExplorerUrl,
  type TokenBalanceResult,
  type TransferResult,
} from "@torii-tokens/shared";

interface QueryResultsProps {
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

export default function QueryResults({
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
}: QueryResultsProps) {
  if (!contractAddress && !wallet) {
    return (
      <section className="panel results-panel full-width">
        <h2>Query Results</h2>
        <div className="empty-state">
          Enter a contract address or wallet to query balances and transfers
        </div>
      </section>
    );
  }

  if (loading && erc20Balances.length === 0 && erc20Transfers.length === 0) {
    return (
      <section className="panel results-panel full-width">
        <h2>Query Results</h2>
        <div className="empty-state">Loading...</div>
      </section>
    );
  }

  if (error) {
    return (
      <section className="panel results-panel full-width">
        <h2>Query Results</h2>
        <div className="error-state">{error}</div>
      </section>
    );
  }

  return (
    <section className="panel results-panel full-width">
      <h2>Query Results</h2>

      <div className="results-section">
        <h3>ERC20 Balances</h3>
        {erc20Balances.length === 0 ? (
          <div className="empty-state">No token balances found</div>
        ) : (
          <div className="table-shell" aria-busy={balancesLoading}>
            <div className="table-container">
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
                  {erc20Balances.map((b) => (
                    <tr key={`${b.token}-${b.wallet ?? "none"}`}>
                      <td className="address">
                        <a href={getContractExplorerUrl(b.token)} target="_blank" rel="noopener noreferrer">
                          {b.symbol ? `${b.symbol} (${truncateAddress(b.token)})` : truncateAddress(b.token)}
                        </a>
                      </td>
                      <td className="address">
                        {b.wallet ? (
                          <a href={getContractExplorerUrl(b.wallet)} target="_blank" rel="noopener noreferrer">
                            {truncateAddress(b.wallet)}
                          </a>
                        ) : "—"}
                      </td>
                      <td className="amount">{b.balance}</td>
                      <td>{b.lastBlock}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {balancesLoading && (
              <div className="table-overlay">
                <div className="loading-spinner table-overlay-spinner" />
                <span className="table-overlay-text">Loading balances...</span>
              </div>
            )}
          </div>
        )}
        {erc20Balances.length > 0 && (
          <div className="btn-group" style={{ marginTop: "0.5rem", justifyContent: "flex-end" }}>
            <button className="btn btn-sm" onClick={onBalancesPrev} disabled={!balancesCanPrev || balancesLoading}>
              Prev
            </button>
            <button className="btn btn-sm" onClick={onBalancesNext} disabled={!balancesCanNext || balancesLoading}>
              Next
            </button>
          </div>
        )}
      </div>

      <div className="results-section" style={{ marginTop: "1.5rem" }}>
        <h3>Transfer History ({erc20Transfers.length})</h3>
        {erc20Transfers.length === 0 ? (
          <div className="empty-state">No transfers found</div>
        ) : (
          <div className="table-shell" aria-busy={transfersLoading}>
            <div className="table-container">
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
                  {erc20Transfers.map((t, idx) => (
                    <tr key={`${t.txHash}-${idx}`}>
                      <td className="address">
                        <a href={getContractExplorerUrl(t.from)} target="_blank" rel="noopener noreferrer">
                          {truncateAddress(t.from)}
                        </a>
                      </td>
                      <td className="address">
                        <a href={getContractExplorerUrl(t.to)} target="_blank" rel="noopener noreferrer">
                          {truncateAddress(t.to)}
                        </a>
                      </td>
                      <td className="amount">{t.amount}</td>
                      <td>{t.blockNumber}</td>
                      <td className="timestamp">{formatTimestamp(t.timestamp)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {transfersLoading && (
              <div className="table-overlay">
                <div className="loading-spinner table-overlay-spinner" />
                <span className="table-overlay-text">Loading transfers...</span>
              </div>
            )}
          </div>
        )}
        {erc20Transfers.length > 0 && (
          <div className="btn-group" style={{ marginTop: "0.5rem", justifyContent: "flex-end" }}>
            <button className="btn btn-sm" onClick={onTransfersPrev} disabled={!transfersCanPrev || transfersLoading}>
              Prev
            </button>
            <button className="btn btn-sm" onClick={onTransfersNext} disabled={!transfersCanNext || transfersLoading}>
              Next
            </button>
          </div>
        )}
      </div>
    </section>
  );
}
