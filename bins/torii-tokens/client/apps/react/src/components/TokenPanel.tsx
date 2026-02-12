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

interface TokenPanelProps {
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

export default function TokenPanel({
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
}: TokenPanelProps) {
  return (
    <section className={`panel token-panel ${tokenType}`}>
      <h2>{title}</h2>

      {stats && (
        <div className="status-grid" style={{ marginBottom: "1rem" }}>
          <div className="stat">
            <div className="stat-label">Transfers</div>
            <div className="stat-value">{stats.totalTransfers}</div>
          </div>
          {stats.totalApprovals !== undefined && (
            <div className="stat">
              <div className="stat-label">Approvals</div>
              <div className="stat-value">{stats.totalApprovals}</div>
            </div>
          )}
          <div className="stat">
            <div className="stat-label">
              {tokenType === "erc721" ? "Collections" : "Tokens"}
            </div>
            <div className="stat-value">{stats.uniqueTokens}</div>
          </div>
          <div className="stat">
            <div className="stat-label">
              {tokenType === "erc721" ? "Owners" : "Accounts"}
            </div>
            <div className="stat-value">{stats.uniqueAccounts}</div>
          </div>
        </div>
      )}

      {metadata.length > 0 && (
        <div className="metadata-list" style={{ marginBottom: "1rem" }}>
          <h3 style={{ fontSize: "0.9rem", marginBottom: "0.5rem" }}>Token Metadata</h3>
          <div className="table-shell" aria-busy={metadataLoading}>
            <div className="table-container">
              <table>
                <thead>
                  <tr>
                    <th>Contract</th>
                    <th>Name</th>
                    <th>Symbol</th>
                    {tokenType === "erc20" ? <th>Decimals</th> : null}
                  </tr>
                </thead>
                <tbody>
                  {metadata.map((m) => (
                    <tr key={m.token}>
                      <td className="address">
                        <a href={getContractExplorerUrl(m.token)} target="_blank" rel="noopener noreferrer">
                          {truncateAddress(m.token)}
                        </a>
                      </td>
                      <td>{m.name ?? "—"}</td>
                      <td>{m.symbol ?? "—"}</td>
                      {tokenType === "erc20" ? <td>{m.decimals ?? "—"}</td> : null}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {metadataLoading && (
              <div className="table-overlay">
                <div className="loading-spinner table-overlay-spinner" />
                <span className="table-overlay-text">Loading metadata...</span>
              </div>
            )}
          </div>
          <div className="btn-group" style={{ marginTop: "0.5rem", justifyContent: "flex-end" }}>
            <button className="btn btn-sm" onClick={onMetadataPrev} disabled={!metadataCanPrev || metadataLoading}>
              Prev
            </button>
            <button className="btn btn-sm" onClick={onMetadataNext} disabled={!metadataCanNext || metadataLoading}>
              Next
            </button>
          </div>
        </div>
      )}

      {transfers.length === 0 ? (
        <div className="empty-state">No transfers yet</div>
      ) : (
        <div className="table-shell" aria-busy={transfersLoading}>
          <div className="table-container">
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
                {transfers.map((t) => (
                  <tr key={t.id}>
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
                    <td className="amount">
                      {showAmount ? (t.value ?? t.amount) : t.tokenId}
                    </td>
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
      {transfers.length > 0 && (
        <div className="btn-group" style={{ marginTop: "0.5rem", justifyContent: "flex-end" }}>
          <button className="btn btn-sm" onClick={onTransfersPrev} disabled={!transfersCanPrev || transfersLoading}>
            Prev
          </button>
          <button className="btn btn-sm" onClick={onTransfersNext} disabled={!transfersCanNext || transfersLoading}>
            Next
          </button>
        </div>
      )}
    </section>
  );
}
