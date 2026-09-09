# records-data

The Firm Brokers payroll history as plain JSON, scanned from Robinhood Chain once an hour and served by GitHub Pages. The Records Room on thefirmbrokers.com reads these files and asks the chain only for the last hour.

- `data/head.json` — the block the data is complete to
- `data/rounds.json` — every settled hour `[round, pot, totalWeight, block]`
- `data/t/<id>.json` — one broker's transfers, weight changes and paydays
- `data/w/<address>.json` — every broker a wallet ever received

Read-only. Nothing here is a projection: every row is an event the engine or the NFT emitted.
