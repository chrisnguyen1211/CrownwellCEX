## Binance Spot Trading History Export

### Security first
- Create a new API key on Binance with read-only permissions.
- Do not share your API secret. Use environment variables.

### Setup
```bash
python3 -m venv /Users/nguyenhuycuong/Tradehis/.venv
source /Users/nguyenhuycuong/Tradehis/.venv/bin/activate
pip install -r /Users/nguyenhuycuong/Tradehis/requirements.txt
```

Set environment variables (use your new keys):
```bash
export BINANCE_API_KEY="YOUR_NEW_API_KEY"
export BINANCE_API_SECRET="YOUR_NEW_API_SECRET"
```

### CLI usage
- Infer symbols from your orders within time range and export all trades:
```bash
python /Users/nguyenhuycuong/Tradehis/binance_spot_history.py \
  --start 2024-01-01 \
  --out /Users/nguyenhuycuong/Tradehis/spot_trades.csv
```

- Or specify symbols explicitly:
```bash
python /Users/nguyenhuycuong/Tradehis/binance_spot_history.py \
  --symbols BTCUSDT ETHUSDT \
  --start 2024-01-01 --end 2024-06-30 \
  --out /Users/nguyenhuycuong/Tradehis/spot_trades_q1_q2.csv
```

### Streamlit UI
Run the local UI to browse and export trades:
```bash
streamlit run /Users/nguyenhuycuong/Tradehis/streamlit_app.py
```
In the UI:
- Input your API key and secret (or leave them blank to read from environment vars).
- Provide start/end time or just start.
- Either input symbols or enable "Infer traded symbols" (slower).
- View results in a table and download CSV.
