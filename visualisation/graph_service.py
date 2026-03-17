import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime


def create_graph(adage_data):
    events = adage_data.get("events", [])

    if not events:
        return None

    dates = []
    opens = []
    highs = []
    lows = []
    closes = []

    for event in events:
        attrs = event.get("event_attributes", {})
        timestamp = event.get("event_time_object", {}).get("timestamp", "")

        try:
            date = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            continue

        dates.append(date)
        opens.append(attrs.get("open", 0))
        highs.append(attrs.get("high", 0))
        lows.append(attrs.get("low", 0))
        closes.append(attrs.get("close", 0))

    if not dates:
        return None

    ticker = events[0].get("event_attributes", {}).get("ticker", "")

    fig, ax = plt.subplots(figsize=(14, 6))

    for i, (date, o, h, l, c) in enumerate(
        zip(dates, opens, highs, lows, closes)
    ):
        color = "green" if c >= o else "red"

        # High/Low wick
        ax.plot([i, i], [l, h], color="black", linewidth=1)

        # Open/Close candle body
        ax.bar(
            i,
            abs(c - o),
            bottom=min(o, c),
            width=0.6,
            color=color,
            edgecolor="black",
            linewidth=0.5
        )

    # X axis labels - show every nth date to avoid crowding
    step = max(1, len(dates) // 10)
    ax.set_xticks(range(0, len(dates), step))
    ax.set_xticklabels(
        [dates[i].strftime('%Y-%m-%d') for i in range(0, len(dates), step)],
        rotation=45,
        ha='right'
    )

    ax.set_ylabel("Price (USD)")
    ax.set_title(f"{ticker} — OHLC Candlestick Chart")
    ax.grid(axis='y', linestyle='--', alpha=0.5)

    green_patch = mpatches.Patch(color='green', label='Bullish')
    red_patch = mpatches.Patch(color='red', label='Bearish')
    ax.legend(handles=[green_patch, red_patch])

    plt.tight_layout()
    file_path = "/tmp/financial_graph.png"
    plt.savefig(file_path, dpi=150)
    plt.close()
    return file_path
