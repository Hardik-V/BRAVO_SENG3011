import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime


def create_graph(adage_data):
    event = adage_data.get("event", {})
    attrs = event.get("event_attributes", {})

    if not attrs:
        return None

    # Single event data point
    close = attrs.get("close")
    timestamp = event.get("event_time_object", {}).get("timestamp", "")

    if not close or not timestamp:
        return None

    try:
        date = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        date = datetime.now()

    plt.figure(figsize=(10, 5))
    plt.plot([date], [close], marker="o", color="blue")
    plt.xlabel("Date")
    plt.ylabel("Close Price (USD)")
    plt.title(f"Financial Price Chart - {attrs.get('ticker', '')}")
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.tight_layout()

    file_path = "/tmp/financial_graph.png"
    plt.savefig(file_path)
    plt.close()
    return file_path
