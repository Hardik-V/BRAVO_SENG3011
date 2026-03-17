import pandas as pd
import matplotlib.pyplot as plt


def create_graph(adage_data):
    data_list = []
    for event in adage_data.get("events", []):
        attrs = event.get("attribute", {})
        data_list.append({
            "date": event["time_object"]["timestamp"],
            "close": attrs.get("Close")
        })

    df = pd.DataFrame(data_list)
    if df.empty:
        return None

    plt.figure()
    plt.plot(pd.to_datetime(df["date"]), df["close"], marker="o")
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.title("Financial Price Chart")

    file_path = "/tmp/financial_graph.png"
    plt.savefig(file_path)
    plt.close()
    return file_path
