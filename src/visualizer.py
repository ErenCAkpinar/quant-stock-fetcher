# src/visualizer.py
import pandas as pd
import plotly.graph_objects as go

def plot_candlestick(df: pd.DataFrame, ma_windows: list = (20, 50)):
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df["date"],
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        name="OHLC"
    ))
    for w in ma_windows:
        if len(df) >= w:
            df[f"ma{w}"] = df["close"].rolling(w).mean()
            fig.add_trace(go.Scatter(x=df["date"], y=df[f"ma{w}"], mode="lines", name=f"MA{w}"))
    fig.update_layout(xaxis_rangeslider_visible=False, template="plotly_white")
    return fig

def save_plot_html(fig, path: str):
    fig.write_html(path, include_plotlyjs="cdn")
