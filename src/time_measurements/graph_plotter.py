import pandas as pd
import plotly.express as px

from src.utils.config import FILE_PATH
from src.time_measurements.parser_runners import ParserRunners
from src.time_measurements.results_manager import ResultsManager


class GraphPlotter:
    @staticmethod
    def plot(data):
        """
        Plots a professional benchmark bar chart for parser runtimes.
        The winner (fastest) in each category is highlighted in gold.
        """
        df = pd.DataFrame(data)

        # Identify the winner (minimum time) in each category
        winners_idx = df.groupby("category")["time"].idxmin()
        df["winner"] = df.index.isin(winners_idx)

        # Professional color scheme
        df["color_group"] = df.apply(
            lambda row: "Winner" if row.name in winners_idx else ("Standard" if not row["save"] else "With Save to list"),
            axis=1
        )

        # Add formatted labels
        df["label"] = df.apply(
            lambda row: f"{row['time']:.2f}s ★" if row["winner"] else f"{row['time']:.2f}s",
            axis=1
        )

        # Create the bar chart with professional styling
        fig = px.bar(
            df,
            x="library",
            y="time",
            color="color_group",
            barmode="group",
            facet_col="category",
            title="Parser Performance Benchmark",
            text="label",
            color_discrete_map={
                "Winner": "#FFB300",  # Gold
                "Standard": "#1E88E5",  # Professional blue
                "With Save to list": "#E53935"  # Professional red
            }
        )

        # Professional layout
        fig.update_traces(
            textposition="outside",
            textfont_size=13,
            marker_line_width=0
        )

        fig.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            font=dict(family="Arial, sans-serif", size=13, color="#333333"),
            title=dict(
                text="<b>Parser Performance Benchmark</b><br><sub>Runtime Comparison: All Messages vs GPS Data</sub>",
                x=0.5,
                xanchor="center",
                font=dict(size=22, color="#1a1a1a")
            ),
            yaxis_title="<b>Runtime (seconds)</b>",
            xaxis_title="<b>Parser Library</b>",
            legend=dict(
                title="<b>Performance Type</b>",
                orientation="h",
                yanchor="bottom",
                y=-0.25,
                xanchor="center",
                x=0.5,
                font=dict(size=12)
            ),
            bargap=0.2,
            bargroupgap=0.1,
            margin=dict(t=120, b=100),
            height=550,
            width=1200
        )

        # Update axes styling
        fig.update_xaxes(
            showgrid=False,
            showline=True,
            linewidth=1,
            linecolor="#e0e0e0",
            tickfont=dict(size=12)
        )

        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor="#f0f0f0",
            showline=True,
            linewidth=1,
            linecolor="#e0e0e0",
            tickfont=dict(size=12)
        )

        # Clean facet labels
        fig.for_each_annotation(lambda a: a.update(
            text=f"<b>{a.text.split('=')[-1]}</b>",
            font=dict(size=14)
        ))

        fig.show()


if __name__ == "__main__":
    # Initialize runners and results manager
    runners = ParserRunners(FILE_PATH)
    results_manager = ResultsManager()

    # Load existing results from JSON if available
    data = results_manager.load()

    if not data:
        # Run all parsers if no previous results
        data_all = runners.run_all(category="all messages", save_list=True)
        data_gps = runners.run_all(category="GPS", save_list=True, type_filter=["GPS"])
        data = data_all + data_gps

        # Save the results for future use
        results_manager.save(data)

    # Plot the benchmark chart
    GraphPlotter.plot(data)