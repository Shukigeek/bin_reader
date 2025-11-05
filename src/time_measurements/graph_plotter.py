import pandas as pd
import plotly.express as px

from src.utils.config import FILE_PATH
from src.time_measurements.parser_runners import ParserRunners
from src.time_measurements.results_manager import ResultsManager

class GraphPlotter:
    @staticmethod
    def plot(data):
        """
        Plots a benchmark bar chart for parser runtimes.
        The winner (fastest) in each category is highlighted in gold.
        """
        df = pd.DataFrame(data)

        # Identify the winner (minimum time) in each category
        winners_idx = df.groupby("category")["time"].idxmin()
        df["winner"] = df.index.isin(winners_idx)

        # Assign colors: gold for winners, blue for save=False, red for save=True
        df["color_final"] = df.apply(
            lambda row: "#FFD700" if row.name in winners_idx else ("#636EFA" if not row["save"] else "#EF553B"),
            axis=1
        )
        # Add a star marker for winners
        df["label"] = df.apply(lambda row: f"{row['time']:.2f} ⭐" if row["winner"] else f"{row['time']:.2f}", axis=1)



        # Create the bar chart
        fig = px.bar(
            df,
            x="library",
            y="time",
            color="color_final",
            barmode="group",
            facet_col="category",
            title="Parser Runtime Comparison (All Messages / GPS)",
            text="label",
            color_discrete_map="identity"  # Use exact colors from 'color_final'
        )

        # Update layout for readability
        fig.update_traces(textposition="outside")
        fig.update_layout(
            yaxis_title="Runtime (seconds)",
            xaxis_title="Library",
            font=dict(size=14),
            bargap=0.25,
            title_x=0.5,
            title_font=dict(size=20),
            showlegend=False
        )

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
