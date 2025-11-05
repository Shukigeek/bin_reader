import json
import os

class ResultsManager:
    def __init__(self, results_file="results.json"):
        self.results_file = results_file
        self.results = []

    def load(self):
        if os.path.exists(self.results_file):
            try:
                with open(self.results_file, "r", encoding="utf-8") as f:
                    self.results = json.load(f)
                print(f"📂 Loaded results from {self.results_file}")
            except Exception as e:
                print(f"⚠️ Failed to load results: {e}")
        return self.results

    def save(self, data):
        self.results = data
        try:
            with open(self.results_file, "w", encoding="utf-8") as f:
                json.dump(self.results, f, indent=2)
            print(f"💾 Results saved to {self.results_file}")
        except Exception as e:
            print(f"⚠️ Failed to save results: {e}")