import matplotlib.pyplot as plt
import numpy as np

from . import main_dask, main_pandas, main_polars, main_pyspark
from .common import Processor

GCS_PATH = "gs://single_file/"
OUTPUT_FILENAME = "single.png"
FRAMEWORKS = ("Pandas", "Dask", "Pyspark", "Polars")


def main() -> None:
    pandas_results = get_results(main_pandas.PandasProcessor, path=GCS_PATH)
    dask_results = get_results(main_dask.DaskProcessor, path=GCS_PATH)
    pyspark_results = get_results(main_pyspark.SparkProcessor, path=GCS_PATH)
    polars_results = get_results(main_polars.PolarsProcessor, path=GCS_PATH)

    # Labels and measurements
    labels = ("Reading", "Processing", "Total")
    framework_results = zip(pandas_results, dask_results, pyspark_results, polars_results)
    framework_times = zip(labels, framework_results)

    # Label locations
    x = np.arange(len(FRAMEWORKS))
    width = 0.25  # the width of the bars
    multiplier = 0
    fig, ax = plt.subplots(layout="constrained")

    for label, measurements in framework_times:
        offset = width * multiplier
        rects = ax.bar(x + offset, measurements, width, label=label)
        ax.bar_label(rects, padding=3)
        multiplier += 1

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Benchmark: read and aggregate (Single)")
    ax.set_xticks(x + width, FRAMEWORKS)
    ax.legend(loc="upper left", ncols=3)
    ax.set_ylim(0, 20)

    plt.savefig(OUTPUT_FILENAME)


def get_results(processor_class: type[Processor], path: str) -> tuple[float, float, float]:
    processor = processor_class(path)
    processor.run()
    times = processor.read_time, processor.process_time
    return *times, float(sum(times))


if __name__ == "__main__":
    main()
