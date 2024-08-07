from types import ModuleType

import matplotlib.pyplot
import numpy

from . import main_dask, main_pandas, main_pyspark


def main() -> None:
    pandas_results = get_results(main_pandas)
    dask_results = get_results(main_dask)
    pyspark_results = get_results(main_pyspark, path="gs://celes_single")

    frameworks = ("Pandas", "Dask", "Pyspark")

    framework_times = {
        "Lectura": (pandas_results[0], dask_results[0], pyspark_results[0]),
        "Procesamiento": (pandas_results[1], dask_results[1], pyspark_results[1]),
        "Total": (pandas_results[2], dask_results[2], pyspark_results[2]),
    }

    # Label locations
    x = numpy.arange(len(frameworks))
    width = 0.25  # the width of the bars
    multiplier = 0
    fig, ax = matplotlib.pyplot.subplots(layout="constrained")

    for attribute, measurement in framework_times.items():
        offset = width * multiplier
        rects = ax.bar(x + offset, measurement, width, label=attribute)
        ax.bar_label(rects, padding=3)
        multiplier += 1

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel("Tiempo (segundos)")
    ax.set_title("Benchmark: lectura, ordenamiento y aggregación")
    ax.set_xticks(x + width, frameworks)
    ax.legend(loc="upper left", ncols=3)
    ax.set_ylim(0, 80)

    matplotlib.pyplot.savefig("single.png")


def get_results(module: ModuleType, path: str = "gcs://celes_single") -> tuple[float, float, float]:
    load_time = module.load_time(path)
    proccesing_time = module.processing_time(path)
    total_time = load_time + proccesing_time
    return (load_time, proccesing_time, total_time)


if __name__ == "__main__":
    main()
