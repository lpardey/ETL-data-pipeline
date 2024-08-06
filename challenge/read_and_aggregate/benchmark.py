from types import ModuleType

import main_pandas
import matplotlib.pyplot
import numpy


def get_results(module: ModuleType) -> tuple[float, float, float]:
    load_time = module.load_time()
    proccesing_time = module.processing_time()
    total_time = load_time + proccesing_time
    return (load_time, proccesing_time, total_time)


pandas_results = get_results(main_pandas)


frameworks = ("Pandas",)

framework_times = {
    "Lectura": (pandas_results[0],),
    "Procesamiento": (pandas_results[1],),
    "Total": (pandas_results[2],),
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
ax.set_ylim(0, 250)

matplotlib.pyplot.show()
