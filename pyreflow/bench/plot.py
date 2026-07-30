import sys
from jinja2 import Environment, FileSystemLoader
import polars as pl
from pathlib import Path
from plotnine import (
    ggplot,
    aes,
    geom_col,
    geom_errorbar,
    coord_flip,
    labs,
    scale_fill_discrete,
)

TOOLS = ["fireflow", "fcsparser", "flowio", "flowcore"]


def fill_cartesian[X](
    df: pl.DataFrame, col: str, fill: int | float | None
) -> pl.DataFrame:
    wide = df.select(["name", col, "tool"]).pivot("tool", index="name")
    return (wide.fill_null(fill) if fill is not None else wide).unpivot(
        None,
        index="name",
        variable_name="tool",
        value_name=col,
    )


def parser_enum() -> pl.Enum:
    return pl.Enum(list(reversed(TOOLS)))


def plot_read_text(df: pl.DataFrame, out_dir: Path) -> None:
    df_mean = fill_cartesian(df, "mean_r_text_ns_per_kw", 0)
    df_serr = fill_cartesian(df, "serr_r_text_ns_per_kw", None)
    df_combined = (
        df_mean.join(df_serr, on=["name", "tool"])
        .with_columns(
            (pl.col("mean_r_text_ns_per_kw") - pl.col("serr_r_text_ns_per_kw")).alias(
                "lower"
            ),
            (pl.col("mean_r_text_ns_per_kw") + pl.col("serr_r_text_ns_per_kw")).alias(
                "upper"
            ),
        )
        .with_columns(pl.col("tool").cast(parser_enum()))
    )

    read_text_plt = (
        ggplot(df_combined, aes(y="mean_r_text_ns_per_kw", x="name", fill="tool"))
        + geom_col(position="dodge")
        + geom_errorbar(aes(ymin="lower", ymax="upper"), position="dodge", width=0.9)
        + labs(y="TEXT read time (ns/keyword pair)", x="FCS File", fill="Tool")
        + coord_flip()
        + scale_fill_discrete(limits=TOOLS)
    )
    read_text_plt.save(out_dir / "read_text.svg")


def plot_read_data(df: pl.DataFrame, out_dir: Path) -> None:
    df_mean = fill_cartesian(df, "mean_r_data_diff_ns_per_value", 0)
    df_serr = fill_cartesian(df, "serr_r_data_diff_ns_per_value", None)
    df_combined = (
        df_mean.join(df_serr, on=["name", "tool"])
        .with_columns(
            (
                pl.col("mean_r_data_diff_ns_per_value")
                - pl.col("serr_r_data_diff_ns_per_value")
            ).alias("lower"),
            (
                pl.col("mean_r_data_diff_ns_per_value")
                + pl.col("serr_r_data_diff_ns_per_value")
            ).alias("upper"),
        )
        .with_columns(pl.col("tool").cast(parser_enum()))
    )

    read_text_plt = (
        ggplot(
            df_combined, aes(y="mean_r_data_diff_ns_per_value", x="name", fill="tool")
        )
        + geom_col(position="dodge")
        + geom_errorbar(aes(ymin="lower", ymax="upper"), position="dodge", width=0.9)
        + labs(y="DATA read time (ns/value)", x="FCS File", fill="Tool")
        + coord_flip()
        + scale_fill_discrete(limits=TOOLS)
    )
    read_text_plt.save(out_dir / "read_data.svg")

    read_text_plt = (
        ggplot(
            df_combined.filter(~pl.col("tool").eq("flowcore")),
            aes(y="mean_r_data_diff_ns_per_value", x="name", fill="tool"),
        )
        + geom_col(position="dodge")
        + geom_errorbar(aes(ymin="lower", ymax="upper"), position="dodge", width=0.9)
        + labs(y="DATA read time (ns/value)", x="FCS File", fill="Tool")
        + coord_flip()
        + scale_fill_discrete(limits=[t for t in TOOLS if not t == "flowcore"])
    )
    read_text_plt.save(out_dir / "read_data_no_flowcore.svg")


def plot_write_text(df: pl.DataFrame, out_dir: Path) -> None:
    df_mean = fill_cartesian(df, "mean_w_text_ns_per_kw", 0)
    df_serr = fill_cartesian(df, "serr_w_text_ns_per_kw", None)
    df_combined = (
        df_mean.join(df_serr, on=["name", "tool"])
        .with_columns(
            (pl.col("mean_w_text_ns_per_kw") - pl.col("serr_w_text_ns_per_kw")).alias(
                "lower"
            ),
            (pl.col("mean_w_text_ns_per_kw") + pl.col("serr_w_text_ns_per_kw")).alias(
                "upper"
            ),
        )
        .with_columns(pl.col("tool").cast(parser_enum()))
    ).filter(~pl.col("tool").eq("fcsparser"))

    read_text_plt = (
        ggplot(df_combined, aes(y="mean_w_text_ns_per_kw", x="name", fill="tool"))
        + geom_col(position="dodge")
        + geom_errorbar(aes(ymin="lower", ymax="upper"), position="dodge", width=0.9)
        + labs(y="TEXT write time (ns/keyword pair)", x="FCS File", fill="Tool")
        + coord_flip()
        + scale_fill_discrete(limits=[t for t in TOOLS if not t == "fcsparser"])
    )
    read_text_plt.save(out_dir / "write_text.svg")


def plot_write_data(df: pl.DataFrame, out_dir: Path) -> None:
    df_mean = fill_cartesian(df, "mean_w_data_diff_ns_per_value", 0)
    df_serr = fill_cartesian(df, "serr_w_data_diff_ns_per_value", None)
    df_combined = (
        df_mean.join(df_serr, on=["name", "tool"])
        .with_columns(
            (
                pl.col("mean_w_data_diff_ns_per_value")
                - pl.col("serr_w_data_diff_ns_per_value")
            ).alias("lower"),
            (
                pl.col("mean_w_data_diff_ns_per_value")
                + pl.col("serr_w_data_diff_ns_per_value")
            ).alias("upper"),
        )
        .with_columns(pl.col("tool").cast(parser_enum()))
    ).filter(~pl.col("tool").eq("fcsparser"))

    read_text_plt = (
        ggplot(
            df_combined, aes(y="mean_w_data_diff_ns_per_value", x="name", fill="tool")
        )
        + geom_col(position="dodge")
        + geom_errorbar(aes(ymin="lower", ymax="upper"), position="dodge", width=0.9)
        + labs(y="TEXT write time (ns/keyword pair)", x="FCS File", fill="Tool")
        + coord_flip()
        + scale_fill_discrete(limits=[t for t in TOOLS if not t == "fcsparser"])
    )
    read_text_plt.save(out_dir / "write_data.svg")


def main(args: list[str]) -> None:
    bench_path = Path(args[1])
    template_path = Path(args[2])
    static_dir = Path(args[3])
    readme_path = Path(args[4])
    static_dir.mkdir(parents=True, exist_ok=True)
    df_results = pl.read_csv(bench_path, separator="\t")

    plot_read_text(df_results, static_dir)
    plot_read_data(df_results, static_dir)
    plot_write_text(df_results, static_dir)
    plot_write_data(df_results, static_dir)

    env = Environment(loader=FileSystemLoader(template_path.parent))
    template = env.get_template(template_path.name)
    readme_path.parent.mkdir(exist_ok=True, parents=True)
    with open(readme_path, "w") as f:
        f.write(template.render())


main(sys.argv)
