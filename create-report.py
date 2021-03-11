"""
Create a report file for emailing to participants
"""


def make_a_plot(dataframe, figname, savefig=True):
    """Function to plot data as stacked time series plots"""
    # make the datetimeindex timezone naive to correctly work with plot ticks
    df = dataframe.copy()
    df.index = df.index.tz_localize(None)
    # df = df["02-26-2021":].copy()
    plot_cols = df.columns
    n_plot_cols = 4
    # colors = plt.cm.tab10(np.linspace(0, 1, n_plot_cols))
    # scale the figure to the number of plots
    fig_height = np.ceil(4 * n_plot_cols)
    fig_width = 10
    # make the axis one week long, starting on the 7 days from the current day
    n_day_window = 7
    # get the latest day in the dataset
    datemax = np.datetime64(df.index[-1], "D") + np.timedelta64(1, "D")
    # set a 7 day window from the latest day
    datemin = datemax - np.timedelta64(n_day_window, "D")
    # create the x axis tick labels and pass them, to the pandas plot wrapper
    dates_rng = pd.date_range(datemin, datemax, freq="1D")
    # create a figure layout for the subplots
    fig, axes = plt.subplots(
        n_plot_cols, 1, figsize=(fig_width, fig_height), sharex=False
    )
    # axes.set_prop_cycle(
    #     "color", [plt.cm.tab10(i) for i in np.linspace(0, 1, n_plot_cols)
    # )
    my_colors = [plt.cm.Set1(i) for i in np.linspace(0, 1, n_plot_cols)]
    # df.plot(subplots=True, ax=axes, xticks=dates_rng, marker="o")
    report_title = figname.split(".")[0]
    # axes[0].set_title(report_title)
    fig.suptitle(
        report_title,
        fontsize=26,
        fontweight="bold",
    )

    axes[0].plot(df.index.values, df["Battery Voltage"], marker="o", color="tab:blue")
    # do some axis operations for each subplot
    # add titles
    # add axis labels
    axes[0].set_title("Battery Voltage", fontweight="bold")
    axes[0].set_ylabel("V")
    axes[0].set_xlabel("")
    # round to nearest days.
    axes[0].set_xlim(datemin, datemax)
    # format the ticks
    axes[0].xaxis.set_major_locator(days)
    axes[0].xaxis.set_major_formatter(date_form)
    axes[0].xaxis.set_minor_locator(hours)
    # add cool legend
    # axes[0].legend().remove()
    axes[0].grid(color="k", ls="--", lw=1.25)

    # plot the data
    axes[1].plot(
        df.index.values,
        df["Temperature"],
        marker="^",
        color="tab:orange",
    )
    # format the ticks
    axes[1].xaxis.set_major_locator(days)
    axes[1].xaxis.set_major_formatter(date_form)
    axes[1].xaxis.set_minor_locator(hours)
    # add axis labels
    axes[1].set_title("Temperature", fontweight="bold")
    axes[1].set_ylabel("Celsius")
    axes[1].set_xlabel("")
    axes[1].set_xlim(datemin, datemax)
    axes[1].set_ylim(-5, 30)

    axes[1].grid(color="k", ls="--", lw=1.25)
    # plot the data
    axes[2].plot(df.index.values, df["Net Radiation"], marker="o", color="tab:cyan")
    # format the ticks
    axes[2].xaxis.set_major_locator(days)
    axes[2].xaxis.set_major_formatter(date_form)
    axes[2].xaxis.set_minor_locator(hours)
    axes[2].set_xlim(datemin, datemax)
    # add axis labels
    axes[2].set_title("Net Radiation", fontweight="bold")
    axes[2].set_ylabel(r"$\mathregular{W/m^{2}}$")
    axes[2].set_xlabel("")
    axes[2].grid(color="k", ls="--", lw=1.25)
    # plot the data
    axes[3].plot(
        *splitSerToArr(df["Sensible Heat Flux"].dropna()),
        marker="o",
        markersize=6,
        ls="-",
        color="tab:brown"
    )

    # axes[3].plot(
    #     df.index.values,
    #     df["Sensible Heat Flux"],
    #     marker="o",
    #     markersize=6,
    #     ls="-",
    #     color="tab:brown",
    # )
    # format the ticks
    axes[3].xaxis.set_major_locator(days)
    axes[3].xaxis.set_major_formatter(date_form)
    axes[3].xaxis.set_minor_locator(hours)
    axes[3].set_ylim(-200, 400)
    axes[3].set_xlim(datemin, datemax)
    # add axis labels
    axes[3].set_title("Sensible Heat Flux", fontweight="bold")
    axes[3].set_ylabel(r"$\mathregular{W/m^{2}}$")
    axes[3].set_xlabel("")
    axes[3].grid(color="k", ls="--", lw=1.25)
    # format the plot panel
    fig.patch.set_facecolor("white")
    # use tight layout to shrink it all to fit nicely
    plt.tight_layout()
    plt.subplots_adjust(top=0.93)  # Add space at top
    # save the figure as a pdf file
    image_fmt = figname.split(".")[-1]
    if savefig:
        plt.savefig(figname, format=image_fmt)
    return
