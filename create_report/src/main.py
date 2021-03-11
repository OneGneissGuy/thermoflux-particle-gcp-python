# -*- coding: utf-8 -*-
"""
Created on Wed May 29 13:52:24 2019
script to build a week long water quality data
report and send it as a Gmail attachment
Read the token built by quickstart.py from credentials.json
Follow tutorial here https://developers.google.com/gmail/api/quickstart/python
to enable Google Gmail API and get credentials.json file

This script can be run as a cron job:
5 7 * * * conda activate gcp-thingspeak;python /home/jf/Documents/projects/thermoflux-particle-gcp-python/email_report_gcf.py >> /home/jf/Documents/projects/thermoflux-particle-gcp-python/cron.log 2>&1;conda deactivate

@author: John Franco Saraceno
"""
# import necessary packages
import base64
from datetime import date
from io import BytesIO, StringIO
import os
import sys
from google.cloud import storage
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import seaborn as sns

# set plotting format
days = mdates.DayLocator(interval=1)  # every year
hours = mdates.HourLocator(interval=6)  # every month
date_form = mdates.DateFormatter("%m/%d")
# colors
FLATUI = ["#9b59b6", "#3498db", "#95a5a6", "#e74c3c", "#34495e", "#2ecc71", "#f4cae4"]

sns.set(font_scale=2, style="whitegrid")

plt.rcParams.update({"axes.facecolor": "snow"})
plt.rcParams["xtick.major.size"] = 12
plt.rcParams["ytick.major.size"] = 12
plt.rcParams["ytick.major.width"] = 1
plt.rcParams["xtick.major.width"] = 1
plt.rcParams["xtick.bottom"] = True
plt.rcParams["ytick.left"] = True


def upload_blob(bucket_name, source_file_name, destination_blob_name, storage_client):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name))
    return


def fetch_bucket(bucket_name, bucket_filename):
    """Function to fetch a gcs storage
    object and return a pandas dataframe"""
    bucket = storage_client.get_bucket(bucket_name)
    blob = storage.blob.Blob(bucket_filename, bucket)
    content = blob.download_as_string()
    dfbytes = pd.read_csv(
        BytesIO(content), na_values=["NAN", "nan", -999], index_col="Datetime (PST)"
    )
    dfbytes.index = pd.to_datetime(dfbytes.index)
    return dfbytes


def make_a_plot(df, figname, savefig=True):
    """Function to plot data as stacked time series plots"""
    # make the datetimeindex timezone naive to correctly work with plot ticks
    df.index = df.index.tz_localize(None)
    df = df.copy()
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

    axes[0].plot(
        df.index.values, df["Battery voltage (V)"], marker="o", color="tab:blue"
    )
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
        df["Temperature (C)"],
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
    axes[2].plot(
        df.index.values, df["Net Radiation (W/m2)"], marker="o", color="tab:cyan"
    )
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
        *splitSerToArr(df["Sensible heat flux (W/m2)"].dropna()),
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


def splitSerToArr(ser):
    return [ser.index, ser.values]


def create_report(event, context):
    bucket_name = os.environ["BUCKET_NAME"]  # "thermoflux-output"
    bucket_filename_read = os.environ[
        "BUCKET_FILENAME_READ"
    ]  # "alfalfa_demo_table_output.csv"
    reports_bucket_name = os.environ["REPORTS_BUCKET_NAME"]  # "thermoflux-reports"
    report_filename = os.environ["REPORT_FILENAME"]  # "report.png"
    # bucket_name = "thermoflux-output"
    # bucket_filename_read = "alfalfa_demo_table_output.csv"
    # reports_bucket_name = "thermoflux-reports"
    # report_filename = "report.png"
    sites = ["LT_MicroIQ_Alfalfa"]
    today = date.today().strftime("%m/%d/%Y")

    storage_client = storage.Client()
    for SITE in sites:
        print(SITE)
        # create the output figure name
        FIGNAME = SITE + " - Daily" + ".png"
        data = fetch_bucket(bucket_name, bucket_filename_read)
        # plot the data and save as an image
        make_a_plot(data, FIGNAME)
        # upload the report to the report gcs data bucket
        upload_blob(reports_bucket_name, FIGNAME, report_filename, storage_client)
    print("Done!")
