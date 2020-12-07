# -*- coding: utf-8 -*-
"""
Created on Wed May 29 13:52:24 2019
script to build a weeek long water quality data
report and send it as a gmail attachment
Read the token built by quickstart.py from credentials.json
Follow tutorial here https://developers.google.com/gmail/api/quickstart/python
to enable Google Gmail API and get credentials.json file
@author: jsaracen
"""
# import necessary packages
import base64
from datetime import date
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import fnmatch
from io import BytesIO, StringIO
import mimetypes
import os
import pickle
import sys
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from google.cloud import bigquery, storage
from google.oauth2 import service_account
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
# set the figure color palette
# y
# set the global figure font size
# plt.rcParams.update({"font.size": 20})
# plt.rcParams.update({"axes.facecolor": "snow"})
# plt.rcParams.update({"image.cmap": "tab10"})
# sns.set(font="Arial Black")
# sns.set_style("ticks")
# sns.set_context("poster")
# sns.set(font_scale=2)
sns.set(font_scale=2, style="whitegrid")

plt.rcParams.update({"axes.facecolor": "snow"})
plt.rcParams["xtick.major.size"] = 12
plt.rcParams["ytick.major.size"] = 12
plt.rcParams["ytick.major.width"] = 1
plt.rcParams["xtick.major.width"] = 1
plt.rcParams["xtick.bottom"] = True
plt.rcParams["ytick.left"] = True


def send_message(service, user_id, message):
    """Send an email message.
    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      message: Message to be sent.
    Returns:
      Sent Message.
    """
    try:
        message = (
            service.users().messages().send(userId=user_id, body=message).execute()
        )
        print("Message Id: %s" % message["id"])
        return message
    except HttpError as error:
        print("An error occurred:", error)


def create_message_with_attachment(sender, to, subject, message_text, file):
    """Create a message for an email.
    Args:
      sender: Email address of the sender.
      to: Email address of the receiver.
      subject: The subject of the email message.
      message_text: The text of the email message.
      file: The path to the file to be attached.
    Returns:
      An object containing a base64url encoded email object.
    """
    message = MIMEMultipart()
    if isinstance(to, list):
        message["to"] = ", ".join(to)
    else:
        message["to"] = to

    message["from"] = sender
    message["subject"] = subject

    msg = MIMEText(message_text)
    message.attach(msg)
    content_type, encoding = mimetypes.guess_type(file)

    if content_type is None or encoding is not None:
        content_type = "application/octet-stream"

    main_type, sub_type = content_type.split("/", 1)
    if main_type == "text":
        fp = open(file, "rb")
        msg = MIMEText(fp.read(), _subtype=sub_type)
        fp.close()
    elif main_type == "image":
        fp = open(file, "rb")
        msg = MIMEImage(fp.read(), _subtype=sub_type)
        fp.close()
    else:
        fp = open(file, "rb")
        msg = MIMEBase(main_type, sub_type)
        msg.set_payload(fp.read())
        fp.close()
    filename = os.path.basename(file)
    msg.add_header("Content-Disposition", "attachment", filename=filename)
    message.attach(msg)
    encoded_message = base64.urlsafe_b64encode(message.as_bytes())
    return {"raw": encoded_message.decode()}


def format_dataframe(dataframe, index_name, interval="5Min"):
    dataframe.index = dataframe[index_name]
    # convert the index to a pandas datetimeindex
    dataframe.index = pd.to_datetime(dataframe.index)
    dataframe.drop([index_name], axis=1, inplace=True)
    # drop empty rows
    dataframe = dataframe[dataframe.index.notnull()]
    # sort the dataframe into descending order
    dataframe = dataframe.sort_index()
    # slice data to start at deployment date
    # dataframe = dataframe[deployment_date:]
    # convert values to floating points
    dataframe = dataframe.astype(float)
    # round the data time stamp to the nearest defined interval for later merging
    dataframe.index = dataframe.index.round(interval)
    return dataframe.copy()


def fetch_bucket(bucket_name, bucket_filename):
    """Function to fetch a gcs storage
    object and return a pandas dataframe"""
    bucket = storage_client.get_bucket(bucket_name)
    blob = storage.blob.Blob(bucket_filename, bucket)
    content = blob.download_as_string()
    dfbytes = pd.read_csv(
        BytesIO(content),
        na_values=["NAN", "nan", -999],
    )
    return dfbytes


def push_bucket(dataframe, bucket_name, bucket_filename):
    """Function to push a pandas dataframe
    to a gcs storage object"""
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(bucket_filename_write)
    # saving a data frame to a buffer (same as with a regular file):
    sio = dataframe.to_csv()
    blob.upload_from_string(data=sio)
    return


def make_a_plot(df, figname, savefig=True):
    """Function to plot data as stacked time series plots"""
    # make the datetimeindex timezone naive to correctly work with plot ticks
    df.index = df.index.tz_localize(None)
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
    axes[3].set_ylim(-100, 100)
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


if __name__ == "__main__":
    os.chdir(os.path.dirname(sys.argv[0]))
    # print(os.getcwd())
    print(os.listdir())

    # Now send or store the message
    # SENDER = "anapossensing@gmail.com"
    SENDER = "jfsaraceno@gmail.com"

    # TODO:Read list of recipients from a file
    RECIPIENTS = [
        # "report-testing@googlegroups.com",
        "LandIQ-data-reports@googlegroups.com",
        # "anapossensing@gmail.com",
    ]
    # Read the token built by quickstart.py from credentials.json
    # Follow tutorial https://developers.google.com/gmail/api/quickstart/python
    GMAIL_CREDS = None
    gmail_token_file = "token.pickle"
    if os.path.exists(gmail_token_file):
        print("Using credential file {}".format(gmail_token_file))
        with open(gmail_token_file, "rb") as token:
            GMAIL_CREDS = pickle.load(token)
    # create the gmail service obj from the token built by quickstart.py
    GMAIL_SERVICE = build("gmail", "v1", credentials=GMAIL_CREDS)
    # bq and gcs credentials
    # STORAGE_CREDS = json.load(open("storage_service_account.json"))
    # STORAGE_SERVICE = service_account.Credentials.from_service_account_info(
    #     STORAGE_CREDS
    # )

    storage_client = storage.Client.from_service_account_json(
        "thermoflux-particle-6cb499f95f01.json"
    )
    # validate the service account by listing the projecy buckets
    list(storage_client.list_buckets())
    bucket_name = "thermoflux-bq-data"
    bucket_filename_read = "demo_table_backup.csv"
    bucket_filename_write = "demo-table-export.csv"

    # FILENAME = "gs://{}/{}".format(bucket_name, bucket_filename)

    # storage_client = storage.Client(project = project_id)

    # set path to filename
    # FILENAME = r"gs://thermoflux-bq-data/demo_table_backup.csv"
    df_full = fetch_bucket(bucket_name, bucket_filename_read)
    df_flux = format_dataframe(
        df_full[["fluxTimeStamp", "flux"]], "fluxTimeStamp", interval="30Min"
    )
    df_ancillary = format_dataframe(
        df_full[["temperature", "netRadiation", "battery", "ancillaryTimeStamp"]],
        "ancillaryTimeStamp",
        interval="5Min",
    )
    df = df_ancillary.merge(df_flux, how="outer", left_index=True, right_index=True)
    # shift the time from UTC to local
    df.index = df.index.tz_convert("US/Pacific")
    # set the index name for plotting and output
    df.index.name = "Datetime (PST)"

    push_bucket(df, bucket_name, bucket_filename_write)
    sites = ["LT_MicroIQ_Alfalfa"]
    today = date.today().strftime("%m/%d/%Y")

    for SITE in sites:
        print(SITE)
        data = df.copy()
        # data.rename("")
        columns = data.columns
        rename_cols = [
            "Temperature",
            "Net Radiation",
            "Battery Voltage",
            "Sensible Heat Flux",
        ]
        rename_cols_dict = dict(zip(columns, rename_cols))
        data.rename(columns=rename_cols_dict, inplace=True)
        BODY_TEXT = "This is the daily report for the {} site".format(SITE)
        # create the output figure name
        FIGNAME = SITE + " - Daily " + ".png"
        # plot the data and save as an image
        make_a_plot(data, FIGNAME)
        # build an email and add the report as an attachment
        raw_msg_attch = create_message_with_attachment(
            SENDER,
            RECIPIENTS,
            "{} Daily Report ".format(SITE) + today,
            BODY_TEXT,
            FIGNAME,
        )
        # send the email
        send_message(GMAIL_SERVICE, "me", raw_msg_attch)
        # time.sleep(30)
