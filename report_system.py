import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import logging
import pandas as pd
import pandahouse
from read_db.CH import Getch
import os


def prepare_report_msg():
    """Preparing  text message contains:
        1) metrics(Time In-App im minuts, User Actions Per Session) for yestrday
        2) growth(recession) percentage between yesterday and day before yesterday
        Information is being extracted from simulator_20220320.feed_actions and .message_actions tables.
        In the query, the values are taken with a gap(one day for each side), to avoid inaccuracy.
    """
    data = Getch('''
        SELECT
            toStartOfDay(start) as start_days,
            AVG(length)/60 AS "Time In-App",
            AVG(actions) AS "User Actions Per Session"
        FROM (
            SELECT global_session_id, 
               MAX(time) - MIN(time) AS length,
               MIN(time) AS start,
               COUNT(time) AS actions
                FROM (
                    SELECT user_id,
                       time,
                       SUM(is_new_session) OVER (ORDER BY user_id, time) AS global_session_id,
                       SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY time) AS user_session_id
                    FROM (
                        SELECT *,
                          CASE WHEN time - prev >= (60 * 10)
                                     OR prev IS NULL
                                   THEN 1 ELSE 0 END AS is_new_session
                        FROM (
                            SELECT user_id,
                               time,
                               if(user_id = neighbor(user_id, -1), neighbor(time, -1), NULL) AS prev,
                               time - prev
                            FROM (
                                SELECT * FROM( 
                                    (SELECT user_id, time from simulator_20220320.feed_actions)
                                    UNION ALL
                                    (SELECT user_id, time from simulator_20220320.message_actions))
                                ORDER BY user_id, time)
                            WHERE toStartOfDay(time) >= today() - 3
                            ORDER BY user_id, time
                          )
                        )
                    )
                GROUP BY global_session_id
                ORDER BY global_session_id
            )
        GROUP BY start_days
        ORDER BY start_days
            ''').df
    msg = (f"*Report for {str(data['start_days'][2]).split()[0]}(Feed and messages)*\n"
           f"Time In-App, min: {round((data['Time In-App'][2]),1)}   _{countGrowth(data['Time In-App'])}_\n"
           f"User Actions Per Session: {round(data['User Actions Per Session'][2],1)}   _{countGrowth(data['User Actions Per Session'])}_\n@bebyakin")
    return msg


def countGrowth(pdSeries):
    """Get pdSeries and return procent of growth(recession) between 1 and 2 elements.
    add + simbol if it growing.
    """
    value = float(pdSeries[2])/float(pdSeries[1]) -1 
    return ("+" if value>0 else "") + f"{value:.1%}"


def prepare_report_plot():
    """Preparing image message with plots showing metrics
    (Time In-App im minuts, User Actions Per Session) for previous week.
    Information is being extracted from simulator_20220320.feed_actions and .message_actions tables.
    In the query, the values are taken with a gap(one day for each side), to avoid inaccuracy.
    """
    data = Getch('''SELECT
    formatDateTime(toDate(toStartOfDay(toDateTime(start))), '%d/%m') as start_days,
    AVG(length)/60 AS "Time In-App, min",
    AVG(actions) AS "User Actions Per Session"
    FROM (
            SELECT global_session_id, 
                   MAX(time) - MIN(time) AS length,
                   MIN(time) AS start,
                   COUNT(time) AS actions
            FROM (
                SELECT user_id,
                       time,
                       SUM(is_new_session) OVER (ORDER BY user_id, time) AS global_session_id,
                       SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY time) AS user_session_id
                FROM (
                    SELECT *,
                          CASE WHEN time - prev >= (60 * 10)
                                     OR prev IS NULL
                                   THEN 1 ELSE 0 END AS is_new_session
                    FROM (
                        SELECT user_id,
                               time,
                               if(user_id = neighbor(user_id, -1), neighbor(time, -1), NULL) AS prev,
                               time - prev

                        FROM (
                            SELECT * FROM( 
                                (SELECT user_id, time from simulator_20220320.feed_actions)
                                UNION ALL
                                (SELECT user_id, time from simulator_20220320.message_actions))
                            ORDER BY user_id, time)
                        WHERE toStartOfDay(toDateTime(time)) >= today() - 8
                        AND toStartOfDay(toDateTime(time)) <= today()
                        ORDER BY user_id, time
                        )
                    )
                )
            GROUP BY global_session_id
            ORDER BY global_session_id
        )
    GROUP BY toStartOfDay(toDateTime(start))
    ORDER BY toStartOfDay(toDateTime(start))
    ''').df
    data = data[1:-1]
    d,t,u = data['start_days'],data['Time In-App, min'],data['User Actions Per Session']
    fig, axes = plt.subplots(2, 1, figsize=(8,5))
    fig.suptitle(f"{d.iloc[0]}-{d.iloc[-1]} week report(Feed and messages)", fontsize=17)
    [axi.tick_params(axis='x', colors='w') for axi in axes[:-1].ravel()]
    plt.subplots_adjust(top=0.93, bottom = .1, hspace = .15)
    sns.lineplot(ax=axes[0], x=d, y=t, marker="o")
    sns.lineplot(ax=axes[1], x=d, y=u, color='g',marker="o")
    plt.xticks(fontsize=11)
    plt.xlabel('Date', fontsize=12)
    sns.set_style("whitegrid")
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'test_plot.png'
    plt.close()
    return plot_object


def report(chat=None):
    """Call report_metrics and report_plots function
    to send two messages by telegram bot to chat via chat_id.
    telegram bot get token from environment variable REPORT_BOT_TOKEN
    chat_id is hardcoded into variable 
    """
    chat_id = os.environ.get("REPORT_BOT_TOKEN")
    bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))
    msg = prepare_report_msg()
    plot = prepare_report_plot()
    bot.sendPhoto(chat_id=chat_id, photo=plot, caption=msg, parse_mode="Markdown")


sns.set()
try:
    report()
except Exception as e:
    print(e)  

