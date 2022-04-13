import telegram
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import io
import logging
import pandas as pd
import pandahouse
from read_db.CH import Getch
import os


def check_last_value_anomaly(df, metric, a=4, n=5):
    """Checking is dataframe last value between 0.25 and 0.75 quantile. If not rise is_alert flag"""
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1] :
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df


def prepare_report_msg(value, deviation, metric_label, app_part, link):
    """Preparing text message including: current value, relative deviation,link to dashboard"""
    msg = (f"Anomaly with *{metric_label}({app_part})*\n"
           f"Current vlaue: *{value}*\n"
           f"Relative deviation: *{'+'if deviation>0 else ''}{deviation:.1%}*\n"
           f"More details can be found on dashboard: {link}\n"
           f"Responsible:@bebyakin")
    return msg


def prepare_report_plot(df, metric, metric_label, app_part):
    """Preparing plot file object showing metric changes based on df"""
    sns.set(rc={'figure.figsize':(11,8)})
    plt.tight_layout()
    ax = sns.lineplot(x=df['ts'], y=df[metric], label=metric_label)
    ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
    ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')
    
    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % 2 == 0:
            label.set_visible(1)
        else:
            label.set_visible(0)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    ax.set(xlabel='time')
    ax.set(ylabel=metric_label)
    ax.set_title(f"{metric_label}({app_part}) {df['ts'].min().strftime('%Y-%m-%d, %H:%M')} - {df['ts'].max().strftime('%Y-%m-%d, %H:%M')}", fontsize=17)
    ax.set(ylim=(0, None))

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = f"{metric}.png"
    plt.close()
    return plot_object


def report(msg, photo):
    """Reporting msg and photo to telegram chat"""
    chat_id = os.environ.get("REPORT_CHAT_ID")
    bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))
    bot.sendPhoto(chat_id=chat_id, photo=photo, caption=msg, parse_mode="Markdown")

    
def check_anomaly(data, metric, metric_label, app_part, link):
    """Checkng data frame for anamalies with metric and reporting if found anomaly"""
    is_alert, df = check_last_value_anomaly(df=data, metric=metric)
    if is_alert:
        deviation = df[metric].iloc[-1]/df[metric].iloc[-2] - 1
        msg = prepare_report_msg(value=df[metric].iloc[-1], deviation=deviation, metric_label=metric_label, app_part=app_part, link=link)
        plot = prepare_report_plot(df=df, metric=metric, metric_label=metric_label, app_part=app_part)
        report(msg=msg,photo=plot)

def feed_users_anomaly():
    """Qurey to simulator_20220320.feed_actions and checking anomalies with users activity"""
    data = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , uniqExact(user_id) as users
                    FROM simulator_20220320.feed_actions
                    WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts
                    ORDER BY ts ''').df
    link = "https://superset.lab.karpov.courses/superset/dashboard/590/"
    check_anomaly(data=data, metric='users', metric_label='Active users', app_part="feed", link=link)

def messanger_users_anomaly():
    """Qurey to simulator_20220320.message_actions and checking anomalies with users activity"""
    data = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , uniqExact(user_id) as users
                    FROM simulator_20220320.message_actions
                    WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts
                    ORDER BY ts ''').df
    link = "https://superset.lab.karpov.courses/superset/dashboard/624/"
    check_anomaly(data=data, metric='users', metric_label='Active users', app_part="messages", link=link)
    
    
def views_anomaly():
    """Qurey to simulator_20220320.feed_actions and checking anomalies with views"""
    data = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , countIf(action = 'view') as views
                    FROM simulator_20220320.feed_actions
                    WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts
                    ORDER BY ts ''').df
    link = "https://superset.lab.karpov.courses/superset/dashboard/590/"
    check_anomaly(data=data, metric='views', metric_label='Views', app_part="feed", link=link)


    
def likes_anomaly():
    """Qurey to simulator_20220320.feed_actions and checking anomalies with likes"""
    data = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , countIf(action = 'like') as likes
                    FROM simulator_20220320.feed_actions
                    WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts
                    ORDER BY ts ''').df
    link = "https://superset.lab.karpov.courses/superset/dashboard/590/"
    check_anomaly(data=data, metric='likes', metric_label='Likes', app_part="feed", link=link)


    
def ctr_anomaly():
    """Qurey to simulator_20220320.feed_actions and checking anomalies with CTR"""
    data = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , round(countIf(action = 'like')/countIf(action = 'view'),3) as CTR
                    FROM simulator_20220320.feed_actions
                    WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts
                    ORDER BY ts ''').df
    link = "https://superset.lab.karpov.courses/superset/dashboard/590/"
    check_anomaly(data=data, metric='CTR', metric_label='CTR', app_part="feed", link=link)


def send_messages_anomaly():
    """Qurey to simulator_20220320.message_actions and checking anomalies with send messages"""
    data = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , count(user_id) as message
                    FROM simulator_20220320.message_actions
                    WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts
                    ORDER BY ts ''').df
    link = "https://superset.lab.karpov.courses/superset/dashboard/624/"
    check_anomaly(data=data, metric='message', metric_label='Send messages', app_part="messages", link=link)

def check_anomalies():
    feed_users_anomaly()
    messanger_users_anomaly()
    views_anomaly()
    likes_anomaly()
    ctr_anomaly()
    send_messages_anomaly()

try:
    check_anomalies()
except Exception as e:
    print(e)  

