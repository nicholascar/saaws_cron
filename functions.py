import logging
import settings
import MySQLdb
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def db_connect(host=settings.DB_HOST, user=settings.DB_USER, passwd=settings.DB_PASSWD, db=settings.DB_DB):
    logging.debug('ran db_connect()')

    try:
        conn = MySQLdb.Connection(host=host, user=user, passwd=passwd, db=db)
    except MySQLdb.Error, e:
        logging.error('get_db_values(): ' + e.message)
        return [False, e.message]

    return conn


def db_disconnect(conn):
    logging.debug('ran db_disconnect()')
    conn.close()


def db_query(conn, sql):
    logging.debug('ran db_query(' + sql.replace('\n', ' ').replace('\r', '') + ')')

    if conn is None:
        conn = db_connect()

    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()

    cursor.close()
    conn.commit()

    return rows


def gmail_send(receivers, subject, msg_txt, msg_html=None):
    """
    Send an email using this server's associated GMail account
    """
    logging.debug('[' + ','.join(receivers) + '], "' + subject + '", "' + msg_txt + '", "' + str(msg_html) +'"')

    # make the messages
    msg = MIMEMultipart('alternative')
    msg['Subject'] = '[saaws] ' + subject
    msg['From'] = settings.GMAIL_USR
    msg['To'] = ','.join(receivers)
    msg.attach(MIMEText(msg_txt, 'plain'))
    if msg_html is not None:
        msg.attach(MIMEText(msg_html, 'html'))

    # connect to the GMail server and send, log an error on fail
    try:
        smtpserver = smtplib.SMTP(settings.GMAIL_SVR)
        smtpserver.ehlo()
        smtpserver.starttls()
        smtpserver.ehlo()
        smtpserver.login(settings.GMAIL_USR, settings.GMAIL_PWD)
        smtpserver.sendmail(settings.GMAIL_USR, ','.join(receivers), msg.as_string())
        smtpserver.close()
        logging.debug('gmail email sent, subject: ' + subject)
    except Exception, e:
        logging.error('gmail_send(): ' + str(e))


def make_check_days_sql(day):
    day_str = day.strftime('%Y-%m-%d')
    return '''
        SELECT
            owner, t.*, manager_email, station_base_url
        FROM tbl_stations
        INNER JOIN
        (
            (
                # air temp null
                SELECT
                    aws_id,
                    'airT' AS 'var',
                    'air temp is null' AS 'msg'
                 FROM tbl_data_days
                WHERE
                    stamp = "''' + day_str + '''" AND
                    aws_id NOT LIKE 'TBRG%' AND
                    airT_avg IS NULL
            )
            UNION
            (
                # air temp range
                SELECT
                    aws_id,
                    'airT' AS 'var',
                    'air temp avg is outside allowed range (5 - 35)' AS 'msg'
                 FROM tbl_data_days
                WHERE
                    stamp = "''' + day_str + '''" AND
                    aws_id NOT LIKE 'TBRG%' AND
                    airT_avg NOT BETWEEN 5 AND 35
            )
            UNION
            (
                # ET null
                SELECT
                    aws_id,
                    'et_asce_t' AS 'var',
                    'ET is null' AS 'msg'
                FROM tbl_data_days
                WHERE
                    stamp = "''' + day_str + '''" AND
                    aws_id NOT LIKE 'TBRG%' AND
                    et_asce_t IS NULL
            )
            UNION
            (
                # ET range
                SELECT
                    aws_id,
                    'et_asce_t' AS 'var',
                    'ET outside allowed range (1 - 15)' AS 'msg'
                FROM tbl_data_days
                WHERE
                    stamp = "''' + day_str + '''" AND
                    aws_id NOT LIKE 'TBRG%' AND
                    et_asce_t NOT BETWEEN 1 AND 15
            )
            ORDER BY aws_id
        ) AS t
        ON tbl_stations.aws_id = t.aws_id
        INNER JOIN tbl_owners
        ON tbl_stations.owner = tbl_owners.owner_id;
        '''


def make_check_minutes_sql(day):
    day_str = day.strftime('%Y-%m-%d')
    return '''
        SELECT
            owner, t.*, manager_email, station_base_url
        FROM tbl_stations
        INNER JOIN
        (
            (
                # air temp
                SELECT
                    aws_id,
                    'airT' AS var,
                    'air temp outside allowed range (-10, 50)' AS msg
                FROM tbl_data_minutes
                WHERE
                    aws_id NOT LIKE 'TBRG%' AND
                    DATE(stamp) = "''' + day_str + '''" AND
                    (airT IS NULL OR airT NOT BETWEEN -10 AND 50)
                GROUP BY aws_id
            )
            UNION
            (
                # wind
                SELECT
                    aws_id,
                    'Wmax' AS var,
                    'wind max zero or null' AS msg
                    FROM tbl_data_minutes
                    WHERE
                        aws_id NOT LIKE 'TBRG%' AND
                        DATE(stamp) = "''' + day_str + '''"
                    GROUP BY aws_id
                    HAVING (SUM(Wmax) <= 0 OR SUM(Wmax) IS NULL)
            )
            UNION
            (
                # rain
                SELECT
                    aws_id,
                    'rain' AS var,
                    'rain outside allowed range (0, 100)' AS msg
                FROM tbl_data_minutes
                WHERE
                    DATE(stamp) = "''' + day_str + '''" AND
                    (rain IS NULL OR rain NOT BETWEEN 0 AND 100)
                GROUP BY aws_id
            )
            UNION
            (
                # battery - 6-7 range for AWNRM
                SELECT
                    aws_id,
                    'batt' AS var,
                    'battery outside allowed range (12, 15.5)' AS msg
                FROM tbl_data_minutes
                WHERE
                    DATE(stamp) = "''' + day_str + '''" AND
                    (
                        batt IS NULL OR
                        #batt NOT BETWEEN 6 AND 7 OR -- I don't know why I can't include this range here.
                        batt NOT BETWEEN 12 AND 15.5
                    )
                GROUP BY aws_id
            )
            ORDER BY aws_id
        ) AS t
           ON tbl_stations.aws_id = t.aws_id
           INNER JOIN tbl_owners
           ON tbl_stations.owner = tbl_owners.owner_id;
        '''
