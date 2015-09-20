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

