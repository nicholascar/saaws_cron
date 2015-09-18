import logging
import settings
import MySQLdb
import smtplib
import os


def db_connect(host=settings.DB_HOST, user=settings.DB_USER, passwd=settings.DB_PASSWD, db=settings.DB_DB):
    logging.debug('ran get_db_values()')

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
    logging.debug('ran get_db_values()')

    if conn is None:
        conn = db_connect()

    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()

    cursor.close()
    conn.commit()

    return rows


def gmail_send(receivers, subject, msg):
    """
    Send an email using this server's associated GMail account
    """
    gmail_user = settings.GMAIL_USR
    gmail_pwd = settings.GMAIL_PWD
    smtpserver = smtplib.SMTP(settings.GMAIL_SVR)
    smtpserver.ehlo()
    smtpserver.starttls()
    smtpserver.ehlo
    txt = '\r\n'.join([
        'To:' + ','.join(receivers),
        'From: ' + gmail_user,
        'Subject: ' + subject,
        '',
        msg,
        ''
    ])

    # log an error on fail
    try:
        smtpserver.login(gmail_user, gmail_pwd)
        smtpserver.sendmail(gmail_user, ','.join(receivers), txt)
        smtpserver.close()
        logging.debug('gmail email sent, msg: ' + msg)
    except Exception, e:
        logging.error("gmail_send(): " + str(e))

