import os
import logging
import settings
import datetime
from datetime import timedelta
import functions


def job_check_daemon_running():
    # get the PID from file
    try:
        with open('/opt/processor/procdeamon.pid', 'r') as p:
            pid_file = p.read()
    except IOError, e:
        fail_text = 'PID file not found or cannot be read'
        logging.error(fail_text)
        functions.gmail_send(settings.ERROR_MSG_RECEIVERS, 'ERROR: daemon down', fail_text)
        return

    # get the PID for the running process
    pid_running = None
    p = os.popen("ps x | grep procdeamon | grep -v 'grep' | awk '{print $1}'", "r")
    while 1:
        line = p.readline()
        if not line:
            break
        pid_running = line

    if pid_running is None:
        fail_text = 'PID running not found'
        logging.error(fail_text)
        functions.gmail_send(settings.ERROR_MSG_RECEIVERS, 'ERROR: daemon down', fail_text)
        return
    elif pid_file != pid_running:
        fail_text = 'PID file not equal to PID running'
        logging.error(fail_text)
        functions.gmail_send(settings.ERROR_MSG_RECEIVERS, 'ERROR: daemon down', fail_text)
        return
    else:
        # system is running
        logging.debug('daemon up')
        return


def job_wdtf_export(day):
    logging.debug('ran job_wdtf_export(' + day.strftime('%Y-%m-%d') + ')')

    import functions_wdtf
    s = functions_wdtf.send_add_wdtf_zipfiles_to_bom(day)

    if not s:
        functions.gmail_send(settings.ERROR_MSG_RECEIVERS, 'ERROR: WDTF exporter broken', 'check log')

    return


def job_dfw_csv_export(day):
    logging.debug('ran job_csv_export(' + day.strftime('%Y-%m-%d') + ')')

    import functions_csv
    s = functions_csv.send_all_csv_files_to_dfw(day)

    if not s:
        functions.gmail_send(settings.ERROR_MSG_RECEIVERS, 'ERROR: CSV exporter broken', 'check log')

    return


def job_calc_days(day):
    logging.debug('ran job_calc_days(' + day.strftime('%Y-%m-%d') + ')')

    sql = 'CALL proc_day_calcs("' + day.strftime('%Y-%m-%d') + '")'
    conn = functions.db_connect()
    functions.db_query(conn, sql)
    functions.db_disconnect(conn)

    return [True, 'job calc_days']


def job_check_values(day, minutes_or_days):
    logging.debug('ran job_check_minutes_values(' + day.strftime('%Y-%m-%d') + ',' + minutes_or_days + ')')

    if minutes_or_days == 'minutes':
        timestep = 'minutes'
        view = 'today'
        sql = functions.make_check_minutes_sql(day)
    else:  # days
        timestep = 'days'
        view = '7days'
        sql = functions.make_check_days_sql(day)

    # get the data
    conn = functions.db_connect()
    rows = functions.db_query(conn, sql)
    functions.db_disconnect(conn)

    last_owner = ''
    last_owner_email = ''
    last_owner_html = ''
    admin_html = ''
    html_header = '<h4>Errors in ' + timestep + ' readings for ' + day.strftime('%Y-%m-%d') + ':</h4>\n'
    table_top = '<table>\n'
    table_header_owner = '\t<tr><th>aws_id</th><th>Variable</th><th>Message</th></tr>\n'
    table_header_admin = '\t<tr><th>aws_id</th><th>Variable</th><th>Message</th><th>Owner</th></tr>\n'
    table_bottom = '</table>\n'
    for row in rows:
        print row
        # if we have a new owner...
        if row['owner'] != last_owner:
            # if last owner was a real owner, send email
            if last_owner != '':
                msg = html_header + table_top + table_header_owner + last_owner_html + table_bottom
                functions.gmail_send([last_owner_email], timestep + ' data errors', 'message is in html', msg)
            # create new owner
            last_owner = row['owner']
            last_owner_email = row['manager_email']
            last_owner_html = ''

        last_owner_html += '\t<tr><td><a href="' + row['station_base_url'] + '?aws_id=' + row['aws_id'] + '&view=' + view + '">' + row['aws_id'] + '</a></td><td>' + row['variable'] + '</td><td>' + row['message'] + '</td></tr>\n'
        admin_html += '\t<tr><td><a href="' + row['station_base_url'] + '?aws_id=' + row['aws_id'] + '&view=' + view + '">' + row['aws_id'] + '</a></td><td>' + row['variable'] + '</td><td>' + row['message'] + '</td><td>' + last_owner + '</td></tr>\n'

    # send to the last owner
    msg = html_header + table_top + table_header_owner + last_owner_html + table_bottom
    functions.gmail_send([last_owner_email], timestep + ' data errors', 'message is in html', msg)

    # send the admin email (all stations)
    msg = html_header + table_top + table_header_admin + admin_html + table_bottom
    functions.gmail_send(settings.ERROR_MSG_RECEIVERS, timestep + ' data errors', 'message is in html', msg)

    return


def job_check_latest_readings():
    logging.debug('ran job_check_latest_readings()')
    sql = '''
        SELECT m.aws_id, m.name, m.owner, m.manager_email, m.station_base_url FROM (
            SELECT
                a.aws_id AS aws_id,
                a.name,
                a.owner,
                a.manager_email,
                a.station_base_url,
                b.aws_id AS other
            FROM (
                SELECT
                    aws_id,
                    NAME,
                    OWNER,
                    manager_email,
                    station_base_url
                FROM tbl_stations
                INNER JOIN tbl_owners
                ON tbl_stations.owner = tbl_owners.owner_id
                WHERE STATUS = 'on' ORDER BY aws_id) AS a
            LEFT JOIN (SELECT DISTINCT aws_id FROM tbl_data_minutes WHERE DATE(stamp) = CURDATE()) AS b
            ON a.aws_id = b.aws_id
            HAVING b.aws_id IS NULL) AS m
        ORDER BY OWNER, aws_id;
    '''

    # get the data
    conn = functions.db_connect()
    rows = functions.db_query(conn, sql)
    functions.db_disconnect(conn)

    last_owner = ''
    last_owner_email = ''
    last_owner_html = ''
    admin_html = ''
    html_header = '<h4>Stations that are on but have failed to report today:</h4>\n'
    table_top = '<table>\n'
    table_header_owner = '\t<tr><th>aws_id</th><th>Name</th></tr>\n'
    table_header_admin = '\t<tr><th>aws_id</th><th>Name</th><th>Owner</th></tr>\n'
    table_bottom = '</table>\n'
    for row in rows:
        print row
        # if we have a new owner...
        if row['owner'] != last_owner:
            # if last owner was a real owner, send email
            if last_owner != '':
                msg = html_header + table_top + table_header_owner + last_owner_html + table_bottom
                functions.gmail_send([last_owner_email], 'stations failing to report today', 'message is in html', msg)
            # create new owner
            last_owner = row['owner']
            last_owner_email = row['manager_email']
            last_owner_html = ''

        last_owner_html += '\t<tr><td><a href="' + row['station_base_url'] + '?aws_id=' + row['aws_id'] + '">' + row['aws_id'] + '</a></td><td>' + row['name'] + '</td></tr>\n'
        admin_html += '\t<tr><td><a href="' + row['station_base_url'] + '?aws_id=' + row['aws_id'] + '">' + row['aws_id'] + '</a></td><td>' + row['name'] + '</td><td>' + last_owner + '</td></tr>\n'

    # send to the last owner
    msg = html_header + table_top + table_header_owner + last_owner_html + table_bottom
    functions.gmail_send([last_owner_email], 'stations failing to report today', 'message is in html', msg)

    # send the admin email (all stations)
    msg = html_header + table_top + table_header_admin + admin_html + table_bottom
    functions.gmail_send(settings.ERROR_MSG_RECEIVERS, 'stations failing to report today', 'message is in html', msg)

    return


if __name__ == "__main__":
    logging.basicConfig(filename=settings.LOG_FILE,
                        format='%(asctime)s %(levelname)s %(message)s',
                        level=settings.LOG_LEVEL)
    # logging.debug('ran saaws_cron main()')

    hr = datetime.datetime.now().hour

    try:
        # these run every hour
        job_check_daemon_running()

        # these run on specific hours
        if hr == 1:
            yesterday = datetime.datetime.now() - timedelta(hours=24)
            job_wdtf_export(yesterday)
            job_dfw_csv_export(yesterday)
        elif hr == 2:
            yesterday = datetime.datetime.now() - timedelta(hours=24)
            job_calc_days(yesterday)
        elif hr == 8:
            yesterday = datetime.datetime.now() - timedelta(hours=24)
            job_check_values(yesterday, 'days')
            # check minute values for yesterday to cover time after 15:00 check
            job_check_values(yesterday, 'minutes')
        elif hr in [9, 12, 15]:
            today = datetime.datetime.now()
            job_check_values(today, 'minutes')
        elif hr == 10:
            job_check_latest_readings()
    except Exception, e:
        logging.error(e.message)
else:
    print 'ERROR: this file must be called from the command line, with arguments'
