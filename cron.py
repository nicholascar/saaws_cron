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


def job_check_days_values(day):
    logging.debug('ran check_days_values(' + day.strftime('%Y-%m-%d') + ')')

    sql = '''
        (
            # air temp null
            SELECT
                aws_id,
                'airT' AS 'var',
                'air temp is null' AS 'msg'
             FROM tbl_data_days
            WHERE
                stamp = "''' + day.strftime('%Y-%m-%d') + '''" AND
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
                stamp = "''' + day.strftime('%Y-%m-%d') + '''" AND
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
                stamp = "''' + day.strftime('%Y-%m-%d') + '''" AND
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
                stamp = "''' + day.strftime('%Y-%m-%d') + '''" AND
                aws_id NOT LIKE 'TBRG%' AND
                et_asce_t NOT BETWEEN 1 AND 15
        )
        ORDER BY aws_id;
        '''

    # get the data
    conn = functions.db_connect()
    rows = functions.db_query(conn, sql)
    functions.db_disconnect(conn)

    # make a table of the data
    tbl = '<table>\n'
    tbl += '\t<tr><th>aws_id</th><th>variable</th><th>message</th></tr>\n'
    cnt = 0
    for row in rows:
        tbl += '\t<tr><td><a href="http://aws-samdbnrm.sa.gov.au?aws_id=' + row['aws_id'] + '&view=7days">' + row['aws_id'] + '</a></td><td>' + row['var'] + '</td><td>' + row['msg'] + '</td></tr>\n'
        cnt += 1
    tbl += '</table>'

    html = '<h4>Errors in days readings for ' + day.strftime('%Y-%m-%d') + '</h4>\n' + tbl
    # send an email if there are errors
    if cnt > 0:
        functions.gmail_send(settings.ERROR_MSG_RECEIVERS, 'days data errors', 'message is in html', html)

    return


def job_check_minutes_values(day):
    logging.debug('ran job_check_minutes_values(' + day.strftime('%Y-%m-%d') + ')')

	# for rain gauges, only check rain
    sql = '''
            (
                # air temp
                SELECT
                    aws_id,
                    'airT' AS var,
                    'air temp outside allowed range (-10, 50)' AS msg
                FROM tbl_data_minutes
                WHERE
					aws_id NOT LIKE 'TBRG%' AND
                    DATE(stamp) = "''' + day.strftime('%Y-%m-%d') + '''" AND
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
					DATE(stamp) = "''' + day.strftime('%Y-%m-%d') + '''"
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
                    DATE(stamp) = "''' + day.strftime('%Y-%m-%d') + '''" AND
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
                    DATE(stamp) = "''' + day.strftime('%Y-%m-%d') + '''" AND
					(						
						batt IS NULL OR 
						#batt NOT BETWEEN 6 AND 7 OR -- I don't know why I can't include this range here. 
						batt NOT BETWEEN 12 AND 15.5
					)
                GROUP BY aws_id
            )
            ORDER BY aws_id;
    '''

    # get the data
    conn = functions.db_connect()
    rows = functions.db_query(conn, sql)
    functions.db_disconnect(conn)

    # make a table of the data
    tbl = '<table>\n'
    tbl += '\t<tr><th>aws_id</th><th>variable</th><th>message</th></tr>\n'
    cnt = 0
    for row in rows:
        tbl += '\t<tr><td><a href="http://aws-samdbnrm.sa.gov.au?aws_id=' + row['aws_id'] + '&view=today">' + row['aws_id'] + '</a></td><td>' + row['var'] + '</td><td>' + row['msg'] + '</td></tr>\n'
        cnt += 1
    tbl += '</table>'

    html = '<h4>Errors in minute readings for ' + day.strftime('%Y-%m-%d') + '</h4>\n' + tbl
    # send an email if there are errors
    if cnt > 0:
        functions.gmail_send(settings.ERROR_MSG_RECEIVERS, 'minute data errors', 'message is in html', html)

    return


# TODO: complete function
def job_check_latest_readings():
    logging.debug('ran job_check_latest_readings()')
    sql = '''
	SELECT m.aws_id, m.name, m.owner FROM (
	SELECT a.aws_id AS aws_id, a.name, a.owner, b.aws_id AS other FROM 
	(SELECT aws_id, NAME, OWNER FROM tbl_stations WHERE STATUS = 'on' ORDER BY aws_id) AS a
	LEFT JOIN 
	(SELECT DISTINCT aws_id FROM tbl_data_minutes WHERE DATE(stamp) = CURDATE()) AS b
	ON a.aws_id = b.aws_id
	HAVING b.aws_id IS NULL) AS m;		
	'''
	
    # get the data
    conn = functions.db_connect()
    rows = functions.db_query(conn, sql)
    functions.db_disconnect(conn)
	
    # make a table of the data
    tbl = '<table>\n'
    tbl += '\t<tr><th>aws_id</th><th>Name</th><th>Owner</th></tr>\n'
    cnt = 0
    for row in rows:
        tbl += '\t<tr><td><a href="http://aws-samdbnrm.sa.gov.au?aws_id=' + row['aws_id'] + '">' + row['aws_id'] + '</a></td><td>' + row['name'] + '</td><td>' + row['owner'] + '</td></tr>\n'
        cnt += 1
    tbl += '</table>'	
	
    html = '<h4>Stations not reporting today</h4>\n' + tbl
    # send an email if there are errors
    if cnt > 0:
        functions.gmail_send(settings.ERROR_MSG_RECEIVERS, 'today\'s missing data', 'message is in html', html)	
	
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
            job_check_days_values(yesterday)
            # check minute values for yesterday to cover time after 15:00 check
            job_check_minutes_values(yesterday)
        elif hr in [9, 12, 15]:
            today = datetime.datetime.now()
            job_check_minutes_values(today)
        elif hr == 10:
            job_check_latest_readings()
    except Exception, e:
        logging.error(e.message)
else:
    print 'ERROR: this file must be called from the command line, with arguments'
