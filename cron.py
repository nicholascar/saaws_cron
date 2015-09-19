import os
import logging
import settings
import datetime
from datetime import timedelta
import functions


def job_check_deamon_running():
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
    return [True, 'job calc_days']


def job_check_days_values(day):
    logging.debug('ran check_days_values(' + day.strftime('%Y-%m-%d') + ')')
    return [True, 'job check_days_values']


def job_check_minutes_values(day):
    logging.debug('ran job_check_minutes_values(' + day.strftime('%Y-%m-%d') + ')')
    return [True, 'job check_minutes_values']


def job_check_latest_readings():
    logging.debug('ran job_check_latest_readings()')
    return [True, 'job job_check_latest_readings']


if __name__ == "__main__":
    logging.basicConfig(filename=settings.LOG_FILE,
                        format='%(asctime)s %(levelname)s %(message)s',
                        level=settings.LOG_LEVEL)

    logging.debug('ran saaws_cron main()')

    hr = datetime.datetime.now().hour

    try:
        # these run every hour
        job_check_deamon_running()

        # these run on specific hours
        if hr == 1:
            yesterday = datetime.datetime.now() - timedelta(hours=24)
            job_wdtf_export(yesterday)
            job_dfw_csv_export(yesterday)
        elif hr == 2:
            #yesterday = datetime.datetime.now() - timedelta(hours=24)
            #job_calc_days(yesterday)
            pass
        elif hr == 8:
            #yesterday = datetime.datetime.now() - timedelta(hours=24)
            #job_check_days_values(yesterday)
            # check minute values for yesterday to cover time after 15:00 check
            #job_check_minutes_values(yesterday)
            pass
        elif hr in [9, 12, 15]:
            #today = datetime.datetime.now()
            #job_check_minutes_values(today)
            pass
        elif hr == 10:
            #job_check_latest_readings()
            pass

    except Exception, e:
        logging.error(e.message)
else:
    print 'ERROR: this file must be called from the command line, with arguments'
