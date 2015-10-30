import csv
import StringIO
from ftplib import FTP
import logging
import functions
import settings


def get_minutes_data_as_csv(owner, day):
    """
    Get yesterday's minutes data for an owner's stations as CSV
    :param owner: string
    :param day: datetime
    :return: string (CSV data)
    """
    logging.debug("call get_minutes_data_as_csv(" + owner + ', ' + day.strftime('%Y-%m-%d') + ')')

    # get the data
    sql = """
        SELECT dfw_id, DATE(stamp) AS d, TIME_FORMAT(TIME(stamp), '%H:%i') AS t, airT, appT, dp, rh, deltaT, soilT, gsr, Wmin, Wavg, Wmax, Wdir, rain, leaf, canT, canRH
        FROM tbl_data_minutes
        INNER JOIN tbl_stations
        ON tbl_data_minutes.aws_id = tbl_stations.aws_id
        WHERE
            owner = '""" + owner + """'
            AND DATE(stamp) = '""" + day.strftime('%Y-%m-%d') + """'
            AND dfw_id IS NOT NULL
        ORDER BY dfw_id, stamp;"""

    # write it as CSV to a string
    output = StringIO.StringIO()
    fieldnames = ['dfw_id', 'd', 't', 'airT', 'appT', 'dp', 'rh', 'deltaT', 'soilT', 'gsr', 'Wmin', 'Wavg', 'Wmax', 'Wdir', 'rain', 'leaf', 'canT', 'canRH']
    csv_writer = csv.DictWriter(output,
                                delimiter=',',
                                lineterminator='\n',
                                quotechar='"',
                                quoting=csv.QUOTE_NONNUMERIC,
                                fieldnames=fieldnames)
    conn = functions.db_connect()
    csv_writer.writerows(functions.db_query(conn, sql))
    functions.db_disconnect(conn)

    # add header
    header_row = ','.join([
        'DfW ID',
        'Date',
        'Time',
        'Ave AirTemp (AWS) (degC)',
        'Ave AppTemp (degC)',
        'Ave DewPoint (degC)',
        'Ave Humidity (AWS) (%)',
        'Ave DeltaT (degC)',
        'Ave Soil Temperature (degC)',
        'Ave GSR (W/m^2)',
        'Min WndSpd (m/s)',
        'Ave WndSpd (m/s)',
        'Max WndSpd (m/s)',
        'Ave WndDir (deg)',
        'Total Rain (mm)',
        'Ave LeafWet (% Wet)',
        'Ave AirTemp (Canopy) (degC)',
        'Ave Humidity (Canopy) (%)'])

    # return CSV data as string
    return header_row + '\n' + output.getvalue()


def send_all_csv_files_to_dfw(day):
    """
    Sends the CSV files via FTP somewhere
    :param day: datetime
    :return: True if ok
    """
    logging.debug('call send_all_csv_files_to_dfw(' + day.strftime('%Y-%m-%d') + ')')

    # create the data and file names
    awnrm_file_name = 'AWNRM_' + day.strftime("%Y%m%d") + '.csv'
    awnrm_csv_file = StringIO.StringIO(get_minutes_data_as_csv('AWNRM', day))
    samdb_file_name = 'SAMDB_' + day.strftime("%Y%m%d") + '.csv'
    samdb_csv_file = StringIO.StringIO(get_minutes_data_as_csv('SAMDB', day))
    senrm_file_name = 'SENRM_' + day.strftime("%Y%m%d") + '.csv'
    senrm_csv_file = StringIO.StringIO(get_minutes_data_as_csv('SENRM', day))

    # send the data
    ftp = FTP(settings.DFW_FTP_SRV)
    ftp.set_debuglevel(0)
    ftp.login(settings.DFW_FTP_USR, settings.DFW_FTP_PWD)
    ftp.storbinary('STOR ' + awnrm_file_name, awnrm_csv_file)
    ftp.storbinary('STOR ' + samdb_file_name, samdb_csv_file)
    ftp.storbinary('STOR ' + senrm_file_name, senrm_csv_file)
    ftp.quit()

    # clean up
    samdb_csv_file.close()
    senrm_csv_file.close()

    return True