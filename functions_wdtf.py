import datetime
import logging
import functions
import zipfile
import os
from ftplib import FTP
import settings


# TODO: change the reported station name from the NRM aws_id to the BoM number in tbl_stations
def get_observation_member(conn, wdtf_data_provider_id, bom_number, day, member):
    """
    Add each station's values to the wdtf_file
    :param: live DB connection
    :param aws_id: string
    :param member: DB column
    :param wdtf_data_provider_id: string
    :param day: date
    :return: XML - observationMember
    """
    logging.debug("run functions_wdtf.get_hydrocollection(" + wdtf_data_provider_id + ", " + bom_number + ", " + day.strftime("%Y-%m-%d") + ', ' + member + ')')

    # map the WDTF parameters
    if member == 'rain':
        gml_id = "TS_rain"
        feature = 'Rainfall_mm'
        interpol = 'InstTot'
        units = 'mm'
    elif member == 'Wavg':
        gml_id = "TS_Wavg"
        feature = 'WindSpeed_ms'
        interpol = 'InstVal'
        units = 'm/s'
    elif member == 'gsr':
        gml_id = "TS_gsr"
        feature = 'GlobalSolarIrradianceAverage_Wm2'
        interpol = 'PrecVal'
        units = 'W/m2'
    elif member == 'airT':
        gml_id = "TS_airT"
        feature = 'DryAirTemperature_DegC'
        interpol = 'InstVal'
        units = 'Cel'
    elif member == 'rh':
        gml_id = "TS_rh"
        feature = 'RelativeHumidity_Perc'
        interpol = 'InstVal'
        units = '%'
    #elif member == 'dp':
    #          gml_id = "TS_dp"
    #          feature = 'DewPoint_DegC'
    #          interpol = 'InstVal'
    #          units = 'Cel'
    else:
        e = 'no valid member given to get_hydrocollection'
        logging.error(e)
        raise RuntimeError(e)

    # generate the data for the bom_number and the TVP XML for the member
    sql = """
        SELECT
            CONCAT(DATE_FORMAT(stamp - INTERVAL 9 HOUR - INTERVAL 30 MINUTE, '%Y-%m-%dT%H:%i:%s'),'+09:30') AS stamp,
            """ + member + """ AS member
        FROM tbl_data_minutes
        INNER JOIN tbl_stations
        ON tbl_data_minutes.aws_id = tbl_stations.aws_id
        WHERE
            bom_number = '""" + bom_number + """'
            AND DATE(stamp) = '""" + day.strftime("%Y-%m-%d") + """'
        ORDER BY stamp;
        """

    # make the XML
    wdtf_obsMember = '''
    <wdtf:observationMember>
        <wdtf:TimeSeriesObservation gml:id="''' + gml_id + '''">
            <gml:description>Weatherstation data</gml:description>
            <gml:name codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/feature/TimeSeriesObservation/''' + wdtf_data_provider_id + '''/">1</gml:name>
            <om:procedure xlink:href="urn:ogc:def:nil:OGC::unknown"/>
            <om:observedProperty xlink:href="http://www.bom.gov.au/std/water/xml/wio0.2/property//bom/''' + feature + '''"/>
            <om:featureOfInterest xlink:href="http://www.bom.gov.au/std/water/xml/wio0.2/feature/SamplingPoint/''' + wdtf_data_provider_id + '''/''' + bom_number + '''/1"/>
            <wdtf:metadata>
                <wdtf:TimeSeriesObservationMetadata>
                    <wdtf:relatedTransaction xlink:href="http://www.bom.gov.au/std/water/xml/wio0.2/definition/sync/bom/DataDefined"/>
                    <wdtf:siteId>''' + bom_number + '''</wdtf:siteId>
                    <wdtf:relativeLocationId>1</wdtf:relativeLocationId>
                    <wdtf:relativeSensorId>''' + bom_number + '''_aws</wdtf:relativeSensorId>
                    <wdtf:status>validated</wdtf:status>
                </wdtf:TimeSeriesObservationMetadata>
            </wdtf:metadata>
            <wdtf:result>
                <wdtf:TimeSeries>
                    <wdtf:defaultInterpolationType>''' + interpol + '''</wdtf:defaultInterpolationType>
                    <wdtf:defaultUnitsOfMeasure>''' + units + '''</wdtf:defaultUnitsOfMeasure>
                    <wdtf:defaultQuality>quality-A</wdtf:defaultQuality>\n'''

    for row in functions.db_query(conn, sql):
        wdtf_obsMember += "\t\t\t\t\t<wdtf:timeValuePair time=\"%s\">%s</wdtf:timeValuePair>\n" % (row['stamp'], row['member'])

    wdtf_obsMember = wdtf_obsMember.strip() + '''
                </wdtf:TimeSeries>
            </wdtf:result>
        </wdtf:TimeSeriesObservation>
    </wdtf:observationMember>'''

    return wdtf_obsMember


def get_hydrocollection(conn, wdtf_data_provider_id, bom_number, day, members):
    """
    Get the full WDTF for a particular station. Calls get_observation_member(bom_number, member, wdtf_data_provider_id)
    :param bom_number: string
    :param wdtf_data_provider_id: string
    :param in_date: date
    :return: XML - HydroCollection
    """
    logging.debug('ran get_hydrocollection(' + wdtf_data_provider_id + ', ' + bom_number + ', ' + str(day) + ', [' + ','.join(members) + '])')

    # make the hydrocollection header
    t = datetime.datetime.utcnow()
    hydrocollection = '''<?xml version="1.0"?>
    <wdtf:HydroCollection
        xmlns:sa="http://www.opengis.net/sampling/1.0/sf1"
        xmlns:om="http://www.opengis.net/om/1.0/sf1"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:xlink="http://www.w3.org/1999/xlink"
        xmlns:gml="http://www.opengis.net/gml"
        xmlns:wdtf="http://www.bom.gov.au/std/water/xml/wdtf/1.0"
        xmlns:ahgf="http://www.bom.gov.au/std/water/xml/ahgf/0.2"
        xsi:schemaLocation="http://www.opengis.net/sampling/1.0/sf1 ../sampling/sampling.xsd
        http://www.bom.gov.au/std/water/xml/wdtf/1.0 ../wdtf/water.xsd
        http://www.bom.gov.au/std/water/xml/ahgf/0.2 ../ahgf/waterFeatures.xsd"
        gml:id="timeseries_m">
    <gml:description>This document encodes timeseries data from the SANRM's Automatic Weatherstation Network.</gml:description>
    <gml:name codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/feature/HydroCollection/''' + wdtf_data_provider_id + '''/">wdtf_sanrm</gml:name>
    <wdtf:metadata>
        <wdtf:DocumentInfo>
            <wdtf:version>wdtf-package-v1.0</wdtf:version>
            <wdtf:dataOwner codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/party/person/bom/">''' + wdtf_data_provider_id + '''</wdtf:dataOwner>
            <wdtf:dataProvider codeSpace="http://www.bom.gov.au/std/water/xml/wio0.2/party/person/bom/">''' + wdtf_data_provider_id + '''</wdtf:dataProvider>
            <wdtf:generationDate>''' + t.strftime("%Y-%m-%dT%H:%M:%S") + '''+09:30</wdtf:generationDate>
            <wdtf:generationSystem>KurrawongIC_WDTF</wdtf:generationSystem>
        </wdtf:DocumentInfo>
    </wdtf:metadata>
    '''

    # add each observationMember
    for member in members:
        hydrocollection += get_observation_member(conn, wdtf_data_provider_id, bom_number, day, member)

    # complete hydrocollection
    hydrocollection += "</wdtf:HydroCollection>"

    return hydrocollection


def make_wdtf_zip_file(conn, owner, day, dir, aws_id=None):
    """
    Make a WDTF XML file for each station for a particular owner with status 'on' and returns them as zip file
    calls get_hydrocollection(owner, wdtf_data_provider_id)
    :param owner: string
    :param day: date
    :return: a zip file of XML docs named according to the BoM's naming convention
    """
    logging.debug("make_wdtf_zip_file(" + owner + ')')

    t = datetime.datetime.now()
    # get the stations for this owner with bom_numbers or for this aws_id, if specified
    sql = """
        SELECT
            wdtf_id,
            bom_number
        FROM tbl_stations INNER JOIN tbl_owners
        ON tbl_stations.owner = tbl_owners.owner_id
        WHERE
            OWNER = '""" + owner + """'
            AND bom_number IS NOT NULL
            AND STATUS = 'on';
        """

    # limit the query to a single station if an aws_id is given
    if aws_id is not None:
        sql = sql.strip().rstrip(';') + '\nAND aws_id = "' + aws_id + '";'

    wdtf_files = []

    # NRM AWS station members
    members = [
        'rain',
        'Wavg',
        'gsr',
        'airT',
        'rh',
        #'dp'
    ]

    wdtf_data_provider_id = ''
    # make an XML file for each station
    for row in functions.db_query(conn, sql):
        wdtf_data_provider_id = row['wdtf_id']
        wdtf_file_name = "wdtf." + row['wdtf_id'] + "." + t.strftime("%Y%m%d%H0000") + "." + row['bom_number'] + "-ctsd.xml"
        hydrocollection_result = get_hydrocollection(conn, row['wdtf_id'], row['bom_number'], day, members)
        wdtf_files.append({
            'file_name': wdtf_file_name,
            'data': hydrocollection_result
        })

    # serialise the XML data into a zip file
    zipfile_name = wdtf_data_provider_id + "." + day.strftime("%Y%m%d") + "093000.zip"  # fixed at 9:30am
    zout = zipfile.ZipFile(dir + zipfile_name, "w", zipfile.ZIP_DEFLATED)
    for f in wdtf_files:
        zout.writestr(f['file_name'], f['data'])
    zout.close()

    #we have created a zipfile on disk so return the file name
    return dir + zipfile_name


def send_wdtf_zipfile(conn, zipfile_path):
    """
    Send the zipped WDTF file collection to the BoM by FTP using owner's FTP WDTF details
    Calls make_wdtf_zip_file(owner)
    :param owner: string
    :param in_date: date string
    :return: True/False if successful
    """
    logging.debug("call send_wdtf_zipfile(" + zipfile_path + ')')

    # get the owner's FTP details
    wdtf_data_provider_id = zipfile_path.split('/')[-1].split('.')[0]
    sql = "SELECT wdtf_server, wdtf_id, wdtf_password FROM tbl_owners WHERE wdtf_id = '" + wdtf_data_provider_id + "';"

    if conn is None:
        conn = functions.db_connect()

    for row in functions.db_query(conn, sql):
        svr = row['wdtf_server']
        usr = row['wdtf_id']
        pwd = row['wdtf_password']

    # send the zip file
    ftp = FTP(svr)
    ftp.set_debuglevel(0)
    ftp.login(usr, pwd)
    ftp.cwd('/register/' + usr + '/incoming/data')
    ftp.storbinary("STOR " + zipfile_path.split('/')[-1], open(zipfile_path, 'rb'))
    ftp.quit()

    return True


def send_add_wdtf_zipfiles_to_bom(day):
    """
    Send the all owners' WDTF zipfiles to the BoM by FTP using owner's FTP WDTF details

    :param day: datetime
    :return: True if successful
    """
    logging.debug("call send_add_wdtf_zipfiles_to_bom(" + day.strftime('%Y-%m-%d') + ')')

    conn = functions.db_connect()
    zfp = make_wdtf_zip_file(conn, 'SAMDB', day, settings.APPLICATION_DIR)
    s = send_wdtf_zipfile(conn, zfp)
    os.remove(zfp)

    zfp = make_wdtf_zip_file(conn, 'SENRM', day, settings.APPLICATION_DIR)
    s = send_wdtf_zipfile(conn, zfp)
    os.remove(zfp)

    zfp = make_wdtf_zip_file(conn, 'AWNRM', day, settings.APPLICATION_DIR)
    s = send_wdtf_zipfile(conn, zfp)
    os.remove(zfp)

    zfp = make_wdtf_zip_file(conn, 'LMW', day, settings.APPLICATION_DIR)
    s = send_wdtf_zipfile(conn, zfp)
    os.remove(zfp)

    functions.db_disconnect(conn)

    return s

