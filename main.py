#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector
import argparse
import json


#######################################
# Monitor Type format switcher
#######################################
switcher = {
    "b": "boolean",
    "e": "enum",
    "d": "double",
    "f": "float",
    "l": "long",
    "s": "short",
    "o": "octet",
    "D": "DoubleArray1D",
    "F": "FloatArray1D",
    "L": "LongArray1D",
    "S": "ShortArray1D",
    "O": "OctetArray1D",
    "0": "0",
    "9": "DoubleArray2D",
    "8": "FloatArray2D",
    "7": "LongArray2D",
    "6": "ShortArray2D",
    "5": "OctetArray2D"
}

#######################################
# Handling setup format type
#######################################
def isMonitor(type):
    if type == "D" or type == "F" or type == "L" or type == "S" \
            or type == "O" or type == "9" or type == "8" or type == "7" \
            or type == "6" or type == "5" or type == "d" or type == "f"\
            or type == "l" or type == "s" or type == "o":
        return True
    return False

def isScalar(type):
    if type == "d" or type == "f" or type == "l" or type == "s" \
            or type == "o":
        return True
    return False

def isArray(type):
    if type == "D" or type == "F" or type == "L" or type == "S" \
            or type == "O" or type == "9" or type == "8" or type == "7" \
            or type == "6" or type == "5":
        return True
    return False

def isEnum(type):
    if type == "e":
        return True
    return False

#######################################
# Monitor Conf JSON Format
#######################################
def configurationFormat(description, unit, magnitude_values, magnitude_type, type, upper_limit, lower_limit, default_sampling_period, default_storage_period, dimension_y, dimension_x):
    getType = switcher.get(type)
    monitorResultJson = {
        "type": getType,
    }

    if isEnum(type):
        monitorResultJson["type"] = getType + magnitude_type
        valueNode = {"values": magnitude_values}
        monitorResultJson.update(valueNode)
    elif isMonitor(type):
        monInfo = {
            "description": description,
            "upper_limit": upper_limit,
            "lower_limit": lower_limit,
            "default_sampling_period": default_sampling_period,
            "default_storage_period": default_storage_period,
            "units": unit
        }
        monitorResultJson.update(monInfo)

    if dimension_y and dimension_y != 1:
        heightNode = {"height": dimension_y}
        monitorResultJson.update(heightNode)

    if dimension_x and dimension_x != 1:
        widhtNode = {"width": dimension_x}
        monitorResultJson.update(widhtNode)

    return monitorResultJson


#######################################
# SQl Handling Errors
#######################################
def sqlErrors(error):
    if error:
        if error.errno:
            print("Error code:", error.errno)
        if error.sqlstate:
            print("SQLSTATE:", error.sqlstate)
        if error.msg:
            print("Message:", error.msg)

#######################################
# Get monitors data from database
#######################################
def getConfigurationData():
    json_data = []
    db_cursor.execute("select id as component_id, name, className from monitor_component;")
    result = db_cursor.fetchall()

    for (component_id, name, className) in result:
        print("setting component ---> " + str(component_id))
        content = {
            "instance": name,
            "className": className,
            "monitors": {}
        }
        # -------------------------------------------------------------------------------------------------
        # ------ ------ ------ ------ ------ ------  Monitor_description ------ ------ ------ ------ ------
        # -------------------------------------------------------------------------------------------------
        #
        # monitor description
        db_cursor.execute("select monitor.* from monitor_description monitor "
                          "where monitor.version IN ("
                          "select MAX(monitor2_.version) "
                          "from monitor_description monitor2_ "
                          "where monitor.magnitude like binary monitor2_.magnitude "
                          "AND monitor.id_monitor_component = monitor2_.id_monitor_component "
                          "and monitor2_.id_monitor_component = " + str(component_id) + " "
                          "group by monitor2_.id_monitor_component,monitor2_.magnitude) "
                          "group by id_monitor_component, magnitude;")
        resultmonitor = db_cursor.fetchall()

        for (monitor_id, id_monitor_component, magnitude, version, unit, type, dimension_x, dimension_y, description) in resultmonitor:
            #
            # monitor config
            db_cursor.execute("select id as id_conf, storage_period, propagate_period, id_monitor_description, id_monitor_configuration "
                              "from monitor_config "
                              "where id_monitor_description =" + str(monitor_id) + " "
                              "and id_monitor_configuration = 1;")
            resultmonitorConfig = db_cursor.fetchall()

            for (id_conf, storage_period, propagate_period, id_monitor_description, id_monitor_configuration) in resultmonitorConfig:
                # covert to seconds
                default_sampling_period = str(propagate_period / 1000000)
                default_storage_period = str(storage_period / 1000000)
                #
                # monitor range
                db_cursor.execute("select max, min "
                                  "from monitor_range "
                                  "where id_monitor_config = " + str(id_conf) + ";")
                resultmonitorRange = db_cursor.fetchall()

                upper_limit = "["
                lower_limit = "["
                count_Dx = 1
                count_Dy = 1

                if resultmonitorRange.__len__() == 0:
                    upper_limit = "0"
                    lower_limit = "0"
                else:
                    for max, min in resultmonitorRange:
                        if isScalar(type):
                            upper_limit = str(max)
                            lower_limit = str(min)
                            break
                        elif isArray(type):
                            if count_Dx == dimension_x and count_Dy == dimension_y:
                                upper_limit += str(max) + "]"
                                lower_limit += str(min) + "]"
                                break
                            else:
                                if count_Dx == dimension_x:
                                    upper_limit += str(min) + ";"
                                    lower_limit += str(min) + ";"
                                    count_Dx = 1
                                    count_Dy = count_Dy + 1
                                else:
                                    upper_limit += str(min) + ","
                                    lower_limit += str(min) + ","
                                    count_Dx = count_Dx + 1

            # --------------------------------
            # Set monitor data configuration
            # --------------------------------
            format = configurationFormat(description, unit, False, False, type, upper_limit, lower_limit, default_sampling_period, default_storage_period, dimension_y, dimension_x)
            content['monitors'][magnitude] = (format)

        # ---------------------------------------------------------------------------------------------------
        # ------ ------ ------ ------ ------ ------  Magnitude_description ------ ------ ------ ------ ------
        # ---------------------------------------------------------------------------------------------------
        #
        # magnitude description
        db_cursor.execute("select magnitude, type, id_magnitude_type from magnitude_description monitor "
                          "where monitor.version IN ("
                          "select MAX(monitor2_.version) "
                          "from magnitude_description monitor2_ "
                          "where monitor.magnitude like binary monitor2_.magnitude "
                          "AND monitor.id_monitor_component = monitor2_.id_monitor_component "
                          "and monitor2_.id_monitor_component = " + str(component_id) + " "
                          "group by monitor2_.id_monitor_component,monitor2_.magnitude) "
                          "group by id_monitor_component, magnitude;")
        resultMagnitudes = db_cursor.fetchall()

        for (magnitude, type, id_magnitude_type) in resultMagnitudes:
            #
            # magnitude value
            db_cursor.execute("select name as value_name "
                              "from magnitude_value "
                              "where id_magnitude_type = " + str(id_magnitude_type) + " ")
            resultMagnitudesValues = db_cursor.fetchall()

            magnitude_values = ""
            count_magnitude_values = 1
            for (value_name) in resultMagnitudesValues:
                arrangeName = "%s" % value_name
                if count_magnitude_values == resultMagnitudesValues.__len__():
                    magnitude_values += str(arrangeName)
                    count_magnitude_values = 0
                else:
                    magnitude_values += str(arrangeName) + ", "
                    count_magnitude_values = count_magnitude_values + 1

            #
            # magnitude type
            db_cursor.execute("select name as type_name "
                              "from magnitude_type "
                              "where id =" + str(id_magnitude_type) + ";")
            resultMagnitudesType = db_cursor.fetchall()
            magnitude_type = ""
            for (type_name) in resultMagnitudesType:
                magnitude_type = type_name[0].replace("::", ".").split(".")
                magnitude_type = "_" + str(magnitude_type[magnitude_type.__len__() - 1])

            # --------------------------------
            # Set magnitude data configuration
            # --------------------------------
            format = configurationFormat(False, False, magnitude_values, magnitude_type, type, False, False, "0.0", "0.0", False, False)
            content['monitors'][magnitude] = (format)

        json_data.append(content)
        content = {}

    stud_json = json.dumps(json_data, indent=2)
    print("results: ", stud_json)

    return json_data



#######################################
# Conexión con la base de datos
#######################################
def init():
    try:
        print("Conectando con %s..." % 'calp-ltdb')
        # db_connection = mysql.connector.connect(
        #     host=(args['host'] or 'localhost'),
        #     user=args['user'],
        #     passwd=args['password'],
        #     database=args['database']
        # )
        db_connection = mysql.connector.connect(
            host='calp-ltdb',
            user='mmuser',
            passwd='gtcmysqlbdd',
            database='monitormanager'
        )
        if db_connection.is_connected():
            db_info = db_connection.get_server_info()
            print("Connected to MySQL Server version ", db_info)
            global db_cursor
            db_cursor = db_connection.cursor()

            getData = getConfigurationData()
            with open('output.json', 'w') as file:
                json.dump(getData, file)

            db_connection.close()

    except mysql.connector.Error as e:
        sqlErrors(e)
    except Exception as e:
        print(e)
    finally:
        if db_connection.is_connected():
            db_connection.close()
            print("Ending connection with %s..." % 'calp-ltdb')


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Components Database Format Constructor')
        # Conexión Base de Datos
        parser.add_argument('-x', '--host', help='Host de la base de datos', required=True)
        parser.add_argument('-u', '--user', help='Usuario para acceder a la base de datos', required=True)
        parser.add_argument('-p', '--password', help='Password para acceder a la base de datos', required=True)
        parser.add_argument('-d', '--database', help='Esquema de la base de datos', required=True)
        # args = vars(parser.parse_args())
        init()
    except Exception as error:
        print(error)
        quit()