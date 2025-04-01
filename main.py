import json

import mysql.connector

connection = mysql.connector.connect(
    host="10.254.1.9",
    database="vet2",
    user="ryan",
    password="ryan1"
)
cursor = connection.cursor()

# pair divisions to their work center codes
DIVISIONS = {
    "CNC Swiss": ["CT", "CC", "CS"],
    "Auto": ["AT", "AB"],
    "Turn Mill": ["CM"],
    "Esco": ["EE"],
}


def get_internal_dppm(start_date, end_date):
    """
    retrieves good and bad parts made by all machines between the given dates (inclusive)

    :param start_date: start date in YYYY-MM-DD format
    :param end_date: end date in YYYY-MM-DD format
    :return: dictionary pairing all divisions to their good and bad parts in the time range
    """

    internal_dppm = {}
    for division in DIVISIONS:
        internal_dppm[division] = {"good": 0, "scrap": 0}
        for code in DIVISIONS[division]:
            query = (f"SELECT JC_PARTS_G, JC_SCRAP "
                     f"FROM jcardman "
                     f"WHERE JC_MACH_CO LIKE \"{code}%\" "
                     f"AND JC_OPERATI = 10 "
                     f"AND JC_RUN_DAT >= %s "
                     f"AND JC_RUN_DAT <= %s;")
            cursor.execute(query, (start_date, end_date))
            response = cursor.fetchall()
            for row in response:
                internal_dppm[division]["good"] += row[0] or 0
                internal_dppm[division]["scrap"] += row[1] or 0

    return internal_dppm


def get_external_dppm(start_date, end_date):
    """
    retrieves parts shipped to and returned from all clients between the given dates (inclusive)

    :param start_date: start date in YYYY-MM-DD format
    :param end_date: end date in YYYY-MM-DD format
    :return: dictionary pairing all client codes to the parts they received and returned
    """

    external_dppm = {}
    query = ("SELECT SD_ORDER_N, SD_TOT_QTY "
             "FROM ship_det "
             "WHERE SD_SHIP_DA >= %s "
             "AND SD_SHIP_DA <= %s;")
    cursor.execute(query, (start_date, end_date))
    response = cursor.fetchall()
    for row in response:

        if not row[0] or not row[1]:
            continue

        query = "SELECT OR_CUST_CO FROM `order` WHERE OR_ORDER_N = %s;"
        cursor.execute(query, (row[0],))
        order_row = cursor.fetchone()
        if not order_row:
            continue

        client_code = order_row[0]

        if client_code not in external_dppm:
            if row[1] > 0:
                external_dppm[client_code] = {
                    "shipped": float(row[1]),
                    "returned": 0
                }
            else:
                external_dppm[client_code] = {
                    "shipped": 0,
                    "returned": -1 * float(row[1])
                }
        else:
            if row[1] > 0:
                external_dppm[client_code]["shipped"] += float(row[1])
            else:
                external_dppm[client_code]["returned"] += -1 * float(row[1])

    return external_dppm

external = get_external_dppm("2025-02-01", "2025-02-28")
print(json.dumps(external, indent=2))

cursor.close()
connection.close()
