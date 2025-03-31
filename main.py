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





print(get_internal_dppm("2025-02-01", "2025-02-28"))

cursor.close()
connection.close()
