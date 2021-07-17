import mysql.connector
import mysql.connector.errors as db_errors
from datetime import datetime
import sys
from pprint import pprint
from collections import defaultdict
import traceback

# LEAVE_TYPE_ID 10 for compenstation leave
# Transfer Available Compenstation Leave for each employee regardless of emp_status
# LOGIC  ==> add total number of leave_takens and subtract from total number of leave_credits to get transferrable days

db_user = ""
db_pass = ""
db_name = "vyaguta_dev"
updated_by = 252  # CHANGE THIS BEFORE EXECUTING


def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user=db_user,
        password=db_pass,
        database=db_name,
        auth_plugin="mysql_native_password",
    )


def row_factory(cursor):
    rows = []
    n_fields = len(cursor.column_names)
    fields = [field[0] for field in cursor.description]

    for row in cursor:
        rows.append(dict(zip(fields, row)))

    return rows


def get_employees_list_for_credit_leaves():
    db_conn = connect_db()
    cursor = db_conn.cursor()

    sql = f"""SELECT
        u.id AS user_id,
        lc.leave_type_id AS credit_leave_type_id,
        SUM(COALESCE(lc.leave_days, 0)) AS total_credit_leave_days
    FROM v_users u
        LEFT JOIN leave_credits lc
            ON u.id=lc.user_id
    WHERE lc.fiscal_year_id=(SELECT fy.id FROM fiscal_year fy WHERE fy.is_current=0 AND fy.is_transferred=0 ORDER BY fy.date_created DESC LIMIT 1)
        AND lc.leave_type_id=10
    GROUP BY u.id, lc.user_id;"""

    try:
        cursor.execute(sql)

        return row_factory(cursor)
    except (db_errors.DatabaseError, Exception) as err:
        tb = sys.exc_info()[2]
        raise err.with_traceback(tb)
    finally:
        db_conn.close()


def get_employees_list_for_taken_leaves():
    db_conn = connect_db()
    cursor = db_conn.cursor()

    sql = f"""SELECT
        u.id AS user_id,
        lc.leave_type_id AS taken_leave_type_id,
        SUM(COALESCE(lc.leave_days, 0)) AS total_taken_leave_days
    FROM v_users u
        LEFT JOIN leave_leaves lc
            ON u.id=lc.user_id
    WHERE lc.fiscal_year_id=(SELECT fy.id FROM fiscal_year fy WHERE fy.is_current=0 AND fy.is_transferred=0 ORDER BY fy.date_created DESC LIMIT 1)
        AND lc.leave_type_id=10
    GROUP BY u.id, lc.user_id;"""

    try:
        cursor.execute(sql)

        return row_factory(cursor)
    except (db_errors.DatabaseError, Exception) as err:
        tb = sys.exc_info()[2]
        raise err.with_traceback(tb)
    finally:
        db_conn.close()


def map_final_data(x, fiscal_year_id):
    total_credit = int(x["total_credit_leave_days"])
    total_taken = 0
    if "total_taken_leave_days" in x:
        total_taken = int(x["total_taken_leave_days"])

    transferrable_leave_days = total_credit - total_taken

    if transferrable_leave_days > 0:
        return {
            "user_id": x["user_id"],
            "leave_type_id": x["credit_leave_type_id"],
            "leave_days": total_credit - total_taken,
            "fiscal_year_id": fiscal_year_id,
            "created_by": updated_by,
            "updated_by": updated_by,
        }


def prepare_final_list(l1, l2, fiscal_year_id):
    d = defaultdict(dict)
    for l in (l1, l2):
        for elem in l:
            d[elem["user_id"]].update(elem)

    return list(
        filter(
            lambda i: i, map(lambda x: map_final_data(x, fiscal_year_id), d.values())
        )
    )


def add_new_rows_in_leave_credits_for_current_fiscall_year(emp_list_to_be_added):
    try:
        mydb = connect_db()
    except:
        print("db connection failed")
        return

    cursor = mydb.cursor()
    sql = """INSERT INTO leave_credits
        (user_id, leave_type_id, leave_days, fiscal_year_id, reason, leave_source, created_by, updated_by, created_at, updated_at) 
        VALUES (
            %(user_id)s, %(leave_type_id)s, %(leave_days)s, %(fiscal_year_id)s, 'Transferred from previous fiscal year', 'AUTO', %(created_by)s, %(updated_by)s, Now(), Now()
        )"""

    try:
        cursor.executemany(sql, emp_list_to_be_added)
        mydb.commit()
        for row in emp_list_to_be_added:
            print(
                f"Leave Days ({row['leave_days']}) inserted for User Id ({row['user_id']}) for current fiscal year ({row['fiscal_year_id']}) "
            )
        return cursor.rowcount

    finally:
        mydb.close()


def get_current_fiscal_year_id():
    try:
        mydb = connect_db()
    except:
        print("db connection failed")
        return

    cursor = mydb.cursor()
    sql = "SELECT id FROM fiscal_year  WHERE is_current=1 LIMIT 1"

    try:
        cursor.execute(sql)

        rows = []
        for row in cursor:
            rows.append(row)

        return rows[0][0]
    finally:
        mydb.close()


def main():
    fiscal_year_id = get_current_fiscal_year_id()

    l1 = get_employees_list_for_credit_leaves()
    l2 = get_employees_list_for_taken_leaves()

    final_data = prepare_final_list(l1, l2, fiscal_year_id)

    if len(final_data) > 0:
        row_count = add_new_rows_in_leave_credits_for_current_fiscall_year(final_data)

        return print(f"{row_count} rows inserted in leave_credits")

    print("no any leaves available to transfer")


if __name__ == "__main__":
    db_user = str(input("Enter Your DB user? ")) or "kishorgiri"
    db_pass = str(input("Enter Your DB password? ")) or "Kishor66"

    try:
        main()
        print("***Execution completed SUCCESSFULLY***")
    except Exception as err:
        print(
            "***Execution stopped with ERROR***\n",
            err.with_traceback(sys.exc_info()[2]),
        )
        traceback.print_exc()
