import requests
import psycopg2
import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s-%(levelname)s-%(message)s")


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_DATABASE"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

def start_etl_run():
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        start_time = datetime.now(timezone.utc)

        cursor.execute("""
            INSERT INTO etl_runs
                (start_time, status)
            VALUES
                (%s, %s)
            RETURNING run_id
        """, (start_time, "RUNNING"))

        run_id = cursor.fetchone()[0]

        conn.commit()

        logging.info(f"Started ETL run: {run_id}")

        return run_id

    except psycopg2.Error as e:
        logging.error(f"Failed to start ETL run: {e}")
        return None

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


def update_etl_run(
    run_id,
    status,
    records_extracted=0,
    records_inserted=0,
    records_updated=0,
    records_skipped=0,
    records_rejected=0,
    error_message=None
):
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        end_time = datetime.now(timezone.utc)

        cursor.execute("""
            UPDATE etl_runs
            SET
                end_time = %s,
                status = %s,
                records_extracted = %s,
                records_inserted = %s,
                records_updated = %s,
                records_skipped = %s,
                records_rejected = %s,
                error_message = %s
            WHERE run_id = %s
        """, (
            end_time,
            status,
            records_extracted,
            records_inserted,
            records_updated,
            records_skipped,
            records_rejected,
            error_message,
            run_id
        ))

        conn.commit()

        logging.info(f"ETL run {run_id} marked as {status}")

    except psycopg2.Error as e:
        logging.error(f"Failed to update ETL run: {e}")

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()

def get_last_successful_run():
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT last_successful_run
            FROM etl_control
            WHERE pipeline_name = %s
        """, ("api_users_pipeline",))

        result = cursor.fetchone()

        return result[0] if result else None

    except psycopg2.Error as e:
        logging.error(f"Failed to read ETL watermark: {e}")
        return None

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


def update_watermark():
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        run_time = datetime.now(timezone.utc)

        cursor.execute("""
            INSERT INTO etl_control (pipeline_name, last_successful_run)
            VALUES (%s, %s)
            ON CONFLICT (pipeline_name)
            DO UPDATE SET
                last_successful_run = EXCLUDED.last_successful_run
        """, (
            "api_users_pipeline",
            run_time
        ))

        conn.commit()

        logging.info("ETL watermark updated successfully.")
        return True

    except psycopg2.Error as e:
        if conn:
            conn.rollback()

        logging.error(f"Failed to update ETL watermark: {e}")
        return False

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()

# --------------------
# 1. EXTRACT
# --------------------
def extract_data():
    url = "https://jsonplaceholder.typicode.com/users"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        
        users = response.json()
        
        logging.info(f"Extracted {len(users)} users from API")
        return users
        
        
    
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None


# --------------------
# 2. TRANSFORM
# --------------------

def transform_data(users):
    clean_users = []
    rejected_users = []

    for user in users:
        try:
            clean_users.append({
                "id": user["id"],
                "name": user["name"],
                "email": user["email"],
                "city": user["address"]["city"],
                "company": user["company"]["name"]
            })

        except(KeyError, TypeError) as e:
            logging.error(f"skipping invalid user: {e}")

            rejected_users.append({
                "user": user,
                "reason": str(e)
            })

    logging.info(f"Transformed {len(clean_users)} users")
    logging.info(f"Rejected {len(rejected_users)} users")

    return clean_users, rejected_users 

# --------------------
# 3. LOAD
# --------------------


def load_data(clean_users):
    conn = None
    cursor = None
    

    inserted = 0
    updated = 0
    skipped = 0

    try:
        conn = get_connection()

        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, name, email, city, company
            FROM api_users
        """)

        existing_users = cursor.fetchall()

        existing_users_dict = {
            row[0]: {
                "name": row[1],
                "email": row[2],
                "city": row[3],
                "company": row[4]
            }
            for row in existing_users
        }

        for user in clean_users:

            existing_user = existing_users_dict.get(user["id"])

            if existing_user is None:

                cursor.execute("""
                    INSERT INTO api_users
                        (id, name, email, city, company, last_updated)
                    VALUES
                        (%s, %s, %s, %s, %s, clock_timestamp())
                """, (
                    user["id"],
                    user["name"],
                    user["email"],
                    user["city"],
                    user["company"]
                ))

                inserted += 1

            else:

                
                if(
                    existing_user["name"] != user["name"]
                    or existing_user["email"] != user["email"]
                    or existing_user["city"] != user["city"]
                    or existing_user["company"] != user["company"]
                ):

                    cursor.execute("""
                        UPDATE api_users
                        SET
                            name = %s,
                            email = %s,
                            city = %s,
                            company = %s,
                            last_updated = clock_timestamp()
                        WHERE id = %s
                    """, (
                        user["name"],
                        user["email"],
                        user["city"],
                        user["company"],
                        user["id"]
                    ))

                    updated += 1

                else:
                    skipped += 1

      

        conn.commit()

        logging.info(
            f"Inserted: {inserted}, Updated: {updated}, Skipped: {skipped}"
        )

        return True, inserted, updated, skipped

    except psycopg2.Error as e:

        if conn:
            conn.rollback()

        logging.error(f"Database error: {e}")
        return False, inserted, updated, skipped

    finally:

        if cursor:
            cursor.close()

        if conn:
            conn.close()



def load_rejected_users(rejected_users):
    if not rejected_users:
        logging.info("No rejected users to load.")
        return True

    conn = None
    cursor = None

    try:
        conn = get_connection()

        cursor = conn.cursor()

        for rejected in rejected_users:
            user = rejected["user"]
            reason = rejected["reason"]

            cursor.execute("""
                INSERT INTO rejected_users
                    (id, name, email, rejection_reason, rejected_at)
                VALUES
                    (%s, %s, %s, %s, clock_timestamp())
            """, (
                user.get("id"),
                user.get("name"),
                user.get("email"),
                reason
            ))

        conn.commit()

        logging.info(
            f"Loaded {len(rejected_users)} rejected users"
        )

        return True

    except psycopg2.Error as e:
        if conn:
            conn.rollback()

        logging.error(f"Failed to load rejected users: {e}")
        return False

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()

    
# --------------------
# MAIN ETL PIPELINE
# --------------------

def main():
    """Run the complete API-to-PostgreSQL ETL pipeline."""
    
    run_id = start_etl_run()

    users = extract_data()
 
    last_run = get_last_successful_run()

    logging.info("Last successful run: %s", last_run)


    if users is None:

        update_etl_run(
            run_id,
            "FAILED",
            error_message="Failed during extraction"
        )

        logging.error("ETL pipeline failed during extraction.")

        
    else:
        clean_users, rejected_users = transform_data(users)

        load_success, inserted, updated, skipped = load_data(clean_users)

         

        if load_success:

            rejected_success = load_rejected_users(rejected_users)

            if rejected_success:

                watermark_success = update_watermark()

                if watermark_success:
                    update_etl_run(
                        run_id,
                        "SUCCESS",
                        records_extracted=len(users),
                        records_inserted=inserted,
                        records_updated=updated,
                        records_skipped=skipped,
                        records_rejected=len(rejected_users)
                    )

                    logging.info("ETL pipeline completed successfully!")

                else:
                    update_etl_run(
                        run_id,
                        "FAILED",
                        records_extracted=len(users),
                        records_inserted=inserted,
                        records_updated=updated,
                        records_skipped=skipped,
                        records_rejected=len(rejected_users),
                        error_message="Failed to update ETL watermark"
                    )

                    logging.error(
                        "ETL pipeline failed while updating the watermark."
                    )

            else:
                update_etl_run(
                    run_id,
                    "FAILED",
                    records_extracted=len(users),
                    records_inserted=inserted,
                    records_updated=updated,
                    records_skipped=skipped,
                    records_rejected=len(rejected_users),
                    error_message="Failed while loading rejected records"
                )

                logging.error(
                    "ETL pipeline failed while loading rejected records."
                )

        else:
            update_etl_run(
                run_id,
                "FAILED",
                records_extracted=len(users),
                records_inserted=inserted,
                records_updated=updated,
                records_skipped=skipped,
                records_rejected=len(rejected_users),
                error_message="Failed during loading API users"
            )

            logging.error("ETL pipeline failed during loading.")

if __name__ =="__main__":
    main()

