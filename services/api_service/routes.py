from fastapi import APIRouter
from api_service.database import get_connection

router = APIRouter()

# 1. Lấy data mới nhất
@router.get("/data/latest")
def get_data_latest():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT window_start, window_end, avg_temperature,  avg_windspeed
        FROM stream_aggregates
        ORDER BY window_end DESC
        LIMIT 10
    """)

    rows = cursor.fetchall()

    result = [
        {
            "window_start": r[0],
            "window_end": r[1],
            "avg_temperature": r[2],
            "avg_windspeed": r[3]
        }
        for r in rows
    ]

    cursor.close()
    conn.close()

    return result


# 2. Lấy lịch sử
@router.get("/data/history")
def get_data_history():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT window_start, window_end, avg_temperature,  avg_windspeed
        FROM stream_aggregates
        ORDER BY window_start DESC
        LIMIT 50
    """)

    rows = cursor.fetchall()

    result = [
        {
            "window_start": r[0],
            "window_end": r[1],
            "avg_temperature": r[2],
            "avg_windspeed": r[3]
        }
        for r in rows
    ]

    cursor.close()
    conn.close()

    return result



# 3. test DB connection
@router.get("/health/db")
def check_db():
    try:
        conn = get_connection()
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}