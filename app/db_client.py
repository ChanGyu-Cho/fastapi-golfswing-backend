# backend-api/db_client.py

# MariaDB 드라이버 사용해 db 연결, 업로드 레코드 삽입

import os
import mysql.connector # 예시: MariaDB/MySQL 커넥터 사용 가정
from typing import Optional
from uuid import uuid4

# .env에서 DB 정보 로드
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_TABLE_NAME = os.getenv("DB_TABLE_NAME")
DB_SSL_CA = os.getenv("DB_SSL_CA")  # optional path to RDS CA bundle for SSL connections

class DBClient:
    def __init__(self):
        try:
            # Validate basic config
            if not DB_HOST:
                raise RuntimeError("DB_HOST is not configured")
            if not DB_USER:
                raise RuntimeError("DB_USER is not configured")

            # Ensure port is an int and default to 3306
            try:
                port_val = int(DB_PORT) if DB_PORT else 3306
            except Exception:
                port_val = 3306

            conn_args = {
                'host': DB_HOST,
                'port': port_val,
                'database': DB_NAME,
                'user': DB_USER,
                'password': DB_PASSWORD,
            }

            # If an SSL CA bundle is provided, pass it through to the connector
            if DB_SSL_CA:
                conn_args['ssl_ca'] = DB_SSL_CA

            self.conn = mysql.connector.connect(**conn_args)
        except Exception as e:
            print(f"DB 연결 실패: {e}")
            raise

    def insert_upload_intent(
        self, 
        job_id: str,    # job id를 외부에서 받아옴, websocket.py에서
        user_id: Optional[str],
        non_member_identifier: Optional[str], 
        upload_source: str,                   
        s3_key: str, 
        filename: str, 
        filetype: str, 
        file_size_bytes: int,     
        s3_result_path: Optional[str] = None    # 초기 result는 당연히 None
    ) -> str:
        """업로드 의도 레코드를 DB에 삽입하고, 생성된 UUID를 반환합니다."""
        
        upload_id = job_id  # 외부에서 받아온 job_id 사용

        sql = f"""
            INSERT INTO {DB_TABLE_NAME} (
                id, user_id, non_member_identifier, upload_source, 
                s3_key, original_filename, file_type, file_size_bytes, 
                processing_status, s3_result_path
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', %s
            )
        """
        params = (
            upload_id, # job_id 사용
            user_id, 
            non_member_identifier, 
            upload_source,
            s3_key, 
            filename, 
            filetype, 
            file_size_bytes,
            s3_result_path
        )

        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
        self.conn.commit()
        return upload_id
    
    def update_upload_status(self, job_id: str, status: str, s3_result_path: Optional[str] = None) -> None:
        """
        job_id 레코드의 processing_status와 s3_result_path를 갱신합니다.
        """
        if not DB_TABLE_NAME:
            raise RuntimeError("DB_TABLE_NAME not configured")
        # Extended: allow optional s3_result_path and s3_result_paths (JSON string or list)
        # Try to update both columns if provided; fall back gracefully if the column does not exist.
        try:
            if s3_result_path is not None:
                sql = f"""
                    UPDATE {DB_TABLE_NAME}
                    SET processing_status = %s, s3_result_path = %s
                    WHERE id = %s
                """
                params = (status, s3_result_path, job_id)
            else:
                sql = f"""
                    UPDATE {DB_TABLE_NAME}
                    SET processing_status = %s
                    WHERE id = %s
                """
                params = (status, job_id)

            with self.conn.cursor() as cursor:
                cursor.execute(sql, params)
            self.conn.commit()
        except Exception:
            # If the simple update failed (e.g., missing column), attempt a more conservative update
            try:
                sql = f"""
                    UPDATE {DB_TABLE_NAME}
                    SET processing_status = %s
                    WHERE id = %s
                """
                params = (status, job_id)
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, params)
                self.conn.commit()
            except Exception as e:
                print(f"[DB Update Error - fallback] job_id={job_id} error={e}")
                raise

    def update_upload_status_with_paths(self, job_id: str, status: str, s3_result_path: Optional[str] = None, s3_result_paths: Optional[str] = None) -> None:
        """
        Try to update processing_status, s3_result_path and s3_result_paths (JSON) if available.
        s3_result_paths may be a JSON-encoded string or None. This method will attempt to write
        the additional column but will silently fall back to updating only processing_status and
        s3_result_path if the s3_result_paths column is not present in the DB schema.
        """
        if not DB_TABLE_NAME:
            raise RuntimeError("DB_TABLE_NAME not configured")

        # Prefer single SQL write including s3_result_paths if provided
        try:
            if s3_result_paths is not None and s3_result_path is not None:
                sql = f"""
                    UPDATE {DB_TABLE_NAME}
                    SET processing_status = %s, s3_result_path = %s, s3_result_paths = %s
                    WHERE id = %s
                """
                params = (status, s3_result_path, s3_result_paths, job_id)
            elif s3_result_paths is not None:
                sql = f"""
                    UPDATE {DB_TABLE_NAME}
                    SET processing_status = %s, s3_result_paths = %s
                    WHERE id = %s
                """
                params = (status, s3_result_paths, job_id)
            elif s3_result_path is not None:
                sql = f"""
                    UPDATE {DB_TABLE_NAME}
                    SET processing_status = %s, s3_result_path = %s
                    WHERE id = %s
                """
                params = (status, s3_result_path, job_id)
            else:
                sql = f"""
                    UPDATE {DB_TABLE_NAME}
                    SET processing_status = %s
                    WHERE id = %s
                """
                params = (status, job_id)

            with self.conn.cursor() as cursor:
                cursor.execute(sql, params)
            self.conn.commit()
        except Exception:
            # Fallback: try to update at least the basic status and s3_result_path
            try:
                sql = f"""
                    UPDATE {DB_TABLE_NAME}
                    SET processing_status = %s, s3_result_path = %s
                    WHERE id = %s
                """
                params = (status, s3_result_path, job_id)
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, params)
                self.conn.commit()
            except Exception as e:
                print(f"[DB Update Error - fallback2] job_id={job_id} error={e}")
                raise

    def get_upload_record(self, job_id: str) -> Optional[dict]:
        """
        Return a dictionary with upload record fields used by the API.
        Tries to include s3_result_paths if the column exists; otherwise returns basic fields.
        """
        if not DB_TABLE_NAME:
            raise RuntimeError("DB_TABLE_NAME not configured")

        # Attempt to select s3_result_paths; if that fails, fall back to fewer columns
        try:
            sql = f"SELECT id, user_id, processing_status, s3_result_path, s3_result_paths FROM {DB_TABLE_NAME} WHERE id = %s LIMIT 1"
            with self.conn.cursor() as cursor:
                cursor.execute(sql, (job_id,))
                row = cursor.fetchone()
                if not row:
                    return None
                return {
                    "job_id": row[0],
                    "user_id": row[1],
                    "status": row[2],
                    "s3_result_path": row[3],
                    "s3_result_paths": row[4],
                }
        except Exception:
            # Fallback to select without s3_result_paths
            try:
                sql = f"SELECT id, user_id, processing_status, s3_result_path FROM {DB_TABLE_NAME} WHERE id = %s LIMIT 1"
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, (job_id,))
                    row = cursor.fetchone()
                    if not row:
                        return None
                    return {
                        "job_id": row[0],
                        "user_id": row[1],
                        "status": row[2],
                        "s3_result_path": row[3],
                        "s3_result_paths": None,
                    }
            except Exception as e:
                print(f"[DB Select Error] job_id={job_id} error={e}")
                raise

    def get_job_owner(self, job_id: str) -> Optional[str]:
        """
        Return the user_id (owner) for a given job_id. Returns None if not found.
        """
        if not DB_TABLE_NAME:
            raise RuntimeError("DB_TABLE_NAME not configured")
        sql = f"SELECT user_id FROM {DB_TABLE_NAME} WHERE id = %s LIMIT 1"
        with self.conn.cursor() as cursor:
            cursor.execute(sql, (job_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return row[0]