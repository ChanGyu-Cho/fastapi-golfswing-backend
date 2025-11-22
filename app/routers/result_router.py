# app/routers/result_router.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Request
from pydantic import BaseModel
from typing import Optional

# websocket manager
from app.websocket_manager import manager   # manager는 app.websocket_manager에서
from app.webhook_auth import verify_runpod_signature

# 추가: S3 / DB 클라이언트 임포트
from app.s3_client import S3Client
from app.db_client import DBClient

router = APIRouter(
    prefix="/result",
    tags=["analysis_result"]
)

s3_client = S3Client()
db_client = DBClient()

# ----------------------------------------------------
# 1. WebSocket 연결 엔드포인트: /result/ws/analysis/{job_id}
# ----------------------------------------------------

@router.websocket("/ws/analysis")
async def websocket_preconnect(websocket: WebSocket):
    """
    Preconnect endpoint (주석 참고):
    - FE가 먼저 WebSocket을 엽니다.
      권장: subprotocol으로 "Bearer <JWT>" 전달 (new WebSocket(url, ['Bearer <jwt>']))
      대안: ?token=<jwt> 쿼리 파라미터 (덜 안전)
    - FE가 연결된 후 텍스트 메시지로 등록 요청을 보냅니다:
      {"action":"register", "job_id":"<JOB_ID>"}
    - 서버는 토큰을 검증해 user_id를 얻고, DB에서 job의 owner를 조회해 일치할 때만 연결을 등록합니다.
    """

    # 1) WS 업그레이드 수락: 이제 서버-클라이언트 간 메시지 송수신 가능
    await websocket.accept()

    temp_id = None  # 등록된 job_id를 임시로 저장 (연결 해제 시 정리용)
    try:
        # 2) 토큰 추출 우선순위:
        #    1) Sec-WebSocket-Protocol(subprotocol) - 권장: 'Bearer <JWT>'
        #    2) ?token=<jwt> 쿼리 파라미터
        #    3) HttpOnly 쿠키 (id_token 또는 access_token) - 현재 로그인 흐름에서 token_router가 HttpOnly 쿠키로 설정
        #       브라우저는 동일 출처(relative URL 사용)로 WebSocket 업그레이드시 쿠키를 자동으로 전송하므로
        #       클라이언트에서 토큰을 JS로 노출하지 않아도 인증을 수행할 수 있습니다.
        proto = websocket.headers.get("sec-websocket-protocol")
        token = None
        if proto:
            # 여러 서브프로토콜을 보낼 수 있으므로, 첫 항목을 사용합니다.
            token = proto.split(",")[0].strip()
        # Debug: show what we received from the client for troubleshooting
        try:
            print(f"[WS DEBUG] sec-websocket-protocol header: {proto}")
        except Exception:
            pass
        if not token:
            # subprotocol이 없다면 query param에서 꺼내봅니다: ws://host/ws/analysis?token=<jwt>
            token = websocket.query_params.get("token")
        try:
            print(f"[WS DEBUG] query_params: {websocket.query_params}")
        except Exception:
            pass
        if not token:
            # 마지막으로 HttpOnly 쿠키에 저장된 id_token/access_token을 확인
            # (token_router.py는 로그인 시 HttpOnly 쿠키로 id_token/access_token을 설정합니다)
            cookie_token = None
            try:
                cookie_token = websocket.cookies.get("id_token") or websocket.cookies.get("access_token")
            except Exception:
                # some ASGI servers may not expose cookies in the same way
                cookie_token = None
            if cookie_token:
                token = cookie_token
        try:
            print(f"[WS DEBUG] cookies: {getattr(websocket, 'cookies', None)}")
        except Exception:
            pass
            
        except Exception:
            # websocket.cookies 속성이 없거나 예외 발생 시 무시
            token = None

        # 3) FE가 보낸 첫 텍스트 메시지를 읽음 (이 메시지에서 register 요청을 기대)
        #    예: '{"action":"register","job_id":"abcd-1234"}'
        msg = await websocket.receive_text()
        try:
            import json
            payload = json.loads(msg)
        except Exception:
            # 메시지가 JSON이 아니면 오류 응답 후 연결 종료
            await websocket.send_json({"error": "invalid_json"})
            await websocket.close(code=1003)  # 1003: unsupported data
            return

        # 4) 메시지 내용 검사: action이 register이고 job_id가 있어야 함
        action = payload.get("action")
        if action == "register" and payload.get("job_id"):
            # FE가 올바른 등록 요청을 보냈다 -> 처리 진행
            job_id = payload.get("job_id")
            temp_id = job_id  # 추후 연결 해제 시 매핑을 지우기 위해 저장

            # 5) 토큰 검증: raw token -> user_id 추출
            #    (auth_utils.get_user_id_from_token는 JWT 서명/claims를 검증하여 sub(user id)를 반환)
            from app.auth_utils import get_user_id_from_token
            try:
                user_id = get_user_id_from_token(token) if token else None
            except Exception as e:
                print(f"[WS DEBUG] get_user_id_from_token error: {e}")
                user_id = None
            print(f"[WS DEBUG] resolved user_id from token: {user_id}")

            # 6) DB에서 job의 소유자(owner)를 조회
            #    (여기서 owner가 없으면 job이 아직 DB에 기록되지 않았거나 job_id가 잘못된 것)
            owner_id = db_client.get_job_owner(job_id)
            print(f"[WS DEBUG] lookup owner for job_id={job_id} returned owner_id={owner_id}")
            if owner_id is None:
                # owner가 없다는 것은 job 레코드가 존재하지 않음 -> 등록 실패
                # FE에 에러 응답 후 연결 종료
                await websocket.send_json({"error": "job_not_found"})
                await websocket.close(code=1008)  # 1008: policy violation (사용자 정의로 사용)
                return

            # 7) owner와 토큰에서 얻은 user_id 비교 (권한 검증)
            #    - user_id가 없거나 owner와 불일치면 등록 거부
            if user_id is None or str(user_id) != str(owner_id):
                # 권한 없음
                await websocket.send_json({"error": "forbidden"})
                await websocket.close(code=1008)
                return

            # 8) 검증 통과: manager에 job_id와 websocket(user_id 포함) 등록
            #    이후 runpod/webhook 처리 시 manager.send_result_to_client(job_id, url)로 푸시 가능
            await manager.connect(job_id, websocket, user_id)
            # FE에 등록 성공 메시지 전송
            await websocket.send_json({"status": "registered", "job_id": job_id})

            # 9) 연결 유지 루프: 여기서는 클라이언트가 보내는 메시지를 계속 수신하여
            #    연결이 살아있는 한 루프를 유지합니다. (client가 끊으면 WebSocketDisconnect 발생)
            while True:
                await websocket.receive_text()

        else:
            # 메시지 포맷이 예상과 다르면 에러 응답 및 연결 종료
            await websocket.send_json({"error": "expected register with job_id"})
            await websocket.close(code=1003)
    except WebSocketDisconnect:
        # 10) 클라이언트가 연결을 끊으면 매핑을 정리
        if temp_id:
            manager.disconnect(temp_id)
    except Exception as e:
        # 11) 기타 예외: 매핑 정리 후 로그
        if temp_id:
            manager.disconnect(temp_id)
        print(f"[WS preconnect error]: {e}")


# ----------------------------------------------------
# 2. Webhook 알림 엔드포인트: /result/webhook/job/complete (RunPod 전용)
# ----------------------------------------------------
class WebhookData(BaseModel):
    job_id: str
    s3_result_path: str

# RunPod에서 작업 완료 시 호출하는 Webhook 엔드포인트
@router.post("/webhook/job/complete", status_code=202) # 202 Accepted
async def handle_webhook(request: Request):
    # 0) 서명/타임스탬프 검증 (예외 발생 시 401)
    await verify_runpod_signature(request)

    # 1) 본문 파싱 (검증 후에 수행)
    data = await request.json()
    job_id = data.get("job_id")
    s3_key = data.get("s3_result_path") or data.get("s3_result_key")
    if not job_id or not s3_key:
        raise HTTPException(status_code=400, detail="job_id and s3_result_path required")

    # 2) DB 업데이트: COMPLETED (RunPod 완료) 및 s3_result_paths 저장 시도
    try:
        import json as _json
        s3_paths_json = None
        if data.get("s3_result_paths"):
            try:
                s3_paths_json = _json.dumps(data.get("s3_result_paths"))
            except Exception:
                s3_paths_json = str(data.get("s3_result_paths"))

        # Use new helper that attempts to write s3_result_paths when available
        try:
            db_client.update_upload_status_with_paths(job_id=job_id, status="COMPLETED", s3_result_path=s3_key, s3_result_paths=s3_paths_json)
        except Exception:
            # fallback to old API
            db_client.update_upload_status(job_id=job_id, status="COMPLETED", s3_result_path=s3_key)
    except Exception as e:
        print(f"[DB Update Error] job_id={job_id} error={e}")

    # 3) presigned GET 생성: primary 하나뿐 아니라 전달된 모든 경로에 대해 presigned URL을 생성
    presigned_urls = []
    all_keys = data.get("s3_result_paths") or []
    # Ensure primary key is present at front
    if s3_key and s3_key not in all_keys:
        all_keys.insert(0, s3_key)

    for key in all_keys:
        try:
            url = s3_client.create_presigned_get_url(key)
            presigned_urls.append(url)
        except Exception as e:
            print(f"[S3 Presign Error] key={key} error={e}")
            # skip failing keys but continue

    if not presigned_urls:
        # At least try to generate for the single s3_key that was provided
        try:
            presigned_urls.append(s3_client.create_presigned_get_url(s3_key))
        except Exception as e:
            print(f"[S3 Presign Error] fallback key={s3_key} error={e}")
            raise HTTPException(status_code=500, detail="Failed to generate presigned result URL(s)")

    # 4) WS로 푸시 (리스트 전송)
    try:
        # Debug: list active connections before attempting push
        try:
            print(f"[DEBUG] active_connections before push: {list(manager.active_connections.keys())}")
        except Exception:
            print("[DEBUG] could not read manager.active_connections")
        pushed = await manager.send_result_to_client(job_id, presigned_urls)
    except Exception as e:
        print(f"[WS Push Exception] job_id={job_id} error={e}")
        pushed = False

    response_body = {
        "message": "Result URL(s) generated.",
        "result_urls": presigned_urls,
    }

    if pushed:
        response_body["message"] = "Result pushed successfully."

    return response_body


# ----------------------------------------------------
# 3. 폴링용 상태 조회 엔드포인트: /result/status
#    클라이언트(프론트)는 주기적으로 이 엔드포인트를 호출하여
#    job 상태와 presigned result URL을 확인합니다.
# ----------------------------------------------------
@router.get("/status")
async def get_result_status(job_id: str):
    if not job_id:
        raise HTTPException(status_code=400, detail="job_id is required")

    # DB에서 job 레코드 조회
    try:
        rec = db_client.get_upload_record(job_id)
    except Exception as e:
        print(f"[DB Error] get_upload_record job_id={job_id} error={e}")
        raise HTTPException(status_code=500, detail="internal error")

    if not rec:
        raise HTTPException(status_code=404, detail="job not found")

    status = rec.get("status")
    s3_key = rec.get("s3_result_path")
    s3_paths = rec.get("s3_result_paths")

    # If the job is COMPLETED, prefer to return all presigned URLs if available
    result_urls = None
    if status == "COMPLETED":
        keys = None
        if s3_paths:
            # s3_result_paths might be stored as JSON string by the DB client
            try:
                import json as _json
                keys = _json.loads(s3_paths) if isinstance(s3_paths, str) else s3_paths
            except Exception:
                # not JSON — treat as a single path string
                keys = [s3_paths]
        elif s3_key:
            keys = [s3_key]

        if keys:
            presigned_list = []
            for key in keys:
                try:
                    presigned_list.append(s3_client.create_presigned_get_url(key))
                except Exception as e:
                    print(f"[S3 Presign Error] key={key} error={e}")
                    # skip failing keys

            if presigned_list:
                result_urls = presigned_list

    # For backward compatibility, include single result_url as first item if present
    single = result_urls[0] if result_urls else None
    return {"job_id": job_id, "status": status, "result_url": single, "result_urls": result_urls}