import uvicorn
from app import create_app

app = create_app()

if __name__ == '__main__':
    uvicorn.run(
        app,
        host='0.0.0.0',
        port=8005,
        log_config=None # logging.basicConfig를 사용하므로 Uvicorn의 기본 로깅 설정을 비활성화
    )
