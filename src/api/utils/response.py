from fastapi.responses import JSONResponse


def envelope(success: bool, message: str, status_code: int, data=None):
    return {
        "success": success,
        "message": message,
        "statusCode": status_code,
        "data": data,
    }


def success(data=None, message: str = "OK", status_code: int = 200):
    return envelope(True, message, status_code, data)


def error_json(message: str = "Error", status_code: int = 500, data=None) -> JSONResponse:
    return JSONResponse(content=envelope(False, message, status_code, data), status_code=status_code)