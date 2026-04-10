import requests
import config

class TelegramBot:
    def __init__(self, token=None, chat_id=None):
        self.token = token or config.TELEGRAM_BOT_TOKEN
        self.chat_id = chat_id or config.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.token}"

    def send_message(self, text, parse_mode="Markdown"):
        """
        주어진 텍스트를 텔레그램으로 전송합니다.
        """
        if not self.token or not self.chat_id or "YOUR_" in self.token:
            print("  [Telegram Warning] 봇 토큰이나 Chat ID가 없습니다.")
            return False

        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode
        }

        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                print("  [Telegram Success] 메시지를 성공적으로 발송했습니다.")
                return True
            elif resp.status_code == 400 and parse_mode:
                # [FIX] Markdown 파싱 오류 발생 시 일반 텍스트로 재시도
                print(f"  [Telegram Warning] Markdown 파싱 에러 (400). 일반 텍스트로 재시도합니다.")
                payload.pop("parse_mode")
                resp_retry = requests.post(url, json=payload, timeout=10)
                if resp_retry.status_code == 200:
                    print("  [Telegram Success] 일반 텍스트 모드로 발송 성공")
                    return True
                else:
                    print(f"  [Telegram Error] 재시도 실패: {resp_retry.status_code} - {resp_retry.text}")
                    return False
            else:
                print(f"  [Telegram Error] {resp.status_code} - {resp.text}")
                return False
        except Exception as e:
            print(f"  [Telegram Exception] {e}")
            return False
