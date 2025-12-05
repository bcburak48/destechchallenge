from celery import shared_task
import logging
import time
import random

logger = logging.getLogger(__name__)


class InsuranceAPIError(Exception):
    """Sigorta API'si hatası"""
    pass


@shared_task(bind=True, max_retries=5)
def notify_insurance_company_task(self, request_id):
    """
    Sigorta şirketine bildirim yapan task.
    - Hata durumunda exponential backoff ile retry
      (1 dk, 2 dk, 4 dk, 8 dk, 16 dk şeklinde; max 16 dk).
    """
    try:
        logger.info(f"[InsuranceNotify] Request ID {request_id} için bildirim gönderiliyor")

        # Mock API call
        time.sleep(1)

        # %30 ihtimalle hata fırlat
        if random.random() < 0.3:
            raise InsuranceAPIError("Connection timeout")

        logger.info(f"[InsuranceNotify] Başarılı: Request ID {request_id}")
        return {"status": "success", "request_id": request_id}

    except InsuranceAPIError as exc:
        retry_count = self.request.retries  # Şu ana kadar kaç kez denendi
        if retry_count >= self.max_retries:
            logger.error(
                f"[InsuranceNotify] Maksimum retry sayısına ulaşıldı. Request ID: {request_id}",
                exc_info=True,
            )
            raise

        # Exponential backoff: 1, 2, 4, 8, 16 dakika (dakikayı saniyeye çevir)
        backoff_minutes = 2 ** retry_count  # 1, 2, 4, 8, 16
        countdown = min(backoff_minutes, 16) * 60

        logger.warning(
            f"[InsuranceNotify] Hata: {exc}. "
            f"{backoff_minutes} dakika sonra tekrar denenecek. "
            f"(retry={retry_count + 1}/{self.max_retries})"
        )
        raise self.retry(exc=exc, countdown=countdown)
