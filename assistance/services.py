import math
from django.db import transaction
from .models import AssistanceRequest, Provider, ServiceAssignment
from .tasks import notify_insurance_company_task


class AssistanceService:

    @classmethod
    def create_request(cls, data: dict) -> AssistanceRequest:
        return AssistanceRequest.objects.create(**data)

    # --- A. En Yakın Provider Bulma ---
    @staticmethod
    def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """km cinsinden basit Haversine hesabı"""
        r = 6371  # Earth radius in km
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        d_phi = math.radians(lat2 - lat1)
        d_lambda = math.radians(lon2 - lon1)

        a = (
            math.sin(d_phi / 2) ** 2
            + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return r * c

    @classmethod
    def find_nearest_available_provider(cls, lat: float, lon: float) -> Provider:
        """
        En yakın müsait provider'ı döndür.
        En basit ve okunabilir yaklaşım: tüm müsait provider'lar üzerinde Python tarafında mesafe hesabı.
        """
        available_providers = list(Provider.objects.filter(is_available=True))
        if not available_providers:
            raise ValueError("No available providers found")

        return min(
            available_providers,
            key=lambda p: cls._haversine_distance(lat, lon, p.lat, p.lon),
        )

    # --- B. Race Condition Fix (atomic atama) ---
    @classmethod
    def assign_provider_atomic(cls, request_id: int) -> ServiceAssignment:
        """
        Aynı anda iki request aynı provider'ı kapamasın diye:
        - AssistanceRequest ve Provider satırlarını select_for_update ile kilitliyoruz.
        - Transaction.atomic kullanıyoruz.
        - Sigorta bildirimini transaction başarıyla commit olduktan sonra tetikliyoruz.
        """
        with transaction.atomic():
            # Request satırını kilitle
            req = (
                AssistanceRequest.objects.select_for_update()
                .select_related()
                .get(id=request_id)
            )

            if req.status != AssistanceRequest.STATUS_PENDING:
                raise ValueError("Only pending requests can be dispatched")

            # Müsait provider’ları kilitle
            providers_qs = Provider.objects.select_for_update().filter(is_available=True)
            providers = list(providers_qs)
            if not providers:
                raise ValueError("No available providers to assign")

            # En yakın provider'ı seç
            provider = min(
                providers,
                key=lambda p: cls._haversine_distance(req.lat, req.lon, p.lat, p.lon),
            )

            if not provider.is_available:
                # Teorik olarak select_for_update sonrası çok düşük ihtimal de olsa kontrol
                raise Exception("Provider is busy!")

            # Provider'ı meşgul işaretle
            provider.is_available = False
            provider.save(update_fields=["is_available"])

            # Assignment oluştur
            assignment = ServiceAssignment.objects.create(
                request=req,
                provider=provider,
            )

            # Request'i dispatched durumuna çek
            req.status = AssistanceRequest.STATUS_DISPATCHED
            req.save(update_fields=["status"])

            # Sigorta şirketine bildirim, COMMIT'ten SONRA çalışmalı
            transaction.on_commit(
                lambda rid=req.id: notify_insurance_company_task.delay(rid)
            )

            return assignment

    # --- D. Complete & Cancel İşlemleri ---
    @classmethod
    def complete_request(cls, request_id: int):
        """
        Sadece DISPATCHED olan talep completed olabilir.
        Provider tekrar müsait hale getirilir.
        """
        with transaction.atomic():
            req = (
                AssistanceRequest.objects.select_for_update()
                .select_related("assignment__provider")
                .get(id=request_id)
            )

            if req.status != AssistanceRequest.STATUS_DISPATCHED:
                raise ValueError("Only dispatched requests can be completed")

            assignment = getattr(req, "assignment", None)
            if not assignment:
                raise ValueError("Request has no assignment")

            provider = assignment.provider
            provider.is_available = True
            provider.save(update_fields=["is_available"])

            req.status = AssistanceRequest.STATUS_COMPLETED
            req.save(update_fields=["status"])

    @classmethod
    def cancel_request(cls, request_id: int):
        """
        Talebi iptal et.
        - Eğer DISPATCHED ise provider serbest bırakılır.
        - COMPLETED veya CANCELLED ise ya idempotent davranılır ya da hata verilebilir.
          Burada idempotent davranıp tekrar CANCELLED'e çekmeye çalışmıyoruz,
          zaten CANCELLED ise sessizce dönüyoruz.
        """
        with transaction.atomic():
            req = (
                AssistanceRequest.objects.select_for_update()
                .select_related("assignment__provider")
                .get(id=request_id)
            )

            if req.status == AssistanceRequest.STATUS_COMPLETED:
                raise ValueError("Completed request cannot be cancelled")

            if req.status == AssistanceRequest.STATUS_CANCELLED:
                # İdempotent davranıyoruz
                return

            # Eğer DISPATCHED ise provider'ı serbest bırak
            if req.status == AssistanceRequest.STATUS_DISPATCHED:
                assignment = getattr(req, "assignment", None)
                if assignment:
                    provider = assignment.provider
                    provider.is_available = True
                    provider.save(update_fields=["is_available"])

            # Son olarak request'i CANCELLED yap
            req.status = AssistanceRequest.STATUS_CANCELLED
            req.save(update_fields=["status"])
