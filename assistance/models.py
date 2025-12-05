from django.db import models


class Provider(models.Model):
    name = models.CharField(max_length=100)
    phone = models.CharField(max_length=20)
    lat = models.FloatField()
    lon = models.FloatField()
    is_available = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name

class AssistanceRequest(models.Model):
    id = models.BigAutoField(primary_key=True)

    STATUS_PENDING = 'PENDING'
    STATUS_DISPATCHED = 'DISPATCHED'
    STATUS_COMPLETED = 'COMPLETED'
    STATUS_CANCELLED = 'CANCELLED'

    STATUS_CHOICES = (
        (STATUS_PENDING, 'Pending'),
        (STATUS_DISPATCHED, 'Dispatched'),
        (STATUS_COMPLETED, 'Completed'),
        (STATUS_CANCELLED, 'Cancelled'),
    )

    customer_name = models.CharField(max_length=100)
    policy_number = models.CharField(max_length=50)
    lat = models.FloatField()
    lon = models.FloatField()
    issue_desc = models.TextField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.customer_name} - {self.policy_number}"

class ServiceAssignment(models.Model):
    request = models.OneToOneField(AssistanceRequest, on_delete=models.CASCADE, related_name='assignment')
    provider = models.ForeignKey(Provider, on_delete=models.PROTECT)
    dispatched_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.request.id} -> {self.provider.id}"
